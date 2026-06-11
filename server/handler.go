package server

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/csv"
	"errors"
	"fmt"
	"html"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/goccy/go-json"
	"github.com/goccy/googlesqlite"
	"go.uber.org/zap"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/goccy/bigquery-emulator/internal/connection"
	"github.com/goccy/bigquery-emulator/internal/contentdata"
	"github.com/goccy/bigquery-emulator/internal/logger"
	"github.com/goccy/bigquery-emulator/internal/metadata"
	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
	"github.com/goccy/bigquery-emulator/types"
	"github.com/parquet-go/parquet-go"
)

func errorResponse(ctx context.Context, w http.ResponseWriter, e *ServerError) {
	logger.Logger(ctx).WithOptions(zap.AddCallerSkip(1)).Error(string(e.Reason), zap.Error(e))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.Status)
	w.Write(e.Response())
}

// uploadErrorResponse renders an error returned by the upload content handler.
// When the handler produced a typed *ServerError (e.g. a missing dataset), its
// HTTP status is preserved; any other failure is reported as a job error.
func uploadErrorResponse(ctx context.Context, w http.ResponseWriter, err error) {
	var serr *ServerError
	if errors.As(err, &serr) {
		errorResponse(ctx, w, serr)
		return
	}
	errorResponse(ctx, w, errJobInternalError(err.Error()))
}

func encodeResponse(ctx context.Context, w http.ResponseWriter, response interface{}) {
	b, err := json.Marshal(response)
	if err != nil {
		errorResponse(ctx, w, errInternalError(fmt.Sprintf("failed to encode json: %s", err.Error())))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

const (
	discoveryAPIEndpoint    = "/discovery/v1/apis/bigquery/v2/rest"
	newDiscoveryAPIEndpoint = "/$discovery/rest"
	uploadAPIEndpoint       = "/upload/bigquery/v2/projects/{projectId}/jobs"
)

//go:embed resources/discovery.json
var bigqueryAPIJSON []byte

var (
	discoveryAPIOnce     sync.Once
	discoveryAPIResponse map[string]interface{}
)

type discoveryHandler struct {
	server *Server
}

func newDiscoveryHandler(server *Server) *discoveryHandler {
	return &discoveryHandler{server: server}
}

func (h *discoveryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var decodeJSONErr error
	discoveryAPIOnce.Do(func() {
		if err := json.Unmarshal(bigqueryAPIJSON, &discoveryAPIResponse); err != nil {
			decodeJSONErr = err
			return
		}
	})
	if decodeJSONErr != nil {
		errorResponse(ctx, w, errInternalError(decodeJSONErr.Error()))
		return
	}
	// The discovery document advertises the base URLs that generated clients
	// dereference for every subsequent call, so they must be derived from the
	// incoming request rather than the bind address (issue #16). Patch a
	// shallow copy per request; the parsed template above is shared.
	addr := requestBaseURL(h.server, r)
	response := make(map[string]interface{}, len(discoveryAPIResponse))
	for k, v := range discoveryAPIResponse {
		response[k] = v
	}
	response["mtlsRootUrl"] = addr
	response["rootUrl"] = addr
	response["baseUrl"] = addr
	encodeResponse(ctx, w, response)
}

// requestBaseURL derives the scheme://authority base that response URLs
// (resumable upload session Location, discovery root URLs) should advertise.
// It must reflect the address the CLIENT used, not the server's bind address:
// behind any port mapping (docker -p, compose, k8s, reverse proxy) the bind
// address is unreachable from the client (issue #16). Standard proxy headers
// win, then the request's Host header; the bind address is only a fallback
// for the degenerate case of a missing Host.
func requestBaseURL(server *Server, r *http.Request) string {
	host := r.Header.Get("X-Forwarded-Host")
	if host != "" {
		// Multiple proxies append comma-separated values; the first is the
		// client-facing one.
		if i := strings.IndexByte(host, ','); i >= 0 {
			host = host[:i]
		}
		host = strings.TrimSpace(host)
	}
	if host == "" {
		host = r.Host
	}
	if host == "" {
		addr := server.httpServer.Addr
		if !strings.HasPrefix(addr, "http") {
			addr = "http://" + addr
		}
		return strings.TrimRight(addr, "/")
	}
	scheme := r.Header.Get("X-Forwarded-Proto")
	if i := strings.IndexByte(scheme, ','); i >= 0 {
		scheme = strings.TrimSpace(scheme[:i])
	}
	if scheme == "" {
		if r.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	return scheme + "://" + host
}

type uploadHandler struct{}

type UploadJobConfigurationLoad struct {
	AllowJaggedRows                    bool                                   `json:"allowJaggedRows,omitempty"`
	AllowQuotedNewlines                bool                                   `json:"allowQuotedNewlines,omitempty"`
	Autodetect                         bool                                   `json:"autodetect,omitempty"`
	Clustering                         *bigqueryv2.Clustering                 `json:"clustering,omitempty"`
	CreateDisposition                  string                                 `json:"createDisposition,omitempty"`
	DecimalTargetTypes                 []string                               `json:"decimalTargetTypes,omitempty"`
	DestinationEncryptionConfiguration *bigqueryv2.EncryptionConfiguration    `json:"destinationEncryptionConfiguration,omitempty"`
	DestinationTable                   *bigqueryv2.TableReference             `json:"destinationTable,omitempty"`
	DestinationTableProperties         *bigqueryv2.DestinationTableProperties `json:"destinationTableProperties,omitempty"`
	Encoding                           string                                 `json:"encoding,omitempty"`
	FieldDelimiter                     string                                 `json:"fieldDelimiter,omitempty"`
	HivePartitioningOptions            *bigqueryv2.HivePartitioningOptions    `json:"hivePartitioningOptions,omitempty"`
	IgnoreUnknownValues                bool                                   `json:"ignoreUnknownValues,omitempty"`
	JsonExtension                      string                                 `json:"jsonExtension,omitempty"`
	MaxBadRecords                      int64                                  `json:"maxBadRecords,omitempty"`
	NullMarker                         string                                 `json:"nullMarker,omitempty"`
	ParquetOptions                     *bigqueryv2.ParquetOptions             `json:"parquetOptions,omitempty"`
	PreserveAsciiControlCharacters     bool                                   `json:"preserveAsciiControlCharacters,omitempty"`
	ProjectionFields                   []string                               `json:"projectionFields,omitempty"`
	Quote                              *string                                `json:"quote,omitempty"`
	RangePartitioning                  *bigqueryv2.RangePartitioning          `json:"rangePartitioning,omitempty"`
	Schema                             *bigqueryv2.TableSchema                `json:"schema,omitempty"`
	SchemaInline                       string                                 `json:"schemaInline,omitempty"`
	SchemaInlineFormat                 string                                 `json:"schemaInlineFormat,omitempty"`
	SchemaUpdateOptions                []string                               `json:"schemaUpdateOptions,omitempty"`
	SkipLeadingRows                    json.Number                            `json:"skipLeadingRows,omitempty"`
	SourceFormat                       string                                 `json:"sourceFormat,omitempty"`
	SourceUris                         []string                               `json:"sourceUris,omitempty"`
	TimePartitioning                   *bigqueryv2.TimePartitioning           `json:"timePartitioning,omitempty"`
	UseAvroLogicalTypes                bool                                   `json:"useAvroLogicalTypes,omitempty"`
	WriteDisposition                   string                                 `json:"writeDisposition,omitempty"`
}

type UploadJobConfiguration struct {
	Load *UploadJobConfigurationLoad `json:"load"`
}

type UploadJob struct {
	JobReference  *bigqueryv2.JobReference `json:"jobReference"`
	Configuration *UploadJobConfiguration  `json:"configuration"`
}

// normalize fills in fields that some client libraries omit from the upload
// metadata. The Node.js client in particular sends no jobReference, which
// previously caused a nil pointer dereference when the handler read the job
// id. A missing job id is generated so the upload still gets a stable handle.
func (j *UploadJob) normalize(projectID string) *ServerError {
	if j.Configuration == nil || j.Configuration.Load == nil {
		return errInvalid("upload job is missing configuration.load")
	}
	if j.JobReference == nil {
		j.JobReference = &bigqueryv2.JobReference{}
	}
	if j.JobReference.JobId == "" {
		j.JobReference.JobId = randomID()
	}
	if j.JobReference.ProjectId == "" {
		j.JobReference.ProjectId = projectID
	}
	return nil
}

func (j *UploadJob) ToJob() *bigqueryv2.Job {
	load := j.Configuration.Load
	skipLeadingRows, _ := load.SkipLeadingRows.Int64()
	return &bigqueryv2.Job{
		JobReference: j.JobReference,
		Configuration: &bigqueryv2.JobConfiguration{
			Load: &bigqueryv2.JobConfigurationLoad{
				AllowJaggedRows:                    load.AllowJaggedRows,
				AllowQuotedNewlines:                load.AllowQuotedNewlines,
				Autodetect:                         load.Autodetect,
				Clustering:                         load.Clustering,
				CreateDisposition:                  load.CreateDisposition,
				DecimalTargetTypes:                 load.DecimalTargetTypes,
				DestinationEncryptionConfiguration: load.DestinationEncryptionConfiguration,
				DestinationTable:                   load.DestinationTable,
				DestinationTableProperties:         load.DestinationTableProperties,
				Encoding:                           load.Encoding,
				FieldDelimiter:                     load.FieldDelimiter,
				HivePartitioningOptions:            load.HivePartitioningOptions,
				IgnoreUnknownValues:                load.IgnoreUnknownValues,
				JsonExtension:                      load.JsonExtension,
				MaxBadRecords:                      load.MaxBadRecords,
				NullMarker:                         load.NullMarker,
				ParquetOptions:                     load.ParquetOptions,
				PreserveAsciiControlCharacters:     load.PreserveAsciiControlCharacters,
				ProjectionFields:                   load.ProjectionFields,
				Quote:                              load.Quote,
				RangePartitioning:                  load.RangePartitioning,
				Schema:                             load.Schema,
				SchemaInline:                       load.SchemaInline,
				SchemaInlineFormat:                 load.SchemaInlineFormat,
				SchemaUpdateOptions:                load.SchemaUpdateOptions,
				SkipLeadingRows:                    skipLeadingRows,
				SourceFormat:                       load.SourceFormat,
				SourceUris:                         load.SourceUris,
				TimePartitioning:                   load.TimePartitioning,
				UseAvroLogicalTypes:                load.UseAvroLogicalTypes,
				WriteDisposition:                   load.WriteDisposition,
			},
		},
	}
}

func (h *uploadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Query().Get("uploadType") {
	case "multipart":
		h.serveMultipart(w, r)
	case "resumable":
		h.serveResumable(w, r)
	default:
		errorResponse(r.Context(), w, errInvalid(`uploadType should be "multipart" or "resumable"`))
	}
}

func (h *uploadHandler) serveMultipart(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	contentType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(contentType, "multipart/") {
		errorResponse(ctx, w, errInvalid("expecting a multipart message"))
		return
	}
	mul := multipart.NewReader(r.Body, params["boundary"])
	p, err := mul.NextPart()
	if err != nil {
		errorResponse(ctx, w, errInvalid(fmt.Sprintf("failed to load metadata: %s", err.Error())))
		return
	}
	var job UploadJob
	if err := json.NewDecoder(p).Decode(&job); err != nil {
		errorResponse(ctx, w, errInvalid(fmt.Sprintf("failed to decode job: %s", err.Error())))
		return
	}
	if serr := job.normalize(project.ID); serr != nil {
		errorResponse(ctx, w, serr)
		return
	}
	uploadJob, err := h.Handle(ctx, &uploadRequest{
		server:  server,
		project: project,
		job:     &job,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}

	p, err = mul.NextPart()
	if err != nil {
		errorResponse(ctx, w, errInvalid(fmt.Sprintf("multipart request is invalid: %s", err.Error())))
		return
	}
	u := &uploadContentHandler{}
	err = u.Handle(ctx, &uploadContentRequest{
		server:  server,
		project: project,
		job:     uploadJob,
		reader:  p,
	})
	if err != nil {
		uploadErrorResponse(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, uploadJob.Content())
}

func (h *uploadHandler) serveResumable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	var job UploadJob
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		errorResponse(ctx, w, errInvalid(fmt.Sprintf("failed to decode job: %s", err.Error())))
		return
	}
	if serr := job.normalize(project.ID); serr != nil {
		errorResponse(ctx, w, serr)
		return
	}
	res, err := h.Handle(ctx, &uploadRequest{
		server:  server,
		project: project,
		job:     &job,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	// The session URL must be reachable from the client's side of any port
	// mapping, so mint it from the incoming request, not the bind address
	// (issue #16).
	addr := requestBaseURL(server, r)
	w.Header().Add(
		"Location",
		fmt.Sprintf(
			"%s/upload/bigquery/v2/projects/%s/jobs?uploadType=resumable&upload_id=%s",
			addr,
			project.ID,
			job.JobReference.JobId,
		),
	)
	encodeResponse(ctx, w, res.Content())
}

type uploadRequest struct {
	server  *Server
	project *metadata.Project
	job     *UploadJob
}

func (h *uploadHandler) Handle(ctx context.Context, r *uploadRequest) (*metadata.Job, error) {
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, "")
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	job := metadata.NewJob(r.server.metaRepo, r.project.ID, r.job.JobReference.JobId, r.job.ToJob(), nil, nil)
	if err := r.server.metaRepo.InsertJob(ctx, tx.Tx(), job); err != nil {
		return nil, fmt.Errorf("failed to add job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit job: %w", err)
	}
	return job, nil
}

type uploadContentHandler struct{}

func (h *uploadContentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	query := r.URL.Query()
	uploadType := query["uploadType"]
	if len(uploadType) == 0 {
		errorResponse(ctx, w, errInvalid("uploadType parameter is not found"))
		return
	}
	if uploadType[0] != "resumable" {
		errorResponse(ctx, w, errInvalid(fmt.Sprintf("uploadType parameter is not resumable %s", uploadType[0])))
		return
	}
	uploadID := query["upload_id"]
	if len(uploadID) == 0 {
		errorResponse(ctx, w, errInvalid("upload_id parameter is not found"))
		return
	}
	jobID := uploadID[0]
	job := project.Job(jobID)
	if job == nil {
		errorResponse(ctx, w, errNotFound(fmt.Sprintf("upload job %s is not found", jobID)))
		return
	}
	if err := h.Handle(ctx, &uploadContentRequest{
		server:  server,
		project: project,
		job:     job,
		reader:  r.Body,
	}); err != nil {
		uploadErrorResponse(ctx, w, err)
		return
	}
	content := job.Content()
	content.Status = &bigqueryv2.JobStatus{State: "DONE"}
	encodeResponse(ctx, w, content)
}

type uploadContentRequest struct {
	server  *Server
	project *metadata.Project
	job     *metadata.Job
	reader  io.Reader
}

func (h *uploadContentHandler) getCandidateName(col string, columnNames []string) string {
	var (
		foundName  string
		foundCount int
	)
	for _, name := range columnNames {
		if strings.Contains(name, col) {
			foundName = name
			foundCount++
		}
	}
	if foundCount == 1 {
		return foundName
	}
	return ""
}

func (h *uploadContentHandler) existsColumnNameInCSVHeader(col string, header []string) bool {
	for _, h := range header {
		if col == h {
			return true
		}
	}
	return false
}

func (h *uploadContentHandler) normalizeColumnNameForJSONData(columnMap map[string]*types.Column, data map[string]interface{}) {
	for k, v := range data {
		if _, exists := columnMap[k]; exists {
			continue
		}
		lowerKey := strings.ToLower(k)
		var (
			foundCount int
			columnName string
		)
		for colName := range columnMap {
			if lowerKey == strings.ToLower(colName) {
				foundCount++
				columnName = colName
			}
		}
		if foundCount == 1 {
			delete(data, k)
			data[columnName] = v
		}
	}
}

func (h *uploadContentHandler) Handle(ctx context.Context, r *uploadContentRequest) error {
	load := r.job.Content().Configuration.Load
	// Load jobs (dbt seeds in particular) carry lowercase schema field
	// type names; canonicalize them the way real BigQuery does before the
	// schema is used to create the table or to type the loaded columns.
	types.NormalizeSchema(load.Schema)
	tableRef := load.DestinationTable
	if tableRef == nil {
		return errInvalid("load job is missing configuration.load.destinationTable")
	}
	dataset := r.project.Dataset(tableRef.DatasetId)
	if dataset == nil {
		return errNotFound(fmt.Sprintf("dataset %q is not found", tableRef.DatasetId))
	}
	table := dataset.Table(tableRef.TableId)
	// The write disposition only matters for a table that already exists; a
	// freshly created one is empty regardless.
	tableExisted := table != nil

	// Read CSV content up front so an autodetect load can infer the schema
	// before the destination table is created.
	var (
		csvRecords            [][]string
		csvAutodetectedHeader bool
	)
	if load.SourceFormat == "CSV" {
		records, err := csv.NewReader(r.reader).ReadAll()
		if err != nil {
			return fmt.Errorf("failed to read csv: %w", err)
		}
		csvRecords = records
		if !tableExisted && load.Schema == nil && load.Autodetect {
			schema, err := inferCSVSchema(csvRecords)
			if err != nil {
				return err
			}
			load.Schema = schema
			// The schema's column names came from the first row, so that
			// row is a header and must not be loaded as data (real
			// BigQuery's autodetect detects the header row the same way).
			csvAutodetectedHeader = true
		}
	}
	if table == nil {
		if load.CreateDisposition == "CREATE_NEVER" {
			return fmt.Errorf("`%s` is not found", tableRef.TableId)
		}
		if _, err := (&tablesInsertHandler{}).Handle(ctx, &tablesInsertRequest{
			server:  r.server,
			project: r.project,
			dataset: dataset,
			table: &bigqueryv2.Table{
				Schema:         load.Schema,
				TableReference: tableRef,
			},
		}); err != nil {
			return err
		}
		table = dataset.Table(tableRef.TableId)
	}

	tableContent, err := table.Content()
	if err != nil {
		return err
	}
	columnToType := map[string]types.Type{}
	for _, field := range tableContent.Schema.Fields {
		columnToType[field.Name] = types.Type(field.Type)
	}

	sourceFormat := load.SourceFormat
	columns := []*types.Column{}
	data := types.Data{}
	switch sourceFormat {
	case "CSV":
		records := csvRecords
		// skipLeadingRows defaults to 0: no header is assumed and every
		// row is data. The previous code unconditionally treated the
		// first row as a header, silently dropping the first record of
		// every headerless CSV (issue #10). An autodetected schema took
		// its column names from the first row, so that row is known to
		// be a header even when skipLeadingRows is unset.
		skipRows := load.SkipLeadingRows
		if skipRows < 0 {
			return errInvalid(fmt.Sprintf("skipLeadingRows must be >= 0, got %d", skipRows))
		}
		if csvAutodetectedHeader && skipRows == 0 {
			skipRows = 1
		}
		// When rows are skipped, the first one is BigQuery's header row;
		// if its cells all name table columns it keys the column order of
		// the data rows (dbt seeds upload header CSVs this way). A
		// headerless load maps CSV columns positionally onto the schema.
		useHeaderOrder := false
		if skipRows > 0 && len(records) > 0 {
			useHeaderOrder = true
			for _, col := range records[0] {
				if _, exists := columnToType[col]; !exists {
					useHeaderOrder = false
					break
				}
			}
		}
		if useHeaderOrder {
			for _, col := range records[0] {
				columns = append(columns, &types.Column{
					Name: col,
					Type: columnToType[col],
				})
			}
		} else {
			for _, field := range tableContent.Schema.Fields {
				columns = append(columns, &types.Column{
					Name: field.Name,
					Type: types.Type(field.Type),
				})
			}
		}
		if skipRows >= int64(len(records)) {
			// Every row was skipped (including the empty-body case):
			// the load succeeds and the table is simply left as is.
			return nil
		}
		for _, record := range records[skipRows:] {
			rowData := map[string]interface{}{}
			if len(record) != len(columns) {
				return fmt.Errorf("invalid column number: found broken row data: %v", record)
			}
			for i := 0; i < len(record); i++ {
				colData := record[i]
				if colData == "" {
					rowData[columns[i].Name] = nil
				} else {
					rowData[columns[i].Name] = colData
				}
			}
			data = append(data, rowData)
		}
	case "PARQUET":
		b, err := io.ReadAll(r.reader)
		if err != nil {
			return err
		}
		reader := parquet.NewReader(bytes.NewReader(b))
		defer reader.Close()

		for _, f := range load.Schema.Fields {
			columns = append(columns, &types.Column{
				Name: f.Name,
				Type: types.Type(f.Type),
			})
		}

		for i := 0; i < int(reader.NumRows()); i++ {
			var rowData interface{}
			err := reader.Read(&rowData)
			if err != nil {
				return err
			}

			data = append(data, rowData.(map[string]interface{}))
		}
	case "NEWLINE_DELIMITED_JSON":
		for _, f := range tableContent.Schema.Fields {
			columns = append(columns, &types.Column{
				Name: f.Name,
				Type: types.Type(f.Type),
			})
		}
		columnMap := map[string]*types.Column{}
		for _, col := range columns {
			columnMap[col.Name] = col
		}
		decoder := json.NewDecoder(r.reader)
		decoder.UseNumber()
		for decoder.More() {
			d := make(map[string]interface{})
			if err := decoder.Decode(&d); err != nil {
				return err
			}
			h.normalizeColumnNameForJSONData(columnMap, d)
			data = append(data, d)
		}
	default:
		return fmt.Errorf("not support sourceFormat: %s", sourceFormat)
	}
	tableDef := &types.Table{
		ID:      tableRef.TableId,
		Columns: columns,
		Data:    data,
	}
	conn, err := r.server.connMgr.Connection(ctx, tableRef.ProjectId, tableRef.DatasetId)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	if tableExisted {
		switch load.WriteDisposition {
		case "WRITE_TRUNCATE":
			if err := r.server.contentRepo.TruncateTable(ctx, tx, tableRef.ProjectId, tableRef.DatasetId, tableRef.TableId); err != nil {
				return err
			}
		case "WRITE_EMPTY":
			count, err := r.server.contentRepo.CountTableRows(ctx, tx, tableRef.ProjectId, tableRef.DatasetId, tableRef.TableId)
			if err != nil {
				return err
			}
			if count > 0 {
				return fmt.Errorf("table %s already exists and contains data (WRITE_EMPTY)", tableRef.TableId)
			}
		}
	}
	if err := r.server.contentRepo.AddTableData(ctx, tx, tableRef.ProjectId, tableRef.DatasetId, tableDef); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

const (
	formatOptionsUseInt64TimestampParam = "formatOptions.useInt64Timestamp"
	deleteContentsParam                 = "deleteContents"
)

func isDeleteContents(r *http.Request) bool {
	return parseQueryValueAsBool(r, deleteContentsParam)
}

func isFormatOptionsUseInt64Timestamp(r *http.Request) bool {
	return parseQueryValueAsBool(r, formatOptionsUseInt64TimestampParam)
}

// parseQueryValueAsUint64 reads an unsigned integer query parameter, reporting
// whether it was present and valid.
func parseQueryValueAsUint64(r *http.Request, key string) (uint64, bool) {
	values, exists := r.URL.Query()[key]
	if !exists || len(values) != 1 {
		return 0, false
	}
	v, err := strconv.ParseUint(values[0], 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// applyNullQueryParameters inspects the raw JSON of a query-parameters array
// and clears the ParameterValue of every scalar parameter whose value was JSON
// null or absent. The bigqueryv2 structs store a parameter value as a plain
// string, so they cannot otherwise distinguish a NULL scalar from an empty string.
//
// ARRAY and STRUCT parameters must never be cleared, even when their
// parameterValue is empty (e.g. an empty []string{} omits "arrayValues"
// entirely due to JSON omitempty). The parameter type is used as the
// authoritative signal: any parameter whose type is "ARRAY" or "STRUCT" is
// left untouched.
func applyNullQueryParameters(rawParams []json.RawMessage, params []*bigqueryv2.QueryParameter) {
	for i, raw := range rawParams {
		if i >= len(params) || params[i] == nil {
			continue
		}
		var p struct {
			ParameterType *struct {
				Type string `json:"type"`
			} `json:"parameterType"`
			ParameterValue *json.RawMessage `json:"parameterValue"`
		}
		if err := json.Unmarshal(raw, &p); err != nil {
			continue
		}
		// ARRAY and STRUCT parameters must not be cleared regardless of whether
		// their parameterValue happens to be empty.
		if p.ParameterType != nil {
			switch p.ParameterType.Type {
			case "ARRAY", "STRUCT":
				continue
			}
		}
		// No parameterValue key at all → treat as null scalar.
		if p.ParameterValue == nil {
			params[i].ParameterValue = nil
			continue
		}
		// Decode parameterValue into a key-presence map.
		// JSON null decodes to a nil map; map lookups on nil return false safely.
		var pv map[string]json.RawMessage
		if err := json.Unmarshal(*p.ParameterValue, &pv); err != nil {
			continue
		}
		// Scalar: clear if "value" is absent or is the JSON literal null.
		valueRaw, hasValue := pv["value"]
		if !hasValue || string(valueRaw) == "null" {
			params[i].ParameterValue = nil
		}
	}
}

func parseQueryValueAsBool(r *http.Request, key string) bool {
	queryValues := r.URL.Query()
	values, exists := queryValues[key]
	if !exists {
		return false
	}
	if len(values) != 1 {
		return false
	}
	b, err := strconv.ParseBool(values[0])
	if err != nil {
		return false
	}
	return b
}

func (h *datasetsDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	if err := h.Handle(ctx, &datasetsDeleteRequest{
		server:         server,
		project:        project,
		dataset:        dataset,
		deleteContents: isDeleteContents(r),
	}); err != nil {
		// Preserve typed *ServerError (e.g. a 400 resourceInUse for a
		// non-empty dataset) so the client sees the real HTTP status
		// rather than the 500 retry-forever loop a blanket wrap would
		// cause.
		var serr *ServerError
		if errors.As(err, &serr) {
			errorResponse(ctx, w, serr)
			return
		}
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
}

type datasetsDeleteRequest struct {
	server         *Server
	project        *metadata.Project
	dataset        *metadata.Dataset
	deleteContents bool
}

func (h *datasetsDeleteHandler) Handle(ctx context.Context, r *datasetsDeleteRequest) error {
	// BigQuery rejects deleting a non-empty dataset unless
	// deleteContents=true. Reject up front so the dataset is never
	// removed while its tables remain (which would orphan them and
	// surface as a UNIQUE constraint violation on the next CREATE
	// TABLE with the same name) and so the caller sees a 4xx rather
	// than the 500 the Google SDKs retry indefinitely on.
	if !r.deleteContents && len(r.dataset.Tables()) > 0 {
		return errResourceInUse(fmt.Sprintf(
			"Dataset %s:%s is still in use",
			r.project.ID, r.dataset.ID,
		))
	}
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, r.dataset.ID)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.project.DeleteDataset(ctx, tx.Tx(), r.dataset.ID); err != nil {
		return fmt.Errorf("failed to delete dataset: %w", err)
	}
	if r.deleteContents {
		tables := r.dataset.Tables()
		deletions := make([]contentdata.TableDeletion, 0, len(tables))
		for _, table := range tables {
			if err := table.Delete(ctx, tx.Tx()); err != nil {
				return err
			}
			deletions = append(deletions, contentdata.TableDeletion{
				ID:     table.ID,
				IsView: table.IsView(),
			})
		}
		if err := r.server.contentRepo.DeleteTables(ctx, tx, r.project.ID, r.dataset.ID, deletions); err != nil {
			return fmt.Errorf("failed to delete tables: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit delete dataset: %w", err)
	}
	return nil
}

func (h *datasetsGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	res, err := h.Handle(ctx, &datasetsGetRequest{
		server:  server,
		project: project,
		dataset: dataset,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type datasetsGetRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
}

func (h *datasetsGetHandler) Handle(ctx context.Context, r *datasetsGetRequest) (*bigqueryv2.Dataset, error) {
	newContent := *r.dataset.Content()
	newContent.DatasetReference = &bigqueryv2.DatasetReference{
		ProjectId: r.project.ID,
		DatasetId: r.dataset.ID,
	}
	return &newContent, nil
}

func (h *datasetsInsertHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	var dataset bigqueryv2.Dataset
	if err := json.NewDecoder(r.Body).Decode(&dataset); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &datasetsInsertRequest{
		server:  server,
		project: project,
		dataset: &dataset,
	})
	if err != nil {
		// Creating a dataset that already exists is HTTP 409 with reason
		// "duplicate" in real BigQuery (clients' exists_ok handling and
		// retry policies depend on it: a 500 here gets retried until the
		// client's deadline), not an internalError.
		if errors.Is(err, metadata.ErrDuplicatedDataset) {
			datasetID := ""
			if dataset.DatasetReference != nil {
				datasetID = dataset.DatasetReference.DatasetId
			}
			errorResponse(ctx, w, errDuplicate(fmt.Sprintf("Already Exists: Dataset %s:%s", project.ID, datasetID)))
			return
		}
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type datasetsInsertRequest struct {
	server  *Server
	project *metadata.Project
	dataset *bigqueryv2.Dataset
}

func (h *datasetsInsertHandler) Handle(ctx context.Context, r *datasetsInsertRequest) (*bigqueryv2.DatasetListDatasets, error) {
	if r.dataset.DatasetReference == nil {
		return nil, fmt.Errorf("DatasetReference is nil")
	}
	datasetID := r.dataset.DatasetReference.DatasetId
	if datasetID == "" {
		return nil, fmt.Errorf("dataset id is empty")
	}
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, datasetID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()

	if err := r.project.AddDataset(
		ctx,
		tx.Tx(),
		metadata.NewDataset(
			r.server.metaRepo,
			r.project.ID,
			datasetID,
			r.dataset,
			nil,
			nil,
			nil,
		),
	); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &bigqueryv2.DatasetListDatasets{
		DatasetReference: &bigqueryv2.DatasetReference{
			ProjectId: r.project.ID,
			DatasetId: datasetID,
		},
		Id:              datasetID,
		FriendlyName:    r.dataset.FriendlyName,
		Kind:            r.dataset.Kind,
		Labels:          r.dataset.Labels,
		Location:        r.dataset.Location,
		ForceSendFields: r.dataset.ForceSendFields,
		NullFields:      r.dataset.NullFields,
	}, nil
}

func (h *datasetsListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	res, err := h.Handle(ctx, &datasetsListRequest{
		server:  server,
		project: project,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type datasetsListRequest struct {
	server  *Server
	project *metadata.Project
}

func (h *datasetsListHandler) Handle(ctx context.Context, r *datasetsListRequest) (*bigqueryv2.DatasetList, error) {
	datasetsRes := []*bigqueryv2.DatasetListDatasets{}
	for _, dataset := range r.project.Datasets() {
		content := dataset.Content()
		datasetsRes = append(datasetsRes, &bigqueryv2.DatasetListDatasets{
			DatasetReference: &bigqueryv2.DatasetReference{
				ProjectId: r.project.ID,
				DatasetId: dataset.ID,
			},
			FriendlyName:    content.FriendlyName,
			Id:              dataset.ID,
			Kind:            content.Kind,
			Labels:          content.Labels,
			Location:        content.Location,
			ForceSendFields: content.ForceSendFields,
			NullFields:      content.NullFields,
		})
	}
	return &bigqueryv2.DatasetList{
		Datasets: datasetsRes,
	}, nil
}

func (h *datasetsPatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	var newDataset bigqueryv2.Dataset
	if err := json.NewDecoder(r.Body).Decode(&newDataset); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &datasetsPatchRequest{
		server:     server,
		project:    project,
		dataset:    dataset,
		newDataset: &newDataset,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type datasetsPatchRequest struct {
	server     *Server
	project    *metadata.Project
	dataset    *metadata.Dataset
	newDataset *bigqueryv2.Dataset
}

func (h *datasetsPatchHandler) Handle(ctx context.Context, r *datasetsPatchRequest) (*bigqueryv2.Dataset, error) {
	r.dataset.UpdateContentIfExists(r.newDataset)
	newContent := *r.dataset.Content()
	return &newContent, nil
}

func (h *datasetsUpdateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	var newDataset bigqueryv2.Dataset
	if err := json.NewDecoder(r.Body).Decode(&newDataset); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &datasetsUpdateRequest{
		server:     server,
		project:    project,
		dataset:    dataset,
		newDataset: &newDataset,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type datasetsUpdateRequest struct {
	server     *Server
	project    *metadata.Project
	dataset    *metadata.Dataset
	newDataset *bigqueryv2.Dataset
}

func (h *datasetsUpdateHandler) Handle(ctx context.Context, r *datasetsUpdateRequest) (*bigqueryv2.Dataset, error) {
	r.dataset.UpdateContent(r.newDataset)
	newContent := *r.dataset.Content()
	return &newContent, nil
}

func (h *jobsCancelHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	job := jobFromContext(ctx)
	res, err := h.Handle(ctx, &jobsCancelRequest{
		server:  server,
		project: project,
		job:     job,
	})
	if err != nil {
		errorResponse(ctx, w, errJobInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsCancelRequest struct {
	server  *Server
	project *metadata.Project
	job     *metadata.Job
}

func (h *jobsCancelHandler) Handle(ctx context.Context, r *jobsCancelRequest) (*bigqueryv2.JobCancelResponse, error) {
	if err := r.job.Cancel(ctx); err != nil {
		return nil, err
	}
	// r.job may be the live instance of a running job (the middleware
	// prefers it); snapshot so encoding does not race its completion.
	return &bigqueryv2.JobCancelResponse{Job: r.job.ContentSnapshot()}, nil
}

func (h *jobsDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	job := jobFromContext(ctx)
	if err := h.Handle(ctx, &jobsDeleteRequest{
		server:  server,
		project: project,
		job:     job,
	}); err != nil {
		errorResponse(ctx, w, errJobInternalError(err.Error()))
		return
	}
}

type jobsDeleteRequest struct {
	server  *Server
	project *metadata.Project
	job     *metadata.Job
}

func (h *jobsDeleteHandler) Handle(ctx context.Context, r *jobsDeleteRequest) error {
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, "")
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.project.DeleteJob(ctx, tx.Tx(), r.job.ID); err != nil {
		return fmt.Errorf("failed to delete job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (h *jobsGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	job := jobFromContext(ctx)
	res, err := h.Handle(ctx, &jobsGetRequest{
		server:  server,
		project: project,
		job:     job,
	})
	if err != nil {
		errorResponse(ctx, w, errJobInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsGetRequest struct {
	server  *Server
	project *metadata.Project
	job     *metadata.Job
}

func (h *jobsGetHandler) Handle(ctx context.Context, r *jobsGetRequest) (*bigqueryv2.Job, error) {
	// Query jobs run asynchronously; report the live state while one is in
	// flight. jobs.get stays a plain poll (no long-poll semantics): the
	// python client only falls back to it when getQueryResults is not
	// applicable.
	job := r.job
	if live := r.server.liveJob(r.project.ID, r.job.ID); live != nil {
		job = live
	}
	content := job.ContentSnapshot()
	if content == nil {
		content = &bigqueryv2.Job{}
	}
	if content.Status == nil {
		// Jobs persisted by code paths that never tracked a state (load
		// jobs, YAML-loaded fixtures) were always reported DONE.
		content.Status = &bigqueryv2.JobStatus{State: "DONE"}
	}
	return content, nil
}

func (h *jobsGetQueryResultsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	job := jobFromContext(ctx)
	maxResults, hasMaxResults := parseQueryValueAsUint64(r, "maxResults")
	startIndex, _ := parseQueryValueAsUint64(r, "startIndex")
	// The page token returned by this handler is simply the next row index.
	if token, ok := parseQueryValueAsUint64(r, "pageToken"); ok {
		startIndex = token
	}
	// getQueryResults is a server-side long poll: the request parks until
	// the job completes or timeoutMs (default 10s) elapses, and only then
	// answers jobComplete=false. Answering false immediately would push
	// clients (the python one in particular) into multi-second client-side
	// backoff sleeps.
	timeout := defaultGetQueryResultsTimeout
	if timeoutMs, ok := parseQueryValueAsUint64(r, "timeoutMs"); ok {
		timeout = time.Duration(timeoutMs) * time.Millisecond
		if timeout > maxGetQueryResultsTimeout {
			timeout = maxGetQueryResultsTimeout
		}
	}
	res, err := h.Handle(ctx, &jobsGetQueryResultsRequest{
		server:            server,
		project:           project,
		job:               job,
		useInt64Timestamp: isFormatOptionsUseInt64Timestamp(r),
		maxResults:        maxResults,
		hasMaxResults:     hasMaxResults,
		startIndex:        startIndex,
		timeout:           timeout,
	})
	if err != nil {
		// Keep the typed error of a failed job (e.g. 404 notFound for a
		// CREATE_NEVER destination): clients branch on the reason/code.
		errorResponse(ctx, w, jobErrorProto(project.ID, err))
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsGetQueryResultsRequest struct {
	server            *Server
	project           *metadata.Project
	job               *metadata.Job
	useInt64Timestamp bool
	maxResults        uint64
	hasMaxResults     bool
	startIndex        uint64
	timeout           time.Duration
}

func (h *jobsGetQueryResultsHandler) Handle(ctx context.Context, r *jobsGetQueryResultsRequest) (*internaltypes.GetQueryResultsResponse, error) {
	job := r.job
	if live := r.server.liveJob(r.project.ID, r.job.ID); live != nil {
		job = live
	}
	response, jobErr, completed, err := job.WaitForResult(ctx, r.timeout)
	if err != nil {
		return nil, err
	}
	if !completed {
		return &internaltypes.GetQueryResultsResponse{
			JobReference: &bigqueryv2.JobReference{
				ProjectId: r.project.ID,
				JobId:     r.job.ID,
			},
			JobComplete: false,
		}, nil
	}
	if jobErr != nil {
		return nil, jobErr
	}
	if response == nil {
		response = &internaltypes.QueryResponse{}
	}
	rows := internaltypes.Format(response.Schema, response.Rows, r.useInt64Timestamp)

	// Honor maxResults/startIndex paging. Clients (notably the Python one)
	// poll getQueryResults with maxResults=0 purely to await completion and
	// then fetch the rows through a separate, paged request; returning every
	// row on the completion poll hands them rows in a format they did not ask
	// for.
	total := uint64(len(rows))
	start := r.startIndex
	if start > total {
		start = total
	}
	end := total
	pageToken := ""
	if r.hasMaxResults {
		end = start + r.maxResults
		if end > total {
			end = total
		}
		if end < total {
			pageToken = strconv.FormatUint(end, 10)
		}
	}
	return &internaltypes.GetQueryResultsResponse{
		JobReference: &bigqueryv2.JobReference{
			ProjectId: r.project.ID,
			JobId:     r.job.ID,
		},
		Schema:      response.Schema,
		TotalRows:   response.TotalRows,
		JobComplete: true,
		PageToken:   pageToken,
		Rows:        rows[start:end],
	}, nil
}

func (h *jobsInsertHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	body, err := io.ReadAll(r.Body)
	// A gzip body flushed but not closed delivers all its content yet ends
	// with ErrUnexpectedEOF; the json.Unmarshal below is the real validator.
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	var job bigqueryv2.Job
	if err := json.Unmarshal(body, &job); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	if job.Configuration != nil && job.Configuration.Query != nil {
		var rawReq struct {
			Configuration struct {
				Query struct {
					QueryParameters []json.RawMessage `json:"queryParameters"`
				} `json:"query"`
			} `json:"configuration"`
		}
		if err := json.Unmarshal(body, &rawReq); err == nil {
			applyNullQueryParameters(rawReq.Configuration.Query.QueryParameters, job.Configuration.Query.QueryParameters)
		}
	}
	res, err := h.Handle(ctx, &jobsInsertRequest{
		server:  server,
		project: project,
		job:     &job,
	})
	if err != nil {
		// jobErrorProto preserves typed *ServerError (e.g. a 404 notFound
		// for a missing destination table under CREATE_NEVER) and maps
		// duplicate-object failures to 409/duplicate; the rest fall back
		// to a 400 jobInternalError.
		errorResponse(ctx, w, jobErrorProto(project.ID, err))
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsInsertRequest struct {
	server  *Server
	project *metadata.Project
	job     *bigqueryv2.Job
}

func tableDefFromQueryResponse(tableID string, response *internaltypes.QueryResponse) (*types.Table, error) {
	columns := []*types.Column{}
	for _, field := range response.Schema.Fields {
		columns = append(columns, types.NewColumnWithSchema(field))
	}
	data := types.Data{}
	for _, row := range response.Rows {
		rowData, err := row.Data()
		if err != nil {
			return nil, err
		}
		data = append(data, rowData)
	}
	return types.NewTableWithSchema(
		&bigqueryv2.Table{
			TableReference: &bigqueryv2.TableReference{
				TableId: tableID,
			},
			Schema: response.Schema,
		},
		data,
	)
}

const (
	gcsEmulatorHostEnvName = "STORAGE_EMULATOR_HOST"
	gcsURIPrefix           = "gs://"
)

// gcsClientOptions builds the option set for a Cloud Storage client.
//
//   - When STORAGE_EMULATOR_HOST is set, the client targets that GCS emulator
//     with authentication disabled.
//   - Otherwise, if no Application Default Credentials are configured, the
//     client falls back to anonymous access so that loads from public buckets
//     succeed instead of failing with "could not find default credentials".
//   - When GOOGLE_APPLICATION_CREDENTIALS is set, those credentials are used.
func gcsClientOptions(jsonReads bool) []option.ClientOption {
	if host := os.Getenv(gcsEmulatorHostEnvName); host != "" {
		opts := []option.ClientOption{
			option.WithEndpoint(fmt.Sprintf("%s/storage/v1/", host)),
			option.WithoutAuthentication(),
		}
		if jsonReads {
			opts = append(opts, storage.WithJSONReads())
		}
		return opts
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		return []option.ClientOption{option.WithoutAuthentication()}
	}
	return nil
}

func (h *jobsInsertHandler) importFromGCS(ctx context.Context, r *jobsInsertRequest) (*bigqueryv2.Job, error) {
	client, err := storage.NewClient(ctx, gcsClientOptions(true)...)
	if err != nil {
		return nil, err
	}
	startTime := time.Now()
	// The write disposition applies to the load job as a whole, so it is
	// honored only for the first object imported; every later object (e.g.
	// the matches of a wildcard URI) appends to what the first one wrote.
	importObject := func(reader *storage.Reader) error {
		if err := h.importFromGCSObject(ctx, r, reader); err != nil {
			return err
		}
		r.job.Configuration.Load.WriteDisposition = "WRITE_APPEND"
		return nil
	}
	for _, uri := range r.job.Configuration.Load.SourceUris {
		if !strings.HasPrefix(uri, gcsURIPrefix) {
			return nil, fmt.Errorf("load source uri must start with gs://")
		}
		uri = strings.TrimPrefix(uri, gcsURIPrefix)
		paths := strings.Split(uri, "/")
		if len(paths) < 2 {
			return nil, fmt.Errorf("unexpected gcs uri format %s", uri)
		}
		bucketName := paths[0]
		objectPath := strings.Join(paths[1:], "/")
		switch strings.Count(objectPath, "*") {
		case 0:
			reader, err := client.Bucket(bucketName).Object(objectPath).NewReader(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to get gcs object reader for %s: %w", uri, err)
			}
			if err := importObject(reader); err != nil {
				return nil, err
			}
		case 1:
			splitPath := strings.Split(objectPath, "*")
			prefix := splitPath[0]
			suffix := splitPath[1]
			query := &storage.Query{
				Prefix: prefix,
			}
			query.SetAttrSelection([]string{"Name"})
			it := client.Bucket(bucketName).Objects(ctx, query)
			for {
				attrs, err := it.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return nil, fmt.Errorf("failed to list gcs object for %s: %w", uri, err)
				}
				if strings.HasSuffix(attrs.Name, suffix) {
					reader, err := client.Bucket(bucketName).Object(attrs.Name).NewReader(ctx)
					if err != nil {
						return nil, fmt.Errorf("failed to get gcs object reader for %s: %w", uri, err)
					}
					if err := importObject(reader); err != nil {
						return nil, err
					}
				}
			}
		default:
			return nil, fmt.Errorf("the number of wildcards in gcs uri must be 0 or 1")
		}
	}
	endTime := time.Now()
	job := r.job
	job.Kind = "bigquery#job"
	job.Configuration.JobType = "LOAD"
	job.SelfLink = fmt.Sprintf(
		"http://%s/bigquery/v2/projects/%s/jobs/%s",
		r.server.httpServer.Addr,
		r.project.ID,
		job.JobReference.JobId,
	)
	job.Status = &bigqueryv2.JobStatus{State: "DONE"}
	job.Statistics = &bigqueryv2.JobStatistics{
		CreationTime: startTime.Unix(),
		StartTime:    startTime.Unix(),
		EndTime:      endTime.Unix(),
	}
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, "")
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.server.metaRepo.InsertJob(
		ctx,
		tx.Tx(),
		metadata.NewJob(
			r.server.metaRepo,
			r.project.ID,
			job.JobReference.JobId,
			job,
			nil,
			nil,
		),
	); err != nil {
		return nil, fmt.Errorf("failed to add job: %w", err)
	}
	if !job.Configuration.DryRun {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit job: %w", err)
		}
	}
	return job, nil
}

func (h *jobsInsertHandler) importFromGCSObject(ctx context.Context, r *jobsInsertRequest, reader *storage.Reader) error {
	defer func() {
		_ = reader.Close()
	}()
	job := metadata.NewJob(
		r.server.metaRepo,
		r.project.ID,
		r.job.JobReference.JobId,
		r.job,
		nil,
		nil,
	)
	if err := new(uploadContentHandler).Handle(ctx, &uploadContentRequest{
		server:  r.server,
		project: r.project,
		job:     job,
		reader:  reader,
	}); err != nil {
		return err
	}
	return nil
}

func (h *jobsInsertHandler) exportToGCS(ctx context.Context, r *jobsInsertRequest) (*bigqueryv2.Job, error) {
	client, err := storage.NewClient(ctx, gcsClientOptions(false)...)
	if err != nil {
		return nil, err
	}
	startTime := time.Now()
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, "")
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	extract := r.job.Configuration.Extract
	sourceTable := extract.SourceTable
	response, err := r.server.contentRepo.Query(
		ctx,
		tx,
		sourceTable.ProjectId,
		sourceTable.DatasetId,
		fmt.Sprintf("SELECT * FROM `%s`", sourceTable.TableId),
		nil,
	)
	if err != nil {
		return nil, err
	}
	for _, uri := range extract.DestinationUris {
		if !strings.HasPrefix(uri, gcsURIPrefix) {
			return nil, fmt.Errorf("destination uri must start with gs://")
		}
		uri = strings.TrimPrefix(uri, gcsURIPrefix)
		paths := strings.Split(uri, "/")
		if len(paths) < 2 {
			return nil, fmt.Errorf("unexpected gcs uri format %s", uri)
		}
		bucketName := paths[0]
		objectPath := strings.Join(paths[1:], "/")
		bucket := client.Bucket(bucketName)
		_ = bucket.Create(ctx, r.project.ID, nil) // ignore "already exists" error.
		writer := bucket.Object(objectPath).NewWriter(ctx)
		if err := h.exportToGCSWithObject(ctx, response, extract, writer); err != nil {
			return nil, err
		}
	}
	endTime := time.Now()
	job := r.job
	job.Kind = "bigquery#job"
	job.Configuration.JobType = "EXTRACT"
	job.SelfLink = fmt.Sprintf(
		"http://%s/bigquery/v2/projects/%s/jobs/%s",
		r.server.httpServer.Addr,
		r.project.ID,
		job.JobReference.JobId,
	)
	job.Status = &bigqueryv2.JobStatus{State: "DONE"}
	job.Statistics = &bigqueryv2.JobStatistics{
		CreationTime: startTime.Unix(),
		StartTime:    startTime.Unix(),
		EndTime:      endTime.Unix(),
	}
	if err := r.server.metaRepo.InsertJob(
		ctx,
		tx.Tx(),
		metadata.NewJob(
			r.server.metaRepo,
			r.project.ID,
			job.JobReference.JobId,
			job,
			nil,
			nil,
		),
	); err != nil {
		return nil, fmt.Errorf("failed to add job: %w", err)
	}
	if !job.Configuration.DryRun {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit job: %w", err)
		}
	}
	return job, nil
}

func (h *jobsInsertHandler) exportToGCSWithObject(ctx context.Context, response *internaltypes.QueryResponse, extract *bigqueryv2.JobConfigurationExtract, writer *storage.Writer) (e error) {
	defer func() {
		if err := writer.Close(); err != nil {
			e = err
		}
	}()
	switch extract.DestinationFormat {
	case "CSV":
		if len(response.Rows) == 0 {
			if _, err := writer.Write(nil); err != nil {
				return fmt.Errorf("failed to empty table data to gcs object: %w", err)
			}
			return nil
		}
		csvWriter := csv.NewWriter(writer)
		var columns []string
		for _, cell := range response.Rows[0].F {
			columns = append(columns, cell.Name)
		}
		if extract.PrintHeader == nil {
			if err := csvWriter.Write(columns); err != nil {
				return fmt.Errorf("failed to encode csv columns: %w", err)
			}
		}
		for _, row := range response.Rows {
			data, err := row.Data()
			if err != nil {
				return fmt.Errorf("failed to get data from table row: %w", err)
			}
			var records []string
			for _, col := range columns {
				value := data[col]
				if value == nil {
					records = append(records, "")
					continue
				}
				if v, ok := value.(string); ok {
					records = append(records, v)
					continue
				}
				jsonValue, err := json.Marshal(value)
				if err != nil {
					return fmt.Errorf("failed to encode row value: %w", err)
				}
				records = append(records, string(jsonValue))
			}
			if err := csvWriter.Write(records); err != nil {
				return fmt.Errorf("failed to encode csv data: %w", err)
			}
		}
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			return fmt.Errorf("failed to encode csv data: %w", err)
		}
	case "NEWLINE_DELIMITED_JSON":
		writer.ContentType = "application/json"
		enc := json.NewEncoder(writer)
		for _, row := range response.Rows {
			data, err := row.Data()
			if err != nil {
				return fmt.Errorf("failed to get data from table row: %w", err)
			}
			if err := enc.Encode(data); err != nil {
				return fmt.Errorf("failed to encode table data: %w", err)
			}
		}
	case "PARQUET":
		var opts []parquet.WriterOption
		switch extract.Compression {
		case "GZIP":
			opts = append(opts, parquet.Compression(&parquet.Gzip))
		case "SNAPPY":
			opts = append(opts, parquet.Compression(&parquet.Snappy))
		case "DEFLATE":
			opts = append(opts, parquet.Compression(&parquet.Gzip))
		}
		_ = opts
		fallthrough
	default:
		return fmt.Errorf("failed to export to gcs: unsupported destination format %s", extract.DestinationFormat)
	}
	return nil
}

func (h *jobsInsertHandler) Handle(ctx context.Context, r *jobsInsertRequest) (*bigqueryv2.Job, error) {
	job := r.job
	if job.Configuration == nil {
		return nil, fmt.Errorf("unspecified job configuration")
	}
	// jobReference is optional in a jobs.insert request — real BigQuery
	// generates one (dbt and bare curl repros post only a configuration).
	// Normalize before any branch: the query path, the GCS load path and
	// the GCS extract path all read JobReference.JobId, and the previous
	// unconditional dereference panicked, surfaced as a recovered 500 and
	// rolled back the job's transaction.
	if job.JobReference == nil {
		job.JobReference = &bigqueryv2.JobReference{}
	}
	if job.JobReference.ProjectId == "" {
		job.JobReference.ProjectId = r.project.ID
	}
	if job.JobReference.JobId == "" {
		job.JobReference.JobId = randomID()
	}
	if job.Configuration.Query == nil {
		// The load/extract paths execute synchronously and mutate metadata
		// and content state; they ran under the global request mutex before
		// the jobs.insert route bypassed it, so serialize them here (and
		// invalidate the metadata read cache before releasing the lock,
		// like every serialized write section).
		r.server.seqMu.Lock()
		defer func() {
			r.server.metaCache.invalidate()
			r.server.seqMu.Unlock()
		}()
		// The middleware hydrated r.project from the read cache (the hot
		// query path needs no fresh tree); this branch mutates metadata
		// through the project object, so re-hydrate fresh state now that
		// seqMu is held (issue #14).
		freshProject, err := r.server.metaRepo.FindProject(ctx, r.project.ID)
		if err != nil {
			return nil, err
		}
		if freshProject == nil {
			return nil, errNotFound(fmt.Sprintf("project %s is not found", r.project.ID))
		}
		r.project = freshProject
		if job.Configuration.Load != nil && len(job.Configuration.Load.SourceUris) != 0 {
			// load from google cloud storage
			job, err := h.importFromGCS(ctx, r)
			if err != nil {
				return nil, fmt.Errorf("failed to import from gcs: %w", err)
			}
			return job, nil
		} else if job.Configuration.Extract != nil && len(job.Configuration.Extract.DestinationUris) != 0 {
			job, err := h.exportToGCS(ctx, r)
			if err != nil {
				return nil, fmt.Errorf("failed to export to gcs: %w", err)
			}
			return job, nil
		}
		return nil, fmt.Errorf("unspecified job configuration query")
	}

	job.Kind = "bigquery#job"
	job.Configuration.JobType = "QUERY"
	job.Configuration.Query.Priority = "INTERACTIVE"
	job.SelfLink = fmt.Sprintf(
		"http://%s/bigquery/v2/projects/%s/jobs/%s",
		r.server.httpServer.Addr,
		r.project.ID,
		job.JobReference.JobId,
	)

	// A dry run never has side effects and clients read its statistics from
	// the insert response, so it stays synchronous: run the query, roll the
	// transaction back, report DONE.
	if job.Configuration.DryRun {
		return h.handleDryRun(ctx, r)
	}

	// Query jobs execute asynchronously (issue #3): persist the job in a
	// running state, launch the job goroutine, and give fast queries a
	// short grace to answer DONE the way the old synchronous handler did.
	// Everyone else sees a non-terminal state and observes completion
	// through the getQueryResults long poll (or a jobs.get poll).
	now := time.Now()
	job.Status = &bigqueryv2.JobStatus{State: "RUNNING"}
	insertStats := queryJobStatistics(job.Configuration.Query.Query, nil, 0)
	job.Statistics = &bigqueryv2.JobStatistics{
		CreationTime: now.Unix(),
		StartTime:    now.Unix(),
		// The statement type is known before execution (clients such as
		// dbt-bigquery branch on it); completion refines the rest of the
		// query statistics (ddlTargetTable, bytes).
		Query: insertStats,
	}
	// Real BigQuery pre-fills configuration.query.destinationTable with the
	// to-be-created table on CTAS jobs (issue #11); pre-fill it best effort
	// when the table name parses out of the statement. Completion overwrites
	// it with the authoritative reference from the engine's ChangedCatalog.
	if insertStats.StatementType == "CREATE_TABLE_AS_SELECT" &&
		job.Configuration.Query.DestinationTable == nil {
		if ref := ctasDestinationTableRef(r.project.ID, job.Configuration.Query); ref != nil {
			job.Configuration.Query.DestinationTable = ref
		}
	}
	pendingJob := metadata.NewPendingJob(r.server.metaRepo, r.project.ID, job.JobReference.JobId, job)
	if err := r.server.registerAndPersistJob(ctx, r.project.ID, pendingJob); err != nil {
		return nil, fmt.Errorf("failed to add job: %w", err)
	}
	r.server.startQueryJob(pendingJob)
	// Read-only jobs answer after a short grace (fast ones report DONE, the
	// rest report RUNNING and complete through the long poll — issue #3).
	// State-MUTATING jobs (DDL, DML, CTAS, explicit destinations) answer
	// like the old synchronous handler did, once the job has completed: a
	// client that ran DDL through jobs.insert may read metadata immediately
	// without polling (e.g. the Go client's Query.Run + Tables iterator),
	// and with reads no longer serialized behind the writer (issue #12)
	// that pattern would otherwise race the job goroutine. The wait costs a
	// writer nothing overall — completion gates its node either way — and
	// writers were serialized through their whole request before issue #3.
	wait := insertDoneGrace
	if insertStats.StatementType != "SELECT" || job.Configuration.Query.DestinationTable != nil {
		wait = insertWriteJobWait
	}
	_, _, _, _ = pendingJob.WaitForResult(ctx, wait)
	return pendingJob.ContentSnapshot(), nil
}

// handleDryRun executes a dry-run query job synchronously and side-effect
// free: the transaction is always rolled back and no job is recorded.
func (h *jobsInsertHandler) handleDryRun(ctx context.Context, r *jobsInsertRequest) (*bigqueryv2.Job, error) {
	job := r.job
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, "")
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	startTime := time.Now()
	// DROP SCHEMA is not supported by the dialect layer yet; it is handled
	// at the emulator layer instead (issue #8) — validation only here.
	response, jobErr := handleDropSchemaQuery(ctx, r.server, r.project.ID, job.Configuration.Query.Query, true)
	if jobErr == nil && response == nil {
		response, jobErr = r.server.contentRepo.Query(
			ctx,
			tx,
			r.project.ID,
			"",
			job.Configuration.Query.Query,
			job.Configuration.Query.QueryParameters,
		)
	}
	endTime := time.Now()
	var totalBytes int64
	if response != nil {
		totalBytes = response.TotalBytes
	}
	status := &bigqueryv2.JobStatus{State: "DONE"}
	if jobErr != nil {
		jobProtoErr := jobErrorProto(r.project.ID, jobErr)
		status.ErrorResult = jobProtoErr.ErrorProto()
		status.Errors = []*bigqueryv2.ErrorProto{jobProtoErr.ErrorProto()}
	}
	job.Status = status
	job.Statistics = &bigqueryv2.JobStatistics{
		Query:               queryJobStatistics(job.Configuration.Query.Query, response, totalBytes),
		CreationTime:        startTime.Unix(),
		StartTime:           startTime.Unix(),
		EndTime:             endTime.Unix(),
		TotalBytesProcessed: totalBytes,
	}
	return job, nil
}

func syncCatalog(ctx context.Context, server *Server, cat *googlesqlite.ChangedCatalog) error {
	// Added must upsert, not blind-add: a CREATE OR REPLACE TABLE/VIEW that
	// replaced an existing object also arrives in Added (the engine already
	// performed the replace), and the metadata entry may pre-exist either
	// from earlier statements in this process or from the persistent
	// catalog of a reopened --database file.
	for _, table := range cat.Table.Added {
		if err := upsertTableMetadata(ctx, server, table); err != nil {
			return err
		}
	}
	// Updated (e.g. ALTER TABLE) reshapes an existing object's schema.
	for _, table := range cat.Table.Updated {
		if err := upsertTableMetadata(ctx, server, table); err != nil {
			return err
		}
	}
	for _, table := range cat.Table.Deleted {
		if err := deleteTableMetadata(ctx, server, table); err != nil {
			return err
		}
	}
	return nil
}

// handleDropSchemaQuery executes a DROP SCHEMA statement at the emulator
// layer. The dialect layer does not support DROP SCHEMA yet (it fails with
// "currently unsupported DROP SCHEMA statement" before reaching the engine),
// so the drop is performed directly against the metadata repository and the
// content repository — the same work the REST datasets.delete handler does —
// and an empty engine-shaped result is synthesized (issue #8).
//
// It returns (nil, nil) when the query is not a DROP SCHEMA statement the
// helper can resolve, in which case the caller proceeds to the engine.
// dryRun validates the statement without mutating anything.
func handleDropSchemaQuery(ctx context.Context, server *Server, defaultProjectID, query string, dryRun bool) (*internaltypes.QueryResponse, error) {
	tokens := scanLeadingTokens(query, 8)
	if len(tokens) < 2 || tokens[0] != "DROP" || tokens[1] != "SCHEMA" {
		return nil, nil
	}
	parts := schemaDDLTargetPath(query)
	if len(parts) == 0 || len(parts) > 2 {
		return nil, nil
	}
	ifExists := len(tokens) >= 4 && tokens[2] == "IF" && tokens[3] == "EXISTS"
	cascade := tokens[len(tokens)-1] == "CASCADE"
	projectID := defaultProjectID
	datasetID := parts[len(parts)-1]
	if len(parts) == 2 {
		projectID = parts[0]
	}
	emptyResponse := func() *internaltypes.QueryResponse {
		return &internaltypes.QueryResponse{
			Schema:      &bigqueryv2.TableSchema{},
			Rows:        []*internaltypes.TableRow{},
			JobComplete: true,
			ChangedCatalog: &googlesqlite.ChangedCatalog{
				Table:    &googlesqlite.ChangedTable{},
				Function: &googlesqlite.ChangedFunction{},
			},
		}
	}
	project, err := server.metaRepo.FindProject(ctx, projectID)
	if err != nil {
		return nil, err
	}
	if project == nil {
		if ifExists {
			return emptyResponse(), nil
		}
		return nil, errNotFound(fmt.Sprintf("Not found: Project %s", projectID))
	}
	dataset := project.Dataset(datasetID)
	if dataset == nil {
		if ifExists {
			return emptyResponse(), nil
		}
		return nil, errNotFound(fmt.Sprintf("Not found: Dataset %s:%s", projectID, datasetID))
	}
	tables := dataset.Tables()
	if len(tables) > 0 && !cascade {
		return nil, errInvalid(fmt.Sprintf(
			"Schema %s:%s is not empty, to force deletion use the CASCADE option",
			projectID, datasetID,
		))
	}
	if dryRun {
		return emptyResponse(), nil
	}
	conn, err := server.connMgr.Connection(ctx, projectID, datasetID)
	if err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()
	deletions := make([]contentdata.TableDeletion, 0, len(tables))
	for _, table := range tables {
		if err := table.Delete(ctx, tx.Tx()); err != nil {
			return nil, err
		}
		deletions = append(deletions, contentdata.TableDeletion{
			ID:     table.ID,
			IsView: table.IsView(),
		})
	}
	if err := server.contentRepo.DeleteTables(ctx, tx, projectID, datasetID, deletions); err != nil {
		return nil, fmt.Errorf("failed to delete tables: %w", err)
	}
	if err := project.DeleteDataset(ctx, tx.Tx(), datasetID); err != nil {
		return nil, fmt.Errorf("failed to delete dataset: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return emptyResponse(), nil
}

// syncSchemaDDL mirrors a successful CREATE SCHEMA / DROP SCHEMA executed
// through the query paths into the metadata repository. The dialect layer
// performs the DDL on the engine, but its ChangedCatalog only carries table
// and function changes, so without this the dataset never reaches the
// metadata repository and every later statement that resolves it fails with
// "dataset ... is not found" (issue #8).
func syncSchemaDDL(ctx context.Context, server *Server, defaultProjectID, stmtType, query string) error {
	if stmtType != "CREATE_SCHEMA" && stmtType != "DROP_SCHEMA" {
		return nil
	}
	parts := schemaDDLTargetPath(query)
	if len(parts) == 0 || len(parts) > 2 {
		return nil
	}
	projectID := defaultProjectID
	datasetID := parts[len(parts)-1]
	if len(parts) == 2 {
		projectID = parts[0]
	}
	project, err := server.metaRepo.FindProject(ctx, projectID)
	if err != nil {
		return err
	}
	if project == nil {
		return nil
	}
	conn, err := server.connMgr.Connection(ctx, projectID, datasetID)
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	switch stmtType {
	case "CREATE_SCHEMA":
		if project.Dataset(datasetID) != nil {
			// Already registered: an IF NOT EXISTS create over an
			// existing dataset is a no-op.
			return nil
		}
		if err := project.AddDataset(ctx, tx.Tx(), metadata.NewDataset(
			server.metaRepo,
			projectID,
			datasetID,
			&bigqueryv2.Dataset{
				Id: fmt.Sprintf("%s:%s", projectID, datasetID),
				DatasetReference: &bigqueryv2.DatasetReference{
					ProjectId: projectID,
					DatasetId: datasetID,
				},
			},
			nil, nil, nil,
		)); err != nil {
			return err
		}
	case "DROP_SCHEMA":
		dataset := project.Dataset(datasetID)
		if dataset == nil {
			// Not registered: an IF EXISTS drop of a missing dataset.
			return nil
		}
		// The engine has already dropped the schema (and, for CASCADE,
		// its tables); remove the metadata entries to match.
		for _, table := range dataset.Tables() {
			if err := table.Delete(ctx, tx.Tx()); err != nil {
				return err
			}
		}
		if err := project.DeleteDataset(ctx, tx.Tx(), datasetID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func upsertTableMetadata(ctx context.Context, server *Server, spec *googlesqlite.TableSpec) error {
	if len(spec.NamePath) != 3 {
		return fmt.Errorf("unexpected table name path: %v", spec.NamePath)
	}
	projectID := spec.NamePath[0]
	datasetID := spec.NamePath[1]
	tableID := spec.NamePath[2]
	project, err := server.metaRepo.FindProject(ctx, projectID)
	if err != nil {
		return err
	}
	dataset := project.Dataset(datasetID)
	if dataset == nil {
		return fmt.Errorf("dataset %s is not found", datasetID)
	}
	fields := make([]*bigqueryv2.TableFieldSchema, 0, len(spec.Columns))
	for _, column := range spec.Columns {
		fields = append(fields, types.TableFieldSchemaFromColumnType(column.Name, column.Type))
	}
	conn, err := server.connMgr.Connection(ctx, projectID, datasetID)
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	table := &bigqueryv2.Table{
		TableReference: &bigqueryv2.TableReference{
			ProjectId: projectID,
			DatasetId: datasetID,
			TableId:   tableID,
		},
		Schema: &bigqueryv2.TableSchema{Fields: fields},
	}
	// A view created by a CREATE VIEW DDL statement must be recorded as a
	// view so it is typed correctly and dropped with DROP VIEW.
	if spec.IsView {
		table.View = &bigqueryv2.ViewDefinition{Query: spec.Query}
		table.Type = string(ViewTableType)
	}
	if existing := dataset.Table(tableID); existing != nil {
		// CREATE OR REPLACE (or ALTER): replace the metadata entry's
		// schema and view definition in place instead of erroring with
		// ErrDuplicatedTable on the re-add. Identity fields (id, type,
		// creationTime, ...) are preserved by Table.Replace.
		table.LastModifiedTime = uint64(time.Now().Unix())
		encoded, err := json.Marshal(table)
		if err != nil {
			return err
		}
		var tableMetadata map[string]interface{}
		if err := json.Unmarshal(encoded, &tableMetadata); err != nil {
			return err
		}
		if err := existing.Replace(ctx, tx.Tx(), tableMetadata); err != nil {
			return err
		}
	} else if _, err := createTableMetadata(ctx, tx, server, project, dataset, table); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func deleteTableMetadata(ctx context.Context, server *Server, spec *googlesqlite.TableSpec) error {
	if len(spec.NamePath) != 3 {
		return fmt.Errorf("unexpected table name path: %v", spec.NamePath)
	}
	projectID := spec.NamePath[0]
	datasetID := spec.NamePath[1]
	tableID := spec.NamePath[2]
	project, err := server.metaRepo.FindProject(ctx, projectID)
	if err != nil {
		return err
	}
	dataset := project.Dataset(datasetID)
	if dataset == nil {
		return fmt.Errorf("dataset %s is not found", datasetID)
	}
	table := dataset.Table(tableID)
	conn, err := server.connMgr.Connection(ctx, projectID, datasetID)
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	if err := table.Delete(ctx, tx.Tx()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (h *jobsListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	res, err := h.Handle(ctx, &jobsListRequest{
		server:  server,
		project: project,
	})
	if err != nil {
		errorResponse(ctx, w, errJobInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsListRequest struct {
	server  *Server
	project *metadata.Project
}

func (h *jobsListHandler) Handle(ctx context.Context, r *jobsListRequest) (*bigqueryv2.JobList, error) {
	jobs := []*bigqueryv2.JobListJobs{}
	for _, job := range r.project.Jobs() {
		content := job.Content()
		jobs = append(jobs, &bigqueryv2.JobListJobs{
			Id:           content.Id,
			JobReference: content.JobReference,
			Kind:         content.Kind,
			Statistics:   content.Statistics,
			Status:       content.Status,
			UserEmail:    content.UserEmail,
		})
	}
	return &bigqueryv2.JobList{Jobs: jobs}, nil
}

func (h *jobsQueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	body, err := io.ReadAll(r.Body)
	// A gzip body flushed but not closed delivers all its content yet ends
	// with ErrUnexpectedEOF; the json.Unmarshal below is the real validator.
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	var req bigqueryv2.QueryRequest
	if err := json.Unmarshal(body, &req); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	var rawReq struct {
		QueryParameters []json.RawMessage `json:"queryParameters"`
	}
	if err := json.Unmarshal(body, &rawReq); err == nil {
		applyNullQueryParameters(rawReq.QueryParameters, req.QueryParameters)
	}
	useInt64Timestamp := false
	if options := req.FormatOptions; options != nil {
		useInt64Timestamp = options.UseInt64Timestamp
	}
	useInt64Timestamp = useInt64Timestamp || isFormatOptionsUseInt64Timestamp(r)
	res, err := h.Handle(ctx, &jobsQueryRequest{
		server:            server,
		project:           project,
		queryRequest:      &req,
		useInt64Timestamp: useInt64Timestamp,
	})
	if err != nil {
		// Duplicate-object failures (e.g. CREATE TABLE on an existing
		// table) surface as 409/duplicate like real BigQuery; other typed
		// errors keep their reason; the rest fall back to jobInternalError.
		errorResponse(ctx, w, jobErrorProto(project.ID, err))
		return
	}
	encodeResponse(ctx, w, res)
}

type jobsQueryRequest struct {
	server            *Server
	project           *metadata.Project
	queryRequest      *bigqueryv2.QueryRequest
	useInt64Timestamp bool
}

func (h *jobsQueryHandler) Handle(ctx context.Context, r *jobsQueryRequest) (*internaltypes.QueryResponse, error) {
	// Statement watchdog (issue #14): the synchronous jobs.query path runs
	// the same budget as async jobs — without it a runaway statement here
	// holds the engine unbounded (the async path is wrapped in job_async.go).
	if budget := resolveMaxStatementDuration(); budget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(ctx, budget,
			fmt.Errorf("statement exceeded the %s watchdog budget (%s)", budget, maxStatementDurationEnvName))
		defer cancel()
	}
	var datasetID string
	if r.queryRequest.DefaultDataset != nil {
		datasetID = r.queryRequest.DefaultDataset.DatasetId
	}
	// A connection killed by a prior watchdog/interrupt surfaces here as
	// driver.ErrBadConn on its FIRST use (conn-pinned calls bypass
	// database/sql's retry) — acquire-and-begin retries once on a fresh
	// conn; the statement has not reached the engine yet, so this is safe.
	var conn *connection.Conn
	var tx *connection.Tx
	for attempt := 0; ; attempt++ {
		var err error
		conn, err = r.server.connMgr.Connection(ctx, r.project.ID, datasetID)
		if err != nil {
			return nil, err
		}
		tx, err = conn.Begin(ctx)
		if err == nil {
			break
		}
		if attempt == 0 && strings.Contains(err.Error(), "bad connection") {
			continue
		}
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()
	startTime := time.Now()
	// DROP SCHEMA is not supported by the dialect layer yet; execute it at
	// the emulator layer instead (issue #8).
	response, queryErr := handleDropSchemaQuery(ctx, r.server, r.project.ID, r.queryRequest.Query, r.queryRequest.DryRun)
	if queryErr == nil && response == nil {
		response, queryErr = r.server.contentRepo.Query(
			ctx,
			tx,
			r.project.ID,
			datasetID,
			r.queryRequest.Query,
			r.queryRequest.QueryParameters,
		)
	}
	if queryErr != nil {
		// Attribute watchdog-budget expiries explicitly: WithTimeoutCause's
		// cause is not part of the driver error chain, but clients (and the
		// watchdog test) should see WHY the statement died.
		if cause := context.Cause(ctx); cause != nil && strings.Contains(cause.Error(), "watchdog") {
			queryErr = fmt.Errorf("%v: %w", cause, queryErr)
		}
		return nil, queryErr
	}
	endTime := time.Now()
	// jobs.query allocates jobIDs server-side (real BigQuery does the
	// same). queryRequest.RequestId is the *idempotency* key — same
	// RequestId on retry should return the cached result — and is
	// deliberately not the jobID. The Go BigQuery client in particular
	// instantiates a fresh `uid.NewSpace("request", …)` per call, so its
	// per-Space atomic counter always emits `-0001`; concurrent
	// in-process callers therefore submit identical RequestIds and would
	// collide on AddJob if we routed them through as the jobID. Idempotency
	// (RequestId → cached response) is a TODO; for now every call gets a
	// fresh jobID.
	jobID := randomID()

	// Persist the job in the metadata store, mirroring what
	// jobsInsertHandler.Handle does at line ~1758. The previous behaviour
	// returned a JobReference whose ID was never recorded, so any client
	// that then issued `jobs.get(jobID)` — e.g. Go's
	// `RowIterator.SourceJob().Status(ctx)`, or the Java/Node clients
	// that poll the job for status after a synchronous query — got a 404
	// and (for clients that treat 404 as transient) hung re-polling. The
	// synthetic Job below carries the minimum fields the GET handler
	// returns to the caller; DryRun queries skip both the AddJob and the
	// Commit so they remain side-effect-free.
	var totalBytes int64
	if response != nil {
		totalBytes = response.TotalBytes
	}
	job := &bigqueryv2.Job{
		Kind: "bigquery#job",
		JobReference: &bigqueryv2.JobReference{
			ProjectId: r.project.ID,
			JobId:     jobID,
			Location:  r.queryRequest.Location,
		},
		Configuration: &bigqueryv2.JobConfiguration{
			JobType: "QUERY",
			DryRun:  r.queryRequest.DryRun,
			Query: &bigqueryv2.JobConfigurationQuery{
				Query:           r.queryRequest.Query,
				QueryParameters: r.queryRequest.QueryParameters,
				Priority:        "INTERACTIVE",
			},
		},
		Status: &bigqueryv2.JobStatus{State: "DONE"},
		Statistics: &bigqueryv2.JobStatistics{
			Query:               queryJobStatistics(r.queryRequest.Query, response, totalBytes),
			CreationTime:        startTime.Unix(),
			StartTime:           startTime.Unix(),
			EndTime:             endTime.Unix(),
			TotalBytesProcessed: totalBytes,
		},
		SelfLink: fmt.Sprintf(
			"http://%s/bigquery/v2/projects/%s/jobs/%s",
			r.server.httpServer.Addr,
			r.project.ID,
			jobID,
		),
	}
	// CTAS reports the created table as the job's destination (issue #11),
	// mirroring the async jobs.insert path, so jobs.get on a jobs.query-made
	// job classifies identically.
	if qs := job.Statistics.Query; qs.StatementType == "CREATE_TABLE_AS_SELECT" && qs.DdlTargetTable != nil {
		job.Configuration.Query.DestinationTable = qs.DdlTargetTable
	}
	if !r.queryRequest.DryRun {
		if err := r.server.metaRepo.InsertJob(
			ctx,
			tx.Tx(),
			metadata.NewJob(
				r.server.metaRepo,
				r.project.ID,
				jobID,
				job,
				response,
				nil,
			),
		); err != nil {
			return nil, fmt.Errorf("failed to add job: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		if response.ChangedCatalog.Changed() {
			if err := syncCatalog(ctx, r.server, response.ChangedCatalog); err != nil {
				return nil, err
			}
		}
		if err := syncSchemaDDL(ctx, r.server, r.project.ID, job.Statistics.Query.StatementType, r.queryRequest.Query); err != nil {
			return nil, err
		}
	}
	response.Rows = internaltypes.Format(response.Schema, response.Rows, r.useInt64Timestamp)
	response.JobReference = job.JobReference
	return response, nil
}

func (h *modelsDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	model := modelFromContext(ctx)
	if err := h.Handle(ctx, &modelsDeleteRequest{
		server:  server,
		project: project,
		dataset: dataset,
		model:   model,
	}); err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
	}
}

type modelsDeleteRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	model   *metadata.Model
}

func (h *modelsDeleteHandler) Handle(ctx context.Context, r *modelsDeleteRequest) error {
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, r.dataset.ID)
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.dataset.DeleteModel(ctx, tx.Tx(), r.model.ID); err != nil {
		return fmt.Errorf("failed to delete model: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (h *modelsGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	model := modelFromContext(ctx)
	res, err := h.Handle(ctx, &modelsGetRequest{
		server:  server,
		project: project,
		dataset: dataset,
		model:   model,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type modelsGetRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	model   *metadata.Model
}

func (h *modelsGetHandler) Handle(ctx context.Context, r *modelsGetRequest) (*bigqueryv2.Model, error) {
	return &bigqueryv2.Model{
		BestTrialId:             0,
		CreationTime:            0,
		DefaultTrialId:          0,
		Description:             "",
		EncryptionConfiguration: nil,
		Etag:                    "",
		ExpirationTime:          0,
		FeatureColumns:          nil,
		FriendlyName:            "",
		HparamSearchSpaces:      nil,
		HparamTrials:            nil,
		LabelColumns:            nil,
		Labels:                  nil,
		LastModifiedTime:        0,
		Location:                "",
		ModelReference:          nil,
		ModelType:               "",
		OptimalTrialIds:         nil,
		TrainingRuns:            nil,
	}, nil
}

func (h *modelsListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	res, err := h.Handle(ctx, &modelsListRequest{
		server:  server,
		project: project,
		dataset: dataset,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type modelsListRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
}

func (h *modelsListHandler) Handle(ctx context.Context, r *modelsListRequest) (*bigqueryv2.ListModelsResponse, error) {
	models := []*bigqueryv2.Model{}
	for _, m := range r.dataset.Models() {
		_ = m
		models = append(models, &bigqueryv2.Model{})
	}
	return &bigqueryv2.ListModelsResponse{
		Models: models,
	}, nil
}

func (h *modelsPatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	model := modelFromContext(ctx)
	res, err := h.Handle(ctx, &modelsPatchRequest{
		server:  server,
		project: project,
		dataset: dataset,
		model:   model,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type modelsPatchRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	model   *metadata.Model
}

func (h *modelsPatchHandler) Handle(ctx context.Context, r *modelsPatchRequest) (*bigqueryv2.Model, error) {
	return &bigqueryv2.Model{}, nil
}

func (h *projectsGetServiceAccountHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	res, err := h.Handle(ctx, &projectsGetServiceAccountRequest{
		server:  server,
		project: project,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type projectsGetServiceAccountRequest struct {
	server  *Server
	project *metadata.Project
}

func (h *projectsGetServiceAccountHandler) Handle(ctx context.Context, r *projectsGetServiceAccountRequest) (*bigqueryv2.GetServiceAccountResponse, error) {
	return &bigqueryv2.GetServiceAccountResponse{}, nil
}

func (h *projectsListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &projectsListRequest{
		server: server,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type projectsListRequest struct {
	server *Server
}

func (h *projectsListHandler) Handle(ctx context.Context, r *projectsListRequest) (*bigqueryv2.ProjectList, error) {
	projects, err := r.server.metaRepo.FindAllProjects(ctx)
	if err != nil {
		return nil, err
	}

	projectList := []*bigqueryv2.ProjectListProjects{}
	for i, p := range projects {
		projectList = append(projectList, &bigqueryv2.ProjectListProjects{
			Id:           p.ID,
			NumericId:    uint64(i + 1),
			FriendlyName: p.ID,
		})
	}
	return &bigqueryv2.ProjectList{
		Projects: projectList,
	}, nil
}

func (h *routinesDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	routine := routineFromContext(ctx)
	if err := h.Handle(ctx, &routinesDeleteRequest{
		server:  server,
		project: project,
		dataset: dataset,
		routine: routine,
	}); err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
}

type routinesDeleteRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	routine *metadata.Routine
}

func (h *routinesDeleteHandler) Handle(ctx context.Context, r *routinesDeleteRequest) error {
	return fmt.Errorf("unsupported bigquery.routines.delete")
}

func (h *routinesGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	routine := routineFromContext(ctx)
	res, err := h.Handle(ctx, &routinesGetRequest{
		server:  server,
		project: project,
		dataset: dataset,
		routine: routine,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type routinesGetRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	routine *metadata.Routine
}

func (h *routinesGetHandler) Handle(ctx context.Context, r *routinesGetRequest) (*bigqueryv2.Routine, error) {
	return nil, fmt.Errorf("unsupported bigquery.routines.get")
}

func (h *routinesInsertHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	var routine bigqueryv2.Routine
	if err := json.NewDecoder(r.Body).Decode(&routine); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &routinesInsertRequest{
		server:  server,
		project: project,
		dataset: dataset,
		routine: &routine,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type routinesInsertRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	routine *bigqueryv2.Routine
}

func (h *routinesInsertHandler) Handle(ctx context.Context, r *routinesInsertRequest) (*bigqueryv2.Routine, error) {
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, r.dataset.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.server.contentRepo.AddRoutineByMetaData(ctx, tx, r.routine); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return r.routine, nil
}

func (h *routinesListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	res, err := h.Handle(ctx, &routinesListRequest{
		server:  server,
		project: project,
		dataset: dataset,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type routinesListRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
}

func (h *routinesListHandler) Handle(ctx context.Context, r *routinesListRequest) (*bigqueryv2.ListRoutinesResponse, error) {
	var routineList []*bigqueryv2.Routine
	for _, routine := range r.dataset.Routines() {
		_ = routine
		routineList = append(routineList, &bigqueryv2.Routine{})
	}
	return &bigqueryv2.ListRoutinesResponse{
		Routines: routineList,
	}, fmt.Errorf("unsupported bigquery.routines.list")
}

func (h *routinesUpdateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	routine := routineFromContext(ctx)
	res, err := h.Handle(ctx, &routinesUpdateRequest{
		server:  server,
		project: project,
		dataset: dataset,
		routine: routine,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type routinesUpdateRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	routine *metadata.Routine
}

func (h *routinesUpdateHandler) Handle(ctx context.Context, r *routinesUpdateRequest) (*bigqueryv2.Routine, error) {
	return nil, fmt.Errorf("unsupported bigquery.routines.update")
}

func (h *rowAccessPoliciesGetIamPolicyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &rowAccessPoliciesGetIamPolicyRequest{
		server: server,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type rowAccessPoliciesGetIamPolicyRequest struct {
	server *Server
}

func (h *rowAccessPoliciesGetIamPolicyHandler) Handle(ctx context.Context, r *rowAccessPoliciesGetIamPolicyRequest) (*bigqueryv2.Policy, error) {
	return nil, fmt.Errorf("unsupported bigquery.rowAccessPolicies.getIamPolicy")
}

func (h *rowAccessPoliciesListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	res, err := h.Handle(ctx, &rowAccessPoliciesListRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type rowAccessPoliciesListRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
}

func (h *rowAccessPoliciesListHandler) Handle(ctx context.Context, r *rowAccessPoliciesListRequest) (*bigqueryv2.ListRowAccessPoliciesResponse, error) {
	return nil, fmt.Errorf("unsupported bigquery.rowAccessPolicies.list")
}

func (h *rowAccessPoliciesSetIamPolicyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &rowAccessPoliciesSetIamPolicyRequest{
		server: server,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type rowAccessPoliciesSetIamPolicyRequest struct {
	server *Server
}

func (h *rowAccessPoliciesSetIamPolicyHandler) Handle(ctx context.Context, r *rowAccessPoliciesSetIamPolicyRequest) (*bigqueryv2.Policy, error) {
	return nil, fmt.Errorf("unsupported bigquery.rowAccessPolicies.setIamPolicy")
}

func (h *rowAccessPoliciesTestIamPermissionsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &rowAccessPoliciesTestIamPermissionsRequest{
		server: server,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type rowAccessPoliciesTestIamPermissionsRequest struct {
	server *Server
}

func (h *rowAccessPoliciesTestIamPermissionsHandler) Handle(ctx context.Context, r *rowAccessPoliciesTestIamPermissionsRequest) (*bigqueryv2.TestIamPermissionsResponse, error) {
	return nil, fmt.Errorf("unsupported bigquery.rowAccessPolicies.testIamPermissions")
}

func (h *tabledataInsertAllHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	var req bigqueryv2.TableDataInsertAllRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &tabledataInsertAllRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
		req:     &req,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tabledataInsertAllRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
	req     *bigqueryv2.TableDataInsertAllRequest
}

func normalizeInsertValue(v interface{}, field *bigqueryv2.TableFieldSchema) (interface{}, error) {
	rv := reflect.ValueOf(v)
	kind := rv.Kind()
	if field.Mode == "REPEATED" {
		if kind != reflect.Slice && kind != reflect.Array {
			return nil, fmt.Errorf("invalid value type %T for ARRAY column", v)
		}
		values := make([]interface{}, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			value, err := normalizeInsertValue(rv.Index(i).Interface(), &bigqueryv2.TableFieldSchema{
				Fields: field.Fields,
			})
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	}
	if kind == reflect.Map {
		fieldMap := map[string]*bigqueryv2.TableFieldSchema{}
		for _, f := range field.Fields {
			fieldMap[f.Name] = f
		}
		columnNameToValueMap := map[string]interface{}{}
		for _, key := range rv.MapKeys() {
			if key.Kind() != reflect.String {
				return nil, fmt.Errorf("invalid value type %s for STRUCT column", key.Kind())
			}
			columnName := key.Interface().(string)
			value, err := normalizeInsertValue(rv.MapIndex(key).Interface(), fieldMap[columnName])
			if err != nil {
				return nil, err
			}
			columnNameToValueMap[columnName] = value
		}
		fields := make([]map[string]interface{}, 0, len(fieldMap))
		for _, f := range field.Fields {
			value, exists := columnNameToValueMap[f.Name]
			if !exists {
				return nil, fmt.Errorf("failed to find value from %s", f.Name)
			}
			fields = append(fields, map[string]interface{}{f.Name: value})
		}
		return fields, nil
	}
	return v, nil
}

func (h *tabledataInsertAllHandler) Handle(ctx context.Context, r *tabledataInsertAllRequest) (*bigqueryv2.TableDataInsertAllResponse, error) {
	content, err := r.table.Content()
	if err != nil {
		return nil, err
	}
	var insertErrors []*bigqueryv2.TableDataInsertAllResponseInsertErrors
	data := types.Data{}
	for i, row := range r.req.Rows {
		// A row that carries fields absent from the table schema is rejected
		// unless ignoreUnknownValues is set; it is reported per-row and not
		// inserted, while the remaining rows still go in.
		if !r.req.IgnoreUnknownValues {
			if unknown := types.ValidateRowFields(content.Schema, row.Json); len(unknown) > 0 {
				errs := make([]*bigqueryv2.ErrorProto, 0, len(unknown))
				for _, name := range unknown {
					errs = append(errs, &bigqueryv2.ErrorProto{
						Reason:   "invalid",
						Location: name,
						Message:  fmt.Sprintf("no such field: %s.", name),
					})
				}
				insertErrors = append(insertErrors, &bigqueryv2.TableDataInsertAllResponseInsertErrors{
					Index:  int64(i),
					Errors: errs,
				})
				continue
			}
		}
		rowData := map[string]interface{}{}
		for k, v := range row.Json {
			rowData[k] = v
		}
		data = append(data, rowData)
	}
	if len(data) > 0 {
		tableDef, err := types.NewTableWithSchema(content, data)
		if err != nil {
			return nil, err
		}
		conn, err := r.server.connMgr.Connection(ctx, r.project.ID, r.dataset.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to get connection: %w", err)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.RollbackIfNotCommitted()
		if err := r.server.contentRepo.AddTableData(ctx, tx, r.project.ID, r.dataset.ID, tableDef); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return &bigqueryv2.TableDataInsertAllResponse{InsertErrors: insertErrors}, nil
}

func (h *tabledataListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	res, err := h.Handle(ctx, &tabledataListRequest{
		server:            server,
		project:           project,
		dataset:           dataset,
		table:             table,
		useInt64Timestamp: isFormatOptionsUseInt64Timestamp(r),
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tabledataListRequest struct {
	server            *Server
	project           *metadata.Project
	dataset           *metadata.Dataset
	table             *metadata.Table
	useInt64Timestamp bool
}

func (h *tabledataListHandler) Handle(ctx context.Context, r *tabledataListRequest) (*internaltypes.TableDataList, error) {
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, r.dataset.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	response, err := r.server.contentRepo.Query(
		ctx,
		tx,
		r.project.ID,
		r.dataset.ID,
		fmt.Sprintf("SELECT * FROM `%s`", r.table.ID),
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &internaltypes.TableDataList{
		Rows:      internaltypes.Format(response.Schema, response.Rows, r.useInt64Timestamp),
		TotalRows: response.TotalRows,
	}, nil
}

func (h *tablesDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	if err := h.Handle(ctx, &tablesDeleteRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
	}); err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
	}
}

type tablesDeleteRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
}

func (h *tablesDeleteHandler) Handle(ctx context.Context, r *tablesDeleteRequest) error {
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, r.dataset.ID)
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.RollbackIfNotCommitted()
	// delete table metadata
	if err := r.table.Delete(ctx, tx.Tx()); err != nil {
		return err
	}
	// delete table
	if err := r.server.contentRepo.DeleteTables(
		ctx,
		tx,
		r.project.ID,
		r.dataset.ID,
		[]contentdata.TableDeletion{{ID: r.table.ID, IsView: r.table.IsView()}},
	); err != nil {
		return fmt.Errorf("failed to delete table %s: %w", r.table.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (h *tablesGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	res, err := h.Handle(ctx, &tablesGetRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   table,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesGetRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *metadata.Table
}

func (h *tablesGetHandler) Handle(ctx context.Context, r *tablesGetRequest) (*bigqueryv2.Table, error) {
	table, err := r.table.Content()
	if err != nil {
		return nil, fmt.Errorf("failed to get table content: %w", err)
	}
	// Populate NumRows from the backing table so clients that depend on it
	// (e.g. Table.getNumRows) observe an accurate count. Views and external
	// tables have no backing row store and are left untouched. The count is
	// an engine query, so it is served from the metadata read cache when
	// possible — otherwise a tables.get would queue behind whatever
	// statement the engine is executing (issue #12).
	if table.Type == "" || table.Type == "TABLE" {
		key := r.project.ID + "/" + r.dataset.ID + "/" + r.table.ID
		if numRows, ok, gen := r.server.metaCache.lookupRowCount(key); ok {
			table.NumRows = uint64(numRows)
		} else if numRows, err := h.countRows(ctx, r); err == nil {
			table.NumRows = uint64(numRows)
			r.server.metaCache.storeRowCount(key, numRows, gen)
		}
	}
	return table, nil
}

func (h *tablesGetHandler) countRows(ctx context.Context, r *tablesGetRequest) (int64, error) {
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, r.dataset.ID)
	if err != nil {
		return 0, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.RollbackIfNotCommitted()
	count, err := r.server.contentRepo.CountTableRows(ctx, tx, r.project.ID, r.dataset.ID, r.table.ID)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}

func (h *tablesGetIamPolicyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	var req bigqueryv2.GetIamPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &tablesGetIamPolicyRequest{
		server: server,
		req:    &req,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesGetIamPolicyRequest struct {
	server *Server
	req    *bigqueryv2.GetIamPolicyRequest
}

func (h *tablesGetIamPolicyHandler) Handle(ctx context.Context, r *tablesGetIamPolicyRequest) (*bigqueryv2.Policy, error) {
	return nil, fmt.Errorf("bigquery.tables.getIamPolicy")
}

func (h *tablesInsertHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	var table bigqueryv2.Table
	if err := json.NewDecoder(r.Body).Decode(&table); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &tablesInsertRequest{
		server:  server,
		project: project,
		dataset: dataset,
		table:   &table,
	})
	if err != nil {
		errorResponse(ctx, w, err)
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesInsertRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
	table   *bigqueryv2.Table
}

type TableType string

const (
	DefaultTableType          TableType = "TABLE"
	ViewTableType             TableType = "VIEW"
	ExternalTableType         TableType = "EXTERNAL"
	MaterializedViewTableType TableType = "MATERIALIZED_VIEW"
	SnapshotTableType         TableType = "SNAPSHOT"
)

func createTableMetadata(ctx context.Context, tx *connection.Tx, server *Server, project *metadata.Project, dataset *metadata.Dataset, table *bigqueryv2.Table) (*bigqueryv2.Table, *ServerError) {
	now := time.Now().Unix()
	table.Id = fmt.Sprintf("%s:%s.%s", project.ID, dataset.ID, table.TableReference.TableId)
	table.CreationTime = now
	table.LastModifiedTime = uint64(now)
	table.Type = string(DefaultTableType) // TODO: need to handle other table types
	if table.View != nil {
		table.Type = string(ViewTableType)
	}
	if table.MaterializedView != nil {
		table.Type = string(MaterializedViewTableType)
	}
	table.Kind = "bigquery#table"
	table.SelfLink = fmt.Sprintf(
		"http://%s/bigquery/v2/projects/%s/datasets/%s/tables/%s",
		server.httpServer.Addr,
		project.ID,
		dataset.ID,
		table.TableReference.TableId,
	)
	encodedTableData, err := json.Marshal(table)
	if err != nil {
		return nil, errInternalError(err.Error())
	}
	var tableMetadata map[string]interface{}
	if err := json.Unmarshal(encodedTableData, &tableMetadata); err != nil {
		return nil, errInternalError(err.Error())
	}
	if err := dataset.AddTable(
		ctx,
		tx.Tx(),
		metadata.NewTable(
			server.metaRepo,
			project.ID,
			dataset.ID,
			table.TableReference.TableId,
			tableMetadata,
		),
	); err != nil {
		if errors.Is(err, metadata.ErrDuplicatedTable) {
			// Real BigQuery shape: 409 Conflict, reason "duplicate",
			// "Already Exists: Table project:dataset.table".
			return nil, errDuplicate(fmt.Sprintf(
				"Already Exists: Table %s:%s.%s",
				project.ID, dataset.ID, table.TableReference.TableId,
			))
		}
		return nil, errInternalError(err.Error())
	}
	return table, nil
}

func (h *tablesInsertHandler) Handle(ctx context.Context, r *tablesInsertRequest) (*bigqueryv2.Table, *ServerError) {
	// Type and mode names are case-insensitive in real BigQuery (dbt seeds
	// send lowercase ones); canonicalize once here so the stored metadata
	// and the generated DDL both see uppercase names, and reject genuinely
	// unknown type names with a clear 400 instead of TYPE_UNKNOWN DDL.
	if r.table.Schema != nil {
		types.NormalizeSchema(r.table.Schema)
		if err := types.ValidateSchema(r.table.Schema); err != nil {
			return nil, errInvalid(err.Error())
		}
	}
	if r.table.ExternalDataConfiguration != nil {
		return h.handleExternalTable(ctx, r)
	}
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, r.dataset.ID)
	if err != nil {
		return nil, errInternalError(err.Error())
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, errInternalError(err.Error())
	}
	defer tx.RollbackIfNotCommitted()

	isView := r.table.View != nil || r.table.MaterializedView != nil
	if isView {
		// Create the view first so its resolved column schema can be read
		// back and recorded in the metadata, as real BigQuery does.
		if err := r.server.contentRepo.CreateView(ctx, tx, r.table); err != nil {
			return nil, errInvalid(err.Error())
		}
		schema, err := r.server.contentRepo.ViewSchema(
			ctx, tx, r.project.ID, r.dataset.ID, r.table.TableReference.TableId,
		)
		if err != nil {
			return nil, errInvalid(err.Error())
		}
		r.table.Schema = schema
	}
	table, serverErr := createTableMetadata(ctx, tx, r.server, r.project, r.dataset, r.table)
	if serverErr != nil {
		return nil, serverErr
	}
	if !isView && r.table.Schema != nil {
		if err := r.server.contentRepo.CreateTable(ctx, tx, r.table); err != nil {
			return nil, errInternalError(err.Error())
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, errInternalError(fmt.Errorf("failed to commit table: %w", err).Error())
	}
	return table, nil
}

// handleExternalTable materializes an external table's source data into a
// backing table so the table is registered with the query engine and can be
// queried. Real BigQuery reads an external table live on every query; the
// emulator snapshots the source at creation time, which is enough to make
// the table queryable.
func (h *tablesInsertHandler) handleExternalTable(ctx context.Context, r *tablesInsertRequest) (*bigqueryv2.Table, *ServerError) {
	edc := r.table.ExternalDataConfiguration
	tableRef := r.table.TableReference
	if tableRef == nil {
		return nil, errInvalid("external table is missing tableReference")
	}
	if tableRef.ProjectId == "" {
		tableRef.ProjectId = r.project.ID
	}
	if tableRef.DatasetId == "" {
		tableRef.DatasetId = r.dataset.ID
	}
	if len(edc.SourceUris) == 0 {
		return nil, errInvalid("external table is missing sourceUris")
	}

	// Translate the external data configuration into a load job and route it
	// through the existing load pipeline, which creates the backing table and
	// loads the rows (inferring the schema when autodetect is requested).
	load := &bigqueryv2.JobConfigurationLoad{
		DestinationTable:  tableRef,
		SourceUris:        edc.SourceUris,
		SourceFormat:      edc.SourceFormat,
		Autodetect:        edc.Autodetect,
		Schema:            edc.Schema,
		CreateDisposition: "CREATE_IF_NEEDED",
	}
	if load.Schema == nil {
		load.Schema = r.table.Schema
	}
	if csv := edc.CsvOptions; csv != nil {
		load.Quote = csv.Quote
		load.FieldDelimiter = csv.FieldDelimiter
		load.SkipLeadingRows = csv.SkipLeadingRows
		load.AllowJaggedRows = csv.AllowJaggedRows
		load.AllowQuotedNewlines = csv.AllowQuotedNewlines
		load.Encoding = csv.Encoding
	}
	job := &bigqueryv2.Job{
		JobReference:  &bigqueryv2.JobReference{JobId: randomID(), ProjectId: r.project.ID},
		Configuration: &bigqueryv2.JobConfiguration{Load: load},
	}
	if _, err := (&jobsInsertHandler{}).importFromGCS(ctx, &jobsInsertRequest{
		server:  r.server,
		project: r.project,
		job:     job,
	}); err != nil {
		return nil, errInvalid(fmt.Sprintf("failed to load external table data: %s", err))
	}

	// The load created a plain table; record the external configuration on
	// its metadata so it round-trips on tables.get and tables.list.
	table := r.dataset.Table(tableRef.TableId)
	if table == nil {
		return nil, errInternalError("external table backing data was not created")
	}
	content, err := table.Content()
	if err != nil {
		return nil, errInternalError(err.Error())
	}
	content.ExternalDataConfiguration = edc
	content.Type = string(ExternalTableType)
	encoded, err := json.Marshal(content)
	if err != nil {
		return nil, errInternalError(err.Error())
	}
	var newMetadata map[string]interface{}
	if err := json.Unmarshal(encoded, &newMetadata); err != nil {
		return nil, errInternalError(err.Error())
	}
	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, r.dataset.ID)
	if err != nil {
		return nil, errInternalError(err.Error())
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, errInternalError(err.Error())
	}
	defer tx.RollbackIfNotCommitted()
	if err := table.Replace(ctx, tx.Tx(), newMetadata); err != nil {
		return nil, errInternalError(err.Error())
	}
	if err := tx.Commit(); err != nil {
		return nil, errInternalError(err.Error())
	}
	return content, nil
}

func (h *tablesListHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	res, err := h.Handle(ctx, &tablesListRequest{
		server:  server,
		project: project,
		dataset: dataset,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesListRequest struct {
	server  *Server
	project *metadata.Project
	dataset *metadata.Dataset
}

func (h *tablesListHandler) Handle(ctx context.Context, r *tablesListRequest) (*bigqueryv2.TableList, error) {
	var tables []*bigqueryv2.TableListTables
	for _, tableID := range r.dataset.TableIDs() {
		table, err := r.dataset.Table(tableID).Content()
		if err != nil {
			return nil, fmt.Errorf("failed to get table metadata from %s: %w", tableID, err)
		}
		tables = append(tables, &bigqueryv2.TableListTables{
			Clustering:        table.Clustering,
			CreationTime:      table.CreationTime,
			ExpirationTime:    table.ExpirationTime,
			FriendlyName:      table.FriendlyName,
			Id:                table.Id,
			Kind:              table.Kind,
			Labels:            table.Labels,
			RangePartitioning: table.RangePartitioning,
			TableReference:    table.TableReference,
			TimePartitioning:  table.TimePartitioning,
			Type:              table.Type,
		})
	}
	return &bigqueryv2.TableList{
		Tables:     tables,
		TotalItems: int64(len(tables)),
	}, nil
}

func (h *tablesPatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	var newTable bigqueryv2.Table
	if err := json.NewDecoder(r.Body).Decode(&newTable); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &tablesPatchRequest{
		server:   server,
		project:  project,
		dataset:  dataset,
		table:    table,
		newTable: &newTable,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesPatchRequest struct {
	server   *Server
	project  *metadata.Project
	dataset  *metadata.Dataset
	table    *metadata.Table
	newTable *bigqueryv2.Table
}

func (h *tablesPatchHandler) Handle(ctx context.Context, r *tablesPatchRequest) (*bigqueryv2.Table, error) {
	types.NormalizeSchema(r.newTable.Schema)
	encodedTableData, err := json.Marshal(r.newTable)
	if err != nil {
		return nil, err
	}
	var tableMetadata map[string]interface{}
	if err := json.Unmarshal(encodedTableData, &tableMetadata); err != nil {
		return nil, err
	}

	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, r.dataset.ID)
	if err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.table.Patch(ctx, tx.Tx(), tableMetadata); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	// Return the full, merged table resource (kind/type/id/creationTime
	// included) rather than echoing the request body.
	return r.table.Content()
}

func (h *tablesSetIamPolicyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &tablesSetIamPolicyRequest{
		server: server,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesSetIamPolicyRequest struct {
	server *Server
}

func (h *tablesSetIamPolicyHandler) Handle(ctx context.Context, r *tablesSetIamPolicyRequest) (*bigqueryv2.Policy, error) {
	return nil, fmt.Errorf("unsupported bigquery.tables.setIamPolicy")
}

func (h *tablesTestIamPermissionsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	res, err := h.Handle(ctx, &tablesTestIamPermissionsRequest{
		server: server,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesTestIamPermissionsRequest struct {
	server *Server
}

func (h *tablesTestIamPermissionsHandler) Handle(ctx context.Context, r *tablesTestIamPermissionsRequest) (*bigqueryv2.TestIamPermissionsResponse, error) {
	return nil, fmt.Errorf("unsupported bigquery.tables.testIamPermissions")
}

func (h *tablesUpdateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	project := projectFromContext(ctx)
	dataset := datasetFromContext(ctx)
	table := tableFromContext(ctx)
	var newTable bigqueryv2.Table
	if err := json.NewDecoder(r.Body).Decode(&newTable); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &tablesUpdateRequest{
		server:   server,
		project:  project,
		dataset:  dataset,
		table:    table,
		newTable: &newTable,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

type tablesUpdateRequest struct {
	server   *Server
	project  *metadata.Project
	dataset  *metadata.Dataset
	table    *metadata.Table
	newTable *bigqueryv2.Table
}

func (h *tablesUpdateHandler) Handle(ctx context.Context, r *tablesUpdateRequest) (*bigqueryv2.Table, error) {
	types.NormalizeSchema(r.newTable.Schema)
	encodedTableData, err := json.Marshal(r.newTable)
	if err != nil {
		return nil, err
	}
	var tableMetadata map[string]interface{}
	if err := json.Unmarshal(encodedTableData, &tableMetadata); err != nil {
		return nil, err
	}

	conn, err := r.server.connMgr.Connection(ctx, r.project.ID, r.dataset.ID)
	if err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.RollbackIfNotCommitted()
	if err := r.table.Replace(ctx, tx.Tx(), tableMetadata); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return r.table.Content()
}

type defaultHandler struct{}

func (h *defaultHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	errorResponse(ctx, w, errInternalError(fmt.Sprintf("unexpected request path: %s", html.EscapeString(r.URL.Path))))
}
