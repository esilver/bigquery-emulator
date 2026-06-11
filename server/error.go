package server

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/goccy/go-json"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	"github.com/goccy/bigquery-emulator/internal/metadata"
)

// ServerError represents BigQuery errors.
// documentation is here.
// https://cloud.google.com/bigquery/docs/error-messages
type ServerError struct {
	Status    int         `json:"-"`
	Reason    ErrorReason `json:"reason"`
	Location  string      `json:"location"`
	DebugInfo string      `json:"debugInfo"`
	Message   string      `json:"message"`
}

type ResponseError struct {
	Error *ErrorFormat `json:"error"`
}

type ErrorFormat struct {
	Errors  []*ServerError `json:"errors"`
	Code    int            `json:"code"`
	Message string         `json:"message"`
}

func (e *ServerError) ErrorProto() *bigqueryv2.ErrorProto {
	return &bigqueryv2.ErrorProto{
		Reason:    string(e.Reason),
		Location:  e.Location,
		DebugInfo: e.DebugInfo,
		Message:   e.Message,
	}
}

func (e *ServerError) Response() []byte {
	b, _ := json.Marshal(&ResponseError{
		Error: &ErrorFormat{
			Errors:  []*ServerError{e},
			Code:    e.Status,
			Message: e.Message,
		},
	})
	return b
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("%s: %s", e.Reason, e.Message)
}

type ErrorReason string

const (
	AccessDenied             ErrorReason = "accessDenied"
	BackendError             ErrorReason = "backendError"
	BillingNotEnabled        ErrorReason = "billingNotEnabled"
	BillingTierLimitExceeded ErrorReason = "billingTierLimitExceeded"
	Blocked                  ErrorReason = "blocked"
	Duplicate                ErrorReason = "duplicate"
	InternalError            ErrorReason = "internalError"
	Invalid                  ErrorReason = "invalid"
	InvalidQuery             ErrorReason = "invalidQuery"
	InvalidUser              ErrorReason = "invalidUser"
	JobBackendError          ErrorReason = "jobBackendError"
	JobInternalError         ErrorReason = "jobInternalError"
	NotFound                 ErrorReason = "notFound"
	NotImplemented           ErrorReason = "notImplemented"
	QuotaExceeded            ErrorReason = "quotaExceeded"
	RateLimitExceeded        ErrorReason = "rateLimitExceeded"
	ResourceInUse            ErrorReason = "resourceInUse"
	ResourcesExceeded        ErrorReason = "resourcesExceeded"
	ResponseTooLarge         ErrorReason = "responseTooLarge"
	Stopped                  ErrorReason = "stopped"
	TableUnavailable         ErrorReason = "tableUnavailable"
	Timeout                  ErrorReason = "timeout"
)

func errAccessDenied(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusForbidden,
		Reason:  AccessDenied,
		Message: msg,
	}
}

func errBackendError(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusInternalServerError,
		Reason:  BackendError,
		Message: msg,
	}
}

func errBillingNotEnabled(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusForbidden,
		Reason:  BillingNotEnabled,
		Message: msg,
	}
}

func errBillingTierLimitExceeded(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  BillingTierLimitExceeded,
		Message: msg,
	}
}

func errBlocked(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusForbidden,
		Reason:  Blocked,
		Message: msg,
	}
}

func errDuplicate(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusConflict,
		Reason:  Duplicate,
		Message: msg,
	}
}

// isAlreadyExistsError reports whether err denotes creating an object
// (dataset, table, view, job) that already exists, whichever layer it came
// from: the metadata catalog's typed sentinels, or the query engine's
// "... already exists" execution errors.
func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, metadata.ErrDuplicatedTable) ||
		errors.Is(err, metadata.ErrDuplicatedDataset) ||
		errors.Is(err, metadata.ErrDuplicatedJob) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "already created")
}

// jobErrorProto maps a query-job execution error onto the BigQuery error
// vocabulary. Duplicate-object failures surface with reason "duplicate" and
// an "Already Exists: ..." message — real BigQuery's shape, which clients
// (google-cloud-bigquery's exists_ok handling, dbt) branch on — any other
// typed *ServerError keeps its own reason, and deterministic analyzer/parser
// failures map onto real BigQuery's non-retryable notFound/invalidQuery
// shapes (issue #13); everything else falls back to jobInternalError.
//
// The classification applies wherever a query error surfaces — the HTTP
// error of jobs.query/jobs.insert/getQueryResults and the errorResult of a
// completed async job — so long-poll consumers classify identically.
func jobErrorProto(projectID string, err error) *ServerError {
	if isAlreadyExistsError(err) {
		msg := err.Error()
		if !strings.Contains(msg, "Already Exists") {
			msg = "Already Exists: " + msg
		}
		return errDuplicate(msg)
	}
	var serr *ServerError
	if errors.As(err, &serr) {
		return serr
	}
	if serr := analysisErrorProto(projectID, err.Error()); serr != nil {
		return serr
	}
	return errJobInternalError(err.Error())
}

var (
	// The engine reports a missing table (or a table inside a missing
	// dataset) as "Table not found: <path> [at l:c]" and a missing dataset
	// addressed directly as "Dataset not found: <path>".
	tableNotFoundPattern   = regexp.MustCompile(`Table not found: ([^\s;]+)`)
	datasetNotFoundPattern = regexp.MustCompile(`Dataset not found: ([^\s;]+)`)
)

// analysisErrorProto maps deterministic engine analysis/parse failures onto
// real BigQuery's non-retryable error shapes (issue #13). The google-cloud
// client retry predicates treat internal-error reasons as transient, so a
// missing table wrapped as jobInternalError put clients into an indefinite
// retry loop; real BigQuery answers 404 notFound ("Not found: Table
// project:dataset.table") for missing tables/datasets and 400 invalidQuery
// for every other analysis error (unknown column, syntax, type mismatch).
// It returns nil for anything that is not an analysis error — genuine
// emulator faults keep the internal-error fallback.
func analysisErrorProto(projectID, msg string) *ServerError {
	if m := tableNotFoundPattern.FindStringSubmatch(msg); m != nil {
		return errNotFound("Not found: Table " + qualifiedTableName(projectID, m[1]))
	}
	if m := datasetNotFoundPattern.FindStringSubmatch(msg); m != nil {
		return errNotFound("Not found: Dataset " + qualifiedDatasetName(projectID, m[1]))
	}
	if strings.Contains(msg, "failed to analyze:") || strings.Contains(msg, "failed to parse") {
		return errInvalidQuery(stripAnalysisPrefixes(msg))
	}
	return nil
}

// stripAnalysisPrefixes removes the Go error-wrap prefixes the engine layers
// add, leaving the analyzer's own text ("Unrecognized name: ... [at l:c]",
// "Syntax error: ..."), which is the message shape real BigQuery returns.
func stripAnalysisPrefixes(msg string) string {
	for _, prefix := range []string{
		"failed to parse statements: ",
		"failed to parse statement: ",
		"failed to analyze: ",
	} {
		for strings.HasPrefix(msg, prefix) {
			msg = msg[len(prefix):]
		}
	}
	return msg
}

// qualifiedTableName renders an engine table path in real BigQuery's
// project:dataset.table error spelling, filling in the job's project when
// the path does not carry one.
func qualifiedTableName(projectID, path string) string {
	parts := strings.Split(path, ".")
	switch len(parts) {
	case 3:
		return parts[0] + ":" + parts[1] + "." + parts[2]
	case 2:
		return projectID + ":" + parts[0] + "." + parts[1]
	}
	return projectID + ":" + path
}

// qualifiedDatasetName renders an engine dataset path in real BigQuery's
// project:dataset error spelling.
func qualifiedDatasetName(projectID, path string) string {
	parts := strings.Split(path, ".")
	if len(parts) == 2 {
		return parts[0] + ":" + parts[1]
	}
	return projectID + ":" + path
}

func errInternalError(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusInternalServerError,
		Reason:  InternalError,
		Message: msg,
	}
}

func errInvalid(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  Invalid,
		Message: msg,
	}
}

func errInvalidQuery(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  InvalidQuery,
		Message: msg,
	}
}

func errInvalidUser(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  InvalidUser,
		Message: msg,
	}
}

func errJobBackendError(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  JobBackendError,
		Message: msg,
	}
}

func errJobInternalError(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  JobInternalError,
		Message: msg,
	}
}

func errNotFound(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusNotFound,
		Reason:  NotFound,
		Message: msg,
	}
}

func errNotImplemented(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusNotImplemented,
		Reason:  NotImplemented,
		Message: msg,
	}
}

func errQuotaExceeded(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusForbidden,
		Reason:  QuotaExceeded,
		Message: msg,
	}
}

func errRateLimitExceeded(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusForbidden,
		Reason:  RateLimitExceeded,
		Message: msg,
	}
}

func errResourceInUse(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  ResourceInUse,
		Message: msg,
	}
}

func errResourcesExceeded(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  ResourcesExceeded,
		Message: msg,
	}
}

func errResponseTooLarge(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusForbidden,
		Reason:  ResponseTooLarge,
		Message: msg,
	}
}

func errStopped(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusOK,
		Reason:  Stopped,
		Message: msg,
	}
}

func errTableUnavailable(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  TableUnavailable,
		Message: msg,
	}
}

func errTimeout(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  Timeout,
		Message: msg,
	}
}
