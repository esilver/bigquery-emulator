const test = require("node:test");
const assert = require("node:assert/strict");

const {
  visibleDatasets,
  isGeneratedArtifactDataset,
  normalizeQueryRows,
  normalizeQueryValue,
  splitCsvLine,
  splitCsvRecords,
  prepareCsvUpload,
  inferSchema,
  safeIdentifier,
  safeResourceIdentifier,
  tableRef
} = require("../server");

test("generated artifact datasets are hidden from the explorer", () => {
  assert.equal(isGeneratedArtifactDataset("bqjob_r123"), true);
  assert.equal(isGeneratedArtifactDataset("04c352ea-ccf5-4326-8ca3-97a6759858e1"), true);
  assert.equal(isGeneratedArtifactDataset("dataset1"), false);

  const target = { projectId: "test" };
  const datasets = visibleDatasets([
    { datasetReference: { projectId: "test", datasetId: "dataset1" }, location: "US" },
    { datasetReference: { projectId: "test", datasetId: "bqjob_r123" }, location: "US" },
    { datasetReference: { projectId: "test", datasetId: "04c352ea-ccf5-4326-8ca3-97a6759858e1" }, location: "US" }
  ], target);

  assert.deepEqual(datasets.map(dataset => dataset.id), ["dataset1"]);
});

test("query row normalization decodes repeated records", () => {
  const result = normalizeQueryRows({
    jobReference: { jobId: "job_1" },
    totalRows: "1",
    schema: {
      fields: [
        { name: "publisher_id", type: "INTEGER" },
        {
          name: "top_campaigns",
          type: "RECORD",
          mode: "REPEATED",
          fields: [
            { name: "campaign_id", type: "INTEGER" },
            { name: "amount", type: "FLOAT" }
          ]
        }
      ]
    },
    rows: [
      {
        f: [
          { v: "2" },
          {
            v: [
              { v: { f: [{ v: "10" }, { v: "42.5" }] } },
              { v: { f: [{ v: "11" }, { v: "12.25" }] } }
            ]
          }
        ]
      }
    ]
  });

  assert.equal(result.jobId, "job_1");
  assert.equal(result.totalRows, 1);
  assert.deepEqual(result.rows, [
    {
      publisher_id: "2",
      top_campaigns: [
        { campaign_id: "10", amount: "42.5" },
        { campaign_id: "11", amount: "12.25" }
      ]
    }
  ]);
});

test("CSV splitting handles quoted commas and multiline quoted fields", () => {
  assert.deepEqual(splitCsvLine('a,"b,c","d""e"'), ["a", "b,c", 'd"e']);
  assert.deepEqual(
    splitCsvRecords(Buffer.from('id,note\n1,"hello\nworld"\n2,done\n', "utf8")),
    ["id,note", '1,"hello\nworld"', "2,done"]
  );
});

test("report-style CSV inference skips title rows and trims footer rows", () => {
  const csv = Buffer.from([
    "Daily Activity Report",
    "date,clicks,cost,active",
    "2026-06-01,12,1.25,true",
    "2026-06-02,10,2.50,false",
    "Grand Total:,22,3.75,"
  ].join("\n"), "utf8");

  const inferred = inferSchema(csv, 1);
  assert.equal(inferred.skipLeadingRows, 2);
  assert.deepEqual(inferred.schema.fields, [
    { name: "date", type: "DATE" },
    { name: "clicks", type: "INTEGER" },
    { name: "cost", type: "FLOAT" },
    { name: "active", type: "BOOLEAN" }
  ]);

  const upload = prepareCsvUpload(csv, inferred.skipLeadingRows);
  assert.equal(upload.skipLeadingRows, 1);
  assert.equal(upload.trimmedFooterRows, 1);
  assert.equal(upload.data.toString("utf8"), "date,clicks,cost,active\n2026-06-01,12,1.25,true\n2026-06-02,10,2.50,false\n");
});

test("resource identifiers preserve BigQuery-safe dashes only where allowed", () => {
  assert.equal(safeIdentifier("dataset1", "dataset"), "dataset1");
  assert.throws(() => safeIdentifier("bad-dataset", "dataset"), /Invalid dataset/);
  assert.equal(safeResourceIdentifier("04c352ea-ccf5", "dataset"), "04c352ea-ccf5");

  assert.equal(
    tableRef({ projectId: "test" }, "dataset1", "table_a"),
    "`test.dataset1.table_a`"
  );
});

test("normalizeQueryValue decodes scalars, repeated scalars, and nested structs", () => {
  assert.equal(normalizeQueryValue({ name: "n", type: "INTEGER" }, null), null);
  assert.equal(normalizeQueryValue({ name: "n", type: "INTEGER" }, undefined), null);
  assert.equal(normalizeQueryValue({ name: "n", type: "STRING" }, "hello"), "hello");

  assert.deepEqual(
    normalizeQueryValue({ name: "tags", type: "STRING", mode: "REPEATED" }, { v: [{ v: "a" }, { v: "b" }] }),
    ["a", "b"]
  );

  assert.deepEqual(
    normalizeQueryValue({ name: "tags", type: "STRING", mode: "REPEATED" }, { v: [] }),
    []
  );

  const nestedStruct = normalizeQueryValue(
    {
      name: "outer",
      type: "STRUCT",
      fields: [
        { name: "id", type: "INTEGER" },
        { name: "inner", type: "STRUCT", fields: [{ name: "label", type: "STRING" }] }
      ]
    },
    { v: { f: [{ v: "7" }, { v: { f: [{ v: "deep" }] } }] } }
  );
  assert.deepEqual(nestedStruct, { id: "7", inner: { label: "deep" } });
});
