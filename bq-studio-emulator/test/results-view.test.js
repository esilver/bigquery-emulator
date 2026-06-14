const test = require("node:test");
const assert = require("node:assert/strict");

const { executionDetailRows } = require("../public/app");

test("execution detail rows expose duration, total rows, row limit, and job id", () => {
  const rows = executionDetailRows({
    durationMs: 42,
    totalRows: 1500,
    rowLimit: 1000,
    jobId: "job_abc"
  });

  assert.deepEqual(rows, [
    { label: "Duration", value: "42 ms" },
    { label: "Total rows", value: "1500" },
    { label: "Row limit", value: "1000" },
    { label: "Job ID", value: "job_abc" }
  ]);
});

test("execution detail rows blank out missing fields rather than printing undefined", () => {
  const rows = executionDetailRows({ totalRows: 0 });
  const byLabel = new Map(rows.map(row => [row.label, row.value]));

  assert.equal(byLabel.get("Duration"), "");
  assert.equal(byLabel.get("Total rows"), "0");
  assert.equal(byLabel.get("Row limit"), "");
  assert.equal(byLabel.get("Job ID"), "");
});
