const test = require("node:test");
const assert = require("node:assert/strict");

const { executionDetailRows, computeCompareCellDiffs, debounce } = require("../public/app");

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

function okPane(targetId, fields, rows) {
  return { targetId, ok: true, fields, rows };
}

const NUM_FIELDS = [{ name: "id", type: "INTEGER" }, { name: "amount", type: "FLOAT" }];

test("computeCompareCellDiffs marks only the diverging cells across aligned panes", () => {
  const diffs = computeCompareCellDiffs([
    okPane("duckdb", NUM_FIELDS, [{ id: "1", amount: "10.0" }, { id: "2", amount: "20.0" }]),
    okPane("sqlite", NUM_FIELDS, [{ id: "1", amount: "10.5" }, { id: "2", amount: "20.0" }])
  ]);

  // The amount cell in row 0 differs, every other cell matches. Both panes carry
  // the same mask so each side highlights the same coordinate.
  assert.deepEqual(diffs.get("duckdb"), [[false, true], [false, false]]);
  assert.deepEqual(diffs.get("sqlite"), [[false, true], [false, false]]);
});

test("computeCompareCellDiffs returns null when the panes cannot be read row-by-row", () => {
  // Fewer than two successful panes.
  assert.equal(computeCompareCellDiffs([okPane("duckdb", NUM_FIELDS, [{ id: "1", amount: "1.0" }])]), null);
  assert.equal(
    computeCompareCellDiffs([
      okPane("duckdb", NUM_FIELDS, [{ id: "1", amount: "1.0" }]),
      { targetId: "sqlite", ok: false, error: "boom" }
    ]),
    null
  );

  // Different column lists.
  assert.equal(
    computeCompareCellDiffs([
      okPane("duckdb", NUM_FIELDS, [{ id: "1", amount: "1.0" }]),
      okPane("sqlite", [{ name: "id", type: "INTEGER" }], [{ id: "1" }])
    ]),
    null
  );

  // Different row counts.
  assert.equal(
    computeCompareCellDiffs([
      okPane("duckdb", NUM_FIELDS, [{ id: "1", amount: "1.0" }, { id: "2", amount: "2.0" }]),
      okPane("sqlite", NUM_FIELDS, [{ id: "1", amount: "1.0" }])
    ]),
    null
  );
});

test("computeCompareCellDiffs compares nested values by their rendered form", () => {
  const fields = [{ name: "id", type: "INTEGER" }, { name: "tags", type: "STRING", mode: "REPEATED" }];
  const matching = computeCompareCellDiffs([
    okPane("duckdb", fields, [{ id: "1", tags: ["a", "b"] }]),
    okPane("sqlite", fields, [{ id: "1", tags: ["a", "b"] }])
  ]);
  assert.deepEqual(matching.get("duckdb"), [[false, false]]);

  const diverging = computeCompareCellDiffs([
    okPane("duckdb", fields, [{ id: "1", tags: ["a", "b"] }]),
    okPane("sqlite", fields, [{ id: "1", tags: ["a", "c"] }])
  ]);
  assert.deepEqual(diverging.get("duckdb"), [[false, true]]);
});

test("debounce coalesces a burst into a single trailing call", () => {
  // A manual timer source makes the coalescing deterministic without real time.
  let scheduled = null;
  let nextId = 1;
  const timers = {
    setTimeout(fn) {
      scheduled = fn;
      return nextId++;
    },
    clearTimeout() {
      scheduled = null;
    }
  };

  let calls = 0;
  let lastArg;
  const debounced = debounce(arg => {
    calls += 1;
    lastArg = arg;
  }, 180, timers);

  debounced("a");
  debounced("b");
  debounced("c");
  assert.equal(calls, 0, "no call fires until the timer elapses");

  scheduled();
  assert.equal(calls, 1, "the burst collapses to one trailing call");
  assert.equal(lastArg, "c", "the trailing call keeps the latest arguments");

  // cancel drops a pending call so a teardown does not fire stale work.
  debounced("d");
  debounced.cancel();
  assert.equal(scheduled, null);
});
