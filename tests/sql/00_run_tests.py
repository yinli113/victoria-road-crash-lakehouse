#!/usr/bin/env python3
import json
import subprocess
import time
import pathlib

PROFILE = "accident"
WAREHOUSE_ID = pathlib.Path('config/databricks_sql_warehouse_id.txt').read_text().strip().splitlines()[-1]
TEST_DIR = pathlib.Path('tests/sql')

FAILED = []

for sql_file in sorted(TEST_DIR.glob('*.sql')):
    sql = sql_file.read_text()
    payload = json.dumps({
        "statement": sql,
        "warehouse_id": WAREHOUSE_ID,
        "catalog": "yinli_catalog"
    })
    print(f"Running test: {sql_file.name}")
    proc = subprocess.run([
        "databricks", "api", "post", "/api/2.0/sql/statements",
        "--profile", PROFILE,
        "--json", payload
    ], capture_output=True, text=True, check=True)
    resp = json.loads(proc.stdout)
    statement_id = resp["statement_id"]
    state = resp["status"]["state"]
    while state in {"PENDING", "RUNNING", "QUEUED"}:
        time.sleep(5)
        proc = subprocess.run([
            "databricks", "api", "get", f"/api/2.0/sql/statements/{statement_id}",
            "--profile", PROFILE
        ], capture_output=True, text=True, check=True)
        resp = json.loads(proc.stdout)
        state = resp["status"]["state"]
    if state != "SUCCEEDED":
        FAILED.append((sql_file.name, resp))
        print(f"Test {sql_file.name} FAILED (execution)")
        continue
    result = resp.get('result', {}).get('data_array', [])
    if result and result[0][0] == 'PASS':
        print(f"Test {sql_file.name} PASSED")
    else:
        FAILED.append((sql_file.name, result))
        print(f"Test {sql_file.name} FAILED (assertion)")

if FAILED:
    print("\nSummary: Some tests failed")
    for name, detail in FAILED:
        print(f" - {name}: {detail}")
    raise SystemExit(1)
else:
    print("\nSummary: All tests passed")
