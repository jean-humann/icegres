// A14 — Npgsql probe against live icegres pgwire (bench/SPEC A14).
//
// Npgsql is the driver inside Power BI's native PostgreSQL connector and
// Excel's Power Query — the one major Postgres driver family with no other
// icegres probe. Its sharpest edge is the CONNECT step: on first open,
// Npgsql runs a large pg_catalog type-loading query (pg_type joined with
// pg_namespace, enum/range/composite discovery) and refuses the connection
// if that query cannot be planned/answered. Everything else in this probe
// is the Power BI Import-mode surface: full-table SELECT via the extended
// protocol with BINARY results, parameterized queries, prepared-statement
// reuse, and the GetSchema() metadata calls the navigator pane uses.
//
// Environment: ICEGRES_PROBE_PG_HOST / ICEGRES_PROBE_PG_PORT
//              (default 127.0.0.1:5439), ICEGRES_PROBE_PG_DB (icegres).
// Read-only: demo.trips is only read.
// Exit: 0 = all non-XFAIL steps passed, 2 = failures.
// Prints one line per step and "A14 RESULT: pass=N fail=N xfail=N skip=N".

using Npgsql;

var host = Environment.GetEnvironmentVariable("ICEGRES_PROBE_PG_HOST") ?? "127.0.0.1";
var port = Environment.GetEnvironmentVariable("ICEGRES_PROBE_PG_PORT") ?? "5439";
var db = Environment.GetEnvironmentVariable("ICEGRES_PROBE_PG_DB") ?? "icegres";

int pass = 0, fail = 0, xfail = 0, skip = 0;

void Pass(string name, string detail = "")
{
    pass++;
    Console.WriteLine($"PASS {name}" + (detail.Length > 0 ? $" -- {detail}" : ""));
}
void Fail(string name, Exception e)
{
    fail++;
    var msg = e.Message.Replace('\n', ' ');
    Console.WriteLine($"FAIL {name} -- {msg[..Math.Min(220, msg.Length)]}");
}
void XFail(string name, string detail)
{
    xfail++;
    Console.WriteLine($"XFAIL {name} -- {detail[..Math.Min(220, detail.Length)]}");
}

var connString =
    $"Host={host};Port={port};Database={db};Username=postgres;Password=ignored;" +
    "SSL Mode=Disable;Timeout=15;Command Timeout=60";

NpgsqlConnection? conn = null;

// -- 1. connect: runs Npgsql's pg_catalog type-loading query -----------------
try
{
    conn = new NpgsqlConnection(connString);
    conn.Open();
    Pass("connect + type loading", $"server {conn.PostgreSqlVersion}");
}
catch (Exception e)
{
    Fail("connect + type loading", e);
    Console.WriteLine($"A14 RESULT: pass={pass} fail={fail} xfail={xfail} skip={skip}");
    Environment.Exit(2);
}

// -- 2. trivial query --------------------------------------------------------
try
{
    using var cmd = new NpgsqlCommand("SELECT 1", conn);
    var v = Convert.ToInt64(cmd.ExecuteScalar()!);
    if (v != 1) throw new Exception($"SELECT 1 returned {v}");
    Pass("SELECT 1");
}
catch (Exception e) { Fail("SELECT 1", e); }

// -- 3. typed table read (extended protocol, binary results) -----------------
try
{
    using var cmd = new NpgsqlCommand(
        "SELECT trip_id, city, distance_km, fare, ts FROM demo.trips ORDER BY trip_id LIMIT 5",
        conn);
    using var rdr = cmd.ExecuteReader();
    int rows = 0;
    long firstId = -1;
    string? firstCity = null;
    while (rdr.Read())
    {
        if (rows == 0)
        {
            firstId = rdr.GetInt64(0);
            firstCity = rdr.GetString(1);
            rdr.GetDouble(2);
            rdr.GetDouble(3);
            rdr.GetDateTime(4);
        }
        rows++;
    }
    if (rows != 5) throw new Exception($"expected 5 rows, got {rows}");
    Pass("typed binary read demo.trips", $"5 rows, first=({firstId},{firstCity})");
}
catch (Exception e) { Fail("typed binary read demo.trips", e); }

// -- 4. Import-mode shape: SELECT * full scan --------------------------------
try
{
    using var cmd = new NpgsqlCommand("SELECT * FROM demo.trips", conn);
    using var rdr = cmd.ExecuteReader();
    int rows = 0;
    while (rdr.Read()) rows++;
    if (rows < 100) throw new Exception($"expected seeded table (>=100 rows), got {rows}");
    Pass("Import-mode SELECT * full scan", $"{rows} rows x {rdr.FieldCount} cols");
}
catch (Exception e) { Fail("Import-mode SELECT * full scan", e); }

// -- 5. parameterized query --------------------------------------------------
try
{
    using var cmd = new NpgsqlCommand(
        "SELECT count(*) FROM demo.trips WHERE city = @city", conn);
    cmd.Parameters.AddWithValue("city", "London");
    var n = Convert.ToInt64(cmd.ExecuteScalar()!);
    if (n <= 0) throw new Exception($"count for London = {n}");
    Pass("parameterized query (@city bind)", $"London count={n}");
}
catch (Exception e) { Fail("parameterized query (@city bind)", e); }

// -- 6. prepared statement reuse ---------------------------------------------
try
{
    using var cmd = new NpgsqlCommand(
        "SELECT count(*) FROM demo.trips WHERE city = @city", conn);
    cmd.Parameters.AddWithValue("city", "London");
    cmd.Prepare();
    var a = Convert.ToInt64(cmd.ExecuteScalar()!);
    cmd.Parameters["city"].Value = "Paris";
    var b = Convert.ToInt64(cmd.ExecuteScalar()!);
    Pass("prepared statement reuse", $"London={a} Paris={b}");
}
catch (Exception e) { Fail("prepared statement reuse", e); }

// -- 7. GetSchema: what the Power BI navigator uses --------------------------
try
{
    var tables = conn.GetSchema("Tables");
    bool found = false;
    foreach (System.Data.DataRow row in tables.Rows)
        if ((string)row["table_schema"] == "demo" && (string)row["table_name"] == "trips")
            found = true;
    if (!found) throw new Exception("demo.trips not in GetSchema(Tables)");
    Pass("GetSchema(Tables)", $"{tables.Rows.Count} tables, demo.trips present");
}
catch (Exception e) { Fail("GetSchema(Tables)", e); }

try
{
    var cols = conn.GetSchema("Columns", new string?[] { null, "demo", "trips" });
    if (cols.Rows.Count < 5)
        throw new Exception($"expected >=5 columns, got {cols.Rows.Count}");
    Pass("GetSchema(Columns demo.trips)", $"{cols.Rows.Count} columns");
}
catch (Exception e)
{
    // Known emulation gap: Npgsql's GetSchema(Columns) projects
    // information_schema.columns.udt_schema/udt_name, which the upstream
    // datafusion-postgres information_schema does not carry. The failed
    // statement also takes the connection down, so reopen for the
    // remaining steps. Documented in docs/bi-integration.md §7.
    if (e.Message.Contains("udt_schema") || e.Message.Contains("udt_name"))
        XFail("GetSchema(Columns demo.trips)",
              "information_schema.columns lacks udt_schema/udt_name (emulation gap)");
    else Fail("GetSchema(Columns demo.trips)", e);
    try { conn.Dispose(); } catch { /* already broken */ }
    conn = new NpgsqlConnection(connString);
    conn.Open();
}

// -- 8. aggregate (DirectQuery-shaped) ---------------------------------------
try
{
    using var cmd = new NpgsqlCommand(
        "SELECT city, count(*) AS trips, avg(fare) AS avg_fare FROM demo.trips " +
        "GROUP BY city ORDER BY trips DESC LIMIT 3", conn);
    using var rdr = cmd.ExecuteReader();
    int rows = 0;
    string? top = null;
    while (rdr.Read()) { if (rows == 0) top = rdr.GetString(0); rows++; }
    if (rows < 1) throw new Exception("no aggregate rows");
    Pass("DirectQuery-shaped aggregate", $"top city={top}");
}
catch (Exception e) { Fail("DirectQuery-shaped aggregate", e); }

// -- 9. XFAIL: extended-protocol SELECT inside an explicit transaction -------
// Documented icegres limit (0A000, docs/limitations.md) shared by every
// extended-protocol driver; Npgsql surfaces it if a client opens a txn.
try
{
    using var txn = conn.BeginTransaction();
    using var cmd = new NpgsqlCommand("SELECT count(*) FROM demo.trips", conn, txn);
    cmd.ExecuteScalar();
    txn.Rollback();
    // If the server ever starts supporting this, count it as a pass.
    Pass("in-transaction extended SELECT (limit lifted?)");
}
catch (Exception e)
{
    if (e.Message.Contains("0A000") || e is PostgresException pe && pe.SqlState == "0A000")
        XFail("in-transaction extended SELECT", "documented 0A000 limit (use autocommit reads)");
    else Fail("in-transaction extended SELECT (unexpected error class)", e);
}

conn.Dispose();
Console.WriteLine($"A14 RESULT: pass={pass} fail={fail} xfail={xfail} skip={skip}");
Environment.Exit(fail == 0 ? 0 : 2);
