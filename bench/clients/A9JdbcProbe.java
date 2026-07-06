// A9 JDBC compatibility probe for icegres (bench/SPEC.md A9).
//
// Single-file Java 21 program driven by the stock PostgreSQL JDBC driver
// (pgjdbc). Compile + run via bench/clients/a9_jdbc_probe.sh:
//
//   javac -cp postgresql-<ver>.jar A9JdbcProbe.java
//   java  -cp postgresql-<ver>.jar:. A9JdbcProbe
//
// Environment: ICEGRES_PROBE_HOST (default 127.0.0.1), ICEGRES_PROBE_PORT
// (default 5439), ICEGRES_PROBE_USER (default postgres), ICEGRES_PROBE_PASS
// (default postgres).
//
// Exercises, against a live icegres server:
//   1. DriverManager.getConnection (pgjdbc startup: extra_float_digits,
//      application_name, client_encoding, DateStyle, TimeZone...)
//   2. Connection.getMetaData() product name/version
//   3. DatabaseMetaData.getTables(schema=demo)
//   4. DatabaseMetaData.getColumns(schema=demo, table=trips)
//   5. Statement: SELECT count(*) with deterministic expected value
//   6. PreparedStatement with setLong/setString parameters + executeQuery
//   7. ResultSetMetaData on a live result (names + JDBC types)
//   8. executeUpdate INSERT (scratch trip_id >= 940000) + readback + cleanup
//   9. setAutoCommit(false): INSERT+rollback (invisible), INSERT+commit
//      (visible from a NEW connection), cleanup
//
// Writes use trip_id >= 940000 (own scratch range; e2e/parity use 900000+ and
// A8 uses 930000+) and clean up after themselves, so deterministic assertions
// elsewhere (trip_id 1..280) are unaffected.
//
// Output protocol (machine-read by a9_jdbc_probe.sh / parity.sh / e2e.sh):
//   "PASS: ..." / "XFAIL: ..." / "FAIL: ..." lines, then a final
//   "A9 RESULT: pass=<n> xfail=<n> fail=<n>". Exit code 0 iff fail == 0.

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class A9JdbcProbe {
    static int pass = 0, xfail = 0, fail = 0;

    static void pass(String msg) { pass++; System.out.println("PASS: " + msg); }
    static void xfail(String msg) { xfail++; System.out.println("XFAIL: " + msg); }
    static void fail(String msg, Throwable t) {
        fail++;
        System.out.println("FAIL: " + msg + (t == null ? "" : " -- " + verbatim(t)));
    }

    /** Verbatim single-line rendering of an exception chain. */
    static String verbatim(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (sb.length() > 0) sb.append(" caused by ");
            sb.append(c.getClass().getName()).append(": ")
              .append(String.valueOf(c.getMessage()).replace('\n', ' '));
        }
        return sb.toString();
    }

    static String env(String k, String dflt) {
        String v = System.getenv(k);
        return (v == null || v.isEmpty()) ? dflt : v;
    }

    static final String HOST = env("ICEGRES_PROBE_HOST", "127.0.0.1");
    static final String PORT = env("ICEGRES_PROBE_PORT", "5439");
    static final String USER = env("ICEGRES_PROBE_USER", "postgres");
    static final String PASS = env("ICEGRES_PROBE_PASS", "postgres");
    static final String URL = "jdbc:postgresql://" + HOST + ":" + PORT + "/icegres";
    static final long SCRATCH_BASE = 940_000L; // A9 scratch trip_id range

    static Connection connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASS);
        props.setProperty("ApplicationName", "a9-jdbc-probe");
        // Fail fast instead of hanging when the server is down.
        props.setProperty("connectTimeout", "10");
        props.setProperty("socketTimeout", "60");
        return DriverManager.getConnection(URL, props);
    }

    public static void main(String[] args) {
        Connection conn;
        try {
            conn = connect();
            pass("DriverManager.getConnection(" + URL + ") — pgjdbc startup handshake accepted");
        } catch (Throwable t) {
            fail("DriverManager.getConnection(" + URL + ")", t);
            System.out.println("A9 RESULT: pass=" + pass + " xfail=" + xfail + " fail=" + fail);
            System.exit(1);
            return;
        }

        try { productMetadata(conn); } catch (Throwable t) { fail("connection metadata", t); }
        try { metadataGetTables(conn); } catch (Throwable t) { fail("DatabaseMetaData.getTables(demo)", t); }
        try { metadataGetColumns(conn); } catch (Throwable t) { fail("DatabaseMetaData.getColumns(demo.trips)", t); }
        try { simpleSelect(conn); } catch (Throwable t) { fail("Statement SELECT count(*)", t); }
        try { preparedSelect(conn); } catch (Throwable t) { fail("PreparedStatement with parameters", t); }
        try { resultSetMetadata(conn); } catch (Throwable t) { fail("ResultSetMetaData on live result", t); }
        try { insertAndReadback(conn); } catch (Throwable t) { fail("executeUpdate INSERT + readback", t); }
        try { transactionCycle(conn); } catch (Throwable t) { fail("autoCommit(false) commit/rollback cycle", t); }

        try { conn.close(); pass("Connection.close() clean"); }
        catch (Throwable t) { fail("Connection.close()", t); }

        System.out.println("A9 RESULT: pass=" + pass + " xfail=" + xfail + " fail=" + fail);
        if (fail > 0) System.exit(1);
    }

    static void productMetadata(Connection conn) throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        String product = md.getDatabaseProductName();
        String version = md.getDatabaseProductVersion();
        int major = md.getDatabaseMajorVersion();
        if (product == null || !product.toLowerCase().contains("postgresql")) {
            fail("getDatabaseProductName() = " + product + " (expected PostgreSQL-compatible)", null);
        } else if (major < 10) {
            fail("getDatabaseMajorVersion() = " + major + " (pgjdbc needs a sane server_version)", null);
        } else {
            pass("DatabaseMetaData product=" + product + " version=" + version + " major=" + major);
        }
    }

    static void metadataGetTables(Connection conn) throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        List<String> tables = new ArrayList<>();
        try (ResultSet rs = md.getTables(null, "demo", "%", new String[] {"TABLE"})) {
            while (rs.next()) tables.add(rs.getString("TABLE_NAME"));
        }
        if (tables.contains("trips") && tables.contains("cities") && tables.contains("trips_big")) {
            pass("getTables(schema=demo) lists trips, cities, trips_big (" + tables.size() + " tables)");
        } else {
            fail("getTables(schema=demo) returned " + tables + " (missing trips/cities/trips_big)", null);
        }
    }

    static void metadataGetColumns(Connection conn) throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        List<String> cols = new ArrayList<>();
        String tripIdType = null, tsType = null, fareType = null;
        try (ResultSet rs = md.getColumns(null, "demo", "trips", "%")) {
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                cols.add(name);
                switch (name) {
                    case "trip_id" -> tripIdType = rs.getString("TYPE_NAME");
                    case "ts" -> tsType = rs.getString("TYPE_NAME");
                    case "fare" -> fareType = rs.getString("TYPE_NAME");
                    default -> { }
                }
            }
        }
        if (!cols.contains("trip_id") || !cols.contains("fare")) {
            fail("getColumns(demo.trips) returned " + cols, null);
            return;
        }
        boolean typesOk = tripIdType != null && tripIdType.toLowerCase().contains("int")
                && tsType != null && tsType.toLowerCase().contains("timestamp")
                && fareType != null && (fareType.toLowerCase().contains("float")
                        || fareType.toLowerCase().contains("double")
                        || fareType.toLowerCase().contains("numeric"));
        if (typesOk) {
            pass("getColumns(demo.trips): " + cols.size() + " columns; trip_id=" + tripIdType
                    + " ts=" + tsType + " fare=" + fareType);
        } else {
            fail("getColumns(demo.trips) type names off: trip_id=" + tripIdType
                    + " ts=" + tsType + " fare=" + fareType, null);
        }
    }

    static void simpleSelect(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT count(*) FROM demo.trips WHERE trip_id BETWEEN 1 AND 280")) {
            rs.next();
            long n = rs.getLong(1);
            if (n == 280) pass("Statement SELECT count(*) deterministic slice = 280");
            else fail("Statement SELECT count(*) = " + n + " (expected 280)", null);
        }
    }

    static void preparedSelect(Connection conn) throws SQLException {
        // setLong + setString parameters over the extended protocol; executed
        // several times so pgjdbc switches to a named server-side statement
        // (prepareThreshold defaults to 5).
        String sql = "SELECT count(*), coalesce(sum(t.fare), 0) FROM demo.trips t "
                + "JOIN demo.cities c ON t.city = c.city "
                + "WHERE t.trip_id <= ? AND c.city <> ?";
        long lastCount = -1;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < 7; i++) {
                ps.setLong(1, 280L);
                ps.setString(2, "no-such-city");
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    lastCount = rs.getLong(1);
                    double sum = rs.getDouble(2);
                    if (lastCount != 280 || sum <= 0) {
                        fail("PreparedStatement iteration " + i + ": count=" + lastCount
                                + " sum=" + sum + " (expected 280, >0)", null);
                        return;
                    }
                }
            }
        }
        pass("PreparedStatement setLong/setString x7 executions (crosses prepareThreshold=5) count=" + lastCount);
    }

    static void resultSetMetadata(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT trip_id, city, fare, ts FROM demo.trips WHERE trip_id = 1")) {
            ResultSetMetaData rsmd = rs.getMetaData();
            int n = rsmd.getColumnCount();
            StringBuilder desc = new StringBuilder();
            for (int i = 1; i <= n; i++) {
                if (i > 1) desc.append(", ");
                desc.append(rsmd.getColumnName(i)).append(':').append(rsmd.getColumnTypeName(i));
            }
            boolean ok = n == 4 && rsmd.getColumnName(1).equals("trip_id");
            if (ok && rs.next()) pass("ResultSetMetaData on live result: " + desc);
            else fail("ResultSetMetaData unexpected: n=" + n + " (" + desc + ")", null);
        }
    }

    static long scratchId() {
        // Unique-enough per run; stays inside the A9 scratch range.
        return SCRATCH_BASE + (System.nanoTime() % 9_000L);
    }

    static void insertAndReadback(Connection conn) throws SQLException {
        long id = scratchId();
        try (PreparedStatement ins = conn.prepareStatement(
                "INSERT INTO demo.trips (trip_id, city, distance_km, fare, ts) "
                + "VALUES (?, ?, ?, ?, TIMESTAMP '2026-01-01 00:00:00')")) {
            ins.setLong(1, id);
            ins.setString(2, "a9-city");
            ins.setDouble(3, 1.5);
            ins.setDouble(4, 9.99);
            int updated = ins.executeUpdate();
            if (updated != 1) {
                fail("executeUpdate INSERT returned " + updated + " (expected 1)", null);
                return;
            }
        }
        try (PreparedStatement sel = conn.prepareStatement(
                "SELECT fare FROM demo.trips WHERE trip_id = ?")) {
            sel.setLong(1, id);
            try (ResultSet rs = sel.executeQuery()) {
                if (rs.next() && Math.abs(rs.getDouble(1) - 9.99) < 1e-9) {
                    pass("executeUpdate INSERT (trip_id=" + id + ") + parameterized readback");
                } else {
                    fail("INSERT readback for trip_id=" + id + " missing/wrong", null);
                    return;
                }
            }
        }
        cleanupScratch(conn, id);
    }

    static void transactionCycle(Connection conn) throws SQLException {
        long idRb = scratchId() + 10_000L; // rollback probe
        long idCm = idRb + 1;              // commit probe
        conn.setAutoCommit(false);
        try {
            // Rollback leg: INSERT then rollback; row must NOT exist afterwards.
            // Readbacks run on a FRESH autocommit connection: transactional
            // SELECT is simple-protocol only in icegres (documented 0A000,
            // see the XFAIL below), and pgjdbc always speaks the extended
            // protocol — plus an outside look is the stronger assertion.
            try (Statement st = conn.createStatement()) {
                st.executeUpdate("INSERT INTO demo.trips (trip_id, city, distance_km, fare, ts) "
                        + "VALUES (" + idRb + ", 'a9-city', 1.0, 1.0, TIMESTAMP '2026-01-01 00:00:00')");
            }
            conn.rollback();
            try (Connection fresh = connect();
                 Statement st = fresh.createStatement();
                 ResultSet rs = st.executeQuery(
                     "SELECT count(*) FROM demo.trips WHERE trip_id = " + idRb)) {
                rs.next();
                if (rs.getLong(1) != 0) {
                    fail("rollback leg: row " + idRb + " survived rollback", null);
                    return;
                }
            }
            pass("setAutoCommit(false): INSERT + rollback() leaves no row (checked from a new connection)");

            // Documented limitation: SELECT inside an explicit transaction is
            // simple-protocol only; pgjdbc's extended protocol gets a clean
            // 0A000. Capture it verbatim as an XFAIL, then reopen the txn.
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM demo.trips")) {
                rs.next();
                pass("SELECT inside explicit transaction unexpectedly works now (update docs?)");
                conn.commit();
            } catch (SQLException e) {
                if ("0A000".equals(e.getSQLState())) {
                    xfail("SELECT inside explicit transaction (extended protocol) -- documented 0A000: "
                            + verbatim(e));
                    conn.rollback(); // clear the aborted transaction
                } else {
                    fail("SELECT inside explicit transaction failed with unexpected state "
                            + e.getSQLState(), e);
                    conn.rollback();
                }
            }

            // Commit leg: INSERT then commit; row must be visible from a NEW connection.
            try (Statement st = conn.createStatement()) {
                st.executeUpdate("INSERT INTO demo.trips (trip_id, city, distance_km, fare, ts) "
                        + "VALUES (" + idCm + ", 'a9-city', 2.0, 2.0, TIMESTAMP '2026-01-01 00:00:00')");
            }
            conn.commit();
            try (Connection fresh = connect();
                 Statement st = fresh.createStatement();
                 ResultSet rs = st.executeQuery(
                     "SELECT count(*) FROM demo.trips WHERE trip_id = " + idCm)) {
                rs.next();
                if (rs.getLong(1) == 1) {
                    pass("setAutoCommit(false): INSERT + commit() visible from a new connection");
                } else {
                    fail("commit leg: row " + idCm + " not visible after commit", null);
                }
            }
        } finally {
            conn.setAutoCommit(true);
            cleanupScratch(conn, idCm);
        }
    }

    static void cleanupScratch(Connection conn, long id) {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("DELETE FROM demo.trips WHERE trip_id = " + id);
        } catch (SQLException e) {
            System.out.println("note: scratch cleanup for trip_id=" + id + " failed: " + verbatim(e));
        }
    }
}
