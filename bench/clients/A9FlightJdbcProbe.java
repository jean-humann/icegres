// A9 bonus lane: Arrow Flight SQL JDBC driver probe (bench/SPEC.md A9).
//
// Connects the official Arrow Flight SQL JDBC driver
// (org.apache.arrow:flight-sql-jdbc-driver) to a Flight SQL endpoint and
// runs a SELECT plus metadata calls. Compile + run via
// bench/clients/a9_jdbc_probe.sh --flight:
//
//   javac -cp flight-sql-jdbc-driver-<ver>.jar A9FlightJdbcProbe.java
//   java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED \
//        -cp flight-sql-jdbc-driver-<ver>.jar:. A9FlightJdbcProbe
//
// Environment: ICEGRES_FLIGHT_HOST (default 127.0.0.1), ICEGRES_FLIGHT_PORT
// (default 50051), ICEGRES_FLIGHT_USER/PASS (default none = anonymous).
//
// SELECT correctness is the pass bar. Catalog metadata (getTables) is
// recorded as PASS when served and XFAIL when the endpoint does not
// implement the metadata commands (the round-9 bench/flightsql-server
// implements CommandStatementQuery only) — this lane is informational, not
// the gate.
//
// Output protocol: PASS/XFAIL/FAIL lines + "A9FLIGHT RESULT: pass=<n>
// xfail=<n> fail=<n>". Exit 0 iff fail == 0.

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class A9FlightJdbcProbe {
    static int pass = 0, xfail = 0, fail = 0;

    static void pass(String msg) { pass++; System.out.println("PASS: " + msg); }
    static void xfail(String msg) { xfail++; System.out.println("XFAIL: " + msg); }
    static void fail(String msg, Throwable t) {
        fail++;
        System.out.println("FAIL: " + msg + (t == null ? "" : " -- " + verbatim(t)));
    }

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

    public static void main(String[] args) {
        String host = env("ICEGRES_FLIGHT_HOST", "127.0.0.1");
        String port = env("ICEGRES_FLIGHT_PORT", "50051");
        String url = "jdbc:arrow-flight-sql://" + host + ":" + port + "/?useEncryption=false";
        Properties props = new Properties();
        String user = env("ICEGRES_FLIGHT_USER", "");
        String pass_ = env("ICEGRES_FLIGHT_PASS", "");
        if (!user.isEmpty()) {
            props.setProperty("user", user);
            props.setProperty("password", pass_);
        }

        Connection conn;
        try {
            conn = DriverManager.getConnection(url, props);
            pass("DriverManager.getConnection(" + url + ")");
        } catch (Throwable t) {
            fail("DriverManager.getConnection(" + url + ")", t);
            finish();
            return;
        }

        // SELECT — the pass bar for this lane.
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT count(*) FROM demo.trips WHERE trip_id BETWEEN 1 AND 280")) {
            rs.next();
            long n = rs.getLong(1);
            if (n == 280) pass("Statement SELECT count(*) deterministic slice = 280");
            else fail("SELECT count(*) = " + n + " (expected 280)", null);
        } catch (Throwable t) {
            fail("Statement SELECT count(*)", t);
        }

        // ResultSetMetaData on a live result (Arrow schema -> JDBC types).
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT trip_id, city, fare, ts FROM demo.trips WHERE trip_id = 1")) {
            ResultSetMetaData rsmd = rs.getMetaData();
            StringBuilder desc = new StringBuilder();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                if (i > 1) desc.append(", ");
                desc.append(rsmd.getColumnName(i)).append(':').append(rsmd.getColumnTypeName(i));
            }
            if (rsmd.getColumnCount() == 4 && rs.next()) {
                pass("ResultSetMetaData on live result: " + desc);
            } else {
                fail("ResultSetMetaData unexpected shape: " + desc, null);
            }
        } catch (Throwable t) {
            fail("ResultSetMetaData on live result", t);
        }

        // Catalog metadata — served by full Flight SQL endpoints; the round-9
        // query-only server (CommandStatementQuery) cannot answer these.
        try {
            DatabaseMetaData md = conn.getMetaData();
            List<String> tables = new ArrayList<>();
            try (ResultSet rs = md.getTables(null, "demo", "%", null)) {
                while (rs.next()) tables.add(rs.getString("TABLE_NAME"));
            }
            if (tables.contains("trips")) {
                pass("DatabaseMetaData.getTables(demo) lists trips (" + tables.size() + " tables)");
            } else {
                xfail("DatabaseMetaData.getTables(demo) returned " + tables
                        + " (metadata commands not served by this endpoint)");
            }
        } catch (Throwable t) {
            xfail("DatabaseMetaData.getTables unsupported by endpoint -- " + verbatim(t));
        }

        try { conn.close(); pass("Connection.close() clean"); }
        catch (Throwable t) { fail("Connection.close()", t); }

        finish();
    }

    static void finish() {
        System.out.println("A9FLIGHT RESULT: pass=" + pass + " xfail=" + xfail + " fail=" + fail);
        if (fail > 0) System.exit(1);
    }
}
