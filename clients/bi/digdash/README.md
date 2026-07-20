# DigDash Enterprise ⇄ icegres

DigDash (Java) connects to anything with a JDBC driver registered in its
server-side driver registry —
`<DD install>/apache-tomcat/webapps/ddenterpriseapi/WEB-INF/classes/resources/config/sqldriverrepository.xml`
(see DigDash's "Adding a JDBC driver" documentation). Both icegres lanes
are therefore plain driver registrations. Status for both:
**by-construction** — the driver stacks are probe-verified (pgjdbc = A9,
Flight SQL JDBC = A9F), no DigDash instance has been run against icegres
here.

## Recommended lane — Flight SQL JDBC (columnar)

DigDash data-model refreshes are extract-shaped bulk reads — exactly
where the Arrow lane wins 10–16× (`docs/bi-integration.md` §6).

1. Drop the `org.apache.arrow:flight-sql-jdbc-driver` JAR (Maven Central,
   with dependencies — use the shaded artifact) into DigDash's Tomcat
   `lib/` (or the webapp's `WEB-INF/lib/`).
2. Register it in `sqldriverrepository.xml`, following the sample entries
   already in that file: driver class
   `org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver`, URL template
   `jdbc:arrow-flight-sql://<host>:50051?useEncryption=true`.
3. Create the data source with the `--auth-file` username/password
   (read-only principal, `CanReadData`).

Time travel on this lane uses the `"trips@<snapshot_id>"` table form
(`AS OF` sugar is pgwire-only).

## Fallback lane — stock pgjdbc

Register/point the standard PostgreSQL driver at host:`5439`,
database `icegres`, SSL on. **One caveat that specifically matters for
DigDash**: its documentation recommends `DEFAULT_FETCH_SIZE` to stream
large Postgres results — on pgjdbc that means `autocommit=false` +
`setFetchSize`, the extended-protocol SELECT-in-transaction shape icegres
refuses with `0A000` (`docs/limitations.md`). On this lane either leave
`DEFAULT_FETCH_SIZE` unset (client-side buffering) or add
`preferQueryMode=simple` to the driver properties in the registry entry.
The Flight lane above streams natively and has no such constraint.
