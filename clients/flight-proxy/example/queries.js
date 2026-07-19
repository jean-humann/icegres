// Example named-query registry for @icegres/flight-proxy.
//
//   npx icegres-flight-proxy example/queries.js
//   # then: curl -XPOST localhost:8090/query -d '{"query":"trips_by_city","params":{"limit":5}}'
//
// Each entry maps a query NAME the browser may request to a fixed SQL
// template. The `sql` function receives ALREADY-VALIDATED literals — the
// framework rejects anything that does not match the declared param types
// before this runs, so no untrusted string ever reaches SQL.
export default {
  trips_by_city: {
    description: "Trip counts per city, most active first.",
    params: {
      limit: { type: "int", min: 1, max: 100, default: 10 },
    },
    sql: (p) =>
      `SELECT city, count(*) AS trips FROM demo.trips ` +
      `GROUP BY city ORDER BY trips DESC LIMIT ${p.limit}`,
  },

  trips_in_city: {
    description: "Recent trips for one city (city is an allowlisted value).",
    params: {
      // Text filters are enums, never free-form strings — this is what makes
      // the surface injection-proof.
      city: { type: "enum", values: ["London", "Berlin", "Lisbon", "Paris"] },
      limit: { type: "int", min: 1, max: 500, default: 50 },
    },
    sql: (p) =>
      `SELECT trip_id, city, fare FROM demo.trips ` +
      `WHERE city = ${p.city} ORDER BY trip_id DESC LIMIT ${p.limit}`,
  },
};
