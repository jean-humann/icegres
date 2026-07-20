using System.Diagnostics;
using Npgsql;

var conn = new NpgsqlConnection(
    "Host=127.0.0.1;Port=5459;Database=icegres;Username=postgres;Password=x;SSL Mode=Disable;Command Timeout=300");
conn.Open();
int[] sizes = { 10_000, 100_000, 1_000_000 };
foreach (var n in sizes)
{
    var sql = $"SELECT * FROM demo.wide1m LIMIT {n}";
    // warmup
    Run(sql);
    var times = new List<double>();
    for (int i = 0; i < 5; i++) { times.Add(Run(sql)); }
    times.Sort();
    Console.WriteLine($"{{\"client\": \"Npgsql (typed rows)\", \"rows\": {n}, \"ms\": {times[2]:F1}}}");
}
double Run(string sql)
{
    var sw = Stopwatch.StartNew();
    using var cmd = new NpgsqlCommand(sql, conn);
    using var rdr = cmd.ExecuteReader();
    long rows = 0;
    while (rdr.Read())
    {
        rdr.GetInt64(0); rdr.GetString(1); rdr.GetDouble(2); rdr.GetDouble(3); rdr.GetBoolean(4);
        rows++;
    }
    sw.Stop();
    return sw.Elapsed.TotalMilliseconds;
}
