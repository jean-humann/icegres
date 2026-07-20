"""``icegres-extract`` — one-shot columnar extract, cron-friendly.

Examples::

    # Tableau: full-table extract to .hyper, then publish it
    icegres-extract --dsn grpc+tls://icegres:50051 --username bi \
        --table demo.trips trips.hyper \
        --publish --server https://tableau.example.com --site analytics \
        --project Lakehouse --token-name refresh-bot

    # Power BI: query extract to Parquet
    icegres-extract --dsn grpc://localhost:50051 \
        --query "SELECT city, count(*) AS trips FROM demo.trips GROUP BY city" \
        trips.parquet

Secrets ride environment variables by default (``ICEGRES_PASSWORD``,
``TABLEAU_TOKEN``) so they stay out of argv and shell history; the explicit
flags exist for local development only.
"""

from __future__ import annotations

import argparse
import os
import sys

from .core import publish_hyper, run_extract


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="icegres-extract",
        description="Stream an icegres query over ADBC/Arrow Flight SQL into "
        "a Tableau .hyper or Parquet extract.",
    )
    p.add_argument("out", help="output file: *.hyper (Tableau) or *.parquet (Power BI & co)")
    p.add_argument("--dsn", required=True, help="Flight SQL endpoint, e.g. grpc+tls://host:50051")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--table", help="extract SELECT * of this table (ns.table)")
    src.add_argument("--query", help="extract this SQL statement instead")
    p.add_argument(
        "--at-snapshot",
        type=int,
        metavar="ID",
        help="pin --table to an Iceberg snapshot id (reproducible extract)",
    )
    p.add_argument("--username", help="icegres principal (with --auth-file servers)")
    p.add_argument(
        "--password",
        help="password (prefer ICEGRES_PASSWORD; this flag is for local dev)",
    )
    p.add_argument(
        "--tls-skip-verify",
        action="store_true",
        help="accept an untrusted server certificate (dev only)",
    )
    p.add_argument("--hyper-table", default="Extract", help="table name inside the .hyper file")
    p.add_argument("--parquet-compression", default="zstd", help="parquet codec (default zstd)")

    pub = p.add_argument_group("publish (.hyper to Tableau Server/Cloud)")
    pub.add_argument("--publish", action="store_true", help="publish the .hyper after writing it")
    pub.add_argument("--server", help="Tableau Server/Cloud URL")
    pub.add_argument("--site", default="", help="site content URL ('' = default site)")
    pub.add_argument("--project", help="target project name")
    pub.add_argument("--token-name", help="personal access token name")
    pub.add_argument(
        "--token",
        help="personal access token secret (prefer TABLEAU_TOKEN; flag is for local dev)",
    )
    pub.add_argument("--datasource", help="published datasource name (default: file stem)")
    return p


def main(argv=None) -> int:
    args = _parser().parse_args(argv)

    if args.password:
        print("warning: --password is visible in argv/shell history; "
              "prefer ICEGRES_PASSWORD", file=sys.stderr)
    if args.token:
        print("warning: --token is visible in argv/shell history; "
              "prefer TABLEAU_TOKEN", file=sys.stderr)
    password = args.password or os.environ.get("ICEGRES_PASSWORD")
    if args.publish:
        missing = [
            flag
            for flag, value in (
                ("--server", args.server),
                ("--project", args.project),
                ("--token-name", args.token_name),
            )
            if not value
        ]
        if missing:
            print(f"--publish needs {', '.join(missing)}", file=sys.stderr)
            return 2
        if not args.out.endswith(".hyper"):
            print("--publish applies to .hyper outputs only", file=sys.stderr)
            return 2

    try:
        report = run_extract(
            dsn=args.dsn,
            out_path=args.out,
            table=args.table,
            query=args.query,
            snapshot=args.at_snapshot,
            username=args.username,
            password=password,
            tls_skip_verify=args.tls_skip_verify,
            hyper_table=args.hyper_table,
            parquet_compression=args.parquet_compression,
        )
    except Exception as exc:  # surfaced as one clean line for cron logs
        print(f"icegres-extract: {exc}", file=sys.stderr)
        return 1
    print(report.line())

    if args.publish:
        token = args.token or os.environ.get("TABLEAU_TOKEN")
        if not token:
            print("no token: set TABLEAU_TOKEN or pass --token", file=sys.stderr)
            return 2
        try:
            ds_id = publish_hyper(
                hyper_path=args.out,
                server_url=args.server,
                site=args.site,
                project=args.project,
                token_name=args.token_name,
                token_value=token,
                datasource_name=args.datasource or os.path.splitext(os.path.basename(args.out))[0],
            )
        except Exception as exc:
            print(f"icegres-extract publish: {exc}", file=sys.stderr)
            return 1
        print(f"published datasource id={ds_id} project={args.project}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
