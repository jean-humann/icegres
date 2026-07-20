// Icegres — Power Query custom connector (scaffold).
//
// Same principle as Power BI's in-box Dremio connector, at the layer
// custom connectors are allowed to use today: Power Query has NO public
// Adbc.DataSource extensibility yet (the embedded ADBC drivers are wired
// into the engine for in-box connectors only, behind Implementation="2.0"),
// so a custom connector reaches Arrow Flight SQL the way Dremio's
// connector did pre-ADBC — by wrapping Odbc.DataSource over a Flight SQL
// ODBC driver. Install Dremio's free "Arrow Flight SQL ODBC Driver" on
// every machine (and gateway) that loads this connector, point it at
// `icegres flight-serve`, and the M engine folds queries into SQL exactly
// as it does for the in-box driver-based connectors.
//
// Structure follows microsoft/DataConnectors samples/ODBC/SqlODBC. Build
// with the Power Query SDK (VS Code) into Icegres.mez; see README.md in
// this directory for the full build/sideload/certification path and the
// honest verification status (authored to spec; needs a Windows-side
// build + test — no M toolchain exists for this repo's Linux CI).

[Version = "0.1.0"]
section Icegres;

// The ODBC driver name as registered by Dremio's Arrow Flight SQL ODBC
// driver installer. If your installation registers a different name
// (check odbcinst / ODBC Data Sources), change it here.
Config_DriverName = "Arrow Flight SQL ODBC Driver";

// DirectQuery rides the driver's reported capabilities plus the
// SqlCapabilities/SQLGetInfo overrides below (DataFusion dialect).
Config_EnableDirectQuery = true;

[DataSource.Kind = "Icegres", Publish = "Icegres.Publish"]
shared Icegres.Database = Value.ReplaceType(IcegresImpl, IcegresType);

IcegresType = type function (
    host as (type text meta [
        Documentation.FieldCaption = "Server",
        Documentation.FieldDescription = "icegres Flight SQL host",
        Documentation.SampleValues = { "icegres.example.internal" }
    ]),
    optional port as (type number meta [
        Documentation.FieldCaption = "Port",
        Documentation.FieldDescription = "flight-serve port (default 50051)",
        Documentation.SampleValues = { 50051 }
    ]),
    optional options as (type [
        optional UseEncryption = logical,
        optional DisableCertificateVerification = logical
    ] meta [
        Documentation.FieldCaption = "Advanced options"
    ]))
    as table meta [
        Documentation.Name = "icegres",
        Documentation.LongDescription =
            "Arrow Flight SQL connection to an icegres lakehouse "
            & "(Iceberg tables over DataFusion), via the Arrow Flight SQL "
            & "ODBC driver."
    ];

IcegresImpl = (host as text, optional port as number, optional options as record) as table =>
    let
        Port = if port = null then 50051 else port,
        Options = if options = null then [] else options,
        // TLS on by default: flight-serve should run --tls-cert/--tls-key
        // in production (docs/bi-integration.md, clients/bi/README.md).
        UseEncryption = if Options[UseEncryption]? = null then true else Options[UseEncryption],
        DisableCertVerify =
            if Options[DisableCertificateVerification]? = null then false
            else Options[DisableCertificateVerification],

        ConnectionString = [
            Driver = Config_DriverName,
            HOST = host,
            PORT = Number.ToText(Port),
            UseEncryption = if UseEncryption then "true" else "false",
            DisableCertificateVerification = if DisableCertVerify then "true" else "false"
        ],

        // Credentials never enter the data source path: UID/PWD ride the
        // credential connection string (icegres --auth-file basic auth; a
        // pre-minted token can be supplied as the password).
        Credential = Extension.CurrentCredential(),
        CredentialConnectionString =
            if Credential[AuthenticationKind]? = "UsernamePassword" then
                [ UID = Credential[Username], PWD = Credential[Password] ]
            else
                error Error.Record(
                    "DataSource.Error", "Unsupported authentication kind",
                    Credential[AuthenticationKind]?),

        // DataFusion dialect tuning (see MicrosoftDocs/powerquery-docs
        // odbc.md for every knob): LIMIT-style pagination, no TOP, ANSI
        // literals. Extend these as Windows-side DirectQuery testing maps
        // the folding surface.
        SqlCapabilities = [
            SupportsTop = false,
            LimitClauseKind = LimitClauseKind.Limit,
            FractionalSecondsScale = 6,
            SupportsNumericLiterals = true,
            SupportsStringLiterals = true,
            SupportsOdbcDateLiterals = true,
            SupportsOdbcTimeLiterals = true,
            SupportsOdbcTimestampLiterals = true
        ],

        SQLGetInfo = [
            // Quoted identifiers, double-quote style (DataFusion/Postgres).
            SQL_IDENTIFIER_QUOTE_CHAR = """"
        ],

        OdbcDatasource = Odbc.DataSource(ConnectionString, [
            HierarchicalNavigation = true,
            ClientConnectionPooling = true,
            TolerateConcatOverflow = true,
            SoftNumbers = true,
            CredentialConnectionString = CredentialConnectionString,
            SqlCapabilities = SqlCapabilities,
            SQLGetInfo = SQLGetInfo
        ])
    in
        OdbcDatasource;

Icegres = [
    TestConnection = (dataSourcePath) =>
        let
            json = Json.Document(dataSourcePath),
            host = json[host],
            port = json[port]?
        in
            { "Icegres.Database", host, port },
    Authentication = [
        UsernamePassword = []
    ],
    Label = "icegres"
];

Icegres.Publish = [
    Beta = true,
    Category = "Database",
    ButtonText = { "icegres", "Connect to an icegres lakehouse over Arrow Flight SQL" },
    LearnMoreUrl = "https://github.com/jean-humann/icegres",
    SupportsDirectQuery = Config_EnableDirectQuery
    // TODO before certification: add SourceImage/SourceTypeImage records
    // (16/20/24/32px PNGs embedded via Extension.Contents) — without them
    // Power BI Desktop shows a generic connector icon.
];
