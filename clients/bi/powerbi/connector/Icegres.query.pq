// Power Query SDK evaluation file: run this from the SDK's "Evaluate
// current file" against a live flight-serve to smoke the connector.
let
    result = Icegres.Database("127.0.0.1:50051", [ UseEncryption = false ])
in
    result
