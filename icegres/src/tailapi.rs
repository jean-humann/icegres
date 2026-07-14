//! The open tail read API (roadmap-v2 P1): TailSnapshot / TailSubscribe,
//! served over Arrow Flight by the process that HOLDS the overlay state —
//! the buffering `icegres serve` compute (`--tail-api-port`). `icegres
//! flight-serve` recognizes the same tickets but answers
//! FAILED_PRECONDITION (it never buffers). Full wire spec:
//! docs/open-tail-protocol.md.
//!
//! # Why this shape
//!
//! The buffering compute already maintains the exact union-read overlay
//! (pending appends + keyed ops + retained flushed generations, each with
//! its durable-tail sequence, plus the `icegres.tail-seq.<tail-id>`
//! property protocol that stamps the covered watermark into every flush
//! commit atomically). Serving THAT state makes the protocol backend-
//! agnostic (dir/pg/quorum tails are identical on the wire) and gives any
//! consumer the exactly-once rule for free:
//!
//! > For scan metadata `M`, let `w` = the value of the served
//! > watermark-property key in `M`'s table properties (absent = -∞).
//! > Include a served row iff `row.seq > w`; suppress committed rows whose
//! > key has a served keyed op with `op.seq > w`; among included rows the
//! > newest seq per key wins.
//!
//! Because the watermark is stamped in the SAME atomic commit as the
//! flushed rows, `w >= seq` ⟺ the committed data already contains the op —
//! no snapshot ids needed, correct for stale AND fresh metadata.
//!
//! # Wire format (version 1)
//!
//! Requests are Flight `DoGet` tickets: a protobuf `Any` with
//! `type_url` ∈ {`icegres.tail.v1.Tables`, `icegres.tail.v1.Snapshot`,
//! `icegres.tail.v1.Subscribe`} and a JSON value (`{}`, `{"table":
//! "ns.table"}`, `{"table": "ns.table", "from_seq": N}`).
//!
//! Responses are ONE Arrow stream. For Snapshot/Subscribe the schema is the
//! table's canonical schema with every field made nullable, plus two
//! trailing columns `__icegres_seq` (UInt64) and `__icegres_op` (Utf8:
//! `append` | `upsert` | `delete` | `watermark`); schema-level metadata
//! carries the header (`icegres.tail.version/table/watermark-property/
//! high/pk-cols`). Delete rows populate only the PK columns; watermark
//! heartbeats are one all-null row whose seq is the covered watermark.
//! Self-describing and decodable by ANY Arrow Flight client — no protobuf
//! schema needed beyond hand-rolling the tiny `Any` envelope.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use arrow::array::{new_null_array, ArrayRef, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::sql::Any;
use futures::{stream, SinkExt, Stream, TryStreamExt};
use iceberg::TableIdent;
use tonic::Status;

use crate::buffer::{TailEventKind, TailItem, WriteBuffer};

/// Protocol version served (and demanded) by this build.
pub const TAIL_PROTOCOL_VERSION: &str = "1";

/// Ticket type URLs (protobuf `Any.type_url`).
pub const TICKET_TABLES: &str = "icegres.tail.v1.Tables";
pub const TICKET_SNAPSHOT: &str = "icegres.tail.v1.Snapshot";
pub const TICKET_SUBSCRIBE: &str = "icegres.tail.v1.Subscribe";

/// Schema-metadata header keys.
pub const META_VERSION: &str = "icegres.tail.version";
pub const META_TABLE: &str = "icegres.tail.table";
pub const META_WATERMARK_PROPERTY: &str = "icegres.tail.watermark-property";
pub const META_HIGH: &str = "icegres.tail.high";
pub const META_PK_COLS: &str = "icegres.tail.pk-cols";

/// Trailing wire columns.
pub const SEQ_COL: &str = "__icegres_seq";
pub const OP_COL: &str = "__icegres_op";

/// Interval between liveness heartbeats on a TailSubscribe stream.
const HEARTBEAT_EVERY: Duration = Duration::from_secs(1);

/// Backpressure bound between the subscribe fan-out task and gRPC.
const SUBSCRIBE_CHANNEL: usize = 64;

type FlightStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send>>;

/// A decoded tail-API ticket.
#[derive(Debug, PartialEq)]
pub enum TailTicket {
    /// List the tables with a tail window on this server.
    Tables,
    /// One consistent snapshot of a table's window.
    Snapshot { table: TableIdent },
    /// Incremental events with `seq > from_seq` (+ watermark heartbeats).
    Subscribe { table: TableIdent, from_seq: u64 },
}

impl TailTicket {
    /// Decode from the `Any` a Flight SQL server's do_get dispatcher hands
    /// to `do_get_fallback`. `Ok(None)` = not a tail ticket (some other
    /// unknown command).
    pub fn from_any(message: &Any) -> Result<Option<Self>> {
        let parse_table = |value: &[u8]| -> Result<(TableIdent, Option<u64>)> {
            let v: serde_json::Value = serde_json::from_slice(value)
                .map_err(|e| anyhow!("tail ticket value is not JSON: {e}"))?;
            let table = v
                .get("table")
                .and_then(|t| t.as_str())
                .ok_or_else(|| anyhow!("tail ticket lacks a \"table\" string"))?;
            let parts: Vec<&str> = table.split('.').filter(|p| !p.is_empty()).collect();
            anyhow::ensure!(
                parts.len() >= 2,
                "tail ticket table {table:?} is not of the form namespace.table"
            );
            let ident = TableIdent::from_strs(parts)?;
            let from_seq = v.get("from_seq").and_then(|s| s.as_u64());
            Ok((ident, from_seq))
        };
        match message.type_url.as_str() {
            TICKET_TABLES => Ok(Some(TailTicket::Tables)),
            TICKET_SNAPSHOT => {
                let (table, _) = parse_table(&message.value)?;
                Ok(Some(TailTicket::Snapshot { table }))
            }
            TICKET_SUBSCRIBE => {
                let (table, from_seq) = parse_table(&message.value)?;
                Ok(Some(TailTicket::Subscribe {
                    table,
                    from_seq: from_seq.unwrap_or(0),
                }))
            }
            _ => Ok(None),
        }
    }

    /// Encode to raw Flight ticket bytes (the client side; also what the
    /// docs describe and the python clients hand-roll).
    pub fn encode(&self) -> Vec<u8> {
        use prost::Message as _;
        let (type_url, value) = match self {
            TailTicket::Tables => (TICKET_TABLES, "{}".to_string()),
            TailTicket::Snapshot { table } => (
                TICKET_SNAPSHOT,
                serde_json::json!({ "table": table.to_string() }).to_string(),
            ),
            TailTicket::Subscribe { table, from_seq } => (
                TICKET_SUBSCRIBE,
                serde_json::json!({ "table": table.to_string(), "from_seq": from_seq }).to_string(),
            ),
        };
        Any {
            type_url: type_url.to_string(),
            value: value.into_bytes().into(),
        }
        .encode_to_vec()
    }
}

/// The wire op-column value for an event kind.
pub fn op_str(kind: TailEventKind) -> &'static str {
    match kind {
        TailEventKind::Append => "append",
        TailEventKind::Upsert => "upsert",
        TailEventKind::Delete => "delete",
        TailEventKind::Watermark => "watermark",
    }
}

/// Build the wire schema: the canonical fields with nullability relaxed
/// (delete rows carry nulls outside the PK; heartbeats are all-null), plus
/// the trailing seq/op columns, plus the header as schema metadata.
pub fn wire_schema(
    canonical: &SchemaRef,
    table: &TableIdent,
    watermark_property: &str,
    high: u64,
    pk_cols: &[String],
) -> SchemaRef {
    let mut fields: Vec<Field> = canonical
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_nullable(true))
        .collect();
    fields.push(Field::new(SEQ_COL, DataType::UInt64, false));
    fields.push(Field::new(OP_COL, DataType::Utf8, false));
    let metadata: HashMap<String, String> = HashMap::from([
        (META_VERSION.to_string(), TAIL_PROTOCOL_VERSION.to_string()),
        (META_TABLE.to_string(), table.to_string()),
        (
            META_WATERMARK_PROPERTY.to_string(),
            watermark_property.to_string(),
        ),
        (META_HIGH.to_string(), high.to_string()),
        (META_PK_COLS.to_string(), pk_cols.join(",")),
    ]);
    Arc::new(Schema::new_with_metadata(fields, metadata))
}

/// Number of data (non-seq/op) columns in a wire schema.
fn data_fields(wire: &SchemaRef) -> usize {
    wire.fields().len().saturating_sub(2)
}

/// Build one wire batch: `batch`'s columns matched BY NAME onto the wire
/// schema's data fields (absent = null — a delete's key_row populates only
/// the PK columns), plus the constant seq/op columns. `batch = None` emits
/// a one-row all-null heartbeat.
pub fn wire_batch(
    wire: &SchemaRef,
    seq: u64,
    kind: TailEventKind,
    batch: Option<&RecordBatch>,
) -> Result<RecordBatch> {
    let rows = batch.map_or(1, |b| b.num_rows());
    let n_data = data_fields(wire);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(n_data + 2);
    for field in wire.fields().iter().take(n_data) {
        let col = batch.and_then(|b| {
            b.schema()
                .index_of(field.name())
                .ok()
                .map(|i| b.column(i).clone())
        });
        match col {
            Some(col) => {
                anyhow::ensure!(
                    col.data_type() == field.data_type(),
                    "tail wire column {:?} type mismatch: {} vs {}",
                    field.name(),
                    col.data_type(),
                    field.data_type()
                );
                columns.push(col);
            }
            None => columns.push(new_null_array(field.data_type(), rows)),
        }
    }
    columns.push(Arc::new(UInt64Array::from(vec![seq; rows])) as ArrayRef);
    columns.push(Arc::new(StringArray::from(vec![op_str(kind); rows])) as ArrayRef);
    RecordBatch::try_new(wire.clone(), columns)
        .map_err(|e| anyhow!("cannot assemble a tail wire batch: {e}"))
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(format!("{e:#}"))
}

/// The documented status for a subscriber that lagged past the broadcast
/// capacity: gRPC DATA_LOSS with the re-snapshot recovery hint (F14 — the
/// wire spec's `DATA_LOSS (subscriber lagged; re-snapshot)`).
fn lagged_status(lagged_by: u64) -> Status {
    Status::data_loss(format!(
        "tail subscriber lagged by {lagged_by} events; re-run TailSnapshot and re-subscribe"
    ))
}

/// Wrap record batches into the encoded Flight stream (schema first).
fn encode_batches(wire: SchemaRef, batches: Vec<Result<RecordBatch, FlightError>>) -> FlightStream {
    let flight = FlightDataEncoderBuilder::new()
        .with_schema(wire)
        .build(stream::iter(batches))
        .map_err(Status::from);
    Box::pin(flight)
}

/// `Tables` response: one batch of `(namespace, table)` rows for every
/// table with a tail window on this server (discovery for peer mirrors)
/// that passes `allowed` — the caller's per-table authorization filter, so
/// discovery can never leak a table name past a ReBAC denial.
pub(crate) fn tables_stream(
    buffer: &Arc<WriteBuffer>,
    allowed: impl Fn(&TableIdent) -> bool,
) -> Result<FlightStream, Status> {
    let watermark_property = require_tail(buffer)?;
    let mut idents = buffer.buffered_tables();
    idents.retain(|ident| allowed(ident));
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("namespace", DataType::Utf8, false),
            Field::new("table", DataType::Utf8, false),
        ],
        HashMap::from([
            (META_VERSION.to_string(), TAIL_PROTOCOL_VERSION.to_string()),
            (
                META_WATERMARK_PROPERTY.to_string(),
                watermark_property.clone(),
            ),
        ]),
    ));
    let namespaces: Vec<String> = idents
        .iter()
        .map(|i| i.namespace().clone().inner().join("."))
        .collect();
    let names: Vec<String> = idents.iter().map(|i| i.name().to_string()).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(namespaces)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
        ],
    )
    .map_err(internal)?;
    Ok(encode_batches(schema, vec![Ok(batch)]))
}

/// The buffer's watermark-property key, or the precise FAILED_PRECONDITION
/// story when this process cannot serve the tail API.
fn require_tail(buffer: &Arc<WriteBuffer>) -> Result<String, Status> {
    buffer.tail_watermark_property().ok_or_else(|| {
        Status::failed_precondition(
            "the open tail API requires a durable tail (--tail-dir / --tail-url / \
             --tail-quorum) on the buffering server",
        )
    })
}

/// `TailSnapshot { table }`: one consistent view of the table's window.
pub(crate) fn snapshot_stream(
    buffer: &Arc<WriteBuffer>,
    table: &TableIdent,
) -> Result<FlightStream, Status> {
    let watermark_property = require_tail(buffer)?;
    let snap = buffer.tail_snapshot(table).map_err(internal)?;
    let Some(snap) = snap else {
        return Err(Status::not_found(format!(
            "table {table} has no tail window on this server (nothing buffered yet)"
        )));
    };
    let wire = wire_schema(
        &snap.schema,
        table,
        &watermark_property,
        snap.high,
        &snap.pk_cols,
    );
    let batches: Vec<Result<RecordBatch, FlightError>> = snap
        .items
        .iter()
        .map(|item: &TailItem| {
            wire_batch(&wire, item.seq, item.kind, Some(&item.batch))
                .map_err(|e| FlightError::ExternalError(e.into()))
        })
        .collect();
    Ok(encode_batches(wire, batches))
}

/// `TailSubscribe { table, from_seq }`: backfill the window's durable ops
/// with `from_seq < seq <= high`, then stream durable events with
/// `seq > high` as they ack, plus watermark events and 1 Hz liveness
/// heartbeats. `high` is the snapshot's durable head at subscribe time; the
/// event receiver is registered BEFORE the snapshot is taken, so the cursor
/// is exact (every op is delivered at most once by seq — see the module
/// docs of buffer.rs for the staging-order invariant). A lagged receiver
/// ends the stream with DATA_LOSS; the consumer re-runs TailSnapshot.
/// Concurrent subscribe streams are capped at
/// [`crate::buffer::TAIL_MAX_SUBSCRIBERS`] per server (F15) — beyond it
/// the request is answered RESOURCE_EXHAUSTED.
pub(crate) fn subscribe_stream(
    buffer: &Arc<WriteBuffer>,
    table: &TableIdent,
    from_seq: u64,
) -> Result<FlightStream, Status> {
    let watermark_property = require_tail(buffer)?;
    let permit = buffer.try_subscribe_permit().ok_or_else(|| {
        Status::resource_exhausted(format!(
            "too many concurrent TailSubscribe streams (cap {}); close one and retry",
            crate::buffer::TAIL_MAX_SUBSCRIBERS
        ))
    })?;
    // Register FIRST: any op staged after the snapshot below reaches the
    // receiver; anything staged before is in the snapshot (seq <= high).
    let mut events = buffer.subscribe_events();
    let snap = buffer
        .tail_snapshot(table)
        .map_err(internal)?
        .ok_or_else(|| {
            Status::not_found(format!(
                "table {table} has no tail window on this server (nothing buffered yet)"
            ))
        })?;
    let high = snap.high;
    let wire = wire_schema(
        &snap.schema,
        table,
        &watermark_property,
        high,
        &snap.pk_cols,
    );
    let (mut tx, rx) =
        futures::channel::mpsc::channel::<Result<RecordBatch, FlightError>>(SUBSCRIBE_CHANNEL);
    let task_wire = wire.clone();
    let ident = table.clone();
    let backfill: Vec<TailItem> = snap
        .items
        .into_iter()
        .filter(|i| i.seq > from_seq)
        .collect();
    tokio::spawn(async move {
        // The subscriber slot (F15) lives exactly as long as this fan-out
        // task: dropped on any return below, including the ~1 s
        // heartbeat-send reap of an abandoned consumer.
        let _permit = permit;
        let ext = |e: anyhow::Error| FlightError::ExternalError(e.into());
        for item in backfill {
            let batch = wire_batch(&task_wire, item.seq, item.kind, Some(&item.batch));
            if tx.send(batch.map_err(ext)).await.is_err() {
                return; // consumer went away
            }
        }
        let mut last_watermark = 0u64;
        let mut heartbeat = tokio::time::interval(HEARTBEAT_EVERY);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.reset(); // the backfill was activity enough
        loop {
            tokio::select! {
                ev = events.recv() => match ev {
                    Ok(ev) => {
                        if ev.ident != ident {
                            continue;
                        }
                        match ev.kind {
                            TailEventKind::Watermark => {
                                last_watermark = last_watermark.max(ev.seq);
                                let hb = wire_batch(&task_wire, last_watermark,
                                                    TailEventKind::Watermark, None);
                                if tx.send(hb.map_err(ext)).await.is_err() {
                                    return;
                                }
                            }
                            _ if ev.seq > high => {
                                for batch in &ev.batches {
                                    let wb = wire_batch(&task_wire, ev.seq, ev.kind,
                                                        Some(batch));
                                    if tx.send(wb.map_err(ext)).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            // seq <= high: already delivered by the backfill
                            // (published late, staged before the snapshot).
                            _ => {}
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        // F14: the wire contract (docs/open-tail-protocol.md)
                        // is gRPC DATA_LOSS for a lagged subscriber — send a
                        // real tonic Status so the encoder forwards the code
                        // verbatim (an ExternalError would surface INTERNAL).
                        let _ = tx.send(Err(FlightError::Tonic(Box::new(lagged_status(n))))).await;
                        return;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                },
                _ = heartbeat.tick() => {
                    let hb = wire_batch(&task_wire, last_watermark,
                                        TailEventKind::Watermark, None);
                    if tx.send(hb.map_err(ext)).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
    let flight = FlightDataEncoderBuilder::new()
        .with_schema(wire)
        .build(rx)
        .map_err(Status::from);
    Ok(Box::pin(flight))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    fn ident() -> TableIdent {
        TableIdent::from_strs(["demo", "trips"]).unwrap()
    }

    #[test]
    fn ticket_roundtrip_all_variants() {
        use prost::Message as _;
        for ticket in [
            TailTicket::Tables,
            TailTicket::Snapshot { table: ident() },
            TailTicket::Subscribe {
                table: ident(),
                from_seq: 42,
            },
        ] {
            let bytes = ticket.encode();
            let any = Any::decode(bytes.as_slice()).unwrap();
            let decoded = TailTicket::from_any(&any).unwrap().unwrap();
            assert_eq!(decoded, ticket);
        }
    }

    #[test]
    fn foreign_tickets_are_not_ours() {
        let any = Any {
            type_url: "type.googleapis.com/something.Else".into(),
            value: Vec::new().into(),
        };
        assert_eq!(TailTicket::from_any(&any).unwrap(), None);
    }

    #[test]
    fn malformed_ticket_value_errors() {
        let any = Any {
            type_url: TICKET_SNAPSHOT.into(),
            value: b"not json".to_vec().into(),
        };
        assert!(TailTicket::from_any(&any).is_err());
        let any = Any {
            type_url: TICKET_SNAPSHOT.into(),
            value: br#"{"table": "no_namespace"}"#.to_vec().into(),
        };
        assert!(TailTicket::from_any(&any).is_err());
    }

    fn canonical() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn wire_schema_carries_header_and_relaxed_nullability() {
        let wire = wire_schema(
            &canonical(),
            &ident(),
            "icegres.tail-seq.X",
            7,
            &["id".to_string()],
        );
        assert_eq!(wire.metadata()[META_VERSION], "1");
        assert_eq!(wire.metadata()[META_TABLE], "demo.trips");
        assert_eq!(
            wire.metadata()[META_WATERMARK_PROPERTY],
            "icegres.tail-seq.X"
        );
        assert_eq!(wire.metadata()[META_HIGH], "7");
        assert_eq!(wire.metadata()[META_PK_COLS], "id");
        assert!(wire.field(0).is_nullable(), "data fields become nullable");
        assert_eq!(wire.field(2).name(), SEQ_COL);
        assert_eq!(wire.field(3).name(), OP_COL);
    }

    #[test]
    fn wire_batch_upsert_delete_and_heartbeat_shapes() {
        let wire = wire_schema(&canonical(), &ident(), "k", 0, &["id".to_string()]);
        // Upsert: full canonical row.
        let row = RecordBatch::try_new(
            canonical(),
            vec![
                Arc::new(Int64Array::from(vec![5i64])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef,
            ],
        )
        .unwrap();
        let b = wire_batch(&wire, 9, TailEventKind::Upsert, Some(&row)).unwrap();
        assert_eq!(b.num_rows(), 1);
        let seqs = b.column(2).as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(seqs.value(0), 9);
        let ops = b.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(ops.value(0), "upsert");
        // Delete: key-only batch -> non-PK columns are null.
        let key = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![5i64])) as ArrayRef],
        )
        .unwrap();
        let b = wire_batch(&wire, 10, TailEventKind::Delete, Some(&key)).unwrap();
        assert!(b.column(1).is_null(0), "non-PK column is null on a delete");
        assert!(!b.column(0).is_null(0));
        // Heartbeat: one all-null row carrying the watermark seq.
        let b = wire_batch(&wire, 11, TailEventKind::Watermark, None).unwrap();
        assert_eq!(b.num_rows(), 1);
        assert!(b.column(0).is_null(0) && b.column(1).is_null(0));
        let seqs = b.column(2).as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(seqs.value(0), 11);
    }

    // F14: the lagged-subscriber error reaches the wire as gRPC DATA_LOSS —
    // the exact code docs/open-tail-protocol.md promises — through the same
    // FlightError::Tonic -> Status conversion the stream encoder applies
    // (an ExternalError would be flattened to INTERNAL instead).
    #[test]
    fn lagged_subscriber_maps_to_data_loss_on_the_wire() {
        let status = Status::from(FlightError::Tonic(Box::new(lagged_status(4097))));
        assert_eq!(status.code(), tonic::Code::DataLoss);
        assert!(status.message().contains("lagged by 4097 events"));
        assert!(status.message().contains("re-run TailSnapshot"));
    }
}
