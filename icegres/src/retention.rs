//! Conservative accounting for Arrow batches retained outside operator pools.
//! Each owner keeps its reservation until the batches or response stream drop.
//! Decoder temporaries and non-participating library allocations are not RSS caps.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::prelude::DataFrame;
use futures::TryStreamExt;

pub(crate) const DEFAULT_SESSION_BYTES: usize = 256 * 1024 * 1024;
pub(crate) const DEFAULT_TOTAL_BYTES: usize = 1024 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct MemoryLimit {
    limit: usize,
    scope: &'static str,
}
impl std::fmt::Display for MemoryLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} retained Arrow memory limit of {} bytes exceeded",
            self.scope, self.limit
        )
    }
}
impl std::error::Error for MemoryLimit {}

#[derive(Debug)]
pub(crate) struct RetainedPool {
    limit: usize,
    used: AtomicUsize,
    scope: &'static str,
}
impl RetainedPool {
    pub(crate) fn new(limit: usize, scope: &'static str) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
            scope,
        })
    }
    fn grow(&self, bytes: usize) -> Result<()> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.limit)
            })
            .map_err(|_| MemoryLimit {
                limit: self.limit,
                scope: self.scope,
            })?;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub(crate) struct RetentionBudget {
    session: Arc<RetainedPool>,
    total: Arc<RetainedPool>,
}
impl RetentionBudget {
    pub(crate) fn new(session_bytes: usize, total: Arc<RetainedPool>) -> Arc<Self> {
        Arc::new(Self {
            session: RetainedPool::new(session_bytes, "transaction"),
            total,
        })
    }
    pub(crate) fn reservation(self: &Arc<Self>) -> Reservation {
        Reservation {
            budget: self.clone(),
            bytes: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Reservation {
    budget: Arc<RetentionBudget>,
    bytes: usize,
}
impl Reservation {
    pub(crate) fn grow(&mut self, bytes: usize) -> Result<()> {
        self.budget.session.grow(bytes)?;
        if let Err(error) = self.budget.total.grow(bytes) {
            self.budget.session.used.fetch_sub(bytes, Ordering::AcqRel);
            return Err(error);
        }
        // The session counter already checked overflow for the whole session.
        self.bytes += bytes;
        Ok(())
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget
            .session
            .used
            .fetch_sub(self.bytes, Ordering::AcqRel);
        self.budget
            .total
            .used
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// Incrementally collect and optionally align, reserving each batch before
/// retaining it. A failure drops every previously retained batch/reservation.
pub(crate) async fn collect(
    df: DataFrame,
    budget: Option<Arc<RetentionBudget>>,
    schema: Option<&SchemaRef>,
) -> Result<(Vec<RecordBatch>, Option<Reservation>)> {
    let mut reservation = budget.map(|budget| budget.reservation());
    let mut stream = df.execute_stream().await?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        let batch = match schema {
            Some(schema) => crate::overwrite::align_batch(&batch, schema)?,
            None => batch,
        };
        if let Some(reservation) = reservation.as_mut() {
            // Deliberately counts shared/sliced array allocations
            // conservatively. The vector/RecordBatch itself is counted too.
            reservation.grow(
                batch
                    .get_array_memory_size()
                    .saturating_add(std::mem::size_of::<RecordBatch>()),
            )?;
        }
        batches.push(batch);
    }
    Ok((batches, reservation))
}

pub(crate) fn env_positive_bytes(name: &str, default: usize) -> Result<usize> {
    match std::env::var(name) {
        Ok(value) => parse_positive_bytes(name, &value),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(anyhow::anyhow!("{name}: {error}")),
    }
}
fn parse_positive_bytes(name: &str, value: &str) -> Result<usize> {
    let bytes: usize = value
        .parse()
        .map_err(|_| anyhow::anyhow!("{name} must be a positive byte count"))?;
    anyhow::ensure!(bytes > 0, "{name} must be a positive byte count");
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[test]
    fn shared_capacity_is_released_only_with_owner() {
        let total = RetainedPool::new(100, "all transactions");
        let a = RetentionBudget::new(80, total.clone());
        let b = RetentionBudget::new(80, total.clone());
        let mut first = a.reservation();
        first.grow(70).unwrap();
        let mut second = b.reservation();
        assert!(second.grow(40).is_err());
        assert_eq!(total.used(), 70);
        assert_eq!(b.session.used(), 0);
        second.grow(30).unwrap();
        assert!(first.grow(11).is_err());
        assert_eq!(total.used(), 100);
        drop(first);
        assert_eq!(total.used(), 30);
        second.grow(40).unwrap();
        drop(second);
        assert_eq!(total.used(), 0);
    }

    #[tokio::test]
    async fn collection_failure_releases_previous_batches() {
        let ctx = SessionContext::new();
        let total = RetainedPool::new(4096, "all transactions");
        let budget = RetentionBudget::new(1, total.clone());
        let df = ctx.sql("SELECT repeat('x', 2048) AS value").await.unwrap();
        let error = collect(df, Some(budget.clone()), None).await.unwrap_err();
        assert!(error.downcast_ref::<MemoryLimit>().is_some());
        assert_eq!(total.used(), 0);
        assert_eq!(budget.session.used(), 0);
        let df = ctx.sql("SELECT 1 AS value").await.unwrap();
        let budget = RetentionBudget::new(4096, total.clone());
        let (rows, reservation) = collect(df, Some(budget), None).await.unwrap();
        assert_eq!(rows[0].num_rows(), 1);
        assert!(total.used() > 0);
        drop(rows);
        assert!(
            total.used() > 0,
            "ownership remains with the response reservation"
        );
        drop(reservation);
        assert_eq!(total.used(), 0);
    }

    #[test]
    fn invalid_limits_cannot_disable_enforcement() {
        for value in ["0", "-1", "abc", "18446744073709551616"] {
            assert!(parse_positive_bytes("TEST", value).is_err());
        }
        assert_eq!(parse_positive_bytes("TEST", "1048576").unwrap(), 1048576);
    }
}
