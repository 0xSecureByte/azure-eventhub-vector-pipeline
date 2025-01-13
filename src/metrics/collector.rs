use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;
use parking_lot::RwLock;
use std::collections::HashMap;
use tracing::info;

/// Represents different types of metrics we track
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum MetricType {
    MessagesProcessed,
    ProcessingLatency,
    ErrorCount,
    BackpressureCount,
    BatchSize,
}

/// Stores metric values with atomic operations
#[derive(Debug)]
pub struct MetricValue {
    count: AtomicU64,
    sum: AtomicI64,    // For averages
    max: AtomicI64,    // For maximum values
}

impl MetricValue {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum: AtomicI64::new(0),
            max: AtomicI64::new(0),
        }
    }
}

/// Main metrics collector
#[derive(Debug)]
pub struct MetricsCollector {
    metrics: Arc<RwLock<HashMap<MetricType, MetricValue>>>,
    start_time: Instant,
}

impl Default for MetricsCollector {
    fn default() -> Self {
        let mut metrics = HashMap::new();
        // Initialize all metric types
        metrics.insert(MetricType::MessagesProcessed, MetricValue::new());
        metrics.insert(MetricType::ProcessingLatency, MetricValue::new());
        metrics.insert(MetricType::ErrorCount, MetricValue::new());
        metrics.insert(MetricType::BackpressureCount, MetricValue::new());
        metrics.insert(MetricType::BatchSize, MetricValue::new());

        Self {
            metrics: Arc::new(RwLock::new(metrics)),
            start_time: Instant::now(),
        }
    }
}

impl MetricsCollector {
    /// Creates a new metrics collector
    pub fn new() -> Self {
        Self::default()
    }

    /// Increments a counter metric
    pub fn increment(&self, metric_type: MetricType, value: u64) {
        if let Some(metric) = self.metrics.read().get(&metric_type) {
            metric.count.fetch_add(value, Ordering::Relaxed);
        }
    }

    /// Records a value for averaging
    pub fn record_value(&self, metric_type: MetricType, value: i64) {
        if let Some(metric) = self.metrics.read().get(&metric_type) {
            metric.sum.fetch_add(value, Ordering::Relaxed);
            metric.count.fetch_add(1, Ordering::Relaxed);
            
            // Update max if necessary
            let mut current_max = metric.max.load(Ordering::Relaxed);
            while value > current_max {
                match metric.max.compare_exchange_weak(
                    current_max,
                    value,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => current_max = actual,
                }
            }
        }
    }

    /// Starts the metrics reporting task
    pub async fn start_reporting(&self) {
        let metrics = Arc::clone(&self.metrics);
        let start_time = self.start_time;

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(60));

            loop {
                interval.tick().await;
                let uptime = start_time.elapsed().as_secs();
                let metrics_read = metrics.read();

                // Generate log lines in key=value format
                let mut log_lines = Vec::new();

                if let Some(metric) = metrics_read.get(&MetricType::MessagesProcessed) {
                    let count = metric.count.load(Ordering::Relaxed);
                    log_lines.push(format!("messages_processed={}", count));
                    
                    // Use safe division
                    log_lines.push(format!("messages_per_second={}", 
                        count.checked_div(uptime).unwrap_or(0)
                    ));
                }

                // Processing latency statistics
                if let Some(metric) = metrics_read.get(&MetricType::ProcessingLatency) {
                    let count = metric.count.load(Ordering::Relaxed);
                    if count > 0 {
                        let sum = metric.sum.load(Ordering::Relaxed);
                        let avg = sum as f64 / count as f64;
                        let max = metric.max.load(Ordering::Relaxed);
                        log_lines.push(format!("avg_latency_ms={:.2}", avg));
                        log_lines.push(format!("max_latency_ms={}", max));
                    }
                }

                // Error statistics
                if let Some(metric) = metrics_read.get(&MetricType::ErrorCount) {
                    let errors = metric.count.load(Ordering::Relaxed);
                    log_lines.push(format!("error_count={}", errors));
                }

                // Backpressure statistics
                if let Some(metric) = metrics_read.get(&MetricType::BackpressureCount) {
                    let backpressure = metric.count.load(Ordering::Relaxed);
                    log_lines.push(format!("backpressure_count={}", backpressure));
                }

                // Batch size statistics
                if let Some(metric) = metrics_read.get(&MetricType::BatchSize) {
                    let count = metric.count.load(Ordering::Relaxed);
                    if count > 0 {
                        let sum = metric.sum.load(Ordering::Relaxed);
                        let avg = sum as f64 / count as f64;
                        let max = metric.max.load(Ordering::Relaxed);
                        log_lines.push(format!("avg_batch_size={:.2}", avg));
                        log_lines.push(format!("max_batch_size={}", max));
                    }
                }

                // Log all metrics
                info!("METRICS {}", log_lines.join(" "));
            }
        });
    }
}

/// Convenience wrapper for timing operations
pub struct Timer {
    start: Instant,
    metric_type: MetricType,
    metrics: Arc<MetricsCollector>,
}

impl Timer {
    pub fn new(metric_type: MetricType, metrics: Arc<MetricsCollector>) -> Self {
        Self {
            start: Instant::now(),
            metric_type,
            metrics,
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let duration = self.start.elapsed().as_millis() as i64;
        self.metrics.record_value(self.metric_type.clone(), duration);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_collection() {
        let collector = MetricsCollector::new();
        collector.increment(MetricType::MessagesProcessed, 1);
        collector.record_value(MetricType::ProcessingLatency, 100);
        
        let metrics = collector.metrics.read();
        if let Some(metric) = metrics.get(&MetricType::MessagesProcessed) {
            assert_eq!(metric.count.load(Ordering::Relaxed), 1);
        }
    }
}