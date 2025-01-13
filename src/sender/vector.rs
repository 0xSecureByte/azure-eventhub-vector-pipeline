use anyhow::{Result, Context};
use tokio::{
    net::TcpStream,
    sync::mpsc,
    io::AsyncWriteExt,
};
use std::{time::Duration, sync::Arc};
use tracing::{info, error, warn};
use tokio::sync::Mutex;
use backoff::{ExponentialBackoff, backoff::Backoff};

use crate::pipeline::processor::EventBatch;

#[derive(Clone, Debug)]
pub struct VectorConfig {
    pub host: String,
    pub port: u16,
    pub connection_timeout_ms: u64,
    pub write_timeout_ms: u64,
    pub retry_initial_interval_ms: u64,
    pub retry_max_interval_ms: u64,
    pub retry_max_elapsed_time_ms: u64,
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 9000,
            connection_timeout_ms: 5000,
            write_timeout_ms: 5000,
            retry_initial_interval_ms: 100,
            retry_max_interval_ms: 10000,
            retry_max_elapsed_time_ms: 300000,
        }
    }
}

/// Manages TCP connection to Vector
pub struct VectorSender {
    config: VectorConfig,
    connection: Arc<Mutex<Option<TcpStream>>>,
}

impl VectorSender {
    /// Creates a new Vector sender
    pub fn new(config: VectorConfig) -> Self {
        Self {
            config,
            connection: Arc::new(Mutex::new(None)),
        }
    }

    /// Starts the sender process
    pub async fn start(&self, mut batch_receiver: mpsc::Receiver<EventBatch>) -> Result<()> {
        self.connect().await?;

        let config = self.config.clone();
        let connection = Arc::clone(&self.connection);

        tokio::spawn(async move {
            while let Some(batch) = batch_receiver.recv().await {
                let mut retry_count = 0;
                let mut backoff = ExponentialBackoff {
                    initial_interval: Duration::from_millis(config.retry_initial_interval_ms),
                    max_interval: Duration::from_millis(config.retry_max_interval_ms),
                    max_elapsed_time: Some(Duration::from_millis(config.retry_max_elapsed_time_ms)),
                    ..Default::default()
                };

                loop {
                    match Self::send_batch(&connection, &batch).await {
                        Ok(_) => break,
                        Err(e) => {
                            error!("Failed to send batch: {}", e);
                            retry_count += 1;

                            if let Some(duration) = backoff.next_backoff() {
                                warn!("Retrying send after {:?} (attempt {})", duration, retry_count);
                                tokio::time::sleep(duration).await;
                                
                                // Try to reconnect before next attempt
                                if let Err(e) = Self::reconnect(&connection, &config).await {
                                    error!("Failed to reconnect: {}", e);
                                }
                            } else {
                                error!("Max retries reached for batch");
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    /// Connects to Vector
    async fn connect(&self) -> Result<()> {
        let mut connection = self.connection.lock().await;
        if connection.is_some() {
            return Ok(());
        }

        let addr = format!("{}:{}", self.config.host, self.config.port);
        let stream = tokio::time::timeout(
            Duration::from_millis(self.config.connection_timeout_ms),
            TcpStream::connect(&addr),
        )
        .await
        .context("Connection timeout")??;

        stream.set_nodelay(true)?;
        *connection = Some(stream);
        info!("Connected to Vector at {}", addr);

        Ok(())
    }

    /// Reconnects to Vector
    async fn reconnect(connection: &Arc<Mutex<Option<TcpStream>>>, config: &VectorConfig) -> Result<()> {
        let mut conn = connection.lock().await;
        *conn = None;

        let addr = format!("{}:{}", config.host, config.port);
        let stream = tokio::time::timeout(
            Duration::from_millis(config.connection_timeout_ms),
            TcpStream::connect(&addr),
        )
        .await
        .context("Connection timeout")??;

        stream.set_nodelay(true)?;
        *conn = Some(stream);
        info!("Reconnected to Vector at {}", addr);

        Ok(())
    }

    /// Sends a batch of events
    async fn send_batch(
        connection: &Arc<Mutex<Option<TcpStream>>>,
        batch: &EventBatch,
    ) -> Result<()> {
        let serialized = serde_json::to_vec(&batch.events)
            .context("Failed to serialize batch")?;

        let mut conn_guard = connection.lock().await;
        let stream = conn_guard.as_mut().context("No active connection")?;

        stream.write_all(&serialized).await
            .context("Failed to write to TCP stream")?;
        
        stream.write_all(b"\n").await
            .context("Failed to write delimiter")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_sender_connection() {
        let config = VectorConfig::default();
        let _sender = VectorSender::new(config);
        // Structural test only
    }
}