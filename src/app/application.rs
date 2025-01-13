use std::sync::Arc;
use anyhow::{Result, Context};  // Added Context trait
use tokio::signal;
use tracing::{info, error};
use tokio::sync::Mutex;

use crate::config::ConfigManager;
use crate::connection::{EventHubConnection, EventHubConfig};  // Added EventHubConfig
use crate::consumer::{Event, EventHubConsumer, ConsumerConfig};
use crate::pipeline::{EventBatch, ProcessingPipeline, ProcessingConfig};
use crate::sender::{VectorSender, VectorConfig};  // Added VectorConfig
use crate::metrics::MetricsCollector;

pub struct Application {
    config_manager: Arc<ConfigManager>,
    metrics: Arc<MetricsCollector>,
    shutdown_signal: tokio::sync::broadcast::Sender<()>,
}

impl Application {
    pub async fn new(config_path: String) -> Result<Self> {
        let (config_manager, _) = ConfigManager::new(config_path)
            .await
            .context("Failed to initialize config manager")?;  // Context trait is now in scope
        let config_manager = Arc::new(config_manager);

        let metrics = Arc::new(MetricsCollector::new());
        let (shutdown_signal, _) = tokio::sync::broadcast::channel(1);

        Ok(Self {
            config_manager,
            metrics,
            shutdown_signal,
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!("Starting application...");

        self.config_manager.start_watching().await?;
        let initial_config = self.config_manager.get_config().await;

        self.metrics.start_reporting().await;

        let mut event_hub_connections = Vec::new();
        let mut consumers = Vec::new();
        let mut pipeline_receivers = Vec::new();

        for hub_name in &initial_config.event_hub.event_hub_names {
            let hub_config = EventHubConfig {
                fully_qualified_namespace: initial_config.event_hub.fully_qualified_namespace.clone(),
                event_hub_name: hub_name.clone(),
                consumer_group: initial_config.event_hub.consumer_group.clone(),
            };

            let connection = EventHubConnection::new(hub_config);
            connection.connect().await?;
            
            let (hub_name, locked_client) = connection.get_client().await?;
            info!("Processing Event Hub: {}", hub_name);
            let client_with_mutex = Arc::new(Mutex::new(locked_client));

            let (consumer, receiver) = EventHubConsumer::new(
                client_with_mutex,
                ConsumerConfig {
                    max_batch_size: initial_config.processing.batch_size,
                    partition_count: initial_config.event_hub.partition_count,
                    buffer_size: initial_config.processing.queue_size,
                },
            );
            
            event_hub_connections.push(connection);
            consumers.push(consumer);
            pipeline_receivers.push(receiver);
        }

        let (pipeline, pipeline_output) = ProcessingPipeline::new(
            ProcessingConfig {
                batch_size: initial_config.processing.batch_size,
                batch_timeout_ms: initial_config.processing.batch_timeout_ms,
                worker_count: initial_config.processing.worker_count,
                queue_size: initial_config.processing.queue_size,
            }
        );

        let vector_sender = VectorSender::new(
            VectorConfig {
                host: initial_config.vector.host.clone(),
                port: initial_config.vector.port,
                connection_timeout_ms: initial_config.vector.connection_timeout_ms,
                write_timeout_ms: initial_config.vector.write_timeout_ms,
                retry_initial_interval_ms: initial_config.vector.retry_initial_interval_ms,
                retry_max_interval_ms: initial_config.vector.retry_max_interval_ms,
                retry_max_elapsed_time_ms: initial_config.vector.retry_max_elapsed_time_ms,
            }
        );


        self.start_components(
            consumers,
            pipeline,
            pipeline_receivers,
            pipeline_output,
            vector_sender,
        ).await?;

        self.wait_for_shutdown().await?;

        info!("Application shutdown complete");
        Ok(())
    }

    async fn start_components(
        &self,
        consumers: Vec<EventHubConsumer>,
        pipeline: ProcessingPipeline,
        pipeline_receivers: Vec<tokio::sync::mpsc::Receiver<Event>>,
        pipeline_output: tokio::sync::mpsc::Receiver<EventBatch>,
        vector_sender: VectorSender,
    ) -> Result<()> {
        let mut handles = Vec::new();
        let pipeline = Arc::new(pipeline);

        for (consumer, receiver) in consumers.into_iter().zip(pipeline_receivers) {
            let _metrics = Arc::clone(&self.metrics);
            let mut shutdown = self.shutdown_signal.subscribe();
            
            handles.push(tokio::spawn(async move {
                tokio::select! {
                    res = consumer.start_consuming() => {
                        if let Err(e) = res {
                            error!("Consumer error: {}", e);
                        }
                    }
                    _ = shutdown.recv() => {
                        info!("Consumer received shutdown signal");
                    }
                }
            }));

            let _metrics = Arc::clone(&self.metrics);
            let mut shutdown = self.shutdown_signal.subscribe();
            let pipeline = Arc::clone(&pipeline);
            
            handles.push(tokio::spawn(async move {
                tokio::select! {
                    res = pipeline.start(receiver) => {
                        if let Err(e) = res {
                            error!("Pipeline error: {}", e);
                        }
                    }
                    _ = shutdown.recv() => {
                        info!("Pipeline received shutdown signal");
                    }
                }
            }));
        }

        let _metrics = Arc::clone(&self.metrics);
        let mut shutdown = self.shutdown_signal.subscribe();
        handles.push(tokio::spawn(async move {
            tokio::select! {
                res = vector_sender.start(pipeline_output) => {
                    if let Err(e) = res {
                        error!("Vector sender error: {}", e);
                    }
                }
                _ = shutdown.recv() => {
                    info!("Vector sender received shutdown signal");
                }
            }
        }));

        Ok(())
    }

    async fn wait_for_shutdown(&self) -> Result<()> {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("Failed to listen for ctrl+c");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("Failed to listen for terminate signal")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => info!("Received ctrl+c signal"),
            _ = terminate => info!("Received terminate signal"),
        }

        let _ = self.shutdown_signal.send(());

        Ok(())
    }
}