use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;
use anyhow::{Result, Context};
use notify::{Watcher, RecursiveMode, Event};
use serde::{Serialize, Deserialize};
use tokio::sync::RwLock;
use tracing::{info, error};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointSettings {
    pub enabled: bool,
    pub storage_account_name: String,
    pub container_name: String,
    pub interval_seconds: u64,
    pub retry_max_attempts: u32,
    pub retry_initial_interval_ms: u64,
}

impl CheckpointSettings {
    pub fn validate(&self) -> Result<()> {
        if self.enabled {
            if self.storage_account_name.is_empty() {
                return Err(anyhow::anyhow!("Storage account name cannot be empty when checkpointing is enabled"));
            }
            if self.container_name.is_empty() {
                return Err(anyhow::anyhow!("Container name cannot be empty when checkpointing is enabled"));
            }
            if !self.storage_account_name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
                return Err(anyhow::anyhow!("Storage account name contains invalid characters"));
            }
        }
        Ok(())
    }
}

/// Complete application configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub event_hub: EventHubSettings,
    pub processing: ProcessingSettings,
    pub vector: VectorSettings,
    pub metrics: MetricsSettings,
    pub checkpointing: CheckpointSettings,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventHubSettings {
    pub fully_qualified_namespace: String,
    pub event_hub_names: Vec<String>,
    pub consumer_group: String,
    pub partition_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProcessingSettings {
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub worker_count: usize,
    pub queue_size: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorSettings {
    pub host: String,
    pub port: u16,
    pub connection_timeout_ms: u64,
    pub write_timeout_ms: u64,
    pub retry_initial_interval_ms: u64,
    pub retry_max_interval_ms: u64,
    pub retry_max_elapsed_time_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricsSettings {
    pub report_interval_seconds: u64,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            checkpointing: CheckpointSettings {
                enabled: true,
                storage_account_name: "querr".to_string(),
                container_name: "eventhub-checkpoints".to_string(),
                interval_seconds: 30,
                retry_max_attempts: 3,
                retry_initial_interval_ms: 100,
            },
            event_hub: EventHubSettings {
                fully_qualified_namespace: "namespace.servicebus.windows.net".to_string(),
                event_hub_names: vec!["hub1".to_string(); 8],
                consumer_group: "$Default".to_string(),
                partition_count: 4,
            },
            processing: ProcessingSettings {
                batch_size: 1000,
                batch_timeout_ms: 100,
                worker_count: 4,
                queue_size: 10000,
            },
            vector: VectorSettings {
                host: "localhost".to_string(),
                port: 9000,
                connection_timeout_ms: 5000,
                write_timeout_ms: 5000,
                retry_initial_interval_ms: 100,
                retry_max_interval_ms: 10000,
                retry_max_elapsed_time_ms: 300000,
            },
            metrics: MetricsSettings {
                report_interval_seconds: 60,
            },
        }
    }
}

/// Manages application configuration
pub struct ConfigManager {
    config: Arc<RwLock<AppConfig>>,
    config_path: String,
    update_sender: broadcast::Sender<AppConfig>,
}

impl ConfigManager {
    /// Creates a new configuration manager
    pub async fn new(config_path: String) -> Result<(Self, broadcast::Receiver<AppConfig>)> {
        let config = Self::load_config(&config_path).await?;
        let (update_sender, update_receiver) = broadcast::channel(16);
        
        Ok((Self {
            config: Arc::new(RwLock::new(config)),
            config_path,
            update_sender,
        }, update_receiver))
    }

    /// Loads configuration from TOML file
    async fn load_config(path: &str) -> Result<AppConfig> {
        let content = tokio::fs::read_to_string(path)
            .await
            .context("Failed to read config file")?;
            
        toml::from_str(&content)
            .context("Failed to parse TOML config")
    }

    /// Starts the configuration watcher
    pub async fn start_watching(&self) -> Result<()> {
        let config = Arc::clone(&self.config);
        let sender = self.update_sender.clone();
        let config_path = self.config_path.clone();

        let (tx, rx) = std::sync::mpsc::channel();

        let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                if event.kind.is_modify() {
                    tx.send(()).unwrap_or_else(|e| error!("Failed to send watch event: {}", e));
                }
            }
        })?;

        watcher.watch(
            Path::new(&config_path),
            RecursiveMode::NonRecursive,
        )?;

        tokio::spawn(async move {
            while rx.recv().is_ok() {
                match Self::load_config(&config_path).await {
                    Ok(new_config) => {
                        {
                            let mut current_config = config.write().await;
                            *current_config = new_config.clone();
                        }

                        if let Err(e) = sender.send(new_config) {
                            error!("Failed to broadcast config update: {}", e);
                        } else {
                            info!("Configuration updated successfully");
                        }
                    }
                    Err(e) => {
                        error!("Failed to load updated config: {}", e);
                    }
                }
            }
        });

        Ok(())
    }

    /// Gets the current configuration
    pub async fn get_config(&self) -> AppConfig {
        self.config.read().await.clone()
    }

    /// Updates a specific configuration value
    pub async fn update_value<F>(&self, updater: F) -> Result<()>
    where
        F: FnOnce(&mut AppConfig),
    {
        {
            let mut config = self.config.write().await;
            updater(&mut config);
        }

        let updated_config = self.get_config().await;
        self.update_sender.send(updated_config)
            .context("Failed to broadcast config update")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[tokio::test]
    async fn test_config_loading() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let config = AppConfig::default();
        let toml_str = toml::to_string(&config).unwrap();
        write!(temp_file, "{}", toml_str).unwrap();

        let (config_manager, _rx) = ConfigManager::new(temp_file.path().to_str().unwrap().to_string())
            .await
            .unwrap();

        let loaded_config = config_manager.get_config().await;
        assert_eq!(loaded_config.event_hub.partition_count, 4);
    }
}