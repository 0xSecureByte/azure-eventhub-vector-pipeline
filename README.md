# Azure Event Hub to Vector Pipeline

A high-performance Rust application for streaming logs from Azure Event Hub to Vector, designed to handle high-throughput event processing with reliability and monitoring capabilities.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Performance Tuning](#performance-tuning)
- [Monitoring](#monitoring)
- [Development](#development)
- [Microsoft Azure Event Hub Setup](#azure-event-hub-setup)
- [Vector Configuration](#vector-configuration)

## Features

- **High Performance**: Processes 100,000 messages per second with optimized batching
- **Scalable**: Default Handles 8 Event Hubs with 4 partitions each (32 total partitions) (Changeable through default/config.toml)
- **Reliable**: Implements at-least-once delivery with configurable retry strategies
- **Monitored**: Comprehensive metrics and structured logging
- **Configurable**: Dynamic TOML configuration with hot-reload support

## Architecture
![Azure Event Hub pipeline](https://raw.githubusercontent.com/0xSecureByte/project-svgs/refs/heads/main/azure-event-hub-pipeline.svg)

## Prerequisites

- Rust 1.75 or higher
- Vector instance running with TCP input configured
- Azure Event Hub namespace with configured event hubs

## Quick Start

1. Clone the repository:
```bash
git clone https://github.com/0xSecureByte/azure-eventhub-vector-pipeline.git
cd azure-eventhub-vector-pipeline
```

2. Configure your environment:
   - Adjust `config/default.toml`
   - Update the configuration with your Event Hub details

3. Build and run:
```bash
cargo build --release
```
```bash
bash ./target/release/azure-eventhub-vector-pipeline
```

## Configuration

The application uses TOML for configuration. Key settings include:

```toml
[event_hub]
namespace = "your-namespace-name.servicebus.windows.net"
event_hub_names = [
    "eventhub-1",
    "eventhub-2",
    "eventhub-3",
    "eventhub-4",
    "eventhub-5",
    "eventhub-6",
    "eventhub-7",
    "eventhub-8"
]
consumer_group = "vector-consumer"
partition_count = 4

[processing]
batch_size = 1000
batch_timeout_ms = 100
worker_count = 4
queue_size = 10000

[vector]
host = "localhost"
port = 9000
connection_timeout_ms = 5000
write_timeout_ms = 5000
retry_initial_interval_ms = 100
retry_max_interval_ms = 10000
retry_max_elapsed_time_ms = 300000

[metrics]
report_interval_seconds = 60
```

## Performance Tuning

### Recommended Hardware

- CPU: 24 cores (16 minimum)
- Memory: 4GB (2GB minimum)
- Network: Bandwidth = Messages/sec × Total bytes per message ≈ 1.12 Gbps with the current setup

### Key Parameters

- `batch_size`: Adjust based on message size (default: 1000)
- `worker_count`: Set to number of available CPU cores
- `queue_size`: Buffer size for handling throughput spikes

## Monitoring

The application exports metrics for:
- Messages processed per second
- Processing latency
- Error rates
- Resource utilization
- Backpressure indicators

## Development

### Building

```bash
# Debug build
cargo build

# Release build
cargo build --release

# Run tests
cargo test
```

### Project Structure

```
src/
├── app/          # Application core
├── config/       # Configuration management
├── connection/   # Event Hub connectivity
├── consumer/     # Event Hub consumer
├── pipeline/     # Processing pipeline
├── sender/       # Vector sender
└── metrics/      # Metrics collection
```

## Vector Configuration

Required Vector configuration (vector.toml):

```toml
[sources.tcp_input]
type = "socket"
address = "0.0.0.0:9000"
mode = "tcp"
decoding.codec = "json"

[sinks.console]
type = "console"
inputs = ["tcp_input"]
encoding.codec = "json"
```

## Azure Event Hub Setup

Follow these steps to set up your Azure Event Hub environment:

1. Install Azure CLI:
```bash
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

2. Login to Azure:
```bash
az login
```

3. Create Resource Group:
```bash
az group create --name eventhub-vector-rg --location eastus
```

4. Create Event Hubs Namespace (Premium SKU for 8 Event Hubs):
```bash
az eventhubs namespace create \
    --name your-namespace-name \
    --resource-group eventhub-vector-rg \
    --sku Premium \
    --capacity 1
```

5. Create Event Hubs:
```bash
for i in {1..8}; do
    az eventhubs eventhub create \
        --name eventhub-$i \
        --namespace-name your-namespace-name \
        --resource-group eventhub-vector-rg \
        --partition-count 4
done
```

6. Create Consumer Groups:
```bash
for i in {1..8}; do
    az eventhubs eventhub consumer-group create \
        --eventhub-name eventhub-$i \
        --name vector-consumer \
        --namespace-name your-namespace-name \
        --resource-group eventhub-vector-rg
done
```

7. Get Connection String:
```bash
az eventhubs namespace authorization-rule keys list \
    --resource-group eventhub-vector-rg \
    --namespace-name your-namespace-name \
    --name RootManageSharedAccessKey \
    --query primaryConnectionString \
    --output tsv
```

8. Verify Setup:
```bash
# Check Event Hub health
az eventhubs namespace show \
    --resource-group eventhub-vector-rg \
    --name your-namespace-name

# Monitor Event Hub metrics
az monitor metrics list \
    --resource /subscriptions/{subscription-id}/resourceGroups/eventhub-vector-rg/providers/Microsoft.EventHub/namespaces/your-namespace-name \
    --metric "IncomingMessages"
```

> **Note**: Replace `your-namespace-name` and `{subscription-id}` with your actual values.

> **Important**: The Premium SKU is required for the 8 Event Hub setup. Standard SKU has limitations that may affect performance.


## Authors

- 0xSecureByte <chirantan.code@gmail.com>

## Acknowledgments

- [Azure Event Hubs](https://github.com/Azure/azure-event-hubs)
- [Vector Project Team](https://github.com/vectordotdev/vector)
- [Rust](https://github.com/rust-lang)

## Troubleshooting

### Common Issues

1. **Connection Timeouts**
   - Check network connectivity
   - Verify Event Hub credentials
   - Ensure proper network bandwidth

2. **High Memory Usage**
   - Reduce batch size
   - Adjust queue size
   - Monitor system resources

3. **Performance Issues**
   - Check CPU utilization
   - Verify network capacity
   - Tune batch processing parameters

## License

This project is licensed under the MIT License - see the LICENSE file for details.