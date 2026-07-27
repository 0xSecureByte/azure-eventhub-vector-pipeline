# Azure Event Hub to Vector Pipeline

Una aplicación de Rust de alto rendimiento para el streaming de logs desde Azure Event Hub hacia Vector, diseñada para manejar el procesamiento de eventos de alto rendimiento con capacidades de confiabilidad y monitoreo.

## Tabla de Contenidos

- [Características](#características)
- [Arquitectura](#arquitectura)
- [Prerrequisitos](#prerrequisitos)
- [Inicio Rápido](#inicio-rápido)
- [Configuración](#configuración)
- [Ajuste de Rendimiento](#ajuste-de-rendimiento)
- [Monitoreo](#monitoreo)
- [Desarrollo](#desarrollo)
- [Configuración de Microsoft Azure Event Hub](#configuración-de-microsoft-azure-event-hub)
- [Configuración de Vector](#configuración-de-vector)

## Características

- **Alto Rendimiento**: Procesa 100,000 mensajes por segundo con un procesamiento por lotes optimizado.
- **Escalable**: Por defecto maneja 8 Event Hubs con 4 particiones cada uno (32 particiones totales) (Modificable a través de default/config.toml).
- **Confiable**: Implementa entrega "al menos una vez" (at-least-once) con estrategias de reintento configurables.
- **Monitoreado**: Métricas exhaustivas y logging estructurado.
- **Configurable**: Configuración dinámica en TOML con soporte para recarga en caliente (hot-reload).

## Arquitectura
![Azure Event Hub pipeline](https://raw.githubusercontent.com/0xSecureByte/project-svgs/refs/heads/main/azure-event-hub-pipeline.svg)

## Prerrequisitos

- Rust 1.75 o superior
- Instancia de Vector ejecutándose con entrada TCP configurada
- Espacio de nombres (namespace) de Azure Event Hub con event hubs configurados

## Inicio Rápido

1. Clonar el repositorio:
```bash
git clone https://github.com/0xSecureByte/azure-eventhub-vector-pipeline.git
cd azure-eventhub-vector-pipeline
```

2. Configurar el entorno:
   - Ajustar `config/default.toml`
   - Actualizar la configuración con los detalles de su Event Hub

3. Compilar y ejecutar:
```bash
cargo build --release
```
```bash
bash ./target/release/azure-eventhub-vector-pipeline
```

## Configuración

La aplicación utiliza TOML para la configuración. Los ajustes clave incluyen:

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

## Ajuste de Rendimiento

### Hardware Recomendado

- CPU: 24 núcleos (16 mínimo)
- Memoria: 4GB (2GB mínimo)
- Red: Ancho de banda = Mensajes/seg × Bytes totales por mensaje ≈ 1.12 Gbps con la configuración actual

### Parámetros Clave

- `batch_size`: Ajustar según el tamaño del mensaje (por defecto: 1000)
- `worker_count`: Establecer según el número de núcleos de CPU disponibles
- `queue_size`: Tamaño del búfer para manejar picos de rendimiento

## Monitoreo

La aplicación exporta métricas de:
- Mensajes procesados por segundo
- Latencia de procesamiento
- Tasas de error
- Utilización de recursos
- Indicadores de contrapresión (backpressure)

## Desarrollo

### Compilación

```bash
# Compilación de depuración (debug)
cargo build

# Compilación de lanzamiento (release)
cargo build --release

# Ejecutar pruebas
cargo test
```

### Estructura del Proyecto

```
src/
├── app/          # Núcleo de la aplicación
├── config/       # Gestión de configuración
├── connection/   # Conectividad con Event Hub
├── consumer/     # Consumidor de Event Hub
├── pipeline/     # Pipeline de procesamiento
├── sender/       # Emisor a Vector
└── metrics/      # Recolección de métricas
```

## Configuración de Vector

Configuración de Vector requerida (vector.toml):

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

## Configuración de Microsoft Azure Event Hub

Siga estos pasos para configurar su entorno de Azure Event Hub:

1. Instalar Azure CLI:
```bash
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

2. Iniciar sesión en Azure:
```bash
az login
```

3. Crear Grupo de Recursos:
```bash
az group create --name eventhub-vector-rg --location eastus
```

4. Crear Espacio de Nombres de Event Hubs (SKU Premium para 8 Event Hubs):
```bash
az eventhubs namespace create \
    --name your-namespace-name \
    --resource-group eventhub-vector-rg \
    --sku Premium \
    --capacity 1
```

5. Crear Event Hubs:
```bash
for i in {1..8}; do
    az eventhubs eventhub create \
        --name eventhub-$i \
        --namespace-name your-namespace-name \
        --resource-group eventhub-vector-rg \
        --partition-count 4
done
```

6. Crear Grupos de Consumidores:
```bash
for i in {1..8}; do
    az eventhubs eventhub consumer-group create \
        --eventhub-name eventhub-$i \
        --name vector-consumer \
        --namespace-name your-namespace-name \
        --resource-group eventhub-vector-rg
done
```

7. Obtener Cadena de Conexión:
```bash
az eventhubs namespace authorization-rule keys list \
    --resource-group eventhub-vector-rg \
    --namespace-name your-namespace-name \
    --name RootManageSharedAccessKey \
    --query primaryConnectionString \
    --output tsv
```

8. Verificar Configuración:
```bash
# Verificar salud del Event Hub
az eventhubs namespace show \
    --resource-group eventhub-vector-rg \
    --name your-namespace-name

# Monitorear métricas del Event Hub
az monitor metrics list \
    --resource /subscriptions/{subscription-id}/resourceGroups/eventhub-vector-rg/providers/Microsoft.EventHub/namespaces/your-namespace-name \
    --metric "IncomingMessages"
```

> **Nota**: Reemplace `your-namespace-name` y `{subscription-id}` con sus valores reales.

> **Importante**: El SKU Premium es necesario para la configuración de 8 Event Hubs. El SKU Standard tiene limitaciones que pueden afectar el rendimiento.


## Autores

- 0xSecureByte <chirantan.code@gmail.com>

## Agradecimientos

- [Azure Event Hubs](https://github.com/Azure/azure-event-hubs)
- [Vector Project Team](https://github.com/vectordotdev/vector)
- [Rust](https://github.com/rust-lang)

## Solución de Problemas

### Problemas Comunes

1. **Tiempos de Espera de Conexión (Connection Timeouts)**
   - Verifique la conectividad de red.
   - Verifique las credenciales de Event Hub.
   - Asegúrese de tener el ancho de banda de red adecuado.

2. **Uso Elevado de Memoria**
   - Reduzca el `batch_size`.
   - Ajuste el `queue_size`.
   - Monitoree los recursos del sistema.

3. **Problemas de Rendimiento**
   - Verifique la utilización de la CPU.
   - Verifique la capacidad de la red.
   - Ajuste los parámetros de procesamiento por lotes.

## Licencia

Este proyecto está licenciado bajo la Licencia MIT; consulte el archivo LICENSE para más detalles.
