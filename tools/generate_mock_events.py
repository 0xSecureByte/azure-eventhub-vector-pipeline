from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import DefaultAzureCredential
import json
import random
import time
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
import sys

console = Console()

def generate_mock_event():
    return {
        "timestamp": int(time.time()),
        "device_id": f"device_{random.randint(1, 100)}",
        "temperature": round(random.uniform(20.0, 40.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2),
        "pressure": round(random.uniform(990.0, 1020.0), 2),
        "status": random.choice(["normal", "warning", "critical"])
    }

def send_mock_events(namespace, eventhub_name, num_events=100000):
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn()
    ) as progress:
        try:
            # Authenticate and create producer
            progress.add_task("[cyan]Authenticating with Azure...", total=None)
            credential = DefaultAzureCredential()
            producer = EventHubProducerClient(
                fully_qualified_namespace=namespace,
                eventhub_name=eventhub_name,
                credential=credential
            )

            # Prepare events
            task = progress.add_task(f"[green]Sending events to {eventhub_name}", total=num_events)
            
            with producer:
                batch = producer.create_batch()
                for i in range(num_events):
                    event = generate_mock_event()
                    batch.add(EventData(json.dumps(event).encode('utf-8')))
                    progress.update(task, advance=1)
                
                producer.send_batch(batch)

            console.print(f"[bold green]✔ Successfully sent {num_events} mock events to {eventhub_name}[/bold green]")

        except Exception as e:
            console.print(f"[bold red]✘ Error sending events: {e}[/bold red]")
            sys.exit(1)

def main():
    console.rule("[bold blue]Azure Event Hub Mock Data Generator[/bold blue]")
    
    namespace = "crystal-cosmic.servicebus.windows.net"
    eventhub_names = [
        "eventhub-1", "eventhub-2", "eventhub-3", 
        "eventhub-4", "eventhub-5", "eventhub-6", 
        "eventhub-7", "eventhub-8"
    ]

    console.print(f"[yellow]Preparing to send mock events to {len(eventhub_names)} Event Hubs[/yellow]")
    
    for hub in eventhub_names:
        send_mock_events(namespace, hub)

    console.rule("[bold green]Data Generation Complete[/bold green]")

if __name__ == "__main__":
    main()