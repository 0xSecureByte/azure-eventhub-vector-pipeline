from azure.eventhub import EventHubConsumerClient
from azure.identity import DefaultAzureCredential
from rich.console import Console
import time

console = Console()

def on_event(partition_context, event):
    console.print(f"[cyan]Partition: {partition_context.partition_id}[/cyan]")
    console.print(f"[green]Event: {event.body_as_str()}[/green]")
    console.print("---")

def main():
    console.rule("[bold blue]Event Hub Message Verifier[/bold blue]")
    
    namespace = "crystal-cosmic.servicebus.windows.net"
    eventhub_name = "eventhub-1"
    consumer_group = "$Default"

    client = EventHubConsumerClient(
        fully_qualified_namespace=namespace,
        eventhub_name=eventhub_name,
        consumer_group=consumer_group,
        credential=DefaultAzureCredential()
    )

    console.print(f"[yellow]Reading events from all partitions in {eventhub_name}[/yellow]")
    
    try:
        with client:
            client.receive(
                on_event=on_event,
                starting_position="-1"  # Read from end
            )
    except KeyboardInterrupt:
        console.print("[yellow]Stopping event reception...[/yellow]")

if __name__ == "__main__":
    main() 