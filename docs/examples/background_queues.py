"""Queue a report job and process it with a local worker."""

__all__ = ("build_report", "main")

# --8<-- [start:example]
import asyncio

from vertebra.framework.queues import QueueConfig, QueueService, TaskDef


async def build_report(customer: str, amounts: list[int]) -> dict[str, str | int]:
    """Summarize the invoice amounts for one customer."""
    return {"customer": customer, "total": sum(amounts)}


async def main() -> None:
    """Submit a report job and show its result in the local worker."""
    queue = QueueService(QueueConfig(backend_url="memory://"))
    queue.register(TaskDef(name="build_report", fn=build_report, lane="io"))
    try:
        job = await queue.enqueue("build_report", "Acme", [120, 80])
        print(f"Queued {job.task_name} in the {job.lane} lane")
        for report in await queue.drain_once(lanes=("io",)):
            print(f"Report for {report['customer']}: {report['total']}")
    finally:
        await queue.close()


if __name__ == "__main__":
    asyncio.run(main())

# --8<-- [end:example]
