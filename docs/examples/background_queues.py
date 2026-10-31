"""Background job queue service, TaskDef registration, and in-memory execution example."""

import asyncio

from vertebra.framework.queues.base import QueueConfig, QueueService, TaskDef
from vertebra.framework.queues.scheduler import CronExpression

__all__ = (
    "main",
    "run_queue_workflow",
    "send_welcome_email",
)


# --8<-- [start:example]
async def send_welcome_email(user_id: int) -> dict[str, int | str]:
    """Deliver a welcome email for the given user_id."""
    return {"user_id": user_id, "status": "sent"}


async def run_queue_workflow() -> None:
    """Register a TaskDef, enqueue an envelope, and drain the in-memory queue."""
    service = QueueService(QueueConfig(backend_url="memory://"))
    service.register(TaskDef(name="send_welcome_email", fn=send_welcome_email, lane="io"))

    envelope = await service.enqueue("send_welcome_email", 42)
    assert envelope.task_name == "send_welcome_email"
    assert envelope.lane == "io"

    outcomes = await service.drain_once(lanes=("io",))
    assert outcomes == [{"user_id": 42, "status": "sent"}]

    cron = CronExpression.parse("*/15 * * * *")
    assert 0 in cron.minutes
    assert 15 in cron.minutes
    await service.close()


def main() -> None:
    """Execute the async background queue workflow."""
    asyncio.run(run_queue_workflow())


# --8<-- [end:example]


if __name__ == "__main__":
    main()
