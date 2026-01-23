from vertebra.state import NativeState


class Counter(NativeState):
    count: int = 0


async def test_native_state_sync() -> None:
    counter = Counter()

    # Start subscription
    iterator = counter.subscribe()

    # Get initial state
    initial = await anext(iterator)
    assert initial.count == 0

    # Mutate
    counter.count = 1

    # Check update
    update = await anext(iterator)
    assert update.count == 1

    counter.count += 5
    update2 = await anext(iterator)
    assert update2.count == 6
