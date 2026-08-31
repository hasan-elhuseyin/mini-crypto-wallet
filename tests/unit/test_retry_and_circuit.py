import pytest
from mcw_common.retry import CircuitBreaker, CircuitOpenError, RetryPolicy, retry_async

pytestmark = pytest.mark.unit


def test_backoff_grows_and_is_capped():
    policy = RetryPolicy(attempts=6, base_delay=1.0, max_delay=8.0, jitter=0.0)
    delays = [policy.delay_for(n) for n in range(1, 6)]
    assert delays == [1.0, 2.0, 4.0, 8.0, 8.0]


def test_jitter_stays_within_bounds():
    policy = RetryPolicy(base_delay=1.0, max_delay=10.0, jitter=0.25)
    for _ in range(100):
        assert 0.75 <= policy.delay_for(1) <= 1.25


async def test_retry_gives_up_and_reraises():
    calls = {"n": 0}

    async def always_fails():
        calls["n"] += 1
        raise ConnectionError("rpc down")

    async def no_sleep(_seconds):
        return None

    with pytest.raises(ConnectionError):
        await retry_async(
            always_fails, policy=RetryPolicy(attempts=3, base_delay=0), sleep=no_sleep
        )
    assert calls["n"] == 3


async def test_retry_returns_on_first_success():
    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] < 2:
            raise ConnectionError("rpc down")
        return "ok"

    async def no_sleep(_seconds):
        return None

    assert await retry_async(
        flaky, policy=RetryPolicy(attempts=5, base_delay=0), sleep=no_sleep
    ) == "ok"
    assert calls["n"] == 2


def test_circuit_opens_after_repeated_failures():
    breaker = CircuitBreaker(failure_threshold=3, reset_timeout=60)
    for _ in range(3):
        breaker.record_failure()
    assert breaker.is_open
    with pytest.raises(CircuitOpenError):
        breaker.guard()


def test_circuit_closes_after_success():
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout=60)
    breaker.record_failure()
    breaker.record_success()
    breaker.record_failure()
    assert not breaker.is_open
