"""
Rate limiter module for controlling API requests per minute (RPM).

This module provides a configurable rate limiter that can:
- Track and limit requests per minute
- Queue requests when approaching or at the limit
- Provide async context managers for automatic rate limiting
- Be exported and reused across different parts of the application
"""

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    max_requests_per_minute: int = 60
    """Maximum number of requests allowed per minute."""

    warning_threshold_percent: float = 80.0
    """Percentage threshold to start warning about approaching limit."""

    enable_queue: bool = True
    """Whether to queue requests when limit is reached."""

    max_queue_size: int = 100
    """Maximum number of requests that can be queued."""

    queue_timeout_seconds: float = 60.0
    """Maximum time a request can wait in queue."""


@dataclass
class RequestWindow:
    """Tracks requests within a time window."""

    timestamps: deque[float] = field(default_factory=deque)
    window_start: float = field(default_factory=time.time)

    def cleanup(self, current_time: float, window_seconds: float = 60.0) -> None:
        """Remove timestamps outside the current window."""
        cutoff = current_time - window_seconds
        while self.timestamps and self.timestamps[0] < cutoff:
            self.timestamps.popleft()
        if not self.timestamps:
            self.window_start = current_time

    def count(self) -> int:
        """Return the number of requests in the current window."""
        return len(self.timestamps)

    def add(self, timestamp: float | None = None) -> None:
        """Add a request timestamp."""
        ts = timestamp if timestamp is not None else time.time()
        self.timestamps.append(ts)

    def get_oldest_timestamp(self) -> float | None:
        """Get the oldest timestamp in the window."""
        return self.timestamps[0] if self.timestamps else None

    def get_newest_timestamp(self) -> float | None:
        """Get the newest timestamp in the window."""
        return self.timestamps[-1] if self.timestamps else None


@dataclass
class QueuedRequest:
    """Represents a queued request waiting to be processed."""

    future: asyncio.Future[Any]
    enqueued_at: float = field(default_factory=time.time)
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)


class RateLimiter:
    """
    Async-safe rate limiter with request queuing capability.

    This class provides rate limiting functionality that can:
    - Track requests per minute using a sliding window
    - Warn when approaching the limit
    - Queue requests when the limit is reached
    - Process queued requests automatically when capacity becomes available
    """

    def __init__(self, config: RateLimitConfig | None = None):
        """Initialize the rate limiter with optional configuration."""
        self.config = config or RateLimitConfig()
        self._window = RequestWindow()
        self._queue: deque[QueuedRequest] = deque()
        self._lock = asyncio.Lock()
        self._processor_task: asyncio.Task[Any] | None = None
        self._shutdown = False
        self._current_usage_percent = 0.0
        self._total_requests = 0
        self._throttled_requests = 0

    @property
    def current_usage_percent(self) -> float:
        """Current usage as a percentage of the limit."""
        return self._current_usage_percent

    @property
    def remaining_requests(self) -> int:
        """Number of requests remaining in the current window."""
        return max(0, self.config.max_requests_per_minute - self._window.count())

    @property
    def is_near_limit(self) -> bool:
        """Check if we're near the rate limit."""
        return self._current_usage_percent >= self.config.warning_threshold_percent or \
               (self._window.count() / self.config.max_requests_per_minute * 100) >= self.config.warning_threshold_percent

    @property
    def is_at_limit(self) -> bool:
        """Check if we've reached the rate limit."""
        return self.remaining_requests == 0

    @property
    def queue_size(self) -> int:
        """Current number of queued requests."""
        return len(self._queue)

    @property
    def stats(self) -> dict[str, Any]:
        """Get current statistics."""
        return {
            "current_requests": self._window.count(),
            "max_requests_per_minute": self.config.max_requests_per_minute,
            "remaining_requests": self.remaining_requests,
            "usage_percent": self._current_usage_percent,
            "is_near_limit": self.is_near_limit,
            "is_at_limit": self.is_at_limit,
            "queue_size": self.queue_size,
            "max_queue_size": self.config.max_queue_size,
            "total_requests": self._total_requests,
            "throttled_requests": self._throttled_requests,
        }

    async def start(self) -> None:
        """Start the background queue processor."""
        if self.config.enable_queue and self._processor_task is None:
            self._shutdown = False
            self._processor_task = asyncio.create_task(self._process_queue())

    async def stop(self) -> None:
        """Stop the rate limiter and cancel pending queue items."""
        self._shutdown = True
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
            self._processor_task = None

        # Cancel all queued requests
        while self._queue:
            queued = self._queue.popleft()
            if not queued.future.done():
                queued.future.set_exception(
                    asyncio.CancelledError("Rate limiter shutdown")
                )

    async def acquire(self) -> bool:
        """
        Acquire permission to make a request.

        Returns True if the request can proceed immediately.
        Returns False if the request was queued or needs to wait.

        Raises:
            asyncio.TimeoutError: If request times out waiting in queue.
            RuntimeError: If queue is full.
        """
        async with self._lock:
            current_time = time.time()
            self._window.cleanup(current_time)

            count = self._window.count()
            self._current_usage_percent = (count / self.config.max_requests_per_minute) * 100

            # Check if we can proceed immediately
            if count < self.config.max_requests_per_minute:
                self._window.add(current_time)
                self._total_requests += 1
                return True

            # We're at the limit
            self._throttled_requests += 1

            if not self.config.enable_queue:
                # Calculate wait time until next slot is available
                wait_time = await self._calculate_wait_time()
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    self._window.cleanup(time.time())
                    self._window.add(time.time())
                    self._total_requests += 1
                    return True

            # Queue the request
            if len(self._queue) >= self.config.max_queue_size:
                raise RuntimeError(
                    f"Request queue is full (max size: {self.config.max_queue_size})"
                )

            future: asyncio.Future[Any] = asyncio.Future()
            queued = QueuedRequest(future=future)
            self._queue.append(queued)

            # Ensure processor is running
            if self._processor_task is None or self._processor_task.done():
                self._processor_task = asyncio.create_task(self._process_queue())

            # Wait for our turn with timeout
            try:
                await asyncio.wait_for(
                    asyncio.shield(future), timeout=self.config.queue_timeout_seconds
                )
                return True
            except asyncio.TimeoutError:
                # Remove from queue if still there
                try:
                    self._queue.remove(queued)
                except ValueError:
                    pass
                raise asyncio.TimeoutError(
                    f"Request timed out after waiting {self.config.queue_timeout_seconds}s in queue"
                )

    async def _calculate_wait_time(self) -> float:
        """Calculate how long to wait until the next request slot opens."""
        oldest = self._window.get_oldest_timestamp()
        if oldest is None:
            return 0.0

        current_time = time.time()
        wait_time = (oldest + 60.0) - current_time
        return max(0.0, wait_time)

    async def _process_queue(self) -> None:
        """Background task to process queued requests."""
        while not self._shutdown:
            try:
                async with self._lock:
                    if not self._queue:
                        await asyncio.sleep(0.1)
                        continue

                    current_time = time.time()
                    self._window.cleanup(current_time)
                    count = self._window.count()

                    if count >= self.config.max_requests_per_minute:
                        # Still at limit, wait a bit
                        wait_time = await self._calculate_wait_time()
                        await asyncio.sleep(min(wait_time, 0.5))
                        continue

                    # Process the next queued request
                    queued = self._queue.popleft()

                    # Check if request has timed out
                    elapsed = current_time - queued.enqueued_at
                    if elapsed > self.config.queue_timeout_seconds:
                        if not queued.future.done():
                            queued.future.set_exception(
                                asyncio.TimeoutError(
                                    f"Request timed out after {elapsed:.1f}s in queue"
                                )
                            )
                        continue

                    # Grant permission
                    self._window.add(current_time)
                    self._total_requests += 1
                    if not queued.future.done():
                        queued.future.set_result(True)

                await asyncio.sleep(0.05)  # Small delay between processing

            except asyncio.CancelledError:
                break
            except Exception:  # noqa: BLE001
                # Log error but keep processing
                await asyncio.sleep(1.0)

    async def __aenter__(self) -> "RateLimiter":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.stop()

    def reset(self) -> None:
        """Reset the rate limiter state."""
        self._window = RequestWindow()
        self._queue.clear()
        self._current_usage_percent = 0.0
        self._total_requests = 0
        self._throttled_requests = 0


class RateLimitedClient:
    """
    Wrapper for making rate-limited API calls.

    This class wraps any async function to enforce rate limiting.
    """

    def __init__(
        self,
        func: Any,
        config: RateLimitConfig | None = None,
        rate_limiter: RateLimiter | None = None,
    ):
        """
        Initialize the rate-limited client.

        Args:
            func: The async function to wrap
            config: Optional rate limit configuration
            rate_limiter: Optional existing rate limiter to use
        """
        self.func = func
        self.rate_limiter = rate_limiter or RateLimiter(config)

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Call the wrapped function with rate limiting."""
        await self.rate_limiter.acquire()
        return await self.func(*args, **kwargs)

    @property
    def stats(self) -> dict[str, Any]:
        """Get rate limiter statistics."""
        return self.rate_limiter.stats


# Convenience function to create a rate limiter with custom RPM
def create_rate_limiter(
    rpm: int = 60,
    warning_threshold: float = 80.0,
    enable_queue: bool = True,
    max_queue_size: int = 100,
    queue_timeout: float = 60.0,
) -> RateLimiter:
    """
    Create a rate limiter with custom configuration.

    Args:
        rpm: Maximum requests per minute
        warning_threshold: Percentage threshold for warnings
        enable_queue: Whether to queue requests at limit
        max_queue_size: Maximum queue size
        queue_timeout: Timeout for queued requests in seconds

    Returns:
        Configured RateLimiter instance
    """
    config = RateLimitConfig(
        max_requests_per_minute=rpm,
        warning_threshold_percent=warning_threshold,
        enable_queue=enable_queue,
        max_queue_size=max_queue_size,
        queue_timeout_seconds=queue_timeout,
    )
    return RateLimiter(config)


# Export commonly used classes and functions
__all__ = [
    "RateLimiter",
    "RateLimitConfig",
    "RateLimitedClient",
    "create_rate_limiter",
    "RequestWindow",
    "QueuedRequest",
]
