"""Structured logging, retry with exponential backoff, and run tracking.

Every step logs records read / written / quarantined plus start, end, duration
and status. The :class:`RunTracker` collects these into a run manifest that is
written to the lake under ``_runs/`` so every run is auditable afterwards.
"""

from __future__ import annotations

import functools
import json
import logging
import sys
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterator, TypeVar

T = TypeVar("T")

_RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """Render each log record as one JSON line (easy to grep, parse and ship)."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: D102
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str = "INFO") -> None:
    """Configure root logging once, writing JSON lines to stdout."""
    root = logging.getLogger()
    if any(getattr(h, "_talent_pipeline", False) for h in root.handlers):
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler._talent_pipeline = True  # type: ignore[attr-defined]
    root.handlers = [handler]
    root.setLevel(level)
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a module logger."""
    return logging.getLogger(name)


def with_retry(
    operation: str,
    max_retries: int = 4,
    base_delay: float = 1.0,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Retry a call with exponential backoff (``base_delay * 2**attempt``).

    Used around S3 and PostgreSQL calls so transient network or throttling
    errors do not fail the whole run. After the last attempt the original
    exception is re-raised so the failure is still visible.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            log = get_logger("pipeline.retry")
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_on as exc:  # type: ignore[misc]
                    if attempt >= max_retries:
                        log.error(
                            "retries exhausted",
                            extra={"operation": operation, "attempts": attempt + 1, "error": repr(exc)},
                        )
                        raise
                    delay = base_delay * (2**attempt)
                    log.warning(
                        "transient failure, retrying",
                        extra={"operation": operation, "attempt": attempt + 1, "delay_s": delay, "error": repr(exc)},
                    )
                    time.sleep(delay)
            raise RuntimeError("unreachable")  # pragma: no cover

        return wrapper

    return decorator


@dataclass
class StepResult:
    """Metrics for one pipeline step (e.g. ``silver.employees``)."""

    name: str
    started_at: str
    ended_at: str | None = None
    duration_s: float | None = None
    status: str = "running"
    metrics: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class RunTracker:
    """Collects per-step metrics and reconciliation results for one run."""

    def __init__(self, run_id: str, run_date: str, pipeline_version: str, settings_snapshot: dict[str, Any]):
        self.run_id = run_id
        self.run_date = run_date
        self.pipeline_version = pipeline_version
        self.settings_snapshot = settings_snapshot
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.steps: list[StepResult] = []
        self.checks: list[dict[str, Any]] = []
        self.status = "running"
        self._log = get_logger("pipeline.run")

    @contextmanager
    def step(self, name: str) -> Iterator[dict[str, Any]]:
        """Time a step; the yielded dict is where the step records its metrics."""
        result = StepResult(name=name, started_at=datetime.now(timezone.utc).isoformat())
        self.steps.append(result)
        start = time.perf_counter()
        self._log.info("step started", extra={"step": name, "run_id": self.run_id})
        try:
            yield result.metrics
            result.status = "success"
        except Exception as exc:
            result.status = "failed"
            result.error = repr(exc)
            self._log.error("step failed", extra={"step": name, "run_id": self.run_id, "error": repr(exc)},
                            exc_info=True)
            raise
        finally:
            result.duration_s = round(time.perf_counter() - start, 3)
            result.ended_at = datetime.now(timezone.utc).isoformat()
            if result.status == "success":
                self._log.info(
                    "step finished",
                    extra={"step": name, "run_id": self.run_id, "duration_s": result.duration_s, **result.metrics},
                )

    def metric(self, step_name: str) -> dict[str, Any]:
        """Return the metrics recorded by an earlier step (empty dict if absent)."""
        for step in reversed(self.steps):
            if step.name == step_name:
                return step.metrics
        return {}

    def to_manifest(self) -> dict[str, Any]:
        """Serialisable summary of the whole run."""
        ended = datetime.now(timezone.utc)
        return {
            "run_id": self.run_id,
            "run_date": self.run_date,
            "pipeline_version": self.pipeline_version,
            "status": self.status,
            "started_at": self.started_at,
            "ended_at": ended.isoformat(),
            "duration_s": round((ended - datetime.fromisoformat(self.started_at)).total_seconds(), 3),
            "settings": self.settings_snapshot,
            "steps": [asdict(s) for s in self.steps],
            "reconciliation": self.checks,
        }
