"""Validated, timezone-aware cron primitives for the quality scheduler."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from apscheduler.triggers.cron import CronTrigger
from supertable.utils.diagnostic_redaction import safe_exception_type


class CronValidationError(ValueError):
    """A persisted cron expression or IANA timezone is invalid."""


@dataclass(frozen=True)
class CronSchedule:
    expression: str
    timezone_name: str
    timezone: ZoneInfo
    trigger: CronTrigger

    @classmethod
    def parse(cls, expression: str, timezone_name: str = "UTC") -> "CronSchedule":
        if not isinstance(expression, str) or not expression.strip():
            raise CronValidationError("cron expression must be non-empty text")
        if not isinstance(timezone_name, str) or not timezone_name.strip():
            raise CronValidationError("timezone must be a non-empty IANA name")
        expression = expression.strip()
        timezone_name = timezone_name.strip()
        try:
            tz = ZoneInfo(timezone_name)
        except (ZoneInfoNotFoundError, ValueError):
            raise CronValidationError("unknown IANA timezone") from None
        try:
            trigger = CronTrigger.from_crontab(expression, timezone=tz)
        except (TypeError, ValueError) as exc:
            raise CronValidationError(
                "invalid five-field cron expression; "
                f"error_type={safe_exception_type(exc)}"
            ) from None
        return cls(expression, timezone_name, tz, trigger)

    def next_after_ms(self, epoch_ms: int) -> int:
        """Return the next scheduled instant strictly after ``epoch_ms``.

        APScheduler evaluates calendar fields in ``timezone`` and carries the
        PEP-495 fold through repeated wall-clock instants. Non-existent spring
        times map to the first corresponding real instant, so DST changes do
        not turn a daily schedule into a fixed-duration interval.
        """

        try:
            value = int(epoch_ms)
        except (TypeError, ValueError):
            raise CronValidationError(
                "cron base time must be epoch milliseconds"
            ) from None
        base = datetime.fromtimestamp(value / 1000.0, tz=self.timezone)
        next_fire = self.trigger.get_next_fire_time(None, base)
        if next_fire is None:
            raise CronValidationError("cron expression has no future fire time")
        next_ms = int(next_fire.timestamp() * 1000)
        if next_ms <= value:
            # from_crontab may return an exactly-equal boundary. Feed that fire
            # back as the previous occurrence to obtain a strict successor.
            next_fire = self.trigger.get_next_fire_time(next_fire, next_fire)
            if next_fire is None:
                raise CronValidationError("cron expression has no future fire time")
            next_ms = int(next_fire.timestamp() * 1000)
        if next_ms <= value:
            raise CronValidationError("cron library did not advance the schedule")
        return next_ms


def validate_cron(expression: str, timezone_name: str = "UTC") -> None:
    CronSchedule.parse(expression, timezone_name)


__all__ = ["CronSchedule", "CronValidationError", "validate_cron"]
