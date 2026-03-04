"""Cron service for scheduling agent tasks."""

import asyncio
import json
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Coroutine

from loguru import logger

from nanobot.cron.types import CronJob, CronJobState, CronPayload, CronSchedule, CronStore


def _now_ms() -> int:
    return int(time.time() * 1000)


def _compute_next_run(schedule: CronSchedule, now_ms: int) -> int | None:
    """Compute next run time in ms."""
    if schedule.kind == "at":
        return schedule.at_ms if schedule.at_ms and schedule.at_ms > now_ms else None
    
    if schedule.kind == "every":
        if not schedule.every_ms or schedule.every_ms <= 0:
            return None
        # Next interval from now
        return now_ms + schedule.every_ms
    
    if schedule.kind == "cron" and schedule.expr:
        try:
            from croniter import croniter
            from zoneinfo import ZoneInfo
            # Use caller-provided reference time for deterministic scheduling
            base_time = now_ms / 1000
            tz = ZoneInfo(schedule.tz) if schedule.tz else datetime.now().astimezone().tzinfo
            base_dt = datetime.fromtimestamp(base_time, tz=tz)
            cron = croniter(schedule.expr, base_dt)
            next_dt = cron.get_next(datetime)
            return int(next_dt.timestamp() * 1000)
        except Exception:
            return None
    
    return None


def _validate_schedule_for_add(schedule: CronSchedule) -> None:
    """Validate schedule fields that would otherwise create non-runnable jobs."""
    if schedule.tz and schedule.kind != "cron":
        raise ValueError("tz can only be used with cron schedules")

    if schedule.kind == "cron" and schedule.tz:
        try:
            from zoneinfo import ZoneInfo

            ZoneInfo(schedule.tz)
        except Exception:
            raise ValueError(f"unknown timezone '{schedule.tz}'") from None


class CronService:
    """Service for managing and executing scheduled jobs."""
    
    def __init__(
        self,
        store_path: Path,
        on_job: Callable[[CronJob], Coroutine[Any, Any, str | None]] | None = None
    ):
        self.store_path = store_path
        self.on_job = on_job  # Callback to execute job, returns response text
        self._store: CronStore | None = None
        self._timer_task: asyncio.Task | None = None
        self._running = False
    
    def _load_store(self) -> CronStore:
        """Load jobs from disk."""
        if self._store:
            return self._store
        
        if self.store_path.exists():
            try:
                data = json.loads(self.store_path.read_text(encoding="utf-8"))
                jobs = []
                for j in data.get("jobs", []):
                    jobs.append(CronJob(
                        id=j["id"],
                        name=j["name"],
                        enabled=j.get("enabled", True),
                        schedule=CronSchedule(
                            kind=j["schedule"]["kind"],
                            at_ms=j["schedule"].get("atMs"),
                            every_ms=j["schedule"].get("everyMs"),
                            expr=j["schedule"].get("expr"),
                            tz=j["schedule"].get("tz"),
                        ),
                        payload=CronPayload(
                            kind=j["payload"].get("kind", "agent_turn"),
                            message=j["payload"].get("message", ""),
                            deliver=j["payload"].get("deliver", False),
                            channel=j["payload"].get("channel"),
                            to=j["payload"].get("to"),
                        ),
                        state=CronJobState(
                            next_run_at_ms=j.get("state", {}).get("nextRunAtMs"),
                            last_run_at_ms=j.get("state", {}).get("lastRunAtMs"),
                            last_status=j.get("state", {}).get("lastStatus"),
                            last_error=j.get("state", {}).get("lastError"),
                        ),
                        created_at_ms=j.get("createdAtMs", 0),
                        updated_at_ms=j.get("updatedAtMs", 0),
                        delete_after_run=j.get("deleteAfterRun", False),
                    ))
                self._store = CronStore(jobs=jobs)
            except Exception as e:
                logger.warning("Failed to load cron store: {}", e)
                self._store = CronStore()
        else:
            self._store = CronStore()
        
        return self._store
    
    def _save_store(self) -> None:
        """Save jobs to disk."""
        if not self._store:
            return
        
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        
        data = {
            "version": self._store.version,
            "jobs": [
                {
                    "id": j.id,
                    "name": j.name,
                    "enabled": j.enabled,
                    "schedule": {
                        "kind": j.schedule.kind,
                        "atMs": j.schedule.at_ms,
                        "everyMs": j.schedule.every_ms,
                        "expr": j.schedule.expr,
                        "tz": j.schedule.tz,
                    },
                    "payload": {
                        "kind": j.payload.kind,
                        "message": j.payload.message,
                        "deliver": j.payload.deliver,
                        "channel": j.payload.channel,
                        "to": j.payload.to,
                    },
                    "state": {
                        "nextRunAtMs": j.state.next_run_at_ms,
                        "lastRunAtMs": j.state.last_run_at_ms,
                        "lastStatus": j.state.last_status,
                        "lastError": j.state.last_error,
                    },
                    "createdAtMs": j.created_at_ms,
                    "updatedAtMs": j.updated_at_ms,
                    "deleteAfterRun": j.delete_after_run,
                }
                for j in self._store.jobs
            ]
        }
        
        self.store_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    
    async def start(self, run_catchup: bool = False) -> list[CronJob] | None:
        """Start the cron service.

        Args:
            run_catchup: If True, execute missed jobs from the last scheduled time.

        Returns:
            List of jobs that were caught up and executed (only when run_catchup=True).
        """
        self._running = True
        self._load_store()
        missed = self._recompute_next_runs(catchup=run_catchup)

        # Execute missed jobs if catchup is enabled
        caught_up: list[CronJob] = []
        if run_catchup and missed and self.on_job:
            for job in missed:
                logger.info("Cron: catching up job '{}' ({})", job.name, job.id)
                await self._execute_job(job)
                caught_up.append(job)

        self._save_store()
        self._arm_timer()
        logger.info("Cron service started with {} jobs", len(self._store.jobs if self._store else []))
        return caught_up if run_catchup else None
    
    def stop(self) -> None:
        """Stop the cron service."""
        self._running = False
        if self._timer_task:
            self._timer_task.cancel()
            self._timer_task = None
    
    def _recompute_next_runs(self, catchup: bool = False) -> list[CronJob]:
        """Recompute next run times for enabled jobs that need it.

        Only recompute if:
        1. next_run_at_ms is None (new job or reset)
        2. next_run_at_ms has already passed (missed run, schedule next valid time)
        Otherwise, preserve the existing next_run_at_ms to avoid skipping scheduled runs.

        Args:
            catchup: If True, return jobs that were missed (for catchup execution).

        Returns:
            List of jobs that were missed (only when catchup=True).
        """
        missed_jobs: list[CronJob] = []
        if not self._store:
            return missed_jobs
        now = _now_ms()
        for job in self._store.jobs:
            if job.enabled:
                next_run = job.state.next_run_at_ms
                # Only recompute if no next_run set, or if it has already passed
                if next_run is None or next_run <= now:
                    if catchup and next_run is not None and next_run <= now:
                        # This is a missed job - check if it should have run today
                        if job.schedule.kind == "cron":
                            missed_jobs.append(job)
                    job.state.next_run_at_ms = _compute_next_run(job.schedule, now)
        return missed_jobs
    
    def _get_next_wake_ms(self) -> int | None:
        """Get the earliest next run time across all jobs."""
        if not self._store:
            return None
        times = [j.state.next_run_at_ms for j in self._store.jobs 
                 if j.enabled and j.state.next_run_at_ms]
        return min(times) if times else None
    
    def _arm_timer(self) -> None:
        """Schedule the next timer tick."""
        if self._timer_task:
            self._timer_task.cancel()
        
        next_wake = self._get_next_wake_ms()
        if not next_wake or not self._running:
            return
        
        delay_ms = max(0, next_wake - _now_ms())
        delay_s = delay_ms / 1000
        
        async def tick():
            await asyncio.sleep(delay_s)
            if self._running:
                await self._on_timer()
        
        self._timer_task = asyncio.create_task(tick())
    
    async def _on_timer(self) -> None:
        """Handle timer tick - run due jobs."""
        if not self._store:
            return
        
        now = _now_ms()
        due_jobs = [
            j for j in self._store.jobs
            if j.enabled and j.state.next_run_at_ms and now >= j.state.next_run_at_ms
        ]
        
        for job in due_jobs:
            await self._execute_job(job)
        
        self._save_store()
        self._arm_timer()
    
    async def _execute_job(self, job: CronJob) -> None:
        """Execute a single job."""
        start_ms = _now_ms()
        logger.info("Cron: executing job '{}' ({})", job.name, job.id)
        
        try:
            response = None
            if self.on_job:
                response = await self.on_job(job)
            
            job.state.last_status = "ok"
            job.state.last_error = None
            logger.info("Cron: job '{}' completed", job.name)
            
        except Exception as e:
            job.state.last_status = "error"
            job.state.last_error = str(e)
            logger.error("Cron: job '{}' failed: {}", job.name, e)
        
        job.state.last_run_at_ms = start_ms
        job.updated_at_ms = _now_ms()
        
        # Handle one-shot jobs
        if job.schedule.kind == "at":
            if job.delete_after_run:
                self._store.jobs = [j for j in self._store.jobs if j.id != job.id]
            else:
                job.enabled = False
                job.state.next_run_at_ms = None
        else:
            # Compute next run
            job.state.next_run_at_ms = _compute_next_run(job.schedule, _now_ms())
    
    # ========== Public API ==========
    
    def list_jobs(self, include_disabled: bool = False) -> list[CronJob]:
        """List all jobs."""
        store = self._load_store()
        jobs = store.jobs if include_disabled else [j for j in store.jobs if j.enabled]
        return sorted(jobs, key=lambda j: j.state.next_run_at_ms or float('inf'))
    
    def add_job(
        self,
        name: str,
        schedule: CronSchedule,
        message: str,
        deliver: bool = False,
        channel: str | None = None,
        to: str | None = None,
        delete_after_run: bool = False,
    ) -> CronJob:
        """Add a new job."""
        store = self._load_store()
        _validate_schedule_for_add(schedule)
        now = _now_ms()
        
        job = CronJob(
            id=str(uuid.uuid4())[:8],
            name=name,
            enabled=True,
            schedule=schedule,
            payload=CronPayload(
                kind="agent_turn",
                message=message,
                deliver=deliver,
                channel=channel,
                to=to,
            ),
            state=CronJobState(next_run_at_ms=_compute_next_run(schedule, now)),
            created_at_ms=now,
            updated_at_ms=now,
            delete_after_run=delete_after_run,
        )
        
        store.jobs.append(job)
        self._save_store()
        self._arm_timer()
        
        logger.info("Cron: added job '{}' ({})", name, job.id)
        return job
    
    def remove_job(self, job_id: str) -> bool:
        """Remove a job by ID."""
        store = self._load_store()
        before = len(store.jobs)
        store.jobs = [j for j in store.jobs if j.id != job_id]
        removed = len(store.jobs) < before
        
        if removed:
            self._save_store()
            self._arm_timer()
            logger.info("Cron: removed job {}", job_id)
        
        return removed
    
    def enable_job(self, job_id: str, enabled: bool = True) -> CronJob | None:
        """Enable or disable a job."""
        store = self._load_store()
        for job in store.jobs:
            if job.id == job_id:
                job.enabled = enabled
                job.updated_at_ms = _now_ms()
                if enabled:
                    job.state.next_run_at_ms = _compute_next_run(job.schedule, _now_ms())
                else:
                    job.state.next_run_at_ms = None
                self._save_store()
                self._arm_timer()
                return job
        return None
    
    async def run_job(self, job_id: str, force: bool = False) -> bool:
        """Manually run a job."""
        store = self._load_store()
        for job in store.jobs:
            if job.id == job_id:
                if not force and not job.enabled:
                    return False
                await self._execute_job(job)
                self._save_store()
                self._arm_timer()
                return True
        return False
    
    def status(self) -> dict:
        """Get service status."""
        store = self._load_store()
        return {
            "enabled": self._running,
            "jobs": len(store.jobs),
            "next_wake_at_ms": self._get_next_wake_ms(),
        }

    def get_missed_jobs(self, lookback_ms: int | None = None) -> list[CronJob]:
        """Get list of jobs that should have run but didn't.

        Args:
            lookback_ms: How far back to check for missed runs. If None,
                        only checks since last_run_at_ms.

        Returns:
            List of jobs that have missed their scheduled run time.
        """
        store = self._load_store()
        now = _now_ms()
        missed = []

        for job in store.jobs:
            if not job.enabled:
                continue

            # For cron schedules, check if we missed a run
            if job.schedule.kind == "cron" and job.schedule.expr:
                from croniter import croniter
                from zoneinfo import ZoneInfo

                tz = ZoneInfo(job.schedule.tz) if job.schedule.tz else datetime.now().astimezone().tzinfo
                last_run = job.state.last_run_at_ms

                # If never run, check from schedule creation time
                if last_run is None:
                    last_run = job.created_at_ms

                # Get all scheduled times between last_run and now
                last_run_dt = datetime.fromtimestamp(last_run / 1000, tz=tz)
                now_dt = datetime.fromtimestamp(now / 1000, tz=tz)

                cron = croniter(job.schedule.expr, last_run_dt)
                next_scheduled = cron.get_next(datetime)

                # If there's a scheduled time between last_run and now, we missed it
                if next_scheduled < now_dt:
                    missed.append(job)

            # For every schedules, check based on interval
            elif job.schedule.kind == "every" and job.schedule.every_ms:
                last_run = job.state.last_run_at_ms
                if last_run is None:
                    last_run = job.created_at_ms

                elapsed = now - last_run
                if elapsed >= job.schedule.every_ms:
                    missed.append(job)

        return missed

    async def run_missed_jobs(self) -> list[tuple[str, str]]:
        """Execute all missed jobs and return results.

        Returns:
            List of (job_id, status) tuples where status is "ok" or error message.
        """
        missed = self.get_missed_jobs()
        results = []

        for job in missed:
            await self._execute_job(job)
            status = job.state.last_status or "unknown"
            if job.state.last_error:
                status = f"error: {job.state.last_error}"
            results.append((job.id, status))

        if missed:
            self._save_store()

        return results
