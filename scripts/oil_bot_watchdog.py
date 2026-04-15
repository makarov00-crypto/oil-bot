#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_BASE_DIR = Path("/opt/oil-bot")
DEFAULT_SERVICE = "oil-bot"
DEFAULT_STALE_SECONDS = 10 * 60
DEFAULT_MIN_RESTART_INTERVAL_SECONDS = 10 * 60


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def append_log(log_path: Path, message: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = utc_now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"[{timestamp}] {message}\n")


def read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def service_is_active(service_name: str, dry_run: bool) -> bool:
    try:
        result = subprocess.run(
            ["systemctl", "is-active", "--quiet", service_name],
            check=False,
        )
    except FileNotFoundError:
        if dry_run:
            return True
        raise
    return result.returncode == 0


def restart_service(service_name: str, dry_run: bool) -> None:
    if dry_run:
        return
    subprocess.run(["systemctl", "restart", service_name], check=True)


def should_throttle_restart(marker_path: Path, min_interval_seconds: int, now: datetime) -> bool:
    last_restart = parse_dt(marker_path.read_text(encoding="utf-8").strip()) if marker_path.exists() else None
    if not last_restart:
        return False
    return (now - last_restart).total_seconds() < min_interval_seconds


def write_restart_marker(marker_path: Path, now: datetime, dry_run: bool) -> None:
    if dry_run:
        return
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text(now.isoformat(), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Restart oil-bot when runtime heartbeat is stale.")
    parser.add_argument("--base-dir", default=str(DEFAULT_BASE_DIR))
    parser.add_argument("--service", default=DEFAULT_SERVICE)
    parser.add_argument("--stale-seconds", type=int, default=DEFAULT_STALE_SECONDS)
    parser.add_argument("--min-restart-interval-seconds", type=int, default=DEFAULT_MIN_RESTART_INTERVAL_SECONDS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    base_dir = Path(args.base_dir).expanduser().resolve()
    runtime_path = base_dir / "bot_state" / "_runtime_status.json"
    log_path = base_dir / "logs" / "automation" / "oil_bot_watchdog.log"
    marker_path = base_dir / ".locks" / "oil_bot_watchdog_last_restart"
    now = utc_now()

    runtime = read_json(runtime_path)
    last_seen = parse_dt(runtime.get("last_cycle_at")) or parse_dt(runtime.get("updated_at"))
    active = service_is_active(args.service, args.dry_run)

    if not active:
        reason = f"service {args.service} is not active"
        if should_throttle_restart(marker_path, args.min_restart_interval_seconds, now):
            append_log(log_path, f"skip_restart throttled reason='{reason}'")
            return 0
        restart_service(args.service, args.dry_run)
        write_restart_marker(marker_path, now, args.dry_run)
        append_log(log_path, f"restart_service reason='{reason}' dry_run={args.dry_run}")
        return 0

    if not last_seen:
        reason = f"runtime heartbeat is missing in {runtime_path}"
    else:
        age_seconds = (now - last_seen).total_seconds()
        if age_seconds <= args.stale_seconds:
            append_log(log_path, f"ok service={args.service} heartbeat_age_seconds={age_seconds:.0f}")
            return 0
        reason = f"runtime heartbeat stale: age_seconds={age_seconds:.0f}, threshold={args.stale_seconds}"

    if should_throttle_restart(marker_path, args.min_restart_interval_seconds, now):
        append_log(log_path, f"skip_restart throttled reason='{reason}'")
        return 0

    restart_service(args.service, args.dry_run)
    write_restart_marker(marker_path, now, args.dry_run)
    append_log(log_path, f"restart_service reason='{reason}' dry_run={args.dry_run}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
