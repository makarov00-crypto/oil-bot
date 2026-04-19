from __future__ import annotations

import argparse
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from daily_ai_review import (
    BASE_DIR,
    DEFAULT_MODEL,
    build_review_prompt,
    parse_target_date,
    request_openai_review,
    save_review,
)
from dotenv import load_dotenv


MOSCOW_TZ = ZoneInfo("Europe/Moscow")
REMOTE_RUNTIME_DIR = Path("logs/remote_runtime")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run AI review locally against server runtime data")
    parser.add_argument("--ssh", default="oilbot@5.101.114.51", help="SSH target for the trading server")
    parser.add_argument("--remote-dir", default="/opt/oil-bot", help="Project directory on the remote server")
    parser.add_argument("--date", dest="target_date", help="Дата в формате YYYY-MM-DD")
    parser.add_argument("--preview", action="store_true", help="Только показать prompt без вызова OpenAI")
    parser.add_argument("--model", default=None, help="Модель OpenAI")
    parser.add_argument(
        "--output",
        default=str(BASE_DIR / "logs" / "ai_reviews" / "latest_review.md"),
        help="Локальный путь для сохранения review",
    )
    parser.add_argument(
        "--publish-to-server",
        action="store_true",
        help="После локального review загрузить markdown обратно на сервер в logs/ai_reviews",
    )
    return parser.parse_args()


def sync_remote_runtime(ssh_target: str, remote_dir: str, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ssh",
        ssh_target,
        (
            f"cd {remote_dir} && "
            "tar -cf - "
            "bot_state "
            "logs/trade_journal.jsonl "
            "logs/ai_reviews "
            "2>/dev/null || true"
        ),
    ]
    proc = subprocess.run(cmd, capture_output=True, check=True)
    tar_proc = subprocess.run(
        ["tar", "-xf", "-", "-C", str(destination)],
        input=proc.stdout,
        capture_output=True,
        check=True,
    )
    if tar_proc.returncode != 0:
        raise RuntimeError("Не удалось распаковать runtime с сервера")


def publish_review(ssh_target: str, remote_dir: str, local_review: Path, target_day: str) -> None:
    remote_latest = f"{ssh_target}:{remote_dir}/logs/ai_reviews/latest_review.md"
    remote_dated = f"{ssh_target}:{remote_dir}/logs/ai_reviews/{target_day}_review.md"
    subprocess.run(["ssh", ssh_target, f"mkdir -p {remote_dir}/logs/ai_reviews"], check=True)
    subprocess.run(["scp", str(local_review), remote_latest], check=True)
    subprocess.run(["scp", str(local_review), remote_dated], check=True)


def main() -> int:
    load_dotenv(BASE_DIR / ".env")
    args = parse_args()
    target_day = parse_target_date(args.target_date)
    model = args.model or DEFAULT_MODEL
    sync_dir = BASE_DIR / REMOTE_RUNTIME_DIR / "server_snapshot"

    sync_remote_runtime(args.ssh, args.remote_dir, sync_dir)
    prompt = build_review_prompt(sync_dir, target_day)

    if args.preview:
        from daily_ai_review import SYSTEM_INSTRUCTIONS

        print("=== SYSTEM ===")
        print(SYSTEM_INSTRUCTIONS)
        print("\n=== USER ===")
        print(prompt)
        return 0

    import os

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("Не найден OPENAI_API_KEY в локальном .env")

    review_text = request_openai_review(api_key, model, prompt)
    output_path = Path(args.output).expanduser().resolve()
    save_review(output_path, target_day, model, review_text)

    if args.publish_to_server:
        publish_review(args.ssh, args.remote_dir, output_path, target_day.isoformat())

    print(review_text)
    print(f"\nСохранено локально: {output_path}")
    if args.publish_to_server:
        print(f"Опубликовано на сервер: {args.remote_dir}/logs/ai_reviews/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
