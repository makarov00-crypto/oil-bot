from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from news_ingest import CHANNEL_URLS, detect_biases_for_posts, fetch_posts_for_day


BOT_STATE_DIR = Path(__file__).with_name("bot_state")


def load_bot_state() -> dict[str, dict]:
    states: dict[str, dict] = {}
    for path in sorted(BOT_STATE_DIR.glob("*.json")):
        try:
            states[path.stem] = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
    return states


def print_header(title: str) -> None:
    print()
    print(title)
    print("=" * len(title))


def main() -> int:
    target_day = datetime.now().astimezone().date()
    states = load_bot_state()

    print_header(f"Новостной отчёт за {target_day.isoformat()}")
    print("Каналы:", ", ".join(CHANNEL_URLS))

    symbol_events: dict[str, list[str]] = defaultdict(list)

    for channel in CHANNEL_URLS:
        posts = fetch_posts_for_day(channel, target_day=target_day)
        biased = detect_biases_for_posts(posts)

        print_header(f"Канал: {channel}")
        print(f"Постов за день: {len(posts)}")
        print(f"Постов с bias: {len(biased)}")

        for post, biases in biased[:20]:
            bias_lines = []
            for bias in biases:
                bias_lines.append(f"{bias.symbol}:{bias.bias}/{bias.strength}")
                symbol_events[bias.symbol].append(
                    f"{post.created_at.astimezone().strftime('%H:%M')} {channel} -> {bias.bias}/{bias.strength} ({bias.reason})"
                )
            print(f"- {post.created_at.astimezone().strftime('%H:%M')} | {post.url}")
            print(f"  {', '.join(bias_lines)}")
            preview = post.text.replace("\n", " ")
            print(f"  {preview[:180]}")

    print_header("Сопоставление с ботом")
    for symbol, state in sorted(states.items()):
        print(f"{symbol}: signal={state.get('last_signal')} error={state.get('last_error','') or '-'} candle={state.get('last_status_candle','-')}")
        events = symbol_events.get(symbol, [])
        if not events:
            print("  Новости по правилам не задетектированы.")
            continue
        for event in events[:5]:
            print(f"  {event}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
