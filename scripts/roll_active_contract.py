from __future__ import annotations

import argparse

from active_contracts import upsert_active_contract


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Переключает активный контракт для шаблонного тикера.")
    parser.add_argument("--template", required=True, help="Шаблонный тикер, например BMM6 или NGK6")
    parser.add_argument("--symbol", help="Новый активный тикер, например BMN6")
    parser.add_argument("--disable", action="store_true", help="Отключить шаблонный инструмент из активного watchlist")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.disable and not args.symbol:
        raise SystemExit("Нужно передать --symbol для ролловера или --disable для отключения инструмента.")
    result = upsert_active_contract(args.template, args.symbol, disabled=args.disable)
    if args.disable:
        print(f"{result['template_symbol']} отключён из активного watchlist.")
    else:
        print(f"{result['template_symbol']} -> {result['active_symbol']} обновлён.")


if __name__ == "__main__":
    main()
