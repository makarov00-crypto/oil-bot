from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tbank_invest import Client

from bot_oil_main import (  # noqa: E402
    APP_NAME,
    InstrumentConfig,
    close_position,
    confirm_pending_close_from_broker,
    get_live_portfolio_positions,
    load_config,
    load_state,
    parse_state_datetime,
    quotation_to_float,
    resolve_instruments,
    sync_state_with_portfolio,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Закрывает одну открытую позицию по тикеру.")
    parser.add_argument("symbol", help="Тикер позиции, например NGN6")
    parser.add_argument("--reason", default="Ролловер контракта", help="Причина закрытия для журнала")
    parser.add_argument("--wait-seconds", type=int, default=60, help="Сколько ждать подтверждения закрытия")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    target_symbol = str(args.symbol or "").strip().upper()
    if not target_symbol:
        raise SystemExit("Нужно передать тикер позиции.")

    config = load_config()
    with Client(config.token, app_name=f"{APP_NAME}-single-close", target=config.target) as client:
        watchlist = resolve_instruments(client, config)
        instrument_by_symbol = {item.symbol.upper(): item for item in watchlist}
        instrument = instrument_by_symbol.get(target_symbol)
        if instrument is None:
            futures = client.instruments.futures().instruments
            item = next((future for future in futures if str(getattr(future, "ticker", "")).upper() == target_symbol), None)
            if item is None:
                raise SystemExit(f"{target_symbol}: инструмент не найден ни в watchlist, ни у брокера.")
            instrument = InstrumentConfig(
                symbol=target_symbol,
                figi=item.figi,
                display_name=item.name or target_symbol,
                lot=int(getattr(item, "lot", 1) or 1),
                min_price_increment=quotation_to_float(getattr(item, "min_price_increment", None)),
                min_price_increment_amount=quotation_to_float(getattr(item, "min_price_increment_amount", None)),
                initial_margin_on_buy=quotation_to_float(getattr(item, "initial_margin_on_buy", None)),
                initial_margin_on_sell=quotation_to_float(getattr(item, "initial_margin_on_sell", None)),
            )

        positions = get_live_portfolio_positions(client, config, [*watchlist, instrument])
        payload = positions.get(target_symbol)
        if payload is None:
            print(f"{target_symbol}: открытая позиция у брокера не найдена.")
            return 0

        state = load_state(instrument.symbol)
        broker_qty = int(payload.get("qty") or 0)
        broker_side = str(payload.get("side") or "").upper()
        if broker_qty <= 0 or broker_side not in {"LONG", "SHORT"}:
            print(f"{target_symbol}: позиция некорректна или уже закрыта: {payload}")
            return 0
        state.position_qty = broker_qty
        state.position_side = broker_side
        if payload.get("entry_price") is not None:
            state.entry_price = float(payload["entry_price"])
        previous_side = state.position_side
        previous_qty = state.position_qty
        previous_entry_price = state.entry_price
        previous_entry_commission = float(state.entry_commission_rub or 0.0)
        previous_strategy = state.entry_strategy or state.last_strategy_name or "manual_rollover"
        previous_entry_time = parse_state_datetime(state.entry_time or "")
        close_position(client, config, instrument, state, args.reason)
        print(f"{target_symbol}: отправлено закрытие {broker_side} {broker_qty} лот.")
        not_before = parse_state_datetime(state.pending_submitted_at or "") or previous_entry_time
        deadline = time.monotonic() + max(0, int(args.wait_seconds or 0))
        while time.monotonic() <= deadline:
            time.sleep(5)
            synced_qty = sync_state_with_portfolio(client, config, instrument, state)
            if synced_qty != 0:
                continue
            if confirm_pending_close_from_broker(
                client,
                config,
                instrument,
                state,
                previous_side=previous_side,
                previous_qty=previous_qty,
                previous_entry_price=previous_entry_price,
                previous_entry_commission=previous_entry_commission,
                previous_strategy=previous_strategy,
                previous_exit_reason=args.reason,
                previous_entry_time=previous_entry_time,
                source="manual_rollover",
                recovered_status="confirmed_close",
                not_before=not_before,
            ):
                print(f"{target_symbol}: закрытие подтверждено и записано в журнал.")
                return 0
        print(f"{target_symbol}: заявка отправлена, но подтверждение не найдено за {args.wait_seconds} сек.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
