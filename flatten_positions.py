import logging
import sys
import time
from dataclasses import dataclass

from tinkoff.invest import Client, OrderDirection
from tinkoff.invest.exceptions import RequestError

from bot_oil_main import (
    APP_NAME,
    build_telegram_card,
    current_moscow_time,
    get_live_portfolio_positions,
    load_config,
    place_market_order,
    resolve_instruments,
    send_msg,
)


@dataclass
class FlattenResult:
    symbol: str
    side: str
    qty: int
    order_id: str | None
    status: str
    detail: str


def format_result_line(item: FlattenResult) -> str:
    base = f"{item.symbol}: {item.side} {item.qty} лот."
    if item.order_id:
        base += f" order_id={item.order_id}."
    return f"{base} {item.status}: {item.detail}"


def flatten_all_positions(reason: str) -> int:
    config = load_config()
    logging.info("Начинаю принудительное закрытие позиций. Причина: %s", reason)

    with Client(config.token, app_name=f"{APP_NAME}-flatten", target=config.target) as client:
        watchlist = resolve_instruments(client, config)
        instrument_by_symbol = {item.symbol: item for item in watchlist}
        positions = get_live_portfolio_positions(client, config, watchlist)

        if not positions:
            message = build_telegram_card(
                "Принудительное закрытие позиций",
                "🟢",
                [
                    f"Причина: {reason}",
                    "Открытых позиций у брокера не найдено.",
                ],
            )
            send_msg(config, message)
            logging.info("Открытых позиций у брокера нет.")
            return 0

        submitted: list[FlattenResult] = []
        for symbol, payload in positions.items():
            instrument = instrument_by_symbol.get(symbol)
            if instrument is None:
                submitted.append(
                    FlattenResult(
                        symbol=symbol,
                        side=str(payload.get("side") or ""),
                        qty=int(payload.get("qty") or 0),
                        order_id=None,
                        status="ошибка",
                        detail="Инструмент не найден в watchlist.",
                    )
                )
                continue

            side = str(payload.get("side") or "")
            qty = int(payload.get("qty") or 0)
            if qty <= 0 or side not in {"LONG", "SHORT"}:
                submitted.append(
                    FlattenResult(
                        symbol=symbol,
                        side=side,
                        qty=qty,
                        order_id=None,
                        status="пропуск",
                        detail="Позиция уже плоская или размер некорректен.",
                    )
                )
                continue

            direction = (
                OrderDirection.ORDER_DIRECTION_SELL
                if side == "LONG"
                else OrderDirection.ORDER_DIRECTION_BUY
            )
            try:
                order_id = place_market_order(client, config, instrument, qty, direction)
                submitted.append(
                    FlattenResult(
                        symbol=symbol,
                        side=side,
                        qty=qty,
                        order_id=order_id,
                        status="отправлено",
                        detail="Заявка на закрытие отправлена в брокер.",
                    )
                )
                logging.info("force_flatten symbol=%s side=%s qty=%s order_id=%s", symbol, side, qty, order_id)
            except RequestError as error:
                submitted.append(
                    FlattenResult(
                        symbol=symbol,
                        side=side,
                        qty=qty,
                        order_id=None,
                        status="ошибка",
                        detail=str(error),
                    )
                )
                logging.exception("Не удалось отправить закрытие для %s", symbol)

        time.sleep(5)
        remaining_positions = get_live_portfolio_positions(client, config, watchlist)
        remaining_lines = []
        for symbol, payload in remaining_positions.items():
            remaining_lines.append(
                f"{symbol}: {payload.get('side')} {payload.get('qty')} лот."
            )

        lines = [
            f"Причина: {reason}",
            f"Время: {current_moscow_time().strftime('%d.%m %H:%M:%S МСК')}",
            "",
            "Результат отправки:",
            *[f"• {format_result_line(item)}" for item in submitted],
        ]
        if remaining_lines:
            lines.extend(["", "После попытки закрытия у брокера ещё остались позиции:"])
            lines.extend(f"• {line}" for line in remaining_lines)
        else:
            lines.extend(["", "После попытки закрытия открытых позиций у брокера не осталось."])

        send_msg(config, build_telegram_card("Принудительное закрытие позиций", "🛑", lines))

        errors = [item for item in submitted if item.status == "ошибка"]
        return 1 if errors else 0


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    reason = "Принудительное закрытие позиций в конце торговой сессии"
    if len(sys.argv) > 1:
        reason = " ".join(sys.argv[1:]).strip() or reason
    return flatten_all_positions(reason)


if __name__ == "__main__":
    raise SystemExit(main())
