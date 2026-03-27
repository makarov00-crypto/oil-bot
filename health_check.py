import os
from dataclasses import dataclass

import requests
from dotenv import load_dotenv
from tinkoff.invest import Client, RequestError
from tinkoff.invest.constants import INVEST_GRPC_API, INVEST_GRPC_API_SANDBOX


load_dotenv()


@dataclass
class CheckConfig:
    token: str
    account_id: str
    symbols: list[str]
    target: str
    tg_token: str | None
    tg_chat_id: str | None


def quotation_to_float(value) -> float:
    if value is None:
        return 0.0
    return float(getattr(value, "units", 0) or 0) + float(getattr(value, "nano", 0) or 0) / 1e9


def load_config() -> CheckConfig:
    token = os.getenv("T_INVEST_TOKEN", "").strip()
    account_id = os.getenv("T_INVEST_ACCOUNT_ID", "").strip()
    symbols = [item.strip().upper() for item in os.getenv("T_INVEST_SYMBOLS", "").split(",") if item.strip()]
    target_name = os.getenv("T_INVEST_TARGET", "PROD").strip().upper()
    target = INVEST_GRPC_API_SANDBOX if target_name == "SANDBOX" else INVEST_GRPC_API

    missing = [
        name
        for name, value in (
            ("T_INVEST_TOKEN", token),
            ("T_INVEST_ACCOUNT_ID", account_id),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(f"Не заданы обязательные переменные окружения: {', '.join(missing)}")
    if not symbols:
        raise RuntimeError("Не заданы тикеры в T_INVEST_SYMBOLS")

    return CheckConfig(
        token=token,
        account_id=account_id,
        symbols=symbols,
        target=target,
        tg_token=os.getenv("TG_TOKEN", "").strip() or None,
        tg_chat_id=os.getenv("TG_CHAT_ID", "").strip() or None,
    )


def print_ok(name: str, details: str) -> None:
    print(f"[OK] {name}: {details}")


def print_fail(name: str, details: str) -> None:
    print(f"[FAIL] {name}: {details}")


def check_telegram(config: CheckConfig) -> bool:
    if not config.tg_token or not config.tg_chat_id:
        print_fail("telegram", "TG_TOKEN или TG_CHAT_ID не заданы")
        return False
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{config.tg_token}/sendMessage",
            json={"chat_id": config.tg_chat_id, "text": "health_check: Telegram доступен"},
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            print_fail("telegram", str(payload))
            return False
        print_ok("telegram", f"message_id={payload['result']['message_id']}")
        return True
    except requests.RequestException as error:
        print_fail("telegram", str(error))
        return False


def check_t_invest(config: CheckConfig) -> bool:
    try:
        with Client(config.token, target=config.target, app_name="oil-bot-health-check") as client:
            accounts = client.users.get_accounts().accounts
            account_ids = {account.id for account in accounts}
            if config.account_id not in account_ids:
                print_fail("account", f"account_id={config.account_id} не найден среди счетов")
                return False
            print_ok("account", f"найден account_id={config.account_id}")

            futures = client.instruments.futures().instruments
            lookup = {instrument.ticker.upper(): instrument for instrument in futures if instrument.ticker}
            for symbol in config.symbols:
                instrument = lookup.get(symbol)
                if instrument is None:
                    print_fail("instrument", f"не найден тикер {symbol}")
                    return False
                prices = client.market_data.get_last_prices(figi=[instrument.figi]).last_prices
                if not prices:
                    print_fail("market_data", f"нет last price для {symbol}")
                    return False
                print_ok(
                    "instrument",
                    f"{symbol} figi={instrument.figi} last_price={quotation_to_float(prices[0].price):.4f}",
                )
            return True
    except RequestError as error:
        print_fail("t_invest", str(error))
        return False
    except Exception as error:
        print_fail("t_invest", str(error))
        return False


def main() -> int:
    try:
        config = load_config()
    except Exception as error:
        print_fail("config", str(error))
        return 1
    print_ok("config", f"watchlist={', '.join(config.symbols)}")
    t_invest_ok = check_t_invest(config)
    telegram_ok = check_telegram(config)
    return 0 if t_invest_ok and telegram_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
