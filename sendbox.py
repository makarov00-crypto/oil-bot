import os
import sys

from dotenv import load_dotenv
from tinkoff.invest import Client, RequestError
from tinkoff.invest.constants import INVEST_GRPC_API, INVEST_GRPC_API_SANDBOX


TOKEN_ENV = "T_INVEST_TOKEN"
TARGET_ENV = "T_INVEST_TARGET"
APP_NAME = "oil-bot-account-checker"


load_dotenv()


def get_token() -> str:
    token = os.getenv(TOKEN_ENV, "").strip()
    if not token:
        raise RuntimeError(
            f"Не задан токен. Установи переменную окружения {TOKEN_ENV}."
        )
    return token


def get_target() -> str:
    target_name = os.getenv(TARGET_ENV, "PROD").strip().upper()
    if target_name == "SANDBOX":
        return INVEST_GRPC_API_SANDBOX
    return INVEST_GRPC_API


def main() -> int:
    try:
        token = get_token()
        target = get_target()
        with Client(token, app_name=APP_NAME, target=target) as client:
            accounts = client.users.get_accounts().accounts

        if not accounts:
            print(f"Счета не найдены. target={os.getenv(TARGET_ENV, 'PROD').upper()}")
            return 0

        print("Найденные счета:")
        for account in accounts:
            print(
                f"account_id={account.id} | type={account.type.name} | "
                f"status={account.status.name} | name={account.name or '-'}"
            )
        return 0

    except RequestError as error:
        print(f"Ошибка API T-Invest: {error}", file=sys.stderr)
        return 1
    except Exception as error:
        print(f"Ошибка: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
