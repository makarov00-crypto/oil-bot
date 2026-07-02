from __future__ import annotations

import itertools
import os
from pathlib import Path
from typing import Any, Optional, Sequence

import grpc
from grpc.aio import ClientInterceptor
from tinkoff.invest.async_services import AsyncServices
from tinkoff.invest.constants import MAX_RECEIVE_MESSAGE_LENGTH
from tinkoff.invest.services import Services
from tinkoff.invest.typedefs import ChannelArgumentType


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_ROOT_CERT_PATH = BASE_DIR / "certs" / "tbank_trust_bundle.pem"
DEFAULT_INVEST_GRPC_API = "invest-public-api.tbank.ru"
DEFAULT_INVEST_GRPC_API_SANDBOX = "sandbox-invest-public-api.tbank.ru"

INVEST_GRPC_API = os.getenv("T_INVEST_ENDPOINT_PROD", "").strip() or DEFAULT_INVEST_GRPC_API
INVEST_GRPC_API_SANDBOX = os.getenv("T_INVEST_ENDPOINT_SANDBOX", "").strip() or DEFAULT_INVEST_GRPC_API_SANDBOX

MAX_RECEIVE_MESSAGE_LENGTH_OPTION = "grpc.max_receive_message_length"


def get_target_by_name(target_name: str) -> str:
    if str(target_name or "").strip().upper() == "SANDBOX":
        return INVEST_GRPC_API_SANDBOX
    return INVEST_GRPC_API


def _load_root_certificates() -> bytes | None:
    path_raw = os.getenv("T_INVEST_ROOT_CERT_PATH", "").strip()
    path = Path(path_raw).expanduser() if path_raw else DEFAULT_ROOT_CERT_PATH
    if not path.exists():
        return None
    return path.read_bytes()


def create_channel(
    *,
    target: Optional[str] = None,
    options: Optional[ChannelArgumentType] = None,
    force_async: bool = False,
    compression: Optional[grpc.Compression] = None,
    interceptors: Optional[Sequence[ClientInterceptor]] = None,
) -> Any:
    creds = grpc.ssl_channel_credentials(root_certificates=_load_root_certificates())
    target = target or INVEST_GRPC_API
    if options is None:
        options = []
    options = _with_max_receive_message_length_option(options)
    args = (target, creds, options, compression)
    if force_async:
        return grpc.aio.secure_channel(*args, interceptors)
    return grpc.secure_channel(*args)


def _with_max_receive_message_length_option(
    options: ChannelArgumentType,
) -> ChannelArgumentType:
    if not _contains_option(options, MAX_RECEIVE_MESSAGE_LENGTH_OPTION):
        option = (MAX_RECEIVE_MESSAGE_LENGTH_OPTION, MAX_RECEIVE_MESSAGE_LENGTH)
        return list(itertools.chain(options, [option]))
    return options


def _contains_option(options: ChannelArgumentType, expected_option_name: str) -> bool:
    for option_name, _ in options:
        if option_name == expected_option_name:
            return True
    return False


class Client:
    def __init__(
        self,
        token: str,
        *,
        target: Optional[str] = None,
        sandbox_token: Optional[str] = None,
        options: Optional[ChannelArgumentType] = None,
        app_name: Optional[str] = None,
        interceptors: Optional[list[ClientInterceptor]] = None,
    ):
        self._token = token
        self._sandbox_token = sandbox_token
        self._options = options
        self._app_name = app_name
        self._channel = create_channel(target=target, options=options)
        if interceptors is None:
            interceptors = []
        for interceptor in interceptors:
            self._channel = grpc.intercept_channel(self._channel, interceptor)

    def __enter__(self) -> Services:
        channel = self._channel.__enter__()
        return Services(
            channel,
            token=self._token,
            sandbox_token=self._sandbox_token,
            app_name=self._app_name,
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._channel.__exit__(exc_type, exc_val, exc_tb)
        return False


class AsyncClient:
    def __init__(
        self,
        token: str,
        *,
        target: Optional[str] = None,
        sandbox_token: Optional[str] = None,
        options: Optional[ChannelArgumentType] = None,
        app_name: Optional[str] = None,
        interceptors: Optional[list[ClientInterceptor]] = None,
    ):
        self._token = token
        self._sandbox_token = sandbox_token
        self._options = options
        self._app_name = app_name
        self._channel = create_channel(
            target=target,
            force_async=True,
            options=options,
            interceptors=interceptors,
        )

    async def __aenter__(self) -> AsyncServices:
        channel = await self._channel.__aenter__()
        return AsyncServices(
            channel,
            token=self._token,
            sandbox_token=self._sandbox_token,
            app_name=self._app_name,
        )

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._channel.__aexit__(exc_type, exc_val, exc_tb)
        return False
