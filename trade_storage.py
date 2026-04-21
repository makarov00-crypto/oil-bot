import hashlib
import json
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1


def _load_journal_rows(journal_path: Path) -> list[dict[str, Any]]:
    if not journal_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in journal_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


def _journal_row_count(journal_path: Path) -> int:
    if not journal_path.exists():
        return 0
    count = 0
    for line in journal_path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            count += 1
    return count


def _event_uid(row: dict[str, Any]) -> str:
    broker_op_id = str(row.get("broker_op_id") or "").strip()
    if broker_op_id:
        base = {
            "broker_op_id": broker_op_id,
            "symbol": str(row.get("symbol") or "").upper(),
            "event": str(row.get("event") or "").upper(),
            "side": str(row.get("side") or "").upper(),
            "strategy": str(row.get("strategy") or ""),
        }
    else:
        base = {
            "time": str(row.get("time") or ""),
            "symbol": str(row.get("symbol") or "").upper(),
            "event": str(row.get("event") or "").upper(),
            "side": str(row.get("side") or "").upper(),
            "qty_lots": int(row.get("qty_lots") or 0),
            "price": round(float(row.get("price") or 0.0), 8),
            "strategy": str(row.get("strategy") or ""),
        }
    payload = json.dumps(base, ensure_ascii=True, sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _trade_date(value: str) -> str:
    raw = str(value or "")
    return raw[:10] if len(raw) >= 10 else ""


def _context_value(context: dict[str, Any], key: str, default: Any = "") -> Any:
    value = context.get(key, default)
    return default if value is None else value


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    return connection


def ensure_trade_db(db_path: Path) -> None:
    with _connect(db_path) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS trade_events (
                event_uid TEXT PRIMARY KEY,
                storage_order INTEGER NOT NULL,
                time TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                display_name TEXT,
                event TEXT NOT NULL,
                side TEXT NOT NULL,
                qty_lots INTEGER NOT NULL,
                lot_size INTEGER,
                price REAL,
                pnl_rub REAL,
                gross_pnl_rub REAL,
                commission_rub REAL,
                net_pnl_rub REAL,
                reason TEXT,
                source TEXT,
                strategy TEXT,
                mode TEXT,
                session TEXT,
                broker_op_id TEXT,
                higher_tf_bias TEXT,
                news_bias TEXT,
                news_impact TEXT,
                allocator_quantity INTEGER,
                allocator_summary TEXT,
                entry_allocator_quantity INTEGER,
                entry_allocator_summary TEXT,
                signal_summary_json TEXT,
                execution_status TEXT,
                context_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_trade_events_trade_date ON trade_events(trade_date);
            CREATE INDEX IF NOT EXISTS idx_trade_events_symbol_strategy ON trade_events(symbol, strategy);
            CREATE INDEX IF NOT EXISTS idx_trade_events_time ON trade_events(time);
            """
        )
        connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")


def _row_to_db_tuple(row: dict[str, Any], storage_order: int) -> tuple[Any, ...]:
    context = row.get("context")
    if not isinstance(context, dict):
        context = {}
    signal_summary = _context_value(context, "signal_summary", [])
    if not isinstance(signal_summary, list):
        signal_summary = [str(signal_summary)]
    return (
        _event_uid(row),
        storage_order,
        str(row.get("time") or ""),
        _trade_date(str(row.get("time") or "")),
        str(row.get("symbol") or ""),
        str(row.get("display_name") or ""),
        str(row.get("event") or ""),
        str(row.get("side") or ""),
        int(row.get("qty_lots") or 0),
        int(row.get("lot_size") or 0) if row.get("lot_size") not in (None, "") else None,
        float(row.get("price")) if row.get("price") not in (None, "") else None,
        float(row.get("pnl_rub")) if row.get("pnl_rub") not in (None, "") else None,
        float(row.get("gross_pnl_rub")) if row.get("gross_pnl_rub") not in (None, "") else None,
        float(row.get("commission_rub")) if row.get("commission_rub") not in (None, "") else None,
        float(row.get("net_pnl_rub")) if row.get("net_pnl_rub") not in (None, "") else None,
        str(row.get("reason") or ""),
        str(row.get("source") or ""),
        str(row.get("strategy") or ""),
        str(row.get("mode") or ""),
        str(row.get("session") or ""),
        str(row.get("broker_op_id") or ""),
        str(_context_value(context, "higher_tf_bias", "")),
        str(_context_value(context, "news_bias", "")),
        str(_context_value(context, "news_impact", "")),
        int(_context_value(context, "allocator_quantity", 0) or 0),
        str(_context_value(context, "allocator_summary", "")),
        int(_context_value(context, "entry_allocator_quantity", 0) or 0),
        str(_context_value(context, "entry_allocator_summary", "")),
        json.dumps(signal_summary, ensure_ascii=False),
        str(_context_value(context, "execution_status", "")),
        json.dumps(context, ensure_ascii=False, sort_keys=True),
    )


def sync_journal_to_db(journal_path: Path, db_path: Path) -> None:
    ensure_trade_db(db_path)
    source_rows = _load_journal_rows(journal_path)
    deduped_by_uid: dict[str, tuple[int, dict[str, Any]]] = {}
    for index, row in enumerate(source_rows):
        deduped_by_uid[_event_uid(row)] = (index, row)
    rows = [row for _, row in sorted(deduped_by_uid.values(), key=lambda item: item[0])]
    with _connect(db_path) as connection:
        connection.execute("DELETE FROM trade_events")
        connection.executemany(
            """
            INSERT INTO trade_events (
                event_uid, storage_order, time, trade_date, symbol, display_name, event, side,
                qty_lots, lot_size, price, pnl_rub, gross_pnl_rub, commission_rub, net_pnl_rub,
                reason, source, strategy, mode, session, broker_op_id,
                higher_tf_bias, news_bias, news_impact, allocator_quantity, allocator_summary,
                entry_allocator_quantity, entry_allocator_summary, signal_summary_json,
                execution_status, context_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [_row_to_db_tuple(row, index) for index, row in enumerate(rows)],
        )


def append_trade_row(db_path: Path, row: dict[str, Any]) -> None:
    ensure_trade_db(db_path)
    with _connect(db_path) as connection:
        next_order = int(
            connection.execute("SELECT COALESCE(MAX(storage_order), -1) + 1 FROM trade_events").fetchone()[0]
        )
        connection.execute(
            """
            INSERT OR REPLACE INTO trade_events (
                event_uid, storage_order, time, trade_date, symbol, display_name, event, side,
                qty_lots, lot_size, price, pnl_rub, gross_pnl_rub, commission_rub, net_pnl_rub,
                reason, source, strategy, mode, session, broker_op_id,
                higher_tf_bias, news_bias, news_impact, allocator_quantity, allocator_summary,
                entry_allocator_quantity, entry_allocator_summary, signal_summary_json,
                execution_status, context_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            _row_to_db_tuple(row, next_order),
        )


def _row_from_db(item: sqlite3.Row) -> dict[str, Any]:
    row = dict(item)
    context_json = str(row.pop("context_json", "") or "")
    signal_summary_json = str(row.pop("signal_summary_json", "") or "")
    context: dict[str, Any] = {}
    if context_json:
        try:
            context = json.loads(context_json)
        except Exception:
            context = {}
    if signal_summary_json and "signal_summary" not in context:
        try:
            context["signal_summary"] = json.loads(signal_summary_json)
        except Exception:
            context["signal_summary"] = []
    for key in (
        "higher_tf_bias",
        "news_bias",
        "news_impact",
        "allocator_quantity",
        "allocator_summary",
        "entry_allocator_quantity",
        "entry_allocator_summary",
        "execution_status",
    ):
        if row.get(key) not in (None, "") and key not in context:
            context[key] = row.get(key)
    for key in (
        "event_uid",
        "storage_order",
        "trade_date",
        "higher_tf_bias",
        "news_bias",
        "news_impact",
        "allocator_quantity",
        "allocator_summary",
        "entry_allocator_quantity",
        "entry_allocator_summary",
        "execution_status",
    ):
        row.pop(key, None)
    if context:
        row["context"] = context
    return row


def ensure_trade_storage(journal_path: Path, db_path: Path) -> None:
    ensure_trade_db(db_path)
    if not journal_path.exists():
        return
    if not db_path.exists() or db_path.stat().st_mtime < journal_path.stat().st_mtime:
        sync_journal_to_db(journal_path, db_path)
        return
    journal_count = _journal_row_count(journal_path)
    with _connect(db_path) as connection:
        db_count = int(connection.execute("SELECT COUNT(*) FROM trade_events").fetchone()[0])
    if journal_count > db_count:
        sync_journal_to_db(journal_path, db_path)


def load_trade_rows(
    journal_path: Path,
    db_path: Path,
    *,
    limit: int | None = None,
    target_day: date | None = None,
) -> list[dict[str, Any]]:
    ensure_trade_storage(journal_path, db_path)
    if not db_path.exists():
        rows = _load_journal_rows(journal_path)
        if target_day is not None:
            rows = [row for row in rows if _trade_date(str(row.get("time") or "")) == target_day.isoformat()]
        return rows[-limit:] if limit is not None else rows
    query = "SELECT * FROM trade_events"
    params: list[Any] = []
    clauses: list[str] = []
    if target_day is not None:
        clauses.append("trade_date = ?")
        params.append(target_day.isoformat())
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    order_clause = " ORDER BY storage_order ASC"
    if limit is not None:
        query = f"SELECT * FROM ({query}{order_clause} LIMIT -1 OFFSET 0) ORDER BY storage_order DESC LIMIT ?"
        params.append(limit)
        with _connect(db_path) as connection:
            rows = [_row_from_db(item) for item in connection.execute(query, params).fetchall()]
        rows.reverse()
        return rows
    with _connect(db_path) as connection:
        return [_row_from_db(item) for item in connection.execute(query + order_clause, params).fetchall()]
