import hashlib
import json
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


SCHEMA_VERSION = 4
MOSCOW_TZ = ZoneInfo("Europe/Moscow")


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
    if not raw:
        return ""
    try:
        parsed = datetime.fromisoformat(raw)
    except Exception:
        return raw[:10] if len(raw) >= 10 else ""
    if parsed.tzinfo is None:
        return parsed.date().isoformat()
    return parsed.astimezone(MOSCOW_TZ).date().isoformat()


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

            CREATE TABLE IF NOT EXISTS signal_observations (
                observation_uid TEXT PRIMARY KEY,
                observed_at TEXT NOT NULL,
                observed_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                signal TEXT NOT NULL,
                strategy TEXT,
                decision TEXT NOT NULL,
                decision_reason TEXT,
                priority_score REAL,
                entry_edge_score REAL,
                market_regime TEXT,
                regime_confidence REAL,
                setup_quality TEXT,
                observed_price REAL,
                horizon_minutes INTEGER NOT NULL,
                evaluated_at TEXT,
                current_price REAL,
                move_pct REAL,
                favorable INTEGER,
                context_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_signal_observations_observed_date ON signal_observations(observed_date);
            CREATE INDEX IF NOT EXISTS idx_signal_observations_symbol ON signal_observations(symbol);
            CREATE INDEX IF NOT EXISTS idx_signal_observations_evaluated ON signal_observations(evaluated_at);

            CREATE TABLE IF NOT EXISTS news_events (
                news_uid TEXT PRIMARY KEY,
                observed_at TEXT NOT NULL,
                observed_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                category TEXT,
                bias TEXT NOT NULL,
                strength TEXT,
                source TEXT,
                source_label TEXT,
                source_type TEXT,
                source_speed REAL,
                source_reliability REAL,
                source_count INTEGER,
                confirming_sources_json TEXT,
                horizon TEXT,
                actionability TEXT,
                score REAL,
                reason TEXT,
                summary TEXT,
                topics_json TEXT,
                message_url TEXT,
                message_text TEXT,
                ai_direction TEXT,
                ai_strength TEXT,
                ai_confidence REAL,
                ai_horizon TEXT,
                ai_event_type TEXT,
                ai_reason TEXT,
                ai_risk TEXT,
                expires_at TEXT,
                observed_price REAL,
                horizon_minutes INTEGER NOT NULL,
                evaluated_at TEXT,
                current_price REAL,
                move_pct REAL,
                favorable INTEGER,
                outcome_status TEXT NOT NULL DEFAULT 'PENDING',
                outcome_note TEXT,
                outcome_checked_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_news_events_observed_date ON news_events(observed_date);
            CREATE INDEX IF NOT EXISTS idx_news_events_symbol ON news_events(symbol);
            CREATE INDEX IF NOT EXISTS idx_news_events_evaluated ON news_events(evaluated_at);
            CREATE INDEX IF NOT EXISTS idx_news_events_source ON news_events(source);
            """
        )
        news_columns = {str(item[1]) for item in connection.execute("PRAGMA table_info(news_events)")}
        for column_name, definition in (
            ("outcome_status", "TEXT NOT NULL DEFAULT 'PENDING'"),
            ("outcome_note", "TEXT"),
            ("outcome_checked_at", "TEXT"),
        ):
            if column_name not in news_columns:
                connection.execute(f"ALTER TABLE news_events ADD COLUMN {column_name} {definition}")
        connection.execute(
            """
            UPDATE news_events
            SET outcome_status = CASE
                WHEN evaluated_at IS NOT NULL AND evaluated_at != '' THEN 'EVALUATED'
                WHEN outcome_status IS NULL OR outcome_status = '' THEN 'PENDING'
                ELSE outcome_status
            END
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_news_events_outcome_status ON news_events(outcome_status)"
        )
        connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")


def _news_event_uid(row: dict[str, Any]) -> str:
    message_url = str(row.get("message_url") or "").strip()
    base = {
        "symbol": str(row.get("symbol") or "").upper(),
        "bias": str(row.get("bias") or "").upper(),
        "source": str(row.get("source") or ""),
        "summary": str(row.get("summary") or "")[:120],
    }
    if message_url:
        base["message_url"] = message_url
    else:
        base["observed_at"] = str(row.get("observed_at") or "")[:16]
    payload = json.dumps(base, ensure_ascii=True, sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _signal_observation_uid(row: dict[str, Any]) -> str:
    context = row.get("context") if isinstance(row.get("context"), dict) else {}
    observation_key = str(
        row.get("observation_key")
        or context.get("observation_key")
        or context.get("candle_time")
        or str(row.get("observed_at") or row.get("time") or "")[:16]
    ).strip()
    base = {
        "observation_key": observation_key,
        "symbol": str(row.get("symbol") or "").upper(),
        "signal": str(row.get("signal") or "").upper(),
        "strategy": str(row.get("strategy") or row.get("strategy_name") or ""),
        "decision": str(row.get("decision") or ""),
    }
    payload = json.dumps(base, ensure_ascii=True, sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def append_signal_observation(db_path: Path, row: dict[str, Any]) -> str:
    ensure_trade_db(db_path)
    context = row.get("context")
    if not isinstance(context, dict):
        context = {}
    observed_at = str(row.get("observed_at") or row.get("time") or "")
    observation_uid = str(row.get("observation_uid") or _signal_observation_uid(row))
    with _connect(db_path) as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO signal_observations (
                observation_uid, observed_at, observed_date, symbol, signal, strategy,
                decision, decision_reason, priority_score, entry_edge_score, market_regime,
                regime_confidence, setup_quality, observed_price, horizon_minutes,
                evaluated_at, current_price, move_pct, favorable, context_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observation_uid,
                observed_at,
                _trade_date(observed_at),
                str(row.get("symbol") or "").upper(),
                str(row.get("signal") or "").upper(),
                str(row.get("strategy") or row.get("strategy_name") or ""),
                str(row.get("decision") or ""),
                str(row.get("decision_reason") or row.get("reason") or ""),
                float(row.get("priority_score")) if row.get("priority_score") not in (None, "") else None,
                float(row.get("entry_edge_score")) if row.get("entry_edge_score") not in (None, "") else None,
                str(row.get("market_regime") or ""),
                float(row.get("regime_confidence")) if row.get("regime_confidence") not in (None, "") else None,
                str(row.get("setup_quality") or row.get("setup_quality_label") or ""),
                float(row.get("observed_price")) if row.get("observed_price") not in (None, "") else None,
                int(row.get("horizon_minutes") or 15),
                str(row.get("evaluated_at") or ""),
                float(row.get("current_price")) if row.get("current_price") not in (None, "") else None,
                float(row.get("move_pct")) if row.get("move_pct") not in (None, "") else None,
                int(bool(row.get("favorable"))) if row.get("favorable") not in (None, "") else None,
                json.dumps(context, ensure_ascii=False, sort_keys=True),
            ),
        )
    return observation_uid


def append_news_event(db_path: Path, row: dict[str, Any]) -> str:
    ensure_trade_db(db_path)
    observed_at = str(row.get("observed_at") or row.get("time") or "")
    news_uid = str(row.get("news_uid") or _news_event_uid(row))
    confirming_sources = row.get("confirming_sources")
    if not isinstance(confirming_sources, list):
        confirming_sources = []
    topics = row.get("topics")
    if not isinstance(topics, list):
        topics = []
    with _connect(db_path) as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO news_events (
                news_uid, observed_at, observed_date, symbol, category, bias, strength,
                source, source_label, source_type, source_speed, source_reliability,
                source_count, confirming_sources_json, horizon, actionability, score,
                reason, summary, topics_json, message_url, message_text,
                ai_direction, ai_strength, ai_confidence, ai_horizon, ai_event_type,
                ai_reason, ai_risk, expires_at, observed_price, horizon_minutes,
                evaluated_at, current_price, move_pct, favorable,
                outcome_status, outcome_note, outcome_checked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                news_uid,
                observed_at,
                _trade_date(observed_at),
                str(row.get("symbol") or "").upper(),
                str(row.get("category") or ""),
                str(row.get("bias") or "").upper(),
                str(row.get("strength") or ""),
                str(row.get("source") or ""),
                str(row.get("source_label") or ""),
                str(row.get("source_type") or ""),
                float(row.get("source_speed")) if row.get("source_speed") not in (None, "") else None,
                float(row.get("source_reliability")) if row.get("source_reliability") not in (None, "") else None,
                int(row.get("source_count") or 1),
                json.dumps(confirming_sources, ensure_ascii=False),
                str(row.get("horizon") or ""),
                str(row.get("actionability") or ""),
                float(row.get("score")) if row.get("score") not in (None, "") else None,
                str(row.get("reason") or ""),
                str(row.get("summary") or ""),
                json.dumps(topics, ensure_ascii=False),
                str(row.get("message_url") or ""),
                str(row.get("message_text") or ""),
                str(row.get("ai_direction") or ""),
                str(row.get("ai_strength") or ""),
                float(row.get("ai_confidence")) if row.get("ai_confidence") not in (None, "") else None,
                str(row.get("ai_horizon") or ""),
                str(row.get("ai_event_type") or ""),
                str(row.get("ai_reason") or ""),
                str(row.get("ai_risk") or ""),
                str(row.get("expires_at") or ""),
                float(row.get("observed_price")) if row.get("observed_price") not in (None, "") else None,
                int(row.get("horizon_minutes") or 60),
                str(row.get("evaluated_at") or ""),
                float(row.get("current_price")) if row.get("current_price") not in (None, "") else None,
                float(row.get("move_pct")) if row.get("move_pct") not in (None, "") else None,
                int(bool(row.get("favorable"))) if row.get("favorable") not in (None, "") else None,
                str(row.get("outcome_status") or "PENDING").upper(),
                str(row.get("outcome_note") or ""),
                str(row.get("outcome_checked_at") or ""),
            ),
        )
    return news_uid


def _news_event_from_db(item: sqlite3.Row) -> dict[str, Any]:
    row = dict(item)
    for json_key, output_key in (
        ("confirming_sources_json", "confirming_sources"),
        ("topics_json", "topics"),
    ):
        raw = str(row.pop(json_key, "") or "")
        if raw:
            try:
                row[output_key] = json.loads(raw)
            except Exception:
                row[output_key] = []
        else:
            row[output_key] = []
    if row.get("favorable") is not None:
        row["favorable"] = bool(row["favorable"])
    return row


def load_news_events(
    db_path: Path,
    *,
    limit: int | None = None,
    target_day: date | None = None,
    unevaluated_only: bool = False,
) -> list[dict[str, Any]]:
    ensure_trade_db(db_path)
    query = "SELECT * FROM news_events"
    params: list[Any] = []
    clauses: list[str] = []
    if target_day is not None:
        clauses.append("observed_date = ?")
        params.append(target_day.isoformat())
    if unevaluated_only:
        clauses.append("(outcome_status IS NULL OR outcome_status = '' OR outcome_status = 'PENDING')")
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY observed_at ASC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    with _connect(db_path) as connection:
        return [_news_event_from_db(item) for item in connection.execute(query, params).fetchall()]


def update_news_event_outcome(
    db_path: Path,
    news_uid: str,
    *,
    evaluated_at: str,
    current_price: float,
    move_pct: float,
    favorable: bool,
) -> None:
    ensure_trade_db(db_path)
    with _connect(db_path) as connection:
        connection.execute(
            """
            UPDATE news_events
            SET evaluated_at = ?, current_price = ?, move_pct = ?, favorable = ?,
                outcome_status = 'EVALUATED', outcome_note = '', outcome_checked_at = ?
            WHERE news_uid = ?
            """,
            (
                evaluated_at,
                float(current_price),
                float(move_pct),
                int(bool(favorable)),
                evaluated_at,
                news_uid,
            ),
        )


def mark_news_event_outcome_unavailable(
    db_path: Path,
    news_uid: str,
    *,
    checked_at: str,
    note: str,
) -> None:
    ensure_trade_db(db_path)
    with _connect(db_path) as connection:
        connection.execute(
            """
            UPDATE news_events
            SET outcome_status = 'UNAVAILABLE', outcome_note = ?, outcome_checked_at = ?
            WHERE news_uid = ?
            """,
            (str(note or "историческая цена недоступна"), checked_at, news_uid),
        )


def summarize_news_source_stats(db_path: Path, *, days: int = 10, limit: int = 12) -> list[dict[str, Any]]:
    ensure_trade_db(db_path)
    with _connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT
                COALESCE(NULLIF(source_label, ''), source) AS source_label,
                source,
                source_type,
                COUNT(*) AS total_count,
                SUM(CASE WHEN favorable = 1 THEN 1 ELSE 0 END) AS favorable_count,
                SUM(CASE WHEN favorable = 0 THEN 1 ELSE 0 END) AS unfavorable_count,
                AVG(move_pct) AS avg_move_pct,
                AVG(source_speed) AS avg_speed,
                AVG(source_reliability) AS avg_reliability
            FROM news_events
            WHERE observed_date >= date('now', ?)
              AND outcome_status = 'EVALUATED'
              AND bias IN ('LONG', 'SHORT')
            GROUP BY COALESCE(NULLIF(source_label, ''), source), source, source_type
            ORDER BY total_count DESC, favorable_count DESC
            LIMIT ?
            """,
            (f"-{int(days)} day", int(limit)),
        ).fetchall()
    stats: list[dict[str, Any]] = []
    for item in rows:
        total = int(item["total_count"] or 0)
        favorable = int(item["favorable_count"] or 0)
        stats.append(
            {
                "source_label": str(item["source_label"] or item["source"] or ""),
                "source": str(item["source"] or ""),
                "source_type": str(item["source_type"] or ""),
                "total_count": total,
                "favorable_count": favorable,
                "unfavorable_count": int(item["unfavorable_count"] or 0),
                "win_rate_pct": round((favorable / total * 100.0), 1) if total else 0.0,
                "avg_move_pct": round(float(item["avg_move_pct"] or 0.0), 4),
                "avg_speed": round(float(item["avg_speed"] or 0.0), 3),
                "avg_reliability": round(float(item["avg_reliability"] or 0.0), 3),
            }
        )
    return stats


def summarize_news_analytics(db_path: Path, *, days: int = 10, limit: int = 8) -> dict[str, Any]:
    """Return outcome quality split by the dimensions used in news decisions."""
    ensure_trade_db(db_path)
    period = f"-{int(days)} day"
    where = "observed_date >= date('now', ?) AND bias IN ('LONG', 'SHORT')"
    with _connect(db_path) as connection:
        totals = connection.execute(
            f"""
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN outcome_status = 'EVALUATED' THEN 1 ELSE 0 END) AS evaluated_count,
                SUM(CASE WHEN outcome_status = 'UNAVAILABLE' THEN 1 ELSE 0 END) AS unavailable_count,
                SUM(CASE WHEN outcome_status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count,
                SUM(CASE WHEN outcome_status = 'EVALUATED' AND favorable = 1 THEN 1 ELSE 0 END) AS favorable_count,
                AVG(CASE WHEN outcome_status = 'EVALUATED' THEN move_pct END) AS avg_move_pct
            FROM news_events
            WHERE {where}
            """,
            (period,),
        ).fetchone()

        dimensions = {
            "sources": "COALESCE(NULLIF(source_label, ''), source)",
            "directions": "bias",
            "horizons": "horizon",
            "actions": "actionability",
            "ai_confirmation": """
                CASE
                    WHEN ai_direction = bias THEN 'ИИ подтвердил'
                    WHEN ai_direction IS NULL OR ai_direction = '' THEN 'Без ИИ-разбора'
                    ELSE 'ИИ не подтвердил'
                END
            """,
        }
        result: dict[str, list[dict[str, Any]]] = {}
        for key, expression in dimensions.items():
            rows = connection.execute(
                f"""
                SELECT
                    {expression} AS label,
                    COUNT(*) AS total_count,
                    SUM(CASE WHEN favorable = 1 THEN 1 ELSE 0 END) AS favorable_count,
                    AVG(move_pct) AS avg_move_pct
                FROM news_events
                WHERE {where} AND outcome_status = 'EVALUATED'
                GROUP BY {expression}
                ORDER BY total_count DESC, favorable_count DESC
                LIMIT ?
                """,
                (period, int(limit)),
            ).fetchall()
            result[key] = [
                {
                    "label": str(row["label"] or "Не указано"),
                    "total_count": int(row["total_count"] or 0),
                    "favorable_count": int(row["favorable_count"] or 0),
                    "win_rate_pct": round(
                        int(row["favorable_count"] or 0) / int(row["total_count"] or 1) * 100.0,
                        1,
                    ),
                    "avg_move_pct": round(float(row["avg_move_pct"] or 0.0), 4),
                }
                for row in rows
            ]

    evaluated_count = int(totals["evaluated_count"] or 0)
    favorable_count = int(totals["favorable_count"] or 0)
    return {
        "days": int(days),
        "total_count": int(totals["total_count"] or 0),
        "evaluated_count": evaluated_count,
        "unavailable_count": int(totals["unavailable_count"] or 0),
        "pending_count": int(totals["pending_count"] or 0),
        "favorable_count": favorable_count,
        "win_rate_pct": round(favorable_count / evaluated_count * 100.0, 1) if evaluated_count else 0.0,
        "avg_move_pct": round(float(totals["avg_move_pct"] or 0.0), 4),
        **result,
    }


def _signal_observation_from_db(item: sqlite3.Row) -> dict[str, Any]:
    row = dict(item)
    context_json = str(row.pop("context_json", "") or "")
    if context_json:
        try:
            row["context"] = json.loads(context_json)
        except Exception:
            row["context"] = {}
    if row.get("favorable") is not None:
        row["favorable"] = bool(row["favorable"])
    return row


def load_signal_observations(
    db_path: Path,
    *,
    limit: int | None = None,
    target_day: date | None = None,
    unevaluated_only: bool = False,
) -> list[dict[str, Any]]:
    ensure_trade_db(db_path)
    query = "SELECT * FROM signal_observations"
    params: list[Any] = []
    clauses: list[str] = []
    if target_day is not None:
        clauses.append("observed_date = ?")
        params.append(target_day.isoformat())
    if unevaluated_only:
        clauses.append("(evaluated_at IS NULL OR evaluated_at = '')")
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY observed_at ASC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    with _connect(db_path) as connection:
        return [_signal_observation_from_db(item) for item in connection.execute(query, params).fetchall()]


def summarize_news_allocator_impact(db_path: Path, *, days: int = 10) -> dict[str, Any]:
    """Summarize only signal decisions whose priority was changed by news."""
    ensure_trade_db(db_path)
    with _connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT decision, favorable, context_json
            FROM signal_observations
            WHERE observed_date >= date('now', ?)
            """,
            (f"-{int(days)} day",),
        ).fetchall()

    impacted: list[tuple[str, bool | None, float]] = []
    for row in rows:
        context: dict[str, Any] = {}
        raw_context = str(row["context_json"] or "")
        if raw_context:
            try:
                context = json.loads(raw_context)
            except Exception:
                continue
        try:
            adjustment = float(context.get("news_priority_adjustment") or 0.0)
        except (TypeError, ValueError):
            adjustment = 0.0
        if abs(adjustment) < 0.001:
            continue
        favorable_raw = row["favorable"]
        favorable = bool(favorable_raw) if favorable_raw is not None else None
        impacted.append((str(row["decision"] or ""), favorable, adjustment))

    selected = [item for item in impacted if item[0] == "selected"]
    evaluated_selected = [item for item in selected if item[1] is not None]
    favorable_selected = sum(1 for _, favorable, _ in evaluated_selected if favorable)
    return {
        "days": int(days),
        "total_count": len(impacted),
        "boost_count": sum(1 for _, _, adjustment in impacted if adjustment > 0.0),
        "penalty_count": sum(1 for _, _, adjustment in impacted if adjustment < 0.0),
        "selected_count": len(selected),
        "deferred_count": sum(1 for decision, _, _ in impacted if decision == "deferred"),
        "evaluated_selected_count": len(evaluated_selected),
        "favorable_selected_count": favorable_selected,
        "selected_win_rate_pct": round(
            favorable_selected / len(evaluated_selected) * 100.0,
            1,
        ) if evaluated_selected else 0.0,
    }


def update_signal_observation_outcome(
    db_path: Path,
    observation_uid: str,
    *,
    evaluated_at: str,
    current_price: float,
    move_pct: float,
    favorable: bool,
) -> None:
    ensure_trade_db(db_path)
    with _connect(db_path) as connection:
        connection.execute(
            """
            UPDATE signal_observations
            SET evaluated_at = ?, current_price = ?, move_pct = ?, favorable = ?
            WHERE observation_uid = ?
            """,
            (evaluated_at, float(current_price), float(move_pct), int(bool(favorable)), observation_uid),
        )


def update_signal_observation_context(
    db_path: Path,
    observation_uid: str,
    context_updates: dict[str, Any],
) -> None:
    ensure_trade_db(db_path)
    with _connect(db_path) as connection:
        row = connection.execute(
            "SELECT context_json FROM signal_observations WHERE observation_uid = ?",
            (observation_uid,),
        ).fetchone()
        if row is None:
            return
        context_json = str(row["context_json"] or "")
        context: dict[str, Any] = {}
        if context_json:
            try:
                context = json.loads(context_json)
            except Exception:
                context = {}
        for key, value in (context_updates or {}).items():
            if value in ("", None):
                context.pop(key, None)
            else:
                context[key] = value
        connection.execute(
            "UPDATE signal_observations SET context_json = ? WHERE observation_uid = ?",
            (json.dumps(context, ensure_ascii=False, sort_keys=True), observation_uid),
        )


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
