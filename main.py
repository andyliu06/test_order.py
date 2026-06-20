import asyncio
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional

import ccxt
from fastapi import FastAPI, HTTPException


# === 置換為你的 API 密鑰與帳號配置 ===
OKX_API_KEY = "cea4a4da-a5e1-4124-b589-d0f82f3166ae"
OKX_API_SECRET = "D03A44C7ED579AADB7B4FA664DDBB92F"
OKX_API_PASSWORD = "Dd_0977030927"

# === 置換為你指定的交易幣種 ===
SYMBOL = os.getenv("OKX_SYMBOL", "BTC/USDT:USDT")

CONTRACTS_PER_TRADE = float(os.getenv("OKX_CONTRACTS_PER_TRADE", "2.0"))
LEVERAGE = float(os.getenv("OKX_LEVERAGE", "47.6"))
TP1_CLOSE_RATIO = float(os.getenv("OKX_TP1_CLOSE_RATIO", "0.5"))
MONITOR_INTERVAL_SECONDS = float(os.getenv("OKX_MONITOR_INTERVAL_SECONDS", "2"))
SYNC_INTERVAL_SECONDS = float(os.getenv("OKX_SYNC_INTERVAL_SECONDS", "15"))
POSITION_TOLERANCE = float(os.getenv("OKX_POSITION_TOLERANCE", "0.000001"))
POSITION_SYNC_GRACE_SECONDS = float(os.getenv("OKX_POSITION_SYNC_GRACE_SECONDS", "10"))

# === 置換為你指定的 47.6x 槓桿對應的 ROE 百分比 ===
SL_PCT = float(os.getenv("OKX_SL_PCT", str(0.55915 / 47.6)))      # 約 1.1747%
TP1_PCT = float(os.getenv("OKX_TP1_PCT", str(0.1579 / 47.6)))     # 約 0.3317%
TP2_PCT = float(os.getenv("OKX_TP2_PCT", str(0.39475 / 47.6)))    # 約 0.8293%


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("okx-virtual-bot")


exchange = ccxt.okx(
    {
        "apiKey": OKX_API_KEY,
        "secret": OKX_API_SECRET,
        "password": OKX_API_PASSWORD,
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap",
            "createMarketBuyOrderRequiresPrice": False,
            "brokerId": "CCXT",
            "defaultMarginMode": "isolated",
        },
    }
)

if os.getenv("OKX_SANDBOX", "true").lower() in {"1", "true", "yes"}:
    exchange.set_sandbox_mode(True)


@dataclass
class VirtualTrade:
    trade_id: str
    symbol: str
    entry_price: float
    side: str
    pos_side: str
    contracts: float
    remaining: float
    status: str = "stage_0"
    tp1_price: float = 0.0
    tp2_price: float = 0.0
    sl_price: float = 0.0
    tp1_done: bool = False
    tp2_done: bool = False
    sl_done: bool = False
    opened_order_id: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


active_trades: Dict[str, VirtualTrade] = {}
trade_tasks: set[asyncio.Task] = set()
side_locks = {"long": asyncio.Lock(), "short": asyncio.Lock()}
registry_lock = asyncio.Lock()


def opposite_side(side: str) -> str:
    return "sell" if side == "buy" else "buy"


def action_to_side(action: str) -> str:
    if action == "Buy Reversal":
        return "buy"
    if action == "Sell Reversal":
        return "sell"
    raise ValueError(f"unsupported action: {action!r}")


def side_to_pos_side(side: str) -> str:
    return "long" if side == "buy" else "short"


def build_trade_id(pos_side: str) -> str:
    return f"{pos_side}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"


def calculate_levels(entry_price: float, side: str) -> tuple[float, float, float]:
    if side == "buy":
        return (
            round(entry_price * (1 + TP1_PCT), 2),
            round(entry_price * (1 + TP2_PCT), 2),
            round(entry_price * (1 - SL_PCT), 2),
        )
    return (
        round(entry_price * (1 - TP1_PCT), 2),
        round(entry_price * (1 - TP2_PCT), 2),
        round(entry_price * (1 + SL_PCT), 2),
    )


def normalize_amount(symbol: str, amount: float) -> float:
    precise = exchange.amount_to_precision(symbol, max(amount, 0.0))
    return float(precise)


def float_value(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def positive_float(value: Any) -> Optional[float]:
    parsed = float_value(value)
    if parsed is None:
        return None
    return parsed if parsed > 0 else None


def extract_order_average(order: Dict[str, Any]) -> Optional[float]:
    average = positive_float(order.get("average"))
    if average:
        return average

    filled = positive_float(order.get("filled"))
    cost = positive_float(order.get("cost"))
    if filled and cost:
        return cost / filled

    info = order.get("info") or {}
    for key in ("avgPx", "avgPxUsd", "fillPx", "px"):
        average = positive_float(info.get(key))
        if average:
            return average
    return None


def extract_order_filled(order: Dict[str, Any], fallback: Optional[float] = None) -> Optional[float]:
    filled = positive_float(order.get("filled"))
    if filled:
        return filled

    info = order.get("info") or {}
    for key in ("accFillSz", "fillSz", "sz"):
        filled = positive_float(info.get(key))
        if filled:
            return filled
    return fallback


async def ccxt_call(method_name: str, *args: Any, **kwargs: Any) -> Any:
    method = getattr(exchange, method_name)
    return await asyncio.to_thread(method, *args, **kwargs)


async def load_markets_once() -> None:
    await ccxt_call("load_markets")


async def fetch_order_details(
    order: Dict[str, Any],
    symbol: str,
    pos_side: str,
) -> Dict[str, Optional[float]]:
    average = extract_order_average(order)
    filled = extract_order_filled(order)
    if average and filled:
        return {"average": average, "filled": filled}

    order_id = order.get("id")
    if order_id:
        for attempt in range(5):
            try:
                fetched_order = await ccxt_call("fetch_order", order_id, symbol)
                average = average or extract_order_average(fetched_order)
                filled = filled or extract_order_filled(fetched_order)
                if average and filled:
                    return {"average": average, "filled": filled}
            except Exception as exc:
                logger.warning("fetch_order fill lookup failed attempt=%s: %s", attempt + 1, exc)
            await asyncio.sleep(0.5)

    position = await fetch_actual_position(symbol, pos_side)
    return {
        "average": average or position["entry_price"] or None,
        "filled": filled,
    }


async def fetch_actual_position(symbol: str, pos_side: str) -> Dict[str, float]:
    positions = await ccxt_call("fetch_positions", [symbol])
    for position in positions:
        if position.get("symbol") != symbol:
            continue

        info = position.get("info") or {}
        side = (position.get("side") or info.get("posSide") or "").lower()
        if side != pos_side:
            continue

        contracts = float_value(position.get("contracts"))
        if contracts is None:
            contracts = float_value(info.get("pos")) or 0.0
        contracts = abs(contracts)

        return {
            "contracts": contracts,
            "entry_price": positive_float(position.get("entryPrice"))
            or positive_float(info.get("avgPx"))
            or 0.0,
        }

    return {"contracts": 0.0, "entry_price": 0.0}


async def virtual_sum(symbol: str, pos_side: str) -> float:
    async with registry_lock:
        return sum(
            trade.remaining
            for trade in active_trades.values()
            if trade.symbol == symbol and trade.pos_side == pos_side and trade.remaining > 0
        )


async def has_recent_trade(symbol: str, pos_side: str) -> bool:
    now = time.time()
    async with registry_lock:
        return any(
            trade.symbol == symbol
            and trade.pos_side == pos_side
            and trade.remaining > POSITION_TOLERANCE
            and now - trade.created_at < POSITION_SYNC_GRACE_SECONDS
            for trade in active_trades.values()
        )


async def remove_finished_trade(trade_id: str) -> None:
    async with registry_lock:
        trade = active_trades.get(trade_id)
        if trade and trade.remaining <= POSITION_TOLERANCE:
            del active_trades[trade_id]


async def sync_side(symbol: str, pos_side: str) -> Dict[str, Any]:
    actual = await fetch_actual_position(symbol, pos_side)
    actual_contracts = actual["contracts"]
    virtual_contracts = await virtual_sum(symbol, pos_side)
    diff = actual_contracts - virtual_contracts
    is_synced = abs(diff) <= POSITION_TOLERANCE
    can_close_virtual_trades = actual_contracts + POSITION_TOLERANCE >= virtual_contracts

    if is_synced:
        return {
            "actual": actual_contracts,
            "virtual": virtual_contracts,
            "diff": diff,
            "is_synced": is_synced,
            "can_close_virtual_trades": can_close_virtual_trades,
        }

    logger.warning(
        "position mismatch symbol=%s pos_side=%s actual=%s virtual=%s diff=%s",
        symbol,
        pos_side,
        actual_contracts,
        virtual_contracts,
        diff,
    )

    if actual_contracts < virtual_contracts:
        if await has_recent_trade(symbol, pos_side):
            logger.warning(
                "position mismatch is inside new-trade grace window; blocking closes until actual position catches up"
            )
        else:
            logger.warning(
                "actual position is smaller than virtual trades; virtual trades were not mutated and closes are blocked"
            )
    else:
        logger.warning(
            "actual position is larger than virtual trades; unmanaged contracts will not be closed by this bot"
        )

    return {
        "actual": actual_contracts,
        "virtual": virtual_contracts,
        "diff": diff,
        "is_synced": is_synced,
        "can_close_virtual_trades": can_close_virtual_trades,
    }


async def sync_all_positions() -> None:
    for symbol in {SYMBOL}:
        for pos_side in ("long", "short"):
            async with side_locks[pos_side]:
                await sync_side(symbol, pos_side)


async def sync_loop(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            await sync_all_positions()
        except Exception as exc:
            logger.exception("position sync failed: %s", exc)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=SYNC_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass


async def close_virtual_trade(trade_id: str, requested_amount: float, reason: str) -> float:
    async with registry_lock:
        trade = active_trades.get(trade_id)
        if not trade or trade.remaining <= POSITION_TOLERANCE:
            return 0.0
        pos_side = trade.pos_side
        symbol = trade.symbol

    async with side_locks[pos_side]:
        sync_result = await sync_side(symbol, pos_side)
        if not sync_result["can_close_virtual_trades"]:
            logger.warning(
                "skip close because position is not synchronized trade_id=%s reason=%s actual=%s virtual=%s",
                trade_id,
                reason,
                sync_result["actual"],
                sync_result["virtual"],
            )
            return 0.0

        async with registry_lock:
            trade = active_trades.get(trade_id)
            if not trade or trade.remaining <= POSITION_TOLERANCE:
                return 0.0
            trade_side = trade.side
            trade_remaining = trade.remaining

        actual = await fetch_actual_position(symbol, pos_side)
        close_amount = min(requested_amount, trade_remaining, actual["contracts"])
        close_amount = normalize_amount(symbol, close_amount)
        if close_amount <= POSITION_TOLERANCE:
            logger.warning(
                "skip close trade_id=%s reason=%s requested=%s trade_remaining=%s actual=%s",
                trade_id,
                reason,
                requested_amount,
                trade_remaining,
                actual["contracts"],
            )
            return 0.0

        order = await ccxt_call(
            "create_order",
            symbol,
            "market",
            opposite_side(trade_side),
            close_amount,
            None,
            {
                "tdMode": "isolated",
                "reduceOnly": True,
                "posSide": pos_side,
            },
        )
        filled_from_order = extract_order_filled(order, close_amount) or close_amount
        filled = normalize_amount(symbol, min(filled_from_order, close_amount))

        async with registry_lock:
            trade = active_trades.get(trade_id)
            if not trade:
                return filled

            trade.remaining = normalize_amount(symbol, max(trade.remaining - filled, 0.0))
            trade.updated_at = time.time()

            if reason == "tp1":
                trade.tp1_done = True
                trade.status = "stage_1"
                trade.sl_price = round((trade.entry_price + trade.tp1_price) / 2, 2)
            elif reason == "tp2":
                trade.tp2_done = True
                trade.status = "closed"
            elif reason == "sl":
                trade.sl_done = True
                trade.status = "stopped"

            if trade.remaining <= POSITION_TOLERANCE:
                trade.remaining = 0.0

        await sync_side(symbol, pos_side)
        logger.info("closed trade_id=%s reason=%s amount=%s", trade_id, reason, filled)
        await remove_finished_trade(trade_id)
        return filled


async def monitor_trade(trade_id: str) -> None:
    logger.info("monitor started trade_id=%s", trade_id)
    while True:
        async with registry_lock:
            trade = active_trades.get(trade_id)
            if not trade or trade.remaining <= POSITION_TOLERANCE:
                break

            symbol = trade.symbol
            side = trade.side
            tp1_price = trade.tp1_price
            tp2_price = trade.tp2_price
            sl_price = trade.sl_price
            remaining = trade.remaining
            status = trade.status

        try:
            ticker = await ccxt_call("fetch_ticker", symbol)
            last_price = float(ticker["last"])

            is_stop_loss = last_price <= sl_price if side == "buy" else last_price >= sl_price
            is_tp1 = last_price >= tp1_price if side == "buy" else last_price <= tp1_price
            is_tp2 = last_price >= tp2_price if side == "buy" else last_price <= tp2_price

            if is_stop_loss:
                await close_virtual_trade(trade_id, remaining, "sl")
                break

            if status == "stage_0" and is_tp1:
                await close_virtual_trade(trade_id, remaining * TP1_CLOSE_RATIO, "tp1")
            elif status == "stage_1" and is_tp2:
                await close_virtual_trade(trade_id, remaining, "tp2")
                break

            await asyncio.sleep(MONITOR_INTERVAL_SECONDS)
        except Exception as exc:
            logger.exception("monitor error trade_id=%s: %s", trade_id, exc)
            await asyncio.sleep(5)

    await remove_finished_trade(trade_id)
    logger.info("monitor stopped trade_id=%s", trade_id)


def track_task(task: asyncio.Task) -> None:
    trade_tasks.add(task)

    def cleanup(done: asyncio.Task) -> None:
        trade_tasks.discard(done)
        if done.cancelled():
            return
        exc = done.exception()
        if exc:
            logger.exception("background task failed: %s", exc)

    task.add_done_callback(cleanup)


async def open_virtual_trade(data: Dict[str, Any]) -> VirtualTrade:
    action = data.get("action")
    side = action_to_side(action)
    pos_side = side_to_pos_side(side)
    symbol = data.get("symbol") or SYMBOL
    contracts = normalize_amount(symbol, float(data.get("contracts", CONTRACTS_PER_TRADE)))

    if contracts <= POSITION_TOLERANCE:
        raise ValueError("contracts must be greater than zero")

    trade_id = str(data.get("trade_id") or build_trade_id(pos_side))

    async with registry_lock:
        if trade_id in active_trades:
            raise RuntimeError(f"duplicate trade_id: {trade_id}")

    async with side_locks[pos_side]:
        await ccxt_call("set_leverage", LEVERAGE, symbol, {"mgnMode": "isolated", "posSide": pos_side})
        order = await ccxt_call(
            "create_order",
            symbol,
            "market",
            side,
            contracts,
            None,
            {
                "tdMode": "isolated",
                "posSide": pos_side,
            },
        )

        order_details = await fetch_order_details(order, symbol, pos_side)
        filled_contracts = normalize_amount(symbol, order_details["filled"] or contracts)
        if filled_contracts <= POSITION_TOLERANCE:
            raise RuntimeError(f"entry order did not fill: {order}")

        entry_price = order_details["average"]
        if not entry_price:
            raise RuntimeError(f"entry order has no average price and position fallback failed: {order}")

        tp1, tp2, sl = calculate_levels(entry_price, side)
        trade = VirtualTrade(
            trade_id=trade_id,
            symbol=symbol,
            entry_price=entry_price,
            side=side,
            pos_side=pos_side,
            contracts=filled_contracts,
            remaining=filled_contracts,
            tp1_price=tp1,
            tp2_price=tp2,
            sl_price=sl,
            opened_order_id=order.get("id"),
        )

        async with registry_lock:
            active_trades[trade_id] = trade

        await sync_side(symbol, pos_side)

    task = asyncio.create_task(monitor_trade(trade_id))
    track_task(task)
    logger.info(
        "opened trade_id=%s side=%s pos_side=%s contracts=%s entry=%s tp1=%s tp2=%s sl=%s",
        trade_id,
        side,
        pos_side,
        filled_contracts,
        entry_price,
        tp1,
        tp2,
        sl,
    )
    return trade


async def process_trade(data: Dict[str, Any]) -> None:
    try:
        await open_virtual_trade(data)
    except Exception as exc:
        logger.exception("process_trade failed: %s", exc)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await load_markets_once()
    stop_event = asyncio.Event()
    sync_task = asyncio.create_task(sync_loop(stop_event))
    track_task(sync_task)
    try:
        yield
    finally:
        stop_event.set()
        sync_task.cancel()
        await asyncio.gather(sync_task, return_exceptions=True)


app = FastAPI(lifespan=lifespan)


@app.post("/webhook")
async def tradingview_webhook(data: Dict[str, Any]) -> Dict[str, str]:
    try:
        action_to_side(data.get("action"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    task = asyncio.create_task(process_trade(data))
    track_task(task)
    return {"status": "accepted"}


@app.get("/trades")
async def list_trades() -> Dict[str, Any]:
    async with registry_lock:
        return {
            "active_trades": {
                trade_id: asdict(trade)
                for trade_id, trade in active_trades.items()
            }
        }


@app.post("/sync")
async def force_sync() -> Dict[str, str]:
    await sync_all_positions()
    return {"status": "synced"}
