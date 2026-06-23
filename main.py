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


OKX_API_KEY = "cea4a4da-a5e1-4124-b589-d0f82f3166ae"
OKX_API_SECRET = "D03A44C7ED579AADB7B4FA664DDBB92F"
OKX_API_PASSWORD = "Dd_0977030927"

SYMBOL = os.getenv("OKX_SYMBOL", "BTC/USDT:USDT")
CONTRACTS_PER_TRADE = float(os.getenv("OKX_CONTRACTS_PER_TRADE", "0.2"))
LEVERAGE = float(os.getenv("OKX_LEVERAGE", "47.6"))
TP1_CLOSE_RATIO = float(os.getenv("OKX_TP1_CLOSE_RATIO", "0.5"))
MONITOR_INTERVAL_SECONDS = float(os.getenv("OKX_MONITOR_INTERVAL_SECONDS", "2"))
SYNC_INTERVAL_SECONDS = float(os.getenv("OKX_SYNC_INTERVAL_SECONDS", "15"))
CLOSE_ACTION_PAUSE_SECONDS = float(os.getenv("OKX_CLOSE_ACTION_PAUSE_SECONDS", "0.25"))

POSITION_TOLERANCE = float(os.getenv("OKX_POSITION_TOLERANCE", "0.01"))
POSITION_SYNC_GRACE_SECONDS = float(os.getenv("OKX_POSITION_SYNC_GRACE_SECONDS", "10"))

# 修正後的百分比：直接用價格變動比例
SL_PCT  = float(os.getenv("OKX_SL_PCT",  "0.009383"))  # 跌 0.9383%
TP1_PCT = float(os.getenv("OKX_TP1_PCT", "0.002737"))  # 漲 0.2737%
TP2_PCT = float(os.getenv("OKX_TP2_PCT", "0.006647"))  # 漲 0.6647%

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("okx-virtual-bot")

exchange = ccxt.okx({
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
})

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
    pending_action: Optional[str] = None  # 防止重複觸發
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
    return float(exchange.amount_to_precision(symbol, max(amount, 0.0)))


def float_value(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def positive_float(value: Any) -> Optional[float]:
    parsed = float_value(value)
    return parsed if parsed and parsed > 0 else None


def extract_order_average(order: Dict[str, Any]) -> Optional[float]:
    average = positive_float(order.get("average"))
    if average:
        return average

    info = order.get("info") or {}
    for key in ("avgPx", "avgPxUsd", "fillPx", "px"):
        average = positive_float(info.get(key))
        if average:
            return average

    filled = positive_float(order.get("filled"))
    cost = positive_float(order.get("cost"))
    if filled and cost:
        return cost / filled

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


def extract_trades_average_and_filled(trades: list[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    total_amount = 0.0
    weighted_price_sum = 0.0

    for trade in trades:
        info = trade.get("info") or {}
        amount = positive_float(trade.get("amount")) or positive_float(info.get("fillSz")) or positive_float(info.get("sz"))
        price = positive_float(trade.get("price")) or positive_float(info.get("fillPx")) or positive_float(info.get("px"))

        if not amount or not price:
            continue

        total_amount += amount
        weighted_price_sum += amount * price

    if total_amount <= 0:
        return {"average": None, "filled": None}

    return {"average": weighted_price_sum / total_amount, "filled": total_amount}


async def wait_or_stop(stop_event: asyncio.Event, timeout: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        pass


async def ccxt_call(method_name: str, *args: Any, **kwargs: Any) -> Any:
    method = getattr(exchange, method_name)
    return await asyncio.to_thread(method, *args, **kwargs)


async def load_markets_once() -> None:
    await ccxt_call("load_markets")


async def fetch_order_trade_details(order_id: str, symbol: str) -> Dict[str, Optional[float]]:
    for attempt in range(5):
        try:
            trades = await ccxt_call("fetch_order_trades", order_id, symbol)
            details = extract_trades_average_and_filled(trades)
            if details["average"] and details["filled"]:
                return details
        except Exception as exc:
            logger.warning("fetch_order_trades lookup failed attempt=%s: %s", attempt + 1, exc)

        try:
            trades = await ccxt_call("fetch_my_trades", symbol, None, 100, {"ordId": order_id})
            filtered_trades = [
                trade for trade in trades
                if str(trade.get("order") or (trade.get("info") or {}).get("ordId") or "") == str(order_id)
            ]
            details = extract_trades_average_and_filled(filtered_trades)
            if details["average"] and details["filled"]:
                return details
        except Exception as exc:
            logger.warning("fetch_my_trades lookup failed attempt=%s: %s", attempt + 1, exc)

        await asyncio.sleep(0.5)

    return {"average": None, "filled": None}


async def fetch_order_details(order: Dict[str, Any], symbol: str) -> Dict[str, Optional[float]]:
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
                logger.warning("fetch_order lookup failed attempt=%s: %s", attempt + 1, exc)

            await asyncio.sleep(0.5)

        trade_details = await fetch_order_trade_details(order_id, symbol)
        average = average or trade_details["average"]
        filled = filled or trade_details["filled"]

        if average and filled:
            return {"average": average, "filled": filled}

    raise RuntimeError(f"entry order has no per-order average/fill; refusing to use position average: {order}")


async def fetch_close_order_filled(order: Dict[str, Any], symbol: str) -> Optional[float]:
    filled = extract_order_filled(order)
    if filled:
        return filled

    order_id = order.get("id")
    if not order_id:
        return None

    for attempt in range(5):
        try:
            fetched_order = await ccxt_call("fetch_order", order_id, symbol)
            filled = extract_order_filled(fetched_order)
            if filled:
                return filled
        except Exception as exc:
            logger.warning("fetch close order lookup failed attempt=%s: %s", attempt + 1, exc)

        await asyncio.sleep(0.5)

    trade_details = await fetch_order_trade_details(order_id, symbol)
    return trade_details["filled"]


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
            "entry_price": positive_float(position.get("entryPrice")) or positive_float(info.get("avgPx")) or 0.0,
        }

    return {"contracts": 0.0, "entry_price": 0.0}


async def fetch_position_close_delta(symbol: str, pos_side: str, before_contracts: float) -> float:
    latest_contracts = before_contracts

    for _ in range(8):
        await asyncio.sleep(0.5)
        actual = await fetch_actual_position(symbol, pos_side)
        latest_contracts = actual["contracts"]
        delta = before_contracts - latest_contracts
        if delta > POSITION_TOLERANCE:
            return delta

    return max(before_contracts - latest_contracts, 0.0)


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


async def purge_externally_closed(symbol: str, pos_side: str) -> None:
    """OKX 實際倉位是 0，但虛擬帳本還有紀錄 → 代表外部已平倉（手動/強平/清算），清除虛擬帳本。"""
    async with registry_lock:
        to_remove = [
            tid for tid, t in active_trades.items()
            if t.symbol == symbol and t.pos_side == pos_side
        ]
        for tid in to_remove:
            trade = active_trades[tid]
            trade.remaining = 0.0
            trade.status = "externally_closed"
            trade.pending_action = None
            del active_trades[tid]
            logger.warning(
                "purged trade_id=%s reason=externally_closed symbol=%s pos_side=%s",
                tid, symbol, pos_side,
            )


async def sync_side(symbol: str, pos_side: str) -> Dict[str, Any]:
    actual = await fetch_actual_position(symbol, pos_side)
    actual_contracts = actual["contracts"]
    virtual_contracts = await virtual_sum(symbol, pos_side)
    diff = actual_contracts - virtual_contracts
    is_synced = abs(diff) <= POSITION_TOLERANCE
    can_close_virtual_trades = actual_contracts > POSITION_TOLERANCE

    if not is_synced:
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
                logger.warning("position mismatch is inside new-trade grace window; closes are allowed only when actual can cover requested amount")
            elif actual_contracts <= POSITION_TOLERANCE:
                # 實際倉位是 0，且不在新開倉 grace window 內 → 外部已平倉，清除虛擬帳本
                logger.warning(
                    "actual position is 0 and no recent trade; assuming externally closed symbol=%s pos_side=%s",
                    symbol, pos_side,
                )
                await purge_externally_closed(symbol, pos_side)
                virtual_contracts = 0.0
                diff = 0.0
                is_synced = True
            else:
                logger.warning("actual position is smaller than virtual trades; closes are allowed only when actual can cover requested amount")
        else:
            logger.warning("actual position is larger than virtual trades; unmanaged contracts will not be closed by this bot")

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
        except ccxt.RateLimitExceeded as exc:
            logger.warning("position sync rate limited: %s", exc)
            await wait_or_stop(stop_event, 10)
        except Exception as exc:
            logger.exception("position sync failed: %s", exc)

        await wait_or_stop(stop_event, SYNC_INTERVAL_SECONDS)


async def close_virtual_trade(trade_id: str, requested_amount: float, reason: str) -> float:
    async with registry_lock:
        trade = active_trades.get(trade_id)
        if not trade or trade.remaining <= POSITION_TOLERANCE:
            return 0.0
        pos_side = trade.pos_side
        symbol = trade.symbol

    async with side_locks[pos_side]:
        sync_result = await sync_side(symbol, pos_side)

        async with registry_lock:
            trade = active_trades.get(trade_id)
            if not trade or trade.remaining <= POSITION_TOLERANCE:
                return 0.0
            trade_side = trade.side
            trade_remaining = trade.remaining

        close_amount = normalize_amount(symbol, min(requested_amount, trade_remaining))
        if close_amount <= POSITION_TOLERANCE:
            logger.warning(
                "skip close trade_id=%s reason=%s requested=%s trade_remaining=%s",
                trade_id,
                reason,
                requested_amount,
                trade_remaining,
            )
            # 清除 pending_action
            async with registry_lock:
                trade = active_trades.get(trade_id)
                if trade:
                    trade.pending_action = None
            return 0.0

        actual_before = await fetch_actual_position(symbol, pos_side)
        if actual_before["contracts"] + POSITION_TOLERANCE < close_amount:
            logger.warning(
                "skip close because actual position cannot cover requested close trade_id=%s reason=%s actual=%s close_amount=%s virtual=%s",
                trade_id,
                reason,
                actual_before["contracts"],
                close_amount,
                sync_result["virtual"],
            )
            # 清除 pending_action
            async with registry_lock:
                trade = active_trades.get(trade_id)
                if trade:
                    trade.pending_action = None
            return 0.0

        if not sync_result["is_synced"]:
            logger.warning(
                "position is not fully synchronized but close is allowed trade_id=%s reason=%s actual=%s virtual=%s close_amount=%s",
                trade_id,
                reason,
                sync_result["actual"],
                sync_result["virtual"],
                close_amount,
            )

        order = await ccxt_call(
            "create_order",
            symbol,
            "market",
            opposite_side(trade_side),
            close_amount,
            None,
            {"tdMode": "isolated", "reduceOnly": True, "posSide": pos_side},
        )

        filled_from_order = await fetch_close_order_filled(order, symbol)
        if not filled_from_order:
            position_delta = await fetch_position_close_delta(symbol, pos_side, actual_before["contracts"])
            filled_from_order = position_delta if position_delta > POSITION_TOLERANCE else None

        if not filled_from_order:
            logger.error(
                "close order submitted but filled amount could not be confirmed trade_id=%s reason=%s order=%s",
                trade_id,
                reason,
                order,
            )
            # 清除 pending_action
            async with registry_lock:
                trade = active_trades.get(trade_id)
                if trade:
                    trade.pending_action = None
            return 0.0

        filled = normalize_amount(symbol, min(filled_from_order, close_amount, trade_remaining))
        if filled <= POSITION_TOLERANCE:
            logger.warning(
                "confirmed close fill is too small trade_id=%s reason=%s filled=%s close_amount=%s",
                trade_id,
                reason,
                filled,
                close_amount,
            )
            # 清除 pending_action
            async with registry_lock:
                trade = active_trades.get(trade_id)
                if trade:
                    trade.pending_action = None
            return 0.0

        async with registry_lock:
            trade = active_trades.get(trade_id)
            if not trade:
                return filled

            trade.remaining = normalize_amount(symbol, max(trade.remaining - filled, 0.0))
            trade.updated_at = time.time()
            trade.pending_action = None  # 清除 pending_action

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


async def monitor_symbol_loop(symbol: str, stop_event: asyncio.Event) -> None:
    logger.info("symbol monitor started symbol=%s", symbol)

    while not stop_event.is_set():
        try:
            ticker = await ccxt_call("fetch_ticker", symbol)
            last_price = float(ticker["last"])

            actions: list[tuple[str, float, str]] = []
            async with registry_lock:
                for trade_id, trade in list(active_trades.items()):
                    if trade.symbol != symbol or trade.remaining <= POSITION_TOLERANCE:
                        continue

                    # 已有 pending_action 代表上一輪還在處理中，跳過避免重複觸發
                    if trade.pending_action:
                        continue

                    side = trade.side
                    is_stop_loss = last_price <= trade.sl_price if side == "buy" else last_price >= trade.sl_price
                    is_tp1 = last_price >= trade.tp1_price if side == "buy" else last_price <= trade.tp1_price
                    is_tp2 = last_price >= trade.tp2_price if side == "buy" else last_price <= trade.tp2_price

                    if is_stop_loss:
                        trade.pending_action = "sl"
                        actions.append((trade_id, trade.remaining, "sl"))
                    elif trade.status == "stage_0" and is_tp1:
                        trade.pending_action = "tp1"
                        actions.append((trade_id, trade.remaining * TP1_CLOSE_RATIO, "tp1"))
                    elif trade.status == "stage_1" and is_tp2:
                        trade.pending_action = "tp2"
                        actions.append((trade_id, trade.remaining, "tp2"))

            if actions:
                logger.info("symbol monitor triggered symbol=%s last=%s actions=%s", symbol, last_price, len(actions))

            for trade_id, amount, reason in actions:
                if stop_event.is_set():
                    break

                try:
                    await close_virtual_trade(trade_id, amount, reason)
                except ccxt.RateLimitExceeded as exc:
                    logger.warning("close action rate limited trade_id=%s reason=%s: %s", trade_id, reason, exc)
                    # 清除 pending_action 讓下一輪可以重試
                    async with registry_lock:
                        trade = active_trades.get(trade_id)
                        if trade:
                            trade.pending_action = None
                    await wait_or_stop(stop_event, 10)
                    break
                except Exception as exc:
                    logger.exception("close action failed trade_id=%s reason=%s: %s", trade_id, reason, exc)
                    # 清除 pending_action 讓下一輪可以重試
                    async with registry_lock:
                        trade = active_trades.get(trade_id)
                        if trade:
                            trade.pending_action = None

                await wait_or_stop(stop_event, CLOSE_ACTION_PAUSE_SECONDS)

            await wait_or_stop(stop_event, MONITOR_INTERVAL_SECONDS)

        except ccxt.RateLimitExceeded as exc:
            logger.warning("symbol monitor rate limited symbol=%s: %s", symbol, exc)
            await wait_or_stop(stop_event, 10)
        except Exception as exc:
            logger.exception("symbol monitor error symbol=%s: %s", symbol, exc)
            await wait_or_stop(stop_event, 5)

    logger.info("symbol monitor stopped symbol=%s", symbol)


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
            {"tdMode": "isolated", "posSide": pos_side},
        )

        order_details = await fetch_order_details(order, symbol)
        filled_contracts = normalize_amount(symbol, order_details["filled"] or contracts)
        if filled_contracts <= POSITION_TOLERANCE:
            raise RuntimeError(f"entry order did not fill: {order}")

        entry_price = order_details["average"]
        if not entry_price:
            raise RuntimeError(f"entry order has no average price: {order}")

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
    monitor_task = asyncio.create_task(monitor_symbol_loop(SYMBOL, stop_event))
    track_task(sync_task)
    track_task(monitor_task)

    try:
        yield
    finally:
        stop_event.set()
        sync_task.cancel()
        monitor_task.cancel()
        await asyncio.gather(sync_task, monitor_task, return_exceptions=True)


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
        return {"active_trades": {trade_id: asdict(trade) for trade_id, trade in active_trades.items()}}


@app.post("/sync")
async def force_sync() -> Dict[str, str]:
    await sync_all_positions()
    return {"status": "synced"}
