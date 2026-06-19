import time
import asyncio
from fastapi import FastAPI, BackgroundTasks
import ccxt

app = FastAPI()

exchange = ccxt.okx({
    'apiKey': 'cea4a4da-a5e1-4124-b589-d0f82f3166ae',
    'secret': 'D03A44C7ED579AADB7B4FA664DDBB92F',
    'password': 'Dd_0977030927',
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap',
        'createMarketBuyOrderRequiresPrice': False,
        'brokerId': 'CCXT',
        'defaultMarginMode': 'isolated'
    }  
})
exchange.set_sandbox_mode(True)

@app.get("/ping")
async def ping():
    return {"status": "alive", "timestamp": int(time.time())}

def get_true_range(ohlcv):
    ranges = []
    for i in range(1, len(ohlcv)):
        prev_close = ohlcv[i-1][4]
        high = ohlcv[i][2]
        low = ohlcv[i][3]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        ranges.append(tr)
    return sum(ranges) / len(ranges) if ranges else 2000.0

def get_okx_adr10(symbol='BTC/USDT:USDT'):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1D', limit=11)
        if not ohlcv or len(ohlcv) < 11: 
            return 2000.0
        return get_true_range(ohlcv)
    except: 
        return 2000.0

async def monitor_trade(trade_id, symbol, side, pos_side, entry_price, contracts):
    adr10 = await asyncio.to_thread(get_okx_adr10, symbol)
    
    # Expanded multiplier to secure enough profit against fees
    if side == 'buy':
        tp1 = round(entry_price + (adr10 * 0.15), 1)
        tp2 = round(entry_price + (adr10 * 0.35), 1)
        current_sl = round(entry_price - (adr10 * 0.35), 1)
    else:
        tp1 = round(entry_price - (adr10 * 0.15), 1)
        tp2 = round(entry_price - (adr10 * 0.35), 1)
        current_sl = round(entry_price + (adr10 * 0.35), 1)

    remaining_contracts = contracts
    status = "stage_0"  
    
    order_params = {
        'tdMode': 'isolated', 
        'reduceOnly': True, 
        'posSide': pos_side
    }

    print(f"Monitor Started | ID: {trade_id} | Entry: {entry_price} | SL: {current_sl} | TP1: {tp1}")

    while remaining_contracts > 0:
        try:
            ticker = await asyncio.to_thread(exchange.fetch_ticker, symbol)
            last_price = ticker['last']
            
            is_stop_loss = (last_price <= current_sl) if side == 'buy' else (last_price >= current_sl)
            
            # 1. Market Stop Loss
            if is_stop_loss:
                await asyncio.to_thread(
                    exchange.create_order,
                    symbol, 'market', 'sell' if side == 'buy' else 'buy', remaining_contracts, None,
                    order_params
                )
                print(f"SL Triggered | ID: {trade_id} | Market close at {last_price}")
                remaining_contracts = 0
                break
                
            # 2. Market Take Profit 1
            if status == "stage_0" and ((last_price >= tp1) if side == 'buy' else (last_price <= tp1)):
                close_c = round(remaining_contracts / 2, 3)
                await asyncio.to_thread(
                    exchange.create_order,
                    symbol, 'market', 'sell' if side == 'buy' else 'buy', close_c, None,
                    order_params
                )
                remaining_contracts = round(remaining_contracts - close_c, 3)
                current_sl = round((entry_price + tp1) / 2, 1) 
                
                print(f"TP1 Triggered | ID: {trade_id} | Market close {close_c} | New SL: {current_sl}")
                status = "stage_1"
            
            # 3. Market Take Profit 2
            elif status == "stage_1" and ((last_price >= tp2) if side == 'buy' else (last_price <= tp2)):
                await asyncio.to_thread(
                    exchange.create_order,
                    symbol, 'market', 'sell' if side == 'buy' else 'buy', remaining_contracts, None,
                    order_params
                )
                print(f"TP2 Triggered | ID: {trade_id} | Market full close at {last_price}")
                remaining_contracts = 0
                break

            await asyncio.sleep(2)
        except Exception as e:
            print(f"Monitor Error | ID: {trade_id} | Error: {e}")
            await asyncio.sleep(5) 

@app.post("/webhook")
async def tradingview_webhook(data: dict, background_tasks: BackgroundTasks):
    background_tasks.add_task(process_trade, data)
    return {"status": "success"}

async def process_trade(data: dict):
    try:
        action = data.get("action")
        symbol = 'BTC/USDT:USDT'
        trade_id = f"id_{int(time.time() * 1000)}"
        contracts = 0.2 

        side = 'buy' if action == "Buy Reversal" else 'sell'
        pos_side = 'long' if side == 'buy' else 'short'
        
        try: 
            await asyncio.to_thread(exchange.set_leverage, 47.6, symbol, {'mgnMode': 'isolated', 'posSide': pos_side})
        except: 
            pass

        # Executing Market Entry Order
        order = await asyncio.to_thread(
            exchange.create_order,
            symbol, 'market', side, contracts, None,
            {
                'tdMode': 'isolated', 
                'posSide': pos_side
            }
        )
        
        # Fetch actual execution price from market order response
        entry_price = order.get('average') or order.get('price')
        if not entry_price:
            ticker = await asyncio.to_thread(exchange.fetch_ticker, symbol)
            entry_price = ticker['last']

        print(f"Market Order Executed | ID: {trade_id} | Action: {action} | Entry Price: {entry_price}")
        
        # Give exchange a split second to settle position before loop starts
        await asyncio.sleep(0.5)
        
        await monitor_trade(trade_id, symbol, side, pos_side, entry_price, contracts)
        
    except Exception as e:
        print(f"Background Task Error | Error: {e}")
