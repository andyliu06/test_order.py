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

def get_true_range(ohlcv):
    ranges = []
    for i in range(1, len(ohlcv)):
        prev_close = ohlcv[i-1][4]
        high = ohlcv[i][2]
        low = ohlcv[i][3]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        ranges.append(tr)
    return sum(ranges) / len(ranges) if ranges else 50.0

def get_okx_adr10(symbol='BTC/USDT:USDT'):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1D', limit=11)
        if not ohlcv or len(ohlcv) < 11: return 50.0
        return get_true_range(ohlcv)
    except: return 50.0

async def monitor_trade(trade_id, symbol, side, pos_side, entry_price, contracts):
    adr10 = await asyncio.to_thread(get_okx_adr10, symbol)
    
    # 💡 TP/SL 計算邏輯維持不變
    if side == 'buy':
        tp1 = entry_price + (adr10 * 0.10)
        tp2 = entry_price + (adr10 * 0.25)
        current_sl = entry_price - (adr10 * 0.35)
    else:
        tp1 = entry_price - (adr10 * 0.10)
        tp2 = entry_price - (adr10 * 0.25)
        current_sl = entry_price + (adr10 * 0.35)

    remaining_contracts = contracts
    status = "stage_0"  
    
    order_params = {
        'tdMode': 'isolated', 
        'reduceOnly': True, 
        'posSide': pos_side
    }

    print(f"📡 [ID: {trade_id}] Monitor Started | Symbol: {symbol} | SL: {current_sl} | TP1: {tp1}")

    while remaining_contracts > 0:
        try:
            ticker = await asyncio.to_thread(exchange.fetch_ticker, symbol)
            last_price = ticker['last']
            
            is_stop_loss = (last_price <= current_sl) if side == 'buy' else (last_price >= current_sl)
            
            if is_stop_loss:
                await asyncio.to_thread(
                    exchange.create_order,
                    symbol, 'market', 'sell' if side == 'buy' else 'buy', remaining_contracts, None,
                    order_params
                )
                print(f"🛑 [ID: {trade_id}] 觸發止損出場。")
                remaining_contracts = 0
                break
                
            if status == "stage_0" and ((last_price >= tp1) if side == 'buy' else (last_price <= tp1)):
                # 減倉一半 (例如 0.2 / 2 = 0.1 張)
                close_c = round(remaining_contracts / 2, 3)
                await asyncio.to_thread(
                    exchange.create_order,
                    symbol, 'market', 'sell' if side == 'buy' else 'buy', close_c, None,
                    order_params
                )
                remaining_contracts = round(remaining_contracts - close_c, 3)
                
                # 將止損價移到 入場價 與 TP1 的中點（鎖定獲利）
                current_sl = round((entry_price + tp1) / 2, 2) 
                
                print(f"🎯 [ID: {trade_id}] 觸發 TP1。已平倉 {close_c} 張，剩餘止損移至中點保利價: {current_sl}")
                status = "stage_1"
            
            elif status == "stage_1" and ((last_price >= tp2) if side == 'buy' else (last_price <= tp2)):
                await asyncio.to_thread(
                    exchange.create_order,
                    symbol, 'market', 'sell' if side == 'buy' else 'buy', remaining_contracts, None,
                    order_params
                )
                print(f"🚀 [ID: {trade_id}] 觸發 TP2 全平完勝出場！")
                remaining_contracts = 0
                break

            await asyncio.sleep(2)
        except Exception as e:
            print(f"⚠️ [ID: {trade_id}] Monitor Error: {e}")
            await asyncio.sleep(5) 

@app.post("/webhook")
async def tradingview_webhook(data: dict, background_tasks: BackgroundTasks):
    background_tasks.add_task(process_trade, data)
    return {"status": "success"}

async def process_trade(data: dict):
    try:
        action = data.get("action")
        price = float(data.get("price"))
        # 已改為 BTC
        symbol = 'BTC/USDT:USDT'
        trade_id = f"id_{int(time.time() * 1000)}"
        
        # 設定下單數量為 0.2 張
        contracts = 0.2 

        side = 'buy' if action == "Buy Reversal" else 'sell'
        pos_side = 'long' if side == 'buy' else 'short'
        
        try: 
            # 槓桿維持 47.6 倍
            await asyncio.to_thread(exchange.set_leverage, 47.6, symbol, {'mgnMode': 'isolated', 'posSide': pos_side})
        except: pass

        # 執行開倉
        await asyncio.to_thread(
            exchange.create_order,
            symbol, 'market', side, contracts, None,
            {
                'tdMode': 'isolated', 
                'posSide': pos_side
            }
        )
        print(f"🟢 [ID: {trade_id}] 已執行 {action} 開倉 {contracts} 張 | 槓桿: 47.6x")
        
        await monitor_trade(trade_id, symbol, side, pos_side, price, contracts)
        
    except Exception as e:
        print(f"❌ Background Task Error: {e}")
