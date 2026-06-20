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

# 全域倉位池：用來在邏輯層面將交易所合併的倉位切分成獨立小單
active_trades = {}

@app.get("/ping")
async def ping():
    return {"status": "alive", "timestamp": int(time.time())}

async def monitor_trade(trade_id, symbol, side, pos_side, entry_price, contracts):
    # 💡 依據 47.6x 槓桿與指定收益率（ROE）精準計算價格變動率
    # TP1: 15.79% / 47.6 = 0.0033172 (約 0.3317%)
    # TP2: 39.475% / 47.6 = 0.0082931 (約 0.8293%)
    # SL: 55.915% / 47.6 = 0.0117468 (約 1.1747%)
    
    tp1_pct = 0.1579 / 47.6
    tp2_pct = 0.39475 / 47.6
    sl_pct = 0.55915 / 47.6
    
    if side == 'buy':
        tp1 = round(entry_price * (1 + tp1_pct), 1)
        tp2 = round(entry_price * (1 + tp2_pct), 1)
        current_sl = round(entry_price * (1 - sl_pct), 1)
    else:
        tp1 = round(entry_price * (1 - tp1_pct), 1)
        tp2 = round(entry_price * (1 - tp2_pct), 1)
        current_sl = round(entry_price * (1 + sl_pct), 1)

    status = "stage_0"  
    
    order_params = {
        'tdMode': 'isolated', 
        'reduceOnly': True, 
        'posSide': pos_side
    }

    print(f"📡 [ID: {trade_id}] Monitor Started | Entry: {entry_price} | SL: {current_sl} | TP1: {tp1} | TP2: {tp2}")

    # 檢查全域池內屬於此 trade_id 的剩餘合約量
    while trade_id in active_trades and active_trades[trade_id]["remaining"] > 0:
        try:
            ticker = await asyncio.to_thread(exchange.fetch_ticker, symbol)
            last_price = ticker['last']
            
            is_stop_loss = (last_price <= current_sl) if side == 'buy' else (last_price >= current_sl)
            
            # 1. Market Stop Loss
            if is_stop_loss:
                rem = active_trades[trade_id]["remaining"]
                if rem > 0:
                    await asyncio.to_thread(
                        exchange.create_order,
                        symbol, 'market', 'sell' if side == 'buy' else 'buy', rem, None,
                        order_params
                    )
                    print(f"🛑 [ID: {trade_id}] 觸發獨立小倉位止損出場。市場價格: {last_price}")
                    active_trades[trade_id]["remaining"] = 0
                break
                
            # 2. Market Take Profit 1
            if status == "stage_0" and ((last_price >= tp1) if side == 'buy' else (last_price <= tp1)):
                rem = active_trades[trade_id]["remaining"]
                close_c = round(rem / 2, 3)
                if close_c > 0:
                    await asyncio.to_thread(
                        exchange.create_order,
                        symbol, 'market', 'sell' if side == 'buy' else 'buy', close_c, None,
                        order_params
                    )
                    # 精準扣減全域池中屬於這單的合約數
                    active_trades[trade_id]["remaining"] = round(rem - close_c, 3)
                    
                    # 將止損價移到 入場價 與 TP1 的中點（鎖定獲利保利）
                    current_sl = round((entry_price + tp1) / 2, 1) 
                    
                    print(f"🎯 [ID: {trade_id}] 觸發 TP1。獨立小倉位已平倉 {close_c} 張，剩餘自持張數: {active_trades[trade_id]['remaining']} | 止損移至: {current_sl}")
                status = "stage_1"
            
            # 3. Market Take Profit 2
            elif status == "stage_1" and ((last_price >= tp2) if side == 'buy' else (last_price <= tp2)):
                rem = active_trades[trade_id]["remaining"]
                if rem > 0:
                    await asyncio.to_thread(
                        exchange.create_order,
                        symbol, 'market', 'sell' if side == 'buy' else 'buy', rem, None,
                        order_params
                    )
                    print(f"🚀 [ID: {trade_id}] 觸發 TP2 獨立小倉位全平完勝出場！市場價格: {last_price}")
                    active_trades[trade_id]["remaining"] = 0
                break

            await asyncio.sleep(2)
        except Exception as e:
            print(f"⚠️ [ID: {trade_id}] Monitor Error: {e}")
            await asyncio.sleep(5) 

    # 該筆獨立任務結束，將其從全域池中完全移除
    if trade_id in active_trades:
        del active_trades[trade_id]

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

        # 執行市價開倉
        order = await asyncio.to_thread(
            exchange.create_order,
            symbol, 'market', side, contracts, None,
            {
                'tdMode': 'isolated', 
                'posSide': pos_side
            }
        )
        
        # 自動獲取交易所回傳的真實成交均價（最精準）
        entry_price = order.get('average') or order.get('price')
        if not entry_price:
            ticker = await asyncio.to_thread(exchange.fetch_ticker, symbol)
            entry_price = ticker['last']

        # 開倉成功後，再將此筆交易寫入全域池註冊（依據實際成交均價）
        active_trades[trade_id] = {
            "entry_price": entry_price,
            "contracts": contracts,
            "remaining": contracts,
            "side": side
        }

        print(f"🟢 [ID: {trade_id}] 已執行 {action} 開倉 {contracts} 張 | 實際成交價: {entry_price} | 已獨立註冊至全域池")
        
        # 給交易所半秒鐘時間處理清算
        await asyncio.sleep(0.5)
        
        # 啟動獨立的監控任務
        await monitor_trade(trade_id, symbol, side, pos_side, entry_price, contracts)
        
    except Exception as e:
        if 'trade_id' in locals() and trade_id in active_trades:
            del active_trades[trade_id]
        print(f"❌ Background Task Error: {e}")
