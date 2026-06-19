import ccxt
import logging

# 設定只寫入錯誤到檔案，完全不影響控制台畫面
logging.basicConfig(filename='api_debug.log', level=logging.ERROR)

def test_api():
    try:
        # 請在這裡填入你自己的 OKX 模擬交易 API 資料
        exchange = ccxt.okx({
            'apiKey': 'cea4a4da-a5e1-4124-b589-d0f82f3166ae',
            'secret': 'D03A44C7ED579AADB7B4FA664DDBB92F',
            'password': 'Dd_0977030927',
            'options': {'defaultType': 'swap'}
        })
        exchange.set_sandbox_mode(True) # 確保開啟模擬交易模式
        
        # 嘗試市價買入 0.01 張 ETH 永續合約 (U本位)
        order = exchange.create_market_order('ETH/USDT:USDT', 'buy', 0.01, {
            'posSide': 'long', 'tdMode': 'isolated'
        })
        print("✅ 測試成功！下單ID:", order['id'])
        
    except Exception as e:
        # 如果出錯，錯誤訊息會自動寫入 api_debug.log
        logging.error(f"下單失敗，錯誤原因: {str(e)}")
        print("❌ 下單失敗，請查看 api_debug.log 檔案")

if __name__ == "__main__":
    test_api()
