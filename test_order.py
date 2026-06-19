import ccxt
import logging

# 設定只寫入錯誤到檔案，完全不影響控制台畫面
logging.basicConfig(filename='api_debug.log', level=logging.ERROR)

def test_api():
    try:
        # 請填入你的 API Key
        exchange = ccxt.okx({
            'apiKey': 'YOUR_API_KEY',
            'secret': 'YOUR_SECRET',
            'password': 'YOUR_PASSWORD',
            'options': {'defaultType': 'swap'}
        })
        exchange.set_sandbox_mode(True) # 如果你是在模擬環境
        
        # 嘗試市價買入 0.01 張 (用最小單位測試，風險極低)
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
