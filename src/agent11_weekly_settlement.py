"""
src/agent11_weekly_settlement.py
Agent 11: Weekly Market Settlement Data Collector (WebSquare API)
- Fetches Top 100 US Market Settlement Data (Weekly)
- Saves as Excel (.xlsx) to the specified Google Drive sync folder
- Uses Session and Referer to bypass SEIBRO security
"""

import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
import sys

# 프로젝트 루트를 path에 추가하여 src 패키지 인식
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.utils_telegram import send_telegram_message

# --- Configuration ---
SYNC_PATH = Path(r"C:\Users\1\OneDrive\Documents\Drive Sync")
TG_TOKEN = '8720582478:AAGakD7M2_-8uoGXYSTGK-fsZmJzpxBJZRU'
TG_CHAT_ID = '8356746472'
WS_BASE = 'https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/ovsSec/BIP_CNTS10013V.xml&menuNo=921'
WS_POST = 'https://seibro.or.kr/websquare/engine/proworks/callServletService.jsp'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Content-Type': 'application/xml; charset=UTF-8',
    'Referer': WS_BASE
}

def fetch_weekly_settlement():
    print("="*60)
    print("Agent 11: Weekly US Market Settlement Data Collector")
    print("="*60)

    # Calculate date range: Last 7 days from today
    today = datetime.now()
    # End date: T-1 (as confirmed available on the website)
    # Start date: 7 days ago from end date
    end_dt_obj = today - timedelta(days=1)
    start_dt_obj = end_dt_obj - timedelta(days=6) # 7 days total (e.g. 5.1 ~ 5.7)
    
    start_dt = start_dt_obj.strftime("%Y%m%d")
    end_dt = end_dt_obj.strftime("%Y%m%d")
    
    print(f"  Query Period: {start_dt} ~ {end_dt}")

    payload = f"""<reqParam action="getImptFrcurStkSetlAmtList" task="ksd.safe.bip.cnts.OvsSec.process.OvsSecIsinPTask">
  <MENU_NO value="921"/>
  <PG_START value="1"/>
  <PG_END value="100"/>
  <START_DT value="{start_dt}"/>
  <END_DT value="{end_dt}"/>
  <S_TYPE value="2"/>
  <S_COUNTRY value="US"/>
  <D_TYPE value="3"/>
</reqParam>"""

    try:
        session = requests.Session()
        # Initial hit to get cookies
        session.get(WS_BASE, headers={'User-Agent': HEADERS['User-Agent']})
        
        resp = session.post(WS_POST, data=payload.encode('utf-8'), headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            print(f"  [ERROR] API Call Failed (Status: {resp.status_code})")
            return

        if "<result>" not in resp.text:
            print(f"  [ERROR] No result found. Check if the period is valid.")
            return

        root = ET.fromstring(resp.text)
        results = []
        for res in root.findall(".//result"):
            results.append({
                'Rank': res.find("RNUM").attrib.get("value"),
                'Stock Name': res.find("KOR_SECN_NM").attrib.get("value"),
                'ISIN': res.find("ISIN").attrib.get("value"),
                'Buy Amount (USD)': float(res.find("SUM_FRSEC_BUY_AMT").attrib.get("value", 0)),
                'Sell Amount (USD)': float(res.find("SUM_FRSEC_SELL_AMT").attrib.get("value", 0)),
                'Net Buy Amount (USD)': float(res.find("SUM_FRSEC_NET_BUY_AMT").attrib.get("value", 0)),
                'Total Settlement (USD)': float(res.find("SUM_FRSEC_TOT_AMT").attrib.get("value", 0))
            })

        df = pd.DataFrame(results)
        
        # Ensure sync folder exists
        SYNC_PATH.mkdir(parents=True, exist_ok=True)
        
        filename = f"US_Weekly_Settlement_{start_dt}_{end_dt}.xlsx"
        save_path = SYNC_PATH / filename
        
        # Save to Excel
        df.to_excel(save_path, index=False)
        print(f"  [SUCCESS] Report saved to sync folder: {save_path}")
        
        # Telegram Notification
        tg_msg = f"✅ *미국 주간 결제 데이터 수집 완료*\n\n📅 기간: {start_dt} ~ {end_dt}\n📁 저장: {filename}\n📊 수집 종목: {len(df)}개"
        send_telegram_message(TG_TOKEN, TG_CHAT_ID, tg_msg)
        
    except Exception as e:
        print(f"  [CRITICAL] An error occurred: {e}")

if __name__ == "__main__":
    fetch_weekly_settlement()
