"""
src/agent10_custody_health.py
Agent 10: Custody Data Health Monitoring System
- Tracks SEIBRO custody data continuity
- Identifies Drop-to-Zero and Consistent-Zero tickers
- Maintains custody_health_master.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path
import datetime

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
SNAP_DIR = RAW_DIR / "snapshots"
PROC_DIR = BASE_DIR / "data" / "processed"
HEALTH_MASTER = PROC_DIR / "custody_health_master.csv"
UNIVERSE_DATA = PROC_DIR / "ticker_universe.csv"
HISTORICAL_FILE = RAW_DIR / "custody_weekly.csv.csv"

def run_health_check():
    print("="*60)
    print("Agent 10: Custody Data Health Monitoring")
    print("="*60)

    # 1. 유니버스 로드
    if not UNIVERSE_DATA.exists():
        print("[!] Ticker universe not found.")
        return
    univ_df = pd.read_csv(UNIVERSE_DATA)
    tickers = univ_df['ticker'].tolist()
    
    # 2. 결과 저장을 위한 데이터프레임 초기화
    if HEALTH_MASTER.exists():
        health_df = pd.read_csv(HEALTH_MASTER)
    else:
        health_df = univ_df[['isin', 'ticker', 'name_en']].copy()
        health_df['last_valid_date'] = "1970-01-01"
        health_df['last_valid_amount'] = 0.0
        health_df['status'] = "UNKNOWN"

    # 3. 데이터 스캔 (과거 주간 데이터 + 일간 스냅샷)
    # 3-1. 과거 주간 데이터 처리
    if HISTORICAL_FILE.exists():
        print(f"  - Scanning historical weekly file: {HISTORICAL_FILE.name}")
        hist = pd.read_csv(HISTORICAL_FILE)
        hist['amount_clean'] = pd.to_numeric(hist['amount'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        hist['date_dt'] = pd.to_datetime(hist['datr'])
        
        for idx, row in health_df.iterrows():
            ticker = row['ticker']
            h_data = hist[(hist['ticker'] == ticker) & (hist['amount_clean'] > 0)]
            if not h_data.empty:
                max_date = h_data['date_dt'].max()
                max_amt = h_data.loc[h_data['date_dt'].idxmax(), 'amount_clean']
                if str(max_date.date()) > str(row['last_valid_date']):
                    health_df.at[idx, 'last_valid_date'] = str(max_date.date())
                    health_df.at[idx, 'last_valid_amount'] = max_amt

    # 3-2. 일간 스냅샷 스캔
    snap_files = sorted(SNAP_DIR.glob("custody_*.csv"))
    print(f"  - Scanning {len(snap_files)} snapshots in {SNAP_DIR.name}")
    for f in snap_files:
        try:
            df = pd.read_csv(f)
            # date format might be YYYY-MM-DD or YYYYMMDD
            file_date_str = f.stem.split('_')[1]
            if len(file_date_str) == 8:
                file_date = datetime.datetime.strptime(file_date_str, "%Y%m%d").strftime("%Y-%m-%d")
            else:
                file_date = file_date_str
                
            for idx, row in health_df.iterrows():
                ticker = row['ticker']
                match = df[(df['ticker'] == ticker) & (df['amount'] > 0)]
                if not match.empty:
                    amt = match.iloc[0]['amount']
                    if file_date > str(health_df.at[idx, 'last_valid_date']):
                        health_df.at[idx, 'last_valid_date'] = file_date
                        health_df.at[idx, 'last_valid_amount'] = amt
        except Exception as e:
            print(f"    [!] Error reading {f.name}: {e}")

    # 4. 상태 판정
    # 오늘 날짜 (데이터 기준일)
    latest_snap_date = health_df['last_valid_date'].max()
    
    # 마지막 성공일로부터의 기간 계산
    today = datetime.datetime.now().date()
    
    def determine_status(row):
        last_date = datetime.datetime.strptime(row['last_valid_date'], "%Y-%m-%d").date()
        diff = (today - last_date).days
        
        if row['last_valid_date'] == "1970-01-01":
            return "NEVER_COLLECTED"
        elif diff <= 10:
            return "ACTIVE"
        elif diff <= 90:
            return "INACTIVE_RECENT"
        else:
            return "DEFUNCT_CANDIDATE"

    health_df['status'] = health_df.apply(determine_status, axis=1)
    
    # 5. 결과 저장
    health_df.to_csv(HEALTH_MASTER, index=False, encoding="utf-8-sig")
    
    # 6. 리포트 출력
    print("\n" + "="*60)
    print("Health Status Summary")
    print("="*60)
    summary = health_df['status'].value_counts()
    for status, count in summary.items():
        print(f"  - {status:20}: {count} tickers")
    
    print("\n[Drop-to-Zero] Tickers (Last valid data > 90 days ago):")
    defunct = health_df[health_df['status'] == "DEFUNCT_CANDIDATE"].sort_values('last_valid_date', ascending=False)
    if not defunct.empty:
        print(defunct[['ticker', 'last_valid_date', 'last_valid_amount']].head(20))
    else:
        print("  None")

    print(f"\n[SUCCESS] Health master updated: {HEALTH_MASTER}")

if __name__ == "__main__":
    run_health_check()
