"""
src/agent8_live_index.py
Agent 8: Daily Index Calculation (v5.2 Rules)
- Single Official Daily Index Output (USD & KRW)
- Real-time FX conversion based on Rule 17
- Chaining from the latest official point in seohak100_daily_index.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
PROC_DIR = BASE_DIR / "data" / "processed"
RAW_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = BASE_DIR / "output"

PRICE_MASTER = PROC_DIR / "price_daily_master_live.csv"
WEIGHTS_DAILY = PROC_DIR / "weights_daily_live.csv"
FX_FILE = RAW_DIR / "fx_daily.csv"
INDEX_OUTPUT = OUTPUT_DIR / "seohak100_daily_index.csv"

BASE_FX = 1167.56  # Rule 17: Reference FX rate from 2020-01-06

def run_index_calculation():
    print("="*60)
    print("Agent 8: Daily Index Calculation (v5.2)")
    print("="*60)

    if not all([p.exists() for p in [PRICE_MASTER, WEIGHTS_DAILY, INDEX_OUTPUT, FX_FILE]]):
        print("[!] 필수 데이터 파일이 부족합니다.")
        return

    # 1. 최신 가중치 로드 (W_i = S_i)
    weights_df = pd.read_csv(WEIGHTS_DAILY)
    weight_map = dict(zip(weights_df['ticker'], weights_df['W_i']))
    target_tickers = weights_df['ticker'].unique().tolist()
    
    # Metadata
    leveraged_etf_weight = weights_df['leveraged_etf_weight_total'].iloc[0]

    # 2. 가격 및 지수 이력 로드
    prices = pd.read_csv(PRICE_MASTER)
    prices['date'] = pd.to_datetime(prices['date'], format='mixed')
    
    fx_df = pd.read_csv(FX_FILE)
    fx_df['date'] = pd.to_datetime(fx_df['date'], format='mixed')
    fx_map = dict(zip(fx_df['date'], fx_df['rate']))
    
    index_hist = pd.read_csv(INDEX_OUTPUT)
    index_hist['date'] = pd.to_datetime(index_hist['date'], format='mixed')
    index_hist = index_hist.sort_values('date')
    
    last_row = index_hist.iloc[-1]
    last_date = last_row['date']
    last_index_usd = last_row['index_point_usd']
    
    # 3. 산출 대상 날짜 결정
    available_dates = sorted(prices[prices['date'] > last_date]['date'].unique())
    
    if not available_dates:
        print(f"  - No new price data since {last_date.date()}. Index is up to date.")
        return

    new_records = []
    current_index_usd = last_index_usd
    
    relevant_prices = prices[prices['date'].isin(available_dates)]
    pivot_returns = relevant_prices.pivot(index='date', columns='ticker', values='daily_return').fillna(0)

    for dt in available_dates:
        day_rets = pivot_returns.loc[dt]
        
        daily_ret = 0
        w_sum = 0
        for ticker, weight in weight_map.items():
            if ticker in day_rets.index:
                daily_ret += weight * day_rets[ticker]
                w_sum += weight
        
        if w_sum > 0: daily_ret /= w_sum
        else: daily_ret = 0.0
            
        current_index_usd *= (1 + daily_ret)
        
        # 4. 원화 지수 환산 (Rule 17)
        rate = fx_map.get(dt)
        if rate is None:
            # Look for last available rate
            past_rates = fx_df[fx_df['date'] < dt].sort_values('date', ascending=False)
            rate = past_rates.iloc[0]['rate'] if not past_rates.empty else BASE_FX
        
        current_index_krw = current_index_usd * (rate / BASE_FX)
        
        new_records.append({
            'date': dt.strftime('%Y-%m-%d'),
            'index_point_usd': current_index_usd,
            'index_point_krw': current_index_krw,
            'daily_return': daily_ret,
            'phase': '2',
            'leveraged_etf_weight': leveraged_etf_weight,
            'data_lag_days': 2
        })
        print(f"  - {dt.date()} | USD: {current_index_usd:.2f} | KRW: {current_index_krw:.2f} | Ret: {daily_ret*100:.4f}%")

    # 5. 결과 업데이트 (Rule 21.1 컬럼 구조 준수)
    new_df = pd.DataFrame(new_records)
    updated_index = pd.concat([index_hist, new_df], ignore_index=True)
    
    cols = ['date', 'index_point_usd', 'index_point_krw', 'daily_return', 'phase', 'leveraged_etf_weight', 'data_lag_days']
    # Ensure columns exist in updated_index
    for c in cols:
        if c not in updated_index.columns: updated_index[c] = np.nan
        
    updated_index[cols].to_csv(INDEX_OUTPUT, index=False, encoding="utf-8-sig")

    print(f"  [SUCCESS] {len(new_records)}개 일간 지수 포인트 추가 완료.")
    print(f"  - 최종 지수(USD): {current_index_usd:.2f}")

if __name__ == "__main__":
    run_index_calculation()
