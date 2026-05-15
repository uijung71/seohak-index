"""
src/agent9_live_weights.py
Agent 9: Daily Weights & Strict Monday Rebalancing (v5.2 Rules)
- W_i = S_i (100% Custody Weight)
- Daily Weight Updates based on T+2 Seibro data
- Rebalancing: Every Monday based on 2-week Buffer Zone (90/110)
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROC_DIR = BASE_DIR / "data" / "processed"
OUTPUT_DIR = BASE_DIR / "output"

UNIVERSE_FILE = PROC_DIR / "ticker_universe.csv"
CUSTODY_FILE = RAW_DIR / "custody_daily.csv"
COMPONENTS_FILE = OUTPUT_DIR / "seohak100_components_since_202512.csv"
WEIGHTS_DAILY = PROC_DIR / "weights_daily_live.csv"
WEIGHTS_HISTORY = PROC_DIR / "weights_history_live.csv"
RANK_HISTORY = PROC_DIR / "rank_history_live.csv"

def is_leveraged_etf(ticker, name):
    patterns = [
        r'\d+(\.\d+)?X', r'BULL', r'BEAR', r'ULTRA', r'INVERSE', 
        r'LEVERAGED', r'YIELDMAX', r'REX', r'GRANITESHARES',
        r'DAILY (LONG|SHORT)', r'DOUBLE', r'TRIPLE'
    ]
    combined = (str(ticker) + " " + str(name)).upper()
    for pat in patterns:
        if re.search(pat, combined): return 'Y'
    return 'N'

def extract_ipo_date(status_str):
    match = re.search(r'(\d{4}-\d{2}-\d{2})', str(status_str))
    return match.group(1) if match else "1900-01-01"

def run_weight_generation():
    print("="*60)
    print("Agent 9: Daily Weights & Strict Rebalancing (v5.2)")
    print("="*60)

    if not all([p.exists() for p in [CUSTODY_FILE, UNIVERSE_FILE]]):
        print("[!] 필수 데이터 파일이 부족합니다.")
        return

    # 1. 데이터 로드
    univ = pd.read_csv(UNIVERSE_FILE)
    df_c = pd.read_csv(CUSTODY_FILE)
    latest_date = pd.to_datetime(df_c['date']).max()
    fmt_date = latest_date.strftime("%Y-%m-%d")
    is_monday = (latest_date.weekday() == 0)

    # 2. 유니버스 필터링 (상장일 기준)
    univ['ipo_date'] = univ['status'].apply(extract_ipo_date)
    univ['is_leveraged'] = univ.apply(lambda x: is_leveraged_etf(x['ticker'], x['name_en']), axis=1)
    
    valid_univ = univ[univ['ipo_date'] <= fmt_date]
    valid_isins = set(valid_univ['isin'])

    # 3. 당일 순위 산출 및 이력 업데이트
    df_day = df_c[df_c['date'] == fmt_date].copy()
    df_day = df_day[df_day['isin'].isin(valid_isins)]
    df_day['raw_rank'] = df_day['amount'].rank(ascending=False, method='first').astype(int)
    
    if RANK_HISTORY.exists():
        rank_hist = pd.read_csv(RANK_HISTORY)
        rank_hist = rank_hist[rank_hist['date'] != fmt_date]
        rank_hist = pd.concat([rank_hist, df_day[['isin', 'ticker', 'raw_rank']].copy().assign(date=fmt_date)], ignore_index=True)
    else:
        rank_hist = df_day[['isin', 'ticker', 'raw_rank']].copy().assign(date=fmt_date)
    
    # 최근 14일치(약 10거래일) 유지
    unique_dates = sorted(rank_hist['date'].unique(), reverse=True)[:14]
    rank_hist = rank_hist[rank_hist['date'].isin(unique_dates)]
    rank_hist.to_csv(RANK_HISTORY, index=False, encoding="utf-8-sig")

    # 4. 리밸런싱 로직 (종목 리스트 결정)
    if COMPONENTS_FILE.exists():
        comp_df = pd.read_csv(COMPONENTS_FILE)
        prev_date = comp_df['date'].max()
        prev_members = set(comp_df[comp_df['date'] == prev_date]['ticker'].tolist())
    else:
        prev_members = set()

    if not prev_members:
        # 최초 설정
        final_tickers = df_day.sort_values('raw_rank').head(100)['ticker'].tolist()
        new_comp_needed = True
    elif is_monday:
        # 월요일 리밸런싱 실행
        print(f"  - Monday Detected ({fmt_date}). Checking Buffer Zone Conditions...")
        
        # 2주 연속 조건 확인을 위해 지난 월요일 순위 가져오기
        mondays = sorted([d for d in rank_hist['date'].unique() if pd.to_datetime(d).weekday() == 0], reverse=True)
        
        if len(mondays) >= 2:
            curr_ranks = rank_hist[rank_hist['date'] == mondays[0]].set_index('ticker')['raw_rank'].to_dict()
            prev_ranks = rank_hist[rank_hist['date'] == mondays[1]].set_index('ticker')['raw_rank'].to_dict()
            
            # 퇴출 후보 (2주 연속 110위 밖)
            leavers = [t for t in prev_members if curr_ranks.get(t, 999) > 110 and prev_ranks.get(t, 999) > 110]
            # 진입 후보 (2주 연속 90위 이내)
            entrants = [t for t in df_day['ticker'] if t not in prev_members and curr_ranks.get(t, 999) <= 90 and prev_ranks.get(t, 999) <= 90]
            
            if leavers or entrants:
                print(f"    * Leavers: {leavers}")
                print(f"    * Entrants: {entrants}")
                # 종목 교체
                final_tickers = list((prev_members - set(leavers)) | set(entrants))
                # 100개 조정
                if len(final_tickers) > 100:
                    final_tickers = sorted(final_tickers, key=lambda x: curr_ranks.get(x, 999))[:100]
                elif len(final_tickers) < 100:
                    extras = df_day[~df_day['ticker'].isin(final_tickers)].sort_values('raw_rank').head(100 - len(final_tickers))['ticker'].tolist()
                    final_tickers.extend(extras)
                new_comp_needed = True
            else:
                print("    * No changes met the buffer zone criteria.")
                final_tickers = list(prev_members)
                new_comp_needed = False
        else:
            print("    * Not enough Monday history for buffer check. Maintaining universe.")
            final_tickers = list(prev_members)
            new_comp_needed = False
    else:
        # 평일: 종목 리스트 유지
        final_tickers = list(prev_members)
        new_comp_needed = False

    # 종목 리스트가 변경되었거나 최초인 경우 저장
    if new_comp_needed:
        new_comp = pd.DataFrame({'date': fmt_date, 'ticker': final_tickers})
        if COMPONENTS_FILE.exists():
            old_comp = pd.read_csv(COMPONENTS_FILE)
            old_comp = old_comp[old_comp['date'] != fmt_date]
            new_comp = pd.concat([old_comp, new_comp], ignore_index=True)
        new_comp.to_csv(COMPONENTS_FILE, index=False, encoding="utf-8-sig")
        print(f"  [UPDATE] Ticker Universe Updated for {fmt_date}")

    # 5. 가중치 산출 (W_i = S_i)
    top100 = df_day[df_day['ticker'].isin(final_tickers)].copy()
    
    # Missing Tickers Handling (Universe에 있는데 오늘 데이터에 없는 경우)
    if len(top100) < 100:
        print(f"  [WARN] {100 - len(top100)} tickers missing in today's custody data. Normalizing weights among survivors.")
    
    # Merge leveraged info
    top100 = pd.merge(top100, univ[['isin', 'is_leveraged']], on='isin', how='left')
    
    top100['weight'] = top100['amount'] / top100['amount'].sum()
    top100['S_i'] = top100['weight']
    top100['W_i'] = top100['weight']
    
    leveraged_etf_weight = top100[top100['is_leveraged'] == 'Y']['weight'].sum()

    # 6. 저장
    top100['date'] = fmt_date
    top100 = top100.sort_values(by='amount', ascending=False)
    top100['rank'] = range(1, len(top100) + 1)
    top100['leveraged_etf_weight_total'] = leveraged_etf_weight
    
    top100.to_csv(WEIGHTS_DAILY, index=False, encoding="utf-8-sig")

    # 7. 누적 저장 (Archiving)
    if WEIGHTS_HISTORY.exists():
        history_df = pd.read_csv(WEIGHTS_HISTORY)
        history_df = history_df[history_df['date'] != fmt_date]
        history_df = pd.concat([history_df, top100], ignore_index=True)
        history_df = history_df.sort_values(by=['date', 'rank'], ascending=[False, True])
        # 유지 기간 관리 (최근 100일 스냅샷 정도면 충분)
        history_df = history_df[history_df['date'] >= (pd.to_datetime(fmt_date) - pd.Timedelta(days=100)).strftime('%Y-%m-%d')]
        history_df.to_csv(WEIGHTS_HISTORY, index=False, encoding="utf-8-sig")
    else:
        top100.to_csv(WEIGHTS_HISTORY, index=False, encoding="utf-8-sig")

    print(f"  [SUCCESS] v5.2 일간 가중치 산출 완료 ({fmt_date})")
    print(f"  - 종목 수: {len(top100)}")
    print(f"  - 레버리지 ETF 비중: {leveraged_etf_weight*100:.2f}%")
    print(f"  - 저장 위치: {WEIGHTS_DAILY} (History Updated)")

if __name__ == "__main__":
    run_weight_generation()
