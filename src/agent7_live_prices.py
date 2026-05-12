"""
에이전트 7: 라이브 가격 데이터 수집 (Phase 2 Daily)
- EODHD API로 일간(Daily) Return Price 수집 (period='d')
- 일간 수익률(daily_return) 계산 및 누락 탐지
- price_daily_master_live.csv 및 daily_returns_live.csv 생성
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import time
import requests
import warnings
warnings.filterwarnings("ignore")

# ── 경로 설정 ──────────────────────────────────────────
BASE = Path(__file__).resolve().parent.parent
PROCESSED = BASE / "data" / "processed"
REFERENCE = BASE / "data" / "reference"
REFERENCE.mkdir(parents=True, exist_ok=True)

# 입력 파일
TICKER_UNIVERSE = PROCESSED / "ticker_universe.csv"
DATA_ISSUES = PROCESSED / "data_issues.csv"

# 출력 파일
RETURN_PRICE_FILE = PROCESSED / "return_price_daily_live.csv"
PRICE_FAILED_FILE = PROCESSED / "price_fetch_failed_live.csv"
DAILY_RETURNS_FILE = PROCESSED / "daily_returns_live.csv"
PRICE_MASTER_FILE = PROCESSED / "price_daily_master_live.csv"
DELISTED_FILE = PROCESSED / "delisted_candidates_live.csv"

# 수집 기간 (Phase 2 실시간 지수 산출을 위해 2025-12-01부터 넉넉하게 수집)
START_DATE = "2025-12-01"
# END_DATE는 명시하지 않으면 EODHD에서 오늘까지 가장 최신 데이터를 가져옴.

# ══════════════════════════════════════════════════════
# STEP 1: EODHD로 Daily Return Price 수집
# ══════════════════════════════════════════════════════
def fetch_daily_prices():
    print("=" * 60, flush=True)
    print("STEP 1: EODHD 일간(Daily) 가격 수집", flush=True)
    print("=" * 60, flush=True)

    API_TOKEN = "693abf5882dab9.42616862"

    if not TICKER_UNIVERSE.exists():
        print(f"Error: {TICKER_UNIVERSE} 가 존재하지 않습니다.")
        return

    ticker_universe = pd.read_csv(TICKER_UNIVERSE)
    all_tickers = ticker_universe["ticker"].dropna().unique().tolist()
    ticker_to_isin = dict(zip(ticker_universe["ticker"], ticker_universe["isin"]))

    # EODHD 형식 매핑
    EODHD_TICKER_MAP = {
        "BRK.A": "BRK-A",
        "BRK.B": "BRK-B",
    }

    print(f"  수집 대상: {len(all_tickers)} tickers", flush=True)
    print(f"  시작일: {START_DATE} ~ 최신", flush=True)

    all_results = []
    failed_tickers = []
    success_count = 0

    print("  EODHD API 개별 Fetch 시작...", flush=True)
    for i, orig_ticker in enumerate(all_tickers):
        print(f"    [{i+1}/{len(all_tickers)}] Fetching {orig_ticker}...", end=" ", flush=True)
        eodhd_ticker = EODHD_TICKER_MAP.get(orig_ticker, orig_ticker)
        url = f"https://eodhd.com/api/eod/{eodhd_ticker}.US"
        params = {
            "api_token": API_TOKEN,
            "fmt": "json",
            "period": "d", # 일간단위
            "from": START_DATE
        }
        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if len(data) > 0:
                    isin = ticker_to_isin.get(orig_ticker, "")
                    dates = [item['date'] for item in data]
                    prices = [item['adjusted_close'] for item in data]
                    df_t = pd.DataFrame({
                        "date": dates,
                        "isin": isin,
                        "ticker": orig_ticker,
                        "return_price": prices,
                    })
                    all_results.append(df_t)
                    success_count += 1
                    print("OK", flush=True)
                else:
                    failed_tickers.append(orig_ticker)
                    print("EMPTY", flush=True)
            else:
                failed_tickers.append(orig_ticker)
                print(f"FAIL ({resp.status_code})", flush=True)
        except Exception as e:
            failed_tickers.append(orig_ticker)
            print(f"ERROR ({str(e)})", flush=True)
        
        if (i+1) % 50 == 0:
            print(f"    진행 상황: {i+1} / {len(all_tickers)} 완료", flush=True)

    print(f"EODHD API 수집 완료: 성공 {success_count} / 전체 {len(all_tickers)}", flush=True)

    if all_results:
        return_prices = pd.concat(all_results, ignore_index=True)
        return_prices.to_csv(RETURN_PRICE_FILE, index=False, encoding="utf-8-sig")
        print(f"  -> {RETURN_PRICE_FILE} ({len(return_prices):,} rows)", flush=True)
    else:
        return_prices = pd.DataFrame(columns=["date", "isin", "ticker", "return_price"])
        return_prices.to_csv(RETURN_PRICE_FILE, index=False, encoding="utf-8-sig")

    if failed_tickers:
        failed_df = pd.DataFrame({"ticker": failed_tickers})
        failed_df["isin"] = failed_df["ticker"].map(ticker_to_isin)
        failed_df.to_csv(PRICE_FAILED_FILE, index=False, encoding="utf-8-sig")
        print(f"  -> {PRICE_FAILED_FILE} ({len(failed_tickers)} tickers)", flush=True)

    return return_prices

# ══════════════════════════════════════════════════════
# STEP 2: 일간 수익률 계산
# ══════════════════════════════════════════════════════
def calculate_daily_returns(return_prices):
    print("\n" + "=" * 60, flush=True)
    print("STEP 2: 일간 수익률 계산 및 이슈 기록", flush=True)
    print("=" * 60, flush=True)

    if len(return_prices) == 0:
        print("  처리할 데이터가 없습니다.", flush=True)
        return

    fetched_tickers = return_prices["ticker"].unique().tolist()
    return_prices["date"] = pd.to_datetime(return_prices["date"]).dt.strftime("%Y-%m-%d")

    all_returns = []
    delisted_candidates = []
    
    # data_issues 읽기
    existing_issues = pd.read_csv(DATA_ISSUES) if DATA_ISSUES.exists() else pd.DataFrame()
    new_issues = []

    carry_forward_count = 0
    tickers_with_cf = set()

    for ticker in fetched_tickers:
        df_t = return_prices[return_prices["ticker"] == ticker].copy()
        df_t = df_t.sort_values("date").reset_index(drop=True)

        if len(df_t) < 2:
            continue

        isin = df_t["isin"].iloc[0]

        df_t["daily_return"] = df_t["return_price"].pct_change()
        df_t["data_flag"] = ""

        dates = pd.to_datetime(df_t["date"])
        for i in range(1, len(dates)):
            diff_days = (dates.iloc[i] - dates.iloc[i - 1]).days
            if diff_days > 4:  # 휴장일 감안, 4일 초과 gap 발생 시 이상으로 간주
                if diff_days >= 14:
                    # 2주 이상 연속 누락 -> delisted 후보
                    delisted_candidates.append({
                        "ticker": ticker,
                        "isin": isin,
                        "last_date": dates.iloc[i - 1].strftime("%Y-%m-%d"),
                        "resume_date": dates.iloc[i].strftime("%Y-%m-%d"),
                        "gap_days": diff_days,
                    })
                    new_issues.append({
                        "date": dates.iloc[i - 1].strftime("%Y-%m-%d"),
                        "isin": isin,
                        "ticker": ticker,
                        "issue_type": "DELISTED_CANDIDATE_LIVE",
                        "detail": f"{diff_days}일 연속 가격 누락 — delisted 후보",
                    })
                    df_t.loc[df_t.index[i], "data_flag"] = "DELISTED_CANDIDATE"
                elif diff_days > 4:
                    # 일시적 누락(5~13일) -> carry-forward 기록용
                    carry_forward_count += 1
                    tickers_with_cf.add(ticker)
                    new_issues.append({
                        "date": dates.iloc[i - 1].strftime("%Y-%m-%d"),
                        "isin": isin,
                        "ticker": ticker,
                        "issue_type": "CARRY_FORWARD_LIVE",
                        "detail": f"{diff_days}일 가격 누락 (r_i=0%)",
                    })
                    df_t.loc[df_t.index[i], "daily_return"] = 0.0
                    df_t.loc[df_t.index[i], "data_flag"] = "CARRY_FORWARD"

            # 가격 이상치 탐지 (일간 등락률 30% 이상)
            dr = df_t.loc[df_t.index[i], "daily_return"]
            if pd.notna(dr) and abs(dr) > 0.30:
                new_issues.append({
                    "date": dates.iloc[i].strftime("%Y-%m-%d"),
                    "isin": isin,
                    "ticker": ticker,
                    "issue_type": "PRICE_GAP_LIVE",
                    "detail": f"일간 등락률 {dr*100:.1f}% 초과",
                })
                df_t.loc[df_t.index[i], "data_flag"] = "PRICE_GAP"

        all_returns.append(df_t)

    if all_returns:
        daily_returns = pd.concat(all_returns, ignore_index=True)
        daily_returns = daily_returns[["date", "isin", "ticker", "return_price", "daily_return", "data_flag"]]
        daily_returns.to_csv(DAILY_RETURNS_FILE, index=False, encoding="utf-8-sig")
        daily_returns.to_csv(PRICE_MASTER_FILE, index=False, encoding="utf-8-sig")
        print(f"  -> {PRICE_MASTER_FILE} ({len(daily_returns):,} rows)", flush=True)
    else:
        daily_returns = pd.DataFrame(columns=["date", "isin", "ticker", "return_price", "daily_return", "data_flag"])
        daily_returns.to_csv(PRICE_MASTER_FILE, index=False, encoding="utf-8-sig")

    if delisted_candidates:
        del_df = pd.DataFrame(delisted_candidates)
        del_df.to_csv(DELISTED_FILE, index=False, encoding="utf-8-sig")
        print(f"  -> {DELISTED_FILE} ({len(del_df)} rows)", flush=True)

    if new_issues:
        issues_df = pd.DataFrame(new_issues)
        merged_issues = pd.concat([existing_issues, issues_df], ignore_index=True)
        merged_issues.to_csv(DATA_ISSUES, index=False, encoding="utf-8-sig")

    print(f"  Carry-forward 처리: {carry_forward_count}건 ({len(tickers_with_cf)} tickers)", flush=True)
    print(f"  Delisted 후보: {len(delisted_candidates)}건", flush=True)
    print(f"  Data Issue 신규 기록: {len(new_issues)}건", flush=True)

if __name__ == "__main__":
    prices = fetch_daily_prices()
    if prices is not None:
        calculate_daily_returns(prices)
    print("\n[완료] 에이전트 7 라이브 가격 수집 종료", flush=True)
