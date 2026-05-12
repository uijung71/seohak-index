"""
에이전트 2: 가격 데이터 수집 및 수익률 계산
- yfinance로 Return Price 수집
- 주간 수익률 계산 (carry-forward, delisted 처리)
- price_weekly_master.csv 생성
- 가격 이상 탐지
"""
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path
import time
import warnings
warnings.filterwarnings("ignore")

# ── 경로 설정 ──────────────────────────────────────────
BASE = Path(__file__).resolve().parent.parent
PROCESSED = BASE / "data" / "processed"
REFERENCE = BASE / "data" / "reference"
REFERENCE.mkdir(parents=True, exist_ok=True)

# 입력 파일
CUSTODY_CLEAN = PROCESSED / "custody_weekly_clean.csv"
TICKER_UNIVERSE = PROCESSED / "ticker_universe.csv"
DATA_ISSUES = PROCESSED / "data_issues.csv"

# 출력 파일
RETURN_PRICE_FILE = PROCESSED / "return_price_weekly.csv"
PRICE_FAILED_FILE = PROCESSED / "price_fetch_failed.csv"
WEEKLY_RETURNS_FILE = PROCESSED / "weekly_returns.csv"
PRICE_MASTER_FILE = PROCESSED / "price_weekly_master.csv"
DELISTED_FILE = PROCESSED / "delisted_candidates.csv"
CORP_ACTIONS_FILE = REFERENCE / "corporate_actions_reference.csv"

# 수집 기간
START_DATE = "2019-12-23"
END_DATE = "2026-01-19"

# ════════════════════════════════════════════════════�# ══════════════════════════════════════════════════════
# STEP 2: EODHD로 Return Price 수집
# ══════════════════════════════════════════════════════
print()
print("=" * 60)
print("STEP 2: EODHD Return Price 수집")
print("=" * 60)

import requests
import time

API_TOKEN = "693abf5882dab9.42616862"

ticker_universe = pd.read_csv(TICKER_UNIVERSE)
all_tickers = ticker_universe["ticker"].dropna().unique().tolist()
ticker_to_isin = dict(zip(ticker_universe["ticker"], ticker_universe["isin"]))

# EODHD 형식 매핑 (BRK.B 형태를 BRK-B 등으로 변환 필요시 사용, 여기서는 기본 유지)
EODHD_TICKER_MAP = {
    "BRK.A": "BRK-A",
    "BRK.B": "BRK-B",
}

print(f"  수집 대상: {len(all_tickers)} tickers")
print(f"  기간: {START_DATE} ~ {END_DATE}")

# 캐시 확인 — 이미 수집된 파일이 있으면 재사용
if RETURN_PRICE_FILE.exists():
    print(f"  기존 파일 발견: {RETURN_PRICE_FILE}")
    print(f"  캐시 사용 (재수집 시 파일 삭제 후 재실행)")
    return_prices = pd.read_csv(RETURN_PRICE_FILE)
    failed_tickers = []
    if PRICE_FAILED_FILE.exists():
        failed_df = pd.read_csv(PRICE_FAILED_FILE)
        failed_tickers = failed_df["ticker"].tolist()
    fetched_tickers = return_prices["ticker"].unique().tolist()
else:
    all_results = []
    failed_tickers = []
    success_count = 0

    print("  EODHD API 개별 Fetch 시작...")
    for i, orig_ticker in enumerate(all_tickers):
        eodhd_ticker = EODHD_TICKER_MAP.get(orig_ticker, orig_ticker)
        url = f"https://eodhd.com/api/eod/{eodhd_ticker}.US"
        params = {
            "api_token": API_TOKEN,
            "fmt": "json",
            "period": "w", # 주간단위 리턴
            "from": START_DATE,
            "to": END_DATE
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
                else:
                    failed_tickers.append(orig_ticker)
            else:
                failed_tickers.append(orig_ticker)
        except Exception as e:
            failed_tickers.append(orig_ticker)
        
        if (i+1) % 50 == 0:
            print(f"    진행 상황: {i+1} / {len(all_tickers)} 완료")

    print(f"EODHD API 수집 완료: 성공 {success_count} / 전체 {len(all_tickers)}")


    # 결과 저장
    if all_results:
        return_prices = pd.concat(all_results, ignore_index=True)
        return_prices.to_csv(RETURN_PRICE_FILE, index=False, encoding="utf-8-sig")
        print(f"  -> {RETURN_PRICE_FILE} ({len(return_prices):,} rows)")
    else:
        return_prices = pd.DataFrame(columns=["date", "isin", "ticker", "return_price"])
        return_prices.to_csv(RETURN_PRICE_FILE, index=False, encoding="utf-8-sig")

    # 실패 ticker 저장
    if failed_tickers:
        failed_df = pd.DataFrame({"ticker": failed_tickers})
        failed_df["isin"] = failed_df["ticker"].map(ticker_to_isin)
        failed_df.to_csv(PRICE_FAILED_FILE, index=False, encoding="utf-8-sig")
        print(f"  -> {PRICE_FAILED_FILE} ({len(failed_tickers)} tickers)")

    fetched_tickers = return_prices["ticker"].unique().tolist() if len(return_prices) > 0 else []

print(f"  수집 성공: {len(fetched_tickers)} / {len(all_tickers)} tickers")
print(f"  수집 실패: {len(failed_tickers)} tickers")

if len(failed_tickers) >= 20:
    print(f"  [보고] 수집 실패 {len(failed_tickers)}개 — 20개 이상이므로 보고합니다.")
    print(f"  실패 목록: {', '.join(failed_tickers[:30])}{'...' if len(failed_tickers) > 30 else ''}")

# ══════════════════════════════════════════════════════
# STEP 3: 주간 수익률 계산
# ══════════════════════════════════════════════════════
print()
print("=" * 60)
print("STEP 3: 주간 수익률 계산")
print("=" * 60)

# data_issues 읽기 (에이전트 1에서 생성된 기존 이슈에 추가)
existing_issues = pd.read_csv(DATA_ISSUES) if DATA_ISSUES.exists() else pd.DataFrame()
new_issues = []
delisted_candidates = []

# custody 날짜 목록 (기준 주간 날짜)
custody = pd.read_csv(CUSTODY_CLEAN)
ref_dates = sorted(custody["date"].unique())

return_prices["date"] = pd.to_datetime(return_prices["date"]).dt.strftime("%Y-%m-%d")

all_returns = []
carry_forward_count = 0
tickers_with_cf = set()

for ticker in fetched_tickers:
    df_t = return_prices[return_prices["ticker"] == ticker].copy()
    df_t = df_t.sort_values("date").reset_index(drop=True)

    if len(df_t) < 2:
        continue

    isin = df_t["isin"].iloc[0]

    # 주간 수익률 계산
    df_t["weekly_return"] = df_t["return_price"].pct_change()
    df_t["data_flag"] = ""

    # 연속 누락 탐지 (ref_dates 기준이 아니라, 자체 시계열 gap 기준)
    dates = pd.to_datetime(df_t["date"])
    gaps = []
    for i in range(1, len(dates)):
        diff_days = (dates.iloc[i] - dates.iloc[i - 1]).days
        if diff_days > 10:  # 1주 = 7일, 2주 gap => > 10일
            gap_weeks = diff_days // 7
            if gap_weeks >= 3:
                # 3주 이상 연속 누락 — delisted 후보
                delisted_candidates.append({
                    "ticker": ticker,
                    "isin": isin,
                    "last_date": dates.iloc[i - 1].strftime("%Y-%m-%d"),
                    "resume_date": dates.iloc[i].strftime("%Y-%m-%d"),
                    "gap_weeks": gap_weeks,
                })
                new_issues.append({
                    "date": dates.iloc[i - 1].strftime("%Y-%m-%d"),
                    "isin": isin,
                    "ticker": ticker,
                    "issue_type": "DELISTED_CANDIDATE",
                    "detail": f"{gap_weeks}주 연속 가격 누락 — delisted 후보",
                })
                df_t.loc[df_t.index[i], "data_flag"] = "DELISTED_CANDIDATE"
            elif gap_weeks >= 2:
                # 1~2주 누락 — carry-forward
                carry_forward_count += 1
                tickers_with_cf.add(ticker)
                new_issues.append({
                    "date": dates.iloc[i - 1].strftime("%Y-%m-%d"),
                    "isin": isin,
                    "ticker": ticker,
                    "issue_type": "CARRY_FORWARD",
                    "detail": f"{gap_weeks}주 가격 누락 — carry-forward 적용 (r_i=0%)",
                })
                df_t.loc[df_t.index[i], "weekly_return"] = 0.0
                df_t.loc[df_t.index[i], "data_flag"] = "CARRY_FORWARD"

    all_returns.append(df_t)

# weekly_returns 저장
if all_returns:
    weekly_returns = pd.concat(all_returns, ignore_index=True)
    weekly_returns = weekly_returns[["date", "isin", "ticker", "return_price", "weekly_return", "data_flag"]]
    weekly_returns.to_csv(WEEKLY_RETURNS_FILE, index=False, encoding="utf-8-sig")
    print(f"  -> {WEEKLY_RETURNS_FILE} ({len(weekly_returns):,} rows)")
else:
    weekly_returns = pd.DataFrame(columns=["date", "isin", "ticker", "return_price", "weekly_return", "data_flag"])
    weekly_returns.to_csv(WEEKLY_RETURNS_FILE, index=False, encoding="utf-8-sig")

# delisted_candidates 저장
if delisted_candidates:
    del_df = pd.DataFrame(delisted_candidates)
    del_df.to_csv(DELISTED_FILE, index=False, encoding="utf-8-sig")
    print(f"  -> {DELISTED_FILE} ({len(del_df)} rows)")

print(f"  Carry-forward 처리: {carry_forward_count}건 ({len(tickers_with_cf)} tickers)")
print(f"  Delisted 후보: {len(delisted_candidates)}건")

# ══════════════════════════════════════════════════════
# STEP 4: price_weekly_master.csv 생성
# ══════════════════════════════════════════════════════
print()
print("=" * 60)
print("STEP 4: price_weekly_master.csv 생성")
print("=" * 60)

custody = pd.read_csv(CUSTODY_CLEAN)

# custody의 observed_price
obs = custody[["date", "isin", "ticker", "price_stock"]].copy()
obs.rename(columns={"price_stock": "observed_price"}, inplace=True)

# return_price (weekly_returns에서 가져옴)
ret = weekly_returns[["date", "isin", "ticker", "return_price"]].copy()

# isin + date 기준 결합 (outer join으로 모든 데이터 포함)
master = pd.merge(obs, ret, on=["date", "isin", "ticker"], how="outer")
master = master.sort_values(["ticker", "date"]).reset_index(drop=True)

# price_source 결정
def get_price_source(row):
    has_obs = pd.notna(row["observed_price"])
    has_ret = pd.notna(row["return_price"])
    if has_obs and has_ret:
        return "SEIBRO"  # 두 소스 모두 존재
    elif has_obs:
        return "SEIBRO"
    elif has_ret:
        return "YFINANCE"
    else:
        return ""

master["price_source"] = master.apply(get_price_source, axis=1)
master["event_flag"] = "N"
master["validation_note"] = ""

# 컬럼 정렬
master = master[["date", "isin", "ticker", "observed_price", "return_price",
                  "price_source", "event_flag", "validation_note"]]

master.to_csv(PRICE_MASTER_FILE, index=False, encoding="utf-8-sig")
print(f"  -> {PRICE_MASTER_FILE} ({len(master):,} rows)")

# ══════════════════════════════════════════════════════
# STEP 5: 가격 이상 탐지
# ══════════════════════════════════════════════════════
print()
print("=" * 60)
print("STEP 5: 가격 이상 탐지")
print("=" * 60)

# weekly_returns와 master 결합하여 이상 탐지
anomaly_df = master[
    master["observed_price"].notna() & master["return_price"].notna()
].copy()

# weekly_return 결합
wr_lookup = weekly_returns[["date", "ticker", "weekly_return"]].copy()
anomaly_df = anomaly_df.merge(wr_lookup, on=["date", "ticker"], how="left")

# 조건 1: abs(weekly_return) > 30%
cond1 = anomaly_df["weekly_return"].abs() > 0.30

# 조건 2: 가격 괴리 > 20%
anomaly_df["price_gap"] = (
    (anomaly_df["observed_price"] - anomaly_df["return_price"]).abs()
    / anomaly_df["observed_price"]
)
cond2 = anomaly_df["price_gap"] > 0.20

# 단일 조건 기록
cond1_only = cond1 & ~cond2
cond2_only = cond2 & ~cond1
both_conds = cond1 & cond2

# 조건1만: 기록
for _, row in anomaly_df[cond1_only].iterrows():
    new_issues.append({
        "date": row["date"], "isin": row["isin"], "ticker": row["ticker"],
        "issue_type": "PRICE_GAP",
        "detail": f"주간 변화율 {row['weekly_return']:.1%} (>30%) — 고변동 종목 정상 가능",
    })

# 조건2만: 기록
for _, row in anomaly_df[cond2_only].iterrows():
    new_issues.append({
        "date": row["date"], "isin": row["isin"], "ticker": row["ticker"],
        "issue_type": "PRICE_GAP",
        "detail": f"OBS vs RET 괴리 {row['price_gap']:.1%} (>20%) — 데이터 지연 가능",
    })

# 두 조건 동시: corporate action 후보
corp_action_candidates = []
for _, row in anomaly_df[both_conds].iterrows():
    corp_action_candidates.append({
        "isin": row["isin"],
        "ticker": row["ticker"],
        "event_date": row["date"],
        "event_type": "UNCONFIRMED",
        "ratio": "",
        "old_ticker": "",
        "new_ticker": "",
        "note": f"weekly_return={row['weekly_return']:.1%}, price_gap={row['price_gap']:.1%}",
    })
    new_issues.append({
        "date": row["date"], "isin": row["isin"], "ticker": row["ticker"],
        "issue_type": "SPLIT_DETECTED",
        "detail": f"이중 조건 충족 — corporate action 후보 (weekly_return={row['weekly_return']:.1%}, gap={row['price_gap']:.1%})",
    })
    # event_flag 업데이트
    mask = (master["date"] == row["date"]) & (master["ticker"] == row["ticker"])
    master.loc[mask, "event_flag"] = "Y"
    master.loc[mask, "validation_note"] = f"CA 후보: wr={row['weekly_return']:.1%}, gap={row['price_gap']:.1%}"

# corporate_actions_reference.csv 저장/갱신
if corp_action_candidates:
    ca_df = pd.DataFrame(corp_action_candidates)
    ca_cols = ["isin", "ticker", "event_date", "event_type", "ratio", "old_ticker", "new_ticker", "note"]
    ca_df = ca_df[ca_cols]
    if CORP_ACTIONS_FILE.exists():
        existing_ca = pd.read_csv(CORP_ACTIONS_FILE)
        ca_df = pd.concat([existing_ca, ca_df]).drop_duplicates(
            subset=["isin", "event_date"], keep="first"
        )
    ca_df.to_csv(CORP_ACTIONS_FILE, index=False, encoding="utf-8-sig")
    print(f"  -> {CORP_ACTIONS_FILE} ({len(ca_df)} rows)")

# master 재저장 (event_flag 갱신 반영)
master.to_csv(PRICE_MASTER_FILE, index=False, encoding="utf-8-sig")

print(f"  조건1만 (고변동): {cond1_only.sum()}건")
print(f"  조건2만 (가격괴리): {cond2_only.sum()}건")
print(f"  두 조건 동시 (CA 후보): {both_conds.sum()}건")

# ══════════════════════════════════════════════════════
# data_issues.csv 갱신
# ══════════════════════════════════════════════════════
if new_issues:
    new_issues_df = pd.DataFrame(new_issues)
    all_issues = pd.concat([existing_issues, new_issues_df], ignore_index=True)
    all_issues.to_csv(DATA_ISSUES, index=False, encoding="utf-8-sig")
    print(f"\n  data_issues.csv 갱신: +{len(new_issues)}건 (총 {len(all_issues)}건)")

# ══════════════════════════════════════════════════════
# STEP 6: 처리 결과 요약
# ══════════════════════════════════════════════════════
print()
print("=" * 60)
print("STEP 6: 처리 결과 요약")
print("=" * 60)

print(f"\n[Return Price 수집]")
print(f"  성공: {len(fetched_tickers)} / {len(all_tickers)} tickers")
if failed_tickers:
    print(f"  실패 ({len(failed_tickers)}개): {', '.join(sorted(failed_tickers)[:30])}{'...' if len(failed_tickers) > 30 else ''}")

print(f"\n[주간 수익률]")
if len(weekly_returns) > 0:
    print(f"  총 행수: {len(weekly_returns):,}")
    print(f"  기간: {weekly_returns['date'].min()} ~ {weekly_returns['date'].max()}")
print(f"  Carry-forward: {carry_forward_count}건 ({len(tickers_with_cf)} tickers)")
if tickers_with_cf:
    print(f"    종목: {', '.join(sorted(tickers_with_cf)[:20])}{'...' if len(tickers_with_cf) > 20 else ''}")

print(f"\n[Delisted 후보]")
if delisted_candidates:
    print(f"  총 {len(delisted_candidates)}건:")
    for dc in delisted_candidates[:20]:
        print(f"    - {dc['ticker']}: {dc['last_date']} ~ {dc['resume_date']} ({dc['gap_weeks']}주 gap)")
else:
    print(f"  없음")

print(f"\n[가격 이상 후보 (이중 조건)]")
if corp_action_candidates:
    print(f"  총 {len(corp_action_candidates)}건:")
    for ca in corp_action_candidates[:20]:
        print(f"    - {ca['ticker']} ({ca['event_date']}): {ca['note']}")
else:
    print(f"  없음")

print(f"\n[price_weekly_master.csv]")
print(f"  총 행수: {len(master):,}")

# 최종 판정
has_critical = False
if len(failed_tickers) >= 20:
    print(f"\n[보고] 수집 실패 {len(failed_tickers)}개 (>= 20) — 계속 진행합니다.")

print()
if has_critical:
    print("[결과] 이상 발견 -- 사람의 확인이 필요합니다.")
else:
    print("에이전트 2 완료 -- 에이전트 3 실행 가능")
