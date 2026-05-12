"""
에이전트 1: 데이터 정제 및 날짜 통일
- custody_weekly.csv 처리
- trading_monthly.csv 처리
- ticker_universe.csv 생성
- data_issues.csv 생성
"""
import pandas as pd
import numpy as np
import calendar
from datetime import datetime
from pathlib import Path
import re

# ── 경로 설정 ──────────────────────────────────────────
BASE = Path(__file__).resolve().parent.parent
RAW = BASE / "data" / "raw"
PROCESSED = BASE / "data" / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)

# 실제 파일명 (.csv.csv)
CUSTODY_FILE = RAW / "custody_weekly.csv.csv"
TRADING_FILE = RAW / "trading_monthly.csv.csv"

# ── 이슈 수집 리스트 ──────────────────────────────────
issues = []


def add_issue(date, isin, ticker, issue_type, detail):
    issues.append({
        "date": date,
        "isin": isin,
        "ticker": ticker,
        "issue_type": issue_type,
        "detail": detail,
    })


# ══════════════════════════════════════════════════════
# 1. custody_weekly.csv 처리
# ══════════════════════════════════════════════════════
print("=" * 60)
print("1. custody_weekly.csv 처리")
print("=" * 60)

custody = pd.read_csv(CUSTODY_FILE, dtype=str)

# 컬럼명 'datr' → 'date' 수정
if "datr" in custody.columns:
    custody.rename(columns={"datr": "date"}, inplace=True)

# date → YYYY-MM-DD 통일 (이미 형식이 맞지만 확실히)
custody["date"] = pd.to_datetime(custody["date"], errors="coerce").dt.strftime("%Y-%m-%d")

# ── 날짜 오기 수정 (확인된 오타 3건) ──
DATE_CORRECTIONS = {
    "2023-06-24": "2023-06-19",
    "2023-06-28": "2023-06-25",
    "2025-06-21": "2025-06-16",
}
corrected = custody["date"].map(DATE_CORRECTIONS)
mask_corrected = corrected.notna()
if mask_corrected.any():
    for old, new in DATE_CORRECTIONS.items():
        if (custody["date"] == old).any():
            print(f"  날짜 수정: {old} → {new}")
    custody.loc[mask_corrected, "date"] = corrected[mask_corrected]

# amount, price_stock → 숫자형 (콤마 제거)
for col in ["amount", "price_stock"]:
    custody[col] = custody[col].astype(str).str.replace(",", "", regex=False)
    custody[col] = pd.to_numeric(custody[col], errors="coerce")

# isin 또는 ticker 누락 행 분리
mask_missing = custody["isin"].isna() | (custody["isin"].str.strip() == "") | \
               custody["ticker"].isna() | (custody["ticker"].str.strip() == "")

custody_issues_rows = custody[mask_missing].copy()
custody_clean = custody[~mask_missing].copy()

# 누락 행을 data_issues에 기록
for _, row in custody_issues_rows.iterrows():
    missing_fields = []
    if pd.isna(row["isin"]) or str(row["isin"]).strip() == "":
        missing_fields.append("isin")
    if pd.isna(row["ticker"]) or str(row["ticker"]).strip() == "":
        missing_fields.append("ticker")
    add_issue(
        row["date"], row.get("isin", ""), row.get("ticker", ""),
        "MISSING_KEY_FIELD",
        f"보관 데이터 키 필드 누락: {', '.join(missing_fields)}"
    )

# ── 주간 Core 종목 수 부족 체크 (RULES 2.1)
core_counts = custody_clean.groupby("date").size().reset_index(name="count")
for _, r in core_counts.iterrows():
    if r["count"] < 50:
        add_issue(
            r["date"], "", "",
            "CORE_COUNT_SHORT",
            f"실제 Core 종목 수 {r['count']}개 (기준 50개 미달)"
        )
        if r["count"] < 40:
            print(f"경고: {r['date']} 주의 Core 종목 수가 {r['count']}개로 40개 미만입니다.")
            print(f"      사람의 확인이 필요합니다.")

print(f"  원본 행수: {len(custody)}")
print(f"  정제 행수: {len(custody_clean)}")
print(f"  누락 분리: {len(custody_issues_rows)}행")

# ══════════════════════════════════════════════════════
# 2. trading_monthly.csv 처리
# ══════════════════════════════════════════════════════
print()
print("=" * 60)
print("2. trading_monthly.csv 처리")
print("=" * 60)

trading = pd.read_csv(TRADING_FILE, dtype=str)


def parse_korean_date(s):
    """'2020. 1. 1' → '2020-01-01'"""
    if pd.isna(s):
        return np.nan
    s = str(s).strip()
    m = re.match(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})", s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{y:04d}-{mo:02d}-{d:02d}"
    # 이미 YYYY-MM-DD 형식이면 그대로
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return np.nan


trading["date"] = trading["date"].apply(parse_korean_date)

# 숫자형 변환 (콤마 제거)
for col in ["buy", "sell", "sum", "price"]:
    if col in trading.columns:
        trading[col] = trading[col].astype(str).str.replace(",", "", regex=False)
        trading[col] = pd.to_numeric(trading[col], errors="coerce")

# price 누락 기록 (RULES 2.2)
price_missing_mask = trading["price"].isna()
price_missing_count = price_missing_mask.sum()
for _, row in trading[price_missing_mask].iterrows():
    add_issue(
        row["date"], row.get("isin", ""), row.get("ticker", ""),
        "TRADING_PRICE_MISSING",
        "월간 매매 데이터 price 누락 — N_i 계산은 정상 진행"
    )


def count_mondays(year, month):
    """해당 월의 월요일 수 계산 (RULES: weekly_trading = monthly_sum / 해당 월의 월요일 수)"""
    cal = calendar.Calendar()
    return sum(1 for d in cal.itermonthdays2(year, month)
               if d[0] != 0 and d[1] == 0)  # d[1]==0 → Monday


# weekly_sum 계산
def calc_weekly_sum(row):
    if pd.isna(row["date"]) or pd.isna(row["sum"]):
        return np.nan
    dt = datetime.strptime(row["date"], "%Y-%m-%d")
    n_mondays = count_mondays(dt.year, dt.month)
    if n_mondays == 0:
        return np.nan
    return row["sum"] / n_mondays


trading["weekly_sum"] = trading.apply(calc_weekly_sum, axis=1)

# rank를 숫자로
if "rank" in trading.columns:
    trading["rank"] = pd.to_numeric(trading["rank"], errors="coerce")

trading_clean = trading.copy()

print(f"  원본 행수: {len(trading)}")
print(f"  price 누락: {price_missing_count}행")

# ══════════════════════════════════════════════════════
# 3. ticker_universe.csv 생성 (source 컬럼 포함)
# ══════════════════════════════════════════════════════
print()
print("=" * 60)
print("3. ticker_universe.csv 생성")
print("=" * 60)

custody_tickers_set = set(custody_clean["ticker"].dropna().unique())
trading_tickers_set = set(trading_clean["ticker"].dropna().unique())

# custody 기준 ticker→(isin, name_en) 매핑
tickers_custody = custody_clean[["ticker", "isin", "name_en"]].drop_duplicates(subset=["ticker"])
tickers_trading = trading_clean[["ticker", "isin", "name_en"]].drop_duplicates(subset=["ticker"])

# 합치기 (custody 우선)
ticker_universe = pd.concat([tickers_custody, tickers_trading]).drop_duplicates(subset=["ticker"])
ticker_universe = ticker_universe.sort_values("ticker").reset_index(drop=True)

# source 컬럼 추가
def get_source(ticker):
    in_c = ticker in custody_tickers_set
    in_t = ticker in trading_tickers_set
    if in_c and in_t:
        return "BOTH"
    elif in_c:
        return "CUSTODY"
    else:
        return "TRADING"

ticker_universe["source"] = ticker_universe["ticker"].apply(get_source)
# 컬럼 순서: isin, ticker, name_en, source
ticker_universe = ticker_universe[["isin", "ticker", "name_en", "source"]]

src_counts = ticker_universe["source"].value_counts()
print(f"  전체 고유 ticker 수: {len(ticker_universe)}")
for src, cnt in src_counts.items():
    print(f"    - {src}: {cnt}")

# ══════════════════════════════════════════════════════
# 4. 파일 저장
# ══════════════════════════════════════════════════════
print()
print("=" * 60)
print("4. 파일 저장")
print("=" * 60)

# custody_weekly_clean.csv
custody_clean.to_csv(PROCESSED / "custody_weekly_clean.csv", index=False, encoding="utf-8-sig")
print(f"  -> {PROCESSED / 'custody_weekly_clean.csv'}")

# trading_monthly_clean.csv
trading_clean.to_csv(PROCESSED / "trading_monthly_clean.csv", index=False, encoding="utf-8-sig")
print(f"  -> {PROCESSED / 'trading_monthly_clean.csv'}")

# data_issues.csv
issues_df = pd.DataFrame(issues, columns=["date", "isin", "ticker", "issue_type", "detail"])
issues_df.to_csv(PROCESSED / "data_issues.csv", index=False, encoding="utf-8-sig")
print(f"  -> {PROCESSED / 'data_issues.csv'}")

# ticker_universe.csv
ticker_universe.to_csv(PROCESSED / "ticker_universe.csv", index=False, encoding="utf-8-sig")
print(f"  -> {PROCESSED / 'ticker_universe.csv'}")

# custody_issues (누락 행 별도 보관)
if len(custody_issues_rows) > 0:
    custody_issues_rows.to_csv(PROCESSED / "custody_missing_rows.csv", index=False, encoding="utf-8-sig")
    print(f"  -> {PROCESSED / 'custody_missing_rows.csv'}")

# ══════════════════════════════════════════════════════
# 5. 처리 결과 요약
# ══════════════════════════════════════════════════════
print()
print("=" * 60)
print("5. 처리 결과 요약")
print("=" * 60)

has_critical_issue = False

# ── custody_weekly 요약 ──
custody_dates = pd.to_datetime(custody_clean["date"])
total_weeks = custody_clean["date"].nunique()
print(f"\n[custody_weekly]")
print(f"  전체 행 수: {len(custody_clean):,}")
print(f"  기간: {custody_dates.min().strftime('%Y-%m-%d')} ~ {custody_dates.max().strftime('%Y-%m-%d')}")
print(f"  주간 데이터 총 주 수: {total_weeks}")
print(f"  키 필드 누락 행 수: {len(custody_issues_rows)}")

# Core 종목 수 50개 미만 상세
core_short = core_counts[core_counts["count"] < 50].sort_values("count")
print(f"  Core 종목 수 50개 미만인 주: {len(core_short)}건")
if len(core_short) > 0:
    # 범위별 집계
    bins = [(0, 39, "< 40 (위험)"), (40, 44, "40~44"), (45, 47, "45~47"), (48, 49, "48~49")]
    for lo, hi, label in bins:
        cnt = len(core_short[(core_short["count"] >= lo) & (core_short["count"] <= hi)])
        if cnt > 0:
            print(f"    {label}: {cnt}주")
    # 40 미만 상세
    critical = core_short[core_short["count"] < 40]
    if len(critical) > 0:
        has_critical_issue = True
        for _, r in critical.iterrows():
            print(f"    [위험] {r['date']}: {r['count']}개")

# ── trading_monthly 요약 ──
trading_dates = pd.to_datetime(trading_clean["date"])
print(f"\n[trading_monthly]")
print(f"  전체 행 수: {len(trading_clean):,}")
print(f"  기간: {trading_dates.min().strftime('%Y-%m-%d')} ~ {trading_dates.max().strftime('%Y-%m-%d')}")
print(f"  고유 월 수: {trading_clean['date'].nunique()}")
price_ratio = price_missing_count / len(trading_clean) * 100
print(f"  price 누락: {price_missing_count}개 / {len(trading_clean):,}개 = {price_ratio:.1f}%")

if price_missing_count > 0:
    missing_ticker_counts = (
        trading[price_missing_mask]
        .groupby("ticker").size()
        .sort_values(ascending=False)
    )
    top_n = min(20, len(missing_ticker_counts))
    print(f"  price 누락 종목 (상위 {top_n}개):")
    for ticker, cnt in missing_ticker_counts.head(top_n).items():
        print(f"    - {ticker}: {cnt}건")

# ── data_issues.csv 요약 ──
print(f"\n[data_issues.csv]")
print(f"  총 이슈 행수: {len(issues_df)}")
if len(issues_df) > 0:
    issue_summary = issues_df.groupby("issue_type").size()
    for itype, cnt in issue_summary.items():
        print(f"    - {itype}: {cnt}건")

# ── 최종 판정 ──
print()
if has_critical_issue:
    print("[결과] 이상 발견 -- 사람의 확인이 필요합니다.")
    print("  Core 종목 수 40개 미만인 주가 존재합니다.")
else:
    print("에이전트 1 완료 -- 에이전트 2 실행 가능")
