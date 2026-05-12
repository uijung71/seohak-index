"""
ìì´ì í¸ 2: ê°ê²© ë°ì´í° ìì§ ë° ììµë¥  ê³ì°
- yfinanceë¡ Return Price ìì§
- ì£¼ê° ììµë¥  ê³ì° (carry-forward, delisted ì²ë¦¬)
- price_weekly_master.csv ìì±
- ê°ê²© ì´ì íì§
"""
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path
import time
import warnings
warnings.filterwarnings("ignore")

# ââ ê²½ë¡ ì¤ì  ââââââââââââââââââââââââââââââââââââââââââ
BASE = Path(__file__).resolve().parent.parent
PROCESSED = BASE / "data" / "processed"
REFERENCE = BASE / "data" / "reference"
REFERENCE.mkdir(parents=True, exist_ok=True)

# ìë ¥ íì¼
CUSTODY_CLEAN = PROCESSED / "custody_weekly_clean.csv"
TICKER_UNIVERSE = PROCESSED / "ticker_universe.csv"
DATA_ISSUES = PROCESSED / "data_issues.csv"

# ì¶ë ¥ íì¼
RETURN_PRICE_FILE = PROCESSED / "return_price_weekly.csv"
PRICE_FAILED_FILE = PROCESSED / "price_fetch_failed.csv"
WEEKLY_RETURNS_FILE = PROCESSED / "weekly_returns.csv"
PRICE_MASTER_FILE = PROCESSED / "price_weekly_master.csv"
DELISTED_FILE = PROCESSED / "delisted_candidates.csv"
CORP_ACTIONS_FILE = REFERENCE / "corporate_actions_reference.csv"

# ìì§ ê¸°ê°
START_DATE = "2019-12-23"
END_DATE = "2026-01-19"

# âââââââââââââââââââââââââââââââââââââââââââââââââââââ# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# STEP 2: EODHDë¡ Return Price ìì§
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
print()
print("=" * 60)
print("STEP 2: EODHD Return Price ìì§")
print("=" * 60)

import requests
import time

API_TOKEN = "693abf5882dab9.42616862"

ticker_universe = pd.read_csv(TICKER_UNIVERSE)
all_tickers = ticker_universe["ticker"].dropna().unique().tolist()
ticker_to_isin = dict(zip(ticker_universe["ticker"], ticker_universe["isin"]))

# EODHD íì ë§¤í (BRK.B ííë¥¼ BRK-B ë±ì¼ë¡ ë³í íìì ì¬ì©, ì¬ê¸°ìë ê¸°ë³¸ ì ì§)
EODHD_TICKER_MAP = {
    "BRK.A": "BRK-A",
    "BRK.B": "BRK-B",
}

print(f"  ìì§ ëì: {len(all_tickers)} tickers")
print(f"  ê¸°ê°: {START_DATE} ~ {END_DATE}")

# ìºì íì¸ â ì´ë¯¸ ìì§ë íì¼ì´ ìì¼ë©´ ì¬ì¬ì©
if RETURN_PRICE_FILE.exists():
    print(f"  ê¸°ì¡´ íì¼ ë°ê²¬: {RETURN_PRICE_FILE}")
    print(f"  ìºì ì¬ì© (ì¬ìì§ ì íì¼ ì­ì  í ì¬ì¤í)")
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

    print("  EODHD API ê°ë³ Fetch ìì...")
    for i, orig_ticker in enumerate(all_tickers):
        eodhd_ticker = EODHD_TICKER_MAP.get(orig_ticker, orig_ticker)
        url = f"https://eodhd.com/api/eod/{eodhd_ticker}.US"
        params = {
            "api_token": API_TOKEN,
            "fmt": "json",
            "period": "w", # ì£¼ê°ë¨ì ë¦¬í´
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
            print(f"    ì§í ìí©: {i+1} / {len(all_tickers)} ìë£")

    print(f"EODHD API ìì§ ìë£: ì±ê³µ {success_count} / ì ì²´ {len(all_tickers)}")

    # ê²°ê³¼ ë³í©
    if all_results:
        return_prices = pd.concat(all_results, ignore_index=True)                         "date": series.index.strftime("%Y-%m-%d"),
                            "isin": isin,
                            "ticker": orig_ticker,  # ìë ticker ì ì§
                            "return_price": series.values,
                        })
                        all_results.append(df_t)
                        success_count += 1
                    else:
                        failed_tickers.append(orig_ticker)
                else:
                    failed_tickers.append(orig_ticker)

            print(f"ì±ê³µ {success_count}/{len(batch)}")

        except Exception as e:
            failed_tickers.extend(batch)
            print(f"ì¤ë¥: {e}")

        # rate-limit ëì
        if batch_start + BATCH_SIZE < len(all_tickers):
            time.sleep(1)

    # ê²°ê³¼ ì ì¥
    if all_results:
        return_prices = pd.concat(all_results, ignore_index=True)
        return_prices.to_csv(RETURN_PRICE_FILE, index=False, encoding="utf-8-sig")
        print(f"  -> {RETURN_PRICE_FILE} ({len(return_prices):,} rows)")
    else:
        return_prices = pd.DataFrame(columns=["date", "isin", "ticker", "return_price"])
        return_prices.to_csv(RETURN_PRICE_FILE, index=False, encoding="utf-8-sig")

    # ì¤í¨ ticker ì ì¥
    if failed_tickers:
        failed_df = pd.DataFrame({"ticker": failed_tickers})
        failed_df["isin"] = failed_df["ticker"].map(ticker_to_isin)
        failed_df.to_csv(PRICE_FAILED_FILE, index=False, encoding="utf-8-sig")
        print(f"  -> {PRICE_FAILED_FILE} ({len(failed_tickers)} tickers)")

    fetched_tickers = return_prices["ticker"].unique().tolist() if len(return_prices) > 0 else []

print(f"  ìì§ ì±ê³µ: {len(fetched_tickers)} / {len(all_tickers)} tickers")
print(f"  ìì§ ì¤í¨: {len(failed_tickers)} tickers")

if len(failed_tickers) >= 20:
    print(f"  [ë³´ê³ ] ìì§ ì¤í¨ {len(failed_tickers)}ê° â 20ê° ì´ìì´ë¯ë¡ ë³´ê³ í©ëë¤.")
    print(f"  ì¤í¨ ëª©ë¡: {', '.join(failed_tickers[:30])}{'...' if len(failed_tickers) > 30 else ''}")

# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# STEP 3: ì£¼ê° ììµë¥  ê³ì°
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
print()
print("=" * 60)
print("STEP 3: ì£¼ê° ììµë¥  ê³ì°")
print("=" * 60)

# data_issues ì½ê¸° (ìì´ì í¸ 1ìì ìì±ë ê¸°ì¡´ ì´ìì ì¶ê°)
existing_issues = pd.read_csv(DATA_ISSUES) if DATA_ISSUES.exists() else pd.DataFrame()
new_issues = []
delisted_candidates = []

# custody ë ì§ ëª©ë¡ (ê¸°ì¤ ì£¼ê° ë ì§)
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

    # ì£¼ê° ììµë¥  ê³ì°
    df_t["weekly_return"] = df_t["return_price"].pct_change()
    df_t["data_flag"] = ""

    # ì°ì ëë½ íì§ (ref_dates ê¸°ì¤ì´ ìëë¼, ìì²´ ìê³ì´ gap ê¸°ì¤)
    dates = pd.to_datetime(df_t["date"])
    gaps = []
    for i in range(1, len(dates)):
        diff_days = (dates.iloc[i] - dates.iloc[i - 1]).days
        if diff_days > 10:  # 1ì£¼ = 7ì¼, 2ì£¼ gap => > 10ì¼
            gap_weeks = diff_days // 7
            if gap_weeks >= 3:
                # 3ì£¼ ì´ì ì°ì ëë½ â delisted íë³´
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
                    "detail": f"{gap_weeks}ì£¼ ì°ì ê°ê²© ëë½ â delisted íë³´",
                })
                df_t.loc[df_t.index[i], "data_flag"] = "DELISTED_CANDIDATE"
            elif gap_weeks >= 2:
                # 1~2ì£¼ ëë½ â carry-forward
                carry_forward_count += 1
                tickers_with_cf.add(ticker)
                new_issues.append({
                    "date": dates.iloc[i - 1].strftime("%Y-%m-%d"),
                    "isin": isin,
                    "ticker": ticker,
                    "issue_type": "CARRY_FORWARD",
                    "detail": f"{gap_weeks}ì£¼ ê°ê²© ëë½ â carry-forward ì ì© (r_i=0%)",
                })
                df_t.loc[df_t.index[i], "weekly_return"] = 0.0
                df_t.loc[df_t.index[i], "data_flag"] = "CARRY_FORWARD"

    all_returns.append(df_t)

# weekly_returns ì ì¥
if all_returns:
    weekly_returns = pd.concat(all_returns, ignore_index=True)
    weekly_returns = weekly_returns[["date", "isin", "ticker", "return_price", "weekly_return", "data_flag"]]
    weekly_returns.to_csv(WEEKLY_RETURNS_FILE, index=False, encoding="utf-8-sig")
    print(f"  -> {WEEKLY_RETURNS_FILE} ({len(weekly_returns):,} rows)")
else:
    weekly_returns = pd.DataFrame(columns=["date", "isin", "ticker", "return_price", "weekly_return", "data_flag"])
    weekly_returns.to_csv(WEEKLY_RETURNS_FILE, index=False, encoding="utf-8-sig")

# delisted_candidates ì ì¥
if delisted_candidates:
    del_df = pd.DataFrame(delisted_candidates)
    del_df.to_csv(DELISTED_FILE, index=False, encoding="utf-8-sig")
    print(f"  -> {DELISTED_FILE} ({len(del_df)} rows)")

print(f"  Carry-forward ì²ë¦¬: {carry_forward_count}ê±´ ({len(tickers_with_cf)} tickers)")
print(f"  Delisted íë³´: {len(delisted_candidates)}ê±´")

# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# STEP 4: price_weekly_master.csv ìì±
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
print()
print("=" * 60)
print("STEP 4: price_weekly_master.csv ìì±")
print("=" * 60)

custody = pd.read_csv(CUSTODY_CLEAN)

# custodyì observed_price
obs = custody[["date", "isin", "ticker", "price_stock"]].copy()
obs.rename(columns={"price_stock": "observed_price"}, inplace=True)

# return_price (weekly_returnsìì ê°ì ¸ì´)
ret = weekly_returns[["date", "isin", "ticker", "return_price"]].copy()

# isin + date ê¸°ì¤ ê²°í© (outer joinì¼ë¡ ëª¨ë  ë°ì´í° í¬í¨)
master = pd.merge(obs, ret, on=["date", "isin", "ticker"], how="outer")
master = master.sort_values(["ticker", "date"]).reset_index(drop=True)

# price_source ê²°ì 
def get_price_source(row):
    has_obs = pd.notna(row["observed_price"])
    has_ret = pd.notna(row["return_price"])
    if has_obs and has_ret:
        return "SEIBRO"  # ë ìì¤ ëª¨ë ì¡´ì¬
    elif has_obs:
        return "SEIBRO"
    elif has_ret:
        return "YFINANCE"
    else:
        return ""

master["price_source"] = master.apply(get_price_source, axis=1)
master["event_flag"] = "N"
master["validation_note"] = ""

# ì»¬ë¼ ì ë ¬
master = master[["date", "isin", "ticker", "observed_price", "return_price",
                  "price_source", "event_flag", "validation_note"]]

master.to_csv(PRICE_MASTER_FILE, index=False, encoding="utf-8-sig")
print(f"  -> {PRICE_MASTER_FILE} ({len(master):,} rows)")

# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# STEP 5: ê°ê²© ì´ì íì§
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
print()
print("=" * 60)
print("STEP 5: ê°ê²© ì´ì íì§")
print("=" * 60)

# weekly_returnsì master ê²°í©íì¬ ì´ì íì§
anomaly_df = master[
    master["observed_price"].notna() & master["return_price"].notna()
].copy()

# weekly_return ê²°í©
wr_lookup = weekly_returns[["date", "ticker", "weekly_return"]].copy()
anomaly_df = anomaly_df.merge(wr_lookup, on=["date", "ticker"], how="left")

# ì¡°ê±´ 1: abs(weekly_return) > 30%
cond1 = anomaly_df["weekly_return"].abs() > 0.30

# ì¡°ê±´ 2: ê°ê²© ê´´ë¦¬ > 20%
anomaly_df["price_gap"] = (
    (anomaly_df["observed_price"] - anomaly_df["return_price"]).abs()
    / anomaly_df["observed_price"]
)
cond2 = anomaly_df["price_gap"] > 0.20

# ë¨ì¼ ì¡°ê±´ ê¸°ë¡
cond1_only = cond1 & ~cond2
cond2_only = cond2 & ~cond1
both_conds = cond1 & cond2

# ì¡°ê±´1ë§: ê¸°ë¡
for _, row in anomaly_df[cond1_only].iterrows():
    new_issues.append({
        "date": row["date"], "isin": row["isin"], "ticker": row["ticker"],
        "issue_type": "PRICE_GAP",
        "detail": f"ì£¼ê° ë³íì¨ {row['weekly_return']:.1%} (>30%) â ê³ ë³ë ì¢ëª© ì ì ê°ë¥",
    })

# ì¡°ê±´2ë§: ê¸°ë¡
for _, row in anomaly_df[cond2_only].iterrows():
    new_issues.append({
        "date": row["date"], "isin": row["isin"], "ticker": row["ticker"],
        "issue_type": "PRICE_GAP",
        "detail": f"OBS vs RET ê´´ë¦¬ {row['price_gap']:.1%} (>20%) â ë°ì´í° ì§ì° ê°ë¥",
    })

# ë ì¡°ê±´ ëì: corporate action íë³´
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
        "detail": f"ì´ì¤ ì¡°ê±´ ì¶©ì¡± â corporate action íë³´ (weekly_return={row['weekly_return']:.1%}, gap={row['price_gap']:.1%})",
    })
    # event_flag ìë°ì´í¸
    mask = (master["date"] == row["date"]) & (master["ticker"] == row["ticker"])
    master.loc[mask, "event_flag"] = "Y"
    master.loc[mask, "validation_note"] = f"CA íë³´: wr={row['weekly_return']:.1%}, gap={row['price_gap']:.1%}"

# corporate_actions_reference.csv ì ì¥/ê°±ì 
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

# master ì¬ì ì¥ (event_flag ê°±ì  ë°ì)
master.to_csv(PRICE_MASTER_FILE, index=False, encoding="utf-8-sig")

print(f"  ì¡°ê±´1ë§ (ê³ ë³ë): {cond1_only.sum()}ê±´")
print(f"  ì¡°ê±´2ë§ (ê°ê²©ê´´ë¦¬): {cond2_only.sum()}ê±´")
print(f"  ë ì¡°ê±´ ëì (CA íë³´): {both_conds.sum()}ê±´")

# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# data_issues.csv ê°±ì 
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
if new_issues:
    new_issues_df = pd.DataFrame(new_issues)
    all_issues = pd.concat([existing_issues, new_issues_df], ignore_index=True)
    all_issues.to_csv(DATA_ISSUES, index=False, encoding="utf-8-sig")
    print(f"\n  data_issues.csv ê°±ì : +{len(new_issues)}ê±´ (ì´ {len(all_issues)}ê±´)")

# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# STEP 6: ì²ë¦¬ ê²°ê³¼ ìì½
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
print()
print("=" * 60)
print("STEP 6: ì²ë¦¬ ê²°ê³¼ ìì½")
print("=" * 60)

print(f"\n[Return Price ìì§]")
print(f"  ì±ê³µ: {len(fetched_tickers)} / {len(all_tickers)} tickers")
if failed_tickers:
    print(f"  ì¤í¨ ({len(failed_tickers)}ê°): {', '.join(sorted(failed_tickers)[:30])}{'...' if len(failed_tickers) > 30 else ''}")

print(f"\n[ì£¼ê° ììµë¥ ]")
if len(weekly_returns) > 0:
    print(f"  ì´ íì: {len(weekly_returns):,}")
    print(f"  ê¸°ê°: {weekly_returns['date'].min()} ~ {weekly_returns['date'].max()}")
print(f"  Carry-forward: {carry_forward_count}ê±´ ({len(tickers_with_cf)} tickers)")
if tickers_with_cf:
    print(f"    ì¢ëª©: {', '.join(sorted(tickers_with_cf)[:20])}{'...' if len(tickers_with_cf) > 20 else ''}")

print(f"\n[Delisted íë³´]")
if delisted_candidates:
    print(f"  ì´ {len(delisted_candidates)}ê±´:")
    for dc in delisted_candidates[:20]:
        print(f"    - {dc['ticker']}: {dc['last_date']} ~ {dc['resume_date']} ({dc['gap_weeks']}ì£¼ gap)")
else:
    print(f"  ìì")

print(f"\n[ê°ê²© ì´ì íë³´ (ì´ì¤ ì¡°ê±´)]")
if corp_action_candidates:
    print(f"  ì´ {len(corp_action_candidates)}ê±´:")
    for ca in corp_action_candidates[:20]:
        print(f"    - {ca['ticker']} ({ca['event_date']}): {ca['note']}")
else:
    print(f"  ìì")

print(f"\n[price_weekly_master.csv]")
print(f"  ì´ íì: {len(master):,}")

# ìµì¢ íì 
has_critical = False
if len(failed_tickers) >= 20:
    print(f"\n[ë³´ê³ ] ìì§ ì¤í¨ {len(failed_tickers)}ê° (>= 20) â ê³ì ì§íí©ëë¤.")

print()
if has_critical:
    print("[ê²°ê³¼] ì´ì ë°ê²¬ -- ì¬ëì íì¸ì´ íìí©ëë¤.")
else:
    print("ìì´ì í¸ 2 ìë£ -- ìì´ì í¸ 3 ì¤í ê°ë¥")
