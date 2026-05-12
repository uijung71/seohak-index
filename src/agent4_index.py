"""
에이전트 4: 지수 계산 (Index Calculation)
- weights_weekly.csv와 weekly_returns.csv를 결합
- 체인 수익률(Chain Return) 방식으로 I_t = I_(t-1) * (1 + R_t) 산출
- 결측 종목 발생 시 이벤트로 간주하여 W_i 재정규화
- 연도별 개별 실행
"""

import pandas as pd
import numpy as np
from pathlib import Path
import argparse
import sys

# ── 경로 설정 ──────────────────────────────────────────
BASE = Path(__file__).resolve().parent.parent
PROCESSED = BASE / "data" / "processed"
REFERENCE = BASE / "data" / "reference"
OUTPUT = BASE / "output"
OUTPUT.mkdir(parents=True, exist_ok=True)

WEIGHTS_FILE = PROCESSED / "weights_weekly.csv"
RETURNS_FILE = PROCESSED / "weekly_returns.csv"
DELISTED_FILE = PROCESSED / "delisted_candidates.csv"
CA_FILE = REFERENCE / "corporate_actions_reference.csv"
OP_LOG_FILE = OUTPUT / "operation_log.csv"

# ══════════════════════════════════════════════════════
# MAIN LOGIC
# ══════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Agent 4: Calculate Seohak100 Index by Year")
    parser.add_argument("year", type=int, help="Target year to calculate (e.g., 2020)")
    args = parser.parse_args()
    year = args.year

    if not WEIGHTS_FILE.exists() or not RETURNS_FILE.exists():
        print("필수 입력 파일이 없습니다. (weights_weekly.csv, weekly_returns.csv)")
        sys.exit(1)

    print(f"=== 서학개미 100 지수 산출 ({year}년) ===")

    # 1. 파일 로드
    df_weights = pd.read_csv(WEIGHTS_FILE)
    df_returns = pd.read_csv(RETURNS_FILE)

    # 이벤트 기록용 오퍼레이션 로그
    if OP_LOG_FILE.exists():
        op_logs = pd.read_csv(OP_LOG_FILE).to_dict("records")
    else:
        op_logs = []

    # 2. 거래 주차(week) 목록 추출 (모든 주차)
    all_dates = sorted(df_weights["date"].unique())
    year_dates = sorted([d for d in all_dates if pd.to_datetime(d).isocalendar().year == year])
    
    if not year_dates:
        print(f"{year}년에 해당하는 데이터가 없습니다.")
        sys.exit(1)

    print(f"대상 기간: {year_dates[0]} ~ {year_dates[-1]}")

    # 3. 초기값 설정
    if year == 2020:
        base_index = 1000.0
    else:
        prev_year_file = OUTPUT / f"seohak100_weekly_index_{year-1}.csv"
        if not prev_year_file.exists():
            print(f"[{year-1}]년 지수 파일이 없습니다. 순차적으로 실행해야 합니다.")
            sys.exit(1)
        prev_df = pd.read_csv(prev_year_file)
        base_index = float(prev_df.iloc[-1]["index_point"])

    # 4. 연도별 순회
    results = []
    
    # 2020-01-06 (첫 주)인지 체크
    if year == 2020:
        # 첫 주는 index_point = 1000.0 이고 return은 0 임.
        first_date = year_dates[0]
        curr_w = df_weights[df_weights["date"] == first_date]
        sat_method = curr_w["satellite_method"].iloc[0] if not curr_w.empty else "SYMMETRIC"
        results.append({
            "date": first_date,
            "index_point": base_index,
            "weekly_return": 0.0,
            "component_count": len(curr_w),
            "core_count": curr_w["core_count"].iloc[0],
            "satellite_count": curr_w["satellite_count"].iloc[0],
            "top3_contributors": "",
            "satellite_method": sat_method,
            "phase": 1
        })
        run_dates = year_dates[1:]
    else:
        run_dates = year_dates

    # 순회 시작
    cur_index = base_index
    for t_date in run_dates:
        # t 시점의 수익률 t_date
        # W_i는 t-1 시점의 W_i 사용
        idx_t = all_dates.index(t_date)
        if idx_t == 0:
            continue  # 첫 주는 위에서 하드코딩 처리됨
        t_minus_1_date = all_dates[idx_t - 1]
        
        # t-1 시점의 가중치
        w_t_minus_1 = df_weights[df_weights["date"] == t_minus_1_date].copy()
        
        # t 시점의 수익률
        r_t = df_returns[df_returns["date"] == t_date].copy()

        # 결합
        merged = pd.merge(w_t_minus_1, r_t[["ticker", "weekly_return"]], on="ticker", how="left")
        
        # 결측치(NaN) 제거 및 이벤트 처리 (Return Price 수집이 안 된 종목들)
        missing_mask = merged["weekly_return"].isna()
        if missing_mask.any():
            missing_tickers = merged.loc[missing_mask, "ticker"].tolist()
            for mt in missing_tickers:
                op_logs.append({
                    "date": t_date,
                    "event_type": "MISSING_RETURN/DROPPED",
                    "isin_out": merged.loc[merged["ticker"] == mt, "isin"].values[0],
                    "isin_in": "",
                    "note": f"{mt} 수익률 NaN으로 해당 주 제외 및 재정규화"
                })
            # 유효 종목으로 좁히고 재정규화
            merged = merged[~missing_mask].copy()
            merged["W_i"] = merged["W_i"] / merged["W_i"].sum()

        sat_method = merged["satellite_method"].iloc[0] if not merged.empty else "SYMMETRIC"
        core_c = merged["core_count"].iloc[0] if not merged.empty else 0
        sat_c = merged["satellite_count"].iloc[0] if not merged.empty else 0
        
        # R_t 계산
        merged["contribution"] = merged["W_i"] * merged["weekly_return"]
        R_t = merged["contribution"].sum()
        
        # I_t 계산
        cur_index = cur_index * (1 + R_t)
        
        # Top 3 기여자 계산
        merged_sorted = merged.sort_values("contribution", ascending=False)
        top3 = merged_sorted.head(3)
        top3_str = ", ".join([f"{row['ticker']}({row['contribution']*100:+.2f}%)" for _, row in top3.iterrows()])

        results.append({
            "date": t_date,
            "index_point": cur_index,
            "weekly_return": R_t,
            "component_count": len(merged),
            "core_count": core_c,
            "satellite_count": sat_c,
            "top3_contributors": top3_str,
            "satellite_method": sat_method,
            "phase": 1
        })

    # 5. 저장
    df_res = pd.DataFrame(results)
    out_file = OUTPUT / f"seohak100_weekly_index_{year}.csv"
    df_res.to_csv(out_file, index=False, encoding="utf-8-sig")

    if op_logs:
        pd.DataFrame(op_logs).to_csv(OP_LOG_FILE, index=False, encoding="utf-8-sig")

    # 6. 요약 출력
    idx_min = df_res["index_point"].min()
    idx_max = df_res["index_point"].max()
    date_min = df_res.loc[df_res["index_point"].idxmin(), "date"]
    date_max = df_res.loc[df_res["index_point"].idxmax(), "date"]
    
    # MDD 계산
    df_res["peak"] = df_res["index_point"].cummax()
    df_res["drawdown"] = (df_res["index_point"] - df_res["peak"]) / df_res["peak"]
    mdd = df_res["drawdown"].min()
    
    start_pt = df_res.iloc[0]["index_point"]
    end_pt = df_res.iloc[-1]["index_point"]
    yearly_ret = (end_pt / start_pt) - 1.0

    print(f"\n[{year} 완료] 시작: {start_pt:,.1f} → 종료: {end_pt:,.1f}")
    print(f"연간 수익률: {yearly_ret*100:+.2f}%")
    print(f"최고점: {idx_max:,.1f} ({date_max})")
    print(f"최저점: {idx_min:,.1f} ({date_min})")
    print(f"최대 낙폭(MDD): {mdd*100:.2f}%")

    if year == 2020:
        # 코로나 폭락 구간 (2020-02-24 ~ 2020-03-23)
        corona = df_res[(df_res["date"] >= "2020-02-24") & (df_res["date"] <= "2020-03-23")].copy()
        if not corona.empty:
            corona_start = corona.iloc[0]["index_point"]
            corona_min = corona["index_point"].min()
            corona_mdd = (corona_min / corona_start) - 1.0
            print(f"코로나 팬데믹 구간 낙폭: {corona_mdd*100:.2f}%")

    extreme_wks = df_res[df_res["weekly_return"].abs() > 0.30]
    if not extreme_wks.empty:
        print(f"\n주간 수익률 ±30% 초과 구간: {len(extreme_wks)}주")

    print(f"\n[{year} 완료] — 다음 연도 또는 에이전트 5 실행 가능")


if __name__ == "__main__":
    main()
