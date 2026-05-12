"""
에이전트 3 (v4.0): 가중치 계산 (Phase 1A/1B 분리)
- Phase 1A (2020~2022): 실제 데이터 중심, Satellite 유동 개수 (최대 50)
- Phase 1B (2023~2025): 풀 확장, 생존자 편향 필터링, Satellite 항상 50개 유지
- 10% Cap 적용 제거.
"""
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import argparse

BASE = Path(__file__).resolve().parent.parent
PROCESSED = BASE / "data" / "processed"
REFERENCE = BASE / "data" / "reference"

CUSTODY_FILE = PROCESSED / "custody_weekly_clean.csv"
TRADING_FILE = PROCESSED / "trading_monthly_clean.csv"
PRICE_MASTER_FILE = PROCESSED / "price_weekly_master.csv"
ISSUES_FILE = PROCESSED / "data_issues.csv"
TICKER_UNIVERSE = PROCESSED / "ticker_universe.csv"
IPO_FILE = REFERENCE / "ipo_dates.csv"

OUT_WEIGHTS = PROCESSED / "weights_weekly.csv"
OUT_COMPARE = PROCESSED / "weights_satellite_comparison.csv"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("year", type=int, help="Target year to calculate (e.g., 2020 or 2023)")
    args = parser.parse_args()
    year = args.year

    print(f"=== 에이전트 3 (Phase 1A/1B): {year}년 산출 시작 ===")

    # ── 1. 필수 파일 점검 ──
    missing_files = []
    for f in [CUSTODY_FILE, TRADING_FILE, PRICE_MASTER_FILE, ISSUES_FILE]:
        if not f.exists():
            missing_files.append(f.name)
    
    if year >= 2023 and not IPO_FILE.exists():
        missing_files.append(IPO_FILE.name)
        print("Phase 1B (2023년 이상)를 실행하려면 ipo_dates.csv가 먼저 생성되어야 합니다.")
        print("src/agent3_ipo_fetcher.py 를 먼저 구동해 주세요.")

    if missing_files:
        print("다음 필수 파일이 없습니다:")
        for m in missing_files:
            print(f"  - {m}")
        sys.exit(1)

    # ── 2. 데이터 로드 ──
    df_custody = pd.read_csv(CUSTODY_FILE)
    df_trading = pd.read_csv(TRADING_FILE)
    df_price = pd.read_csv(PRICE_MASTER_FILE)
    df_issues = pd.read_csv(ISSUES_FILE)
    
    ipo_df = pd.DataFrame()
    if year >= 2023:
        ipo_df = pd.read_csv(IPO_FILE)
        # 상장일 date 변환
        ipo_df["ipo_date"] = pd.to_datetime(ipo_df["ipo_date"]).dt.strftime("%Y-%m-%d")

    # ISO Week 구성 (날짜 밀림 보완)
    df_custody["date_dt"] = pd.to_datetime(df_custody["date"])
    cal_c = df_custody["date_dt"].dt.isocalendar()
    df_custody["week_key"] = cal_c["year"].astype(str) + "-W" + cal_c["week"].astype(str).str.zfill(2)

    df_price["date_dt"] = pd.to_datetime(df_price["date"])
    cal_p = df_price["date_dt"].dt.isocalendar()
    df_price["week_key"] = cal_p["year"].astype(str) + "-W" + cal_p["week"].astype(str).str.zfill(2)

    # 해당 연도에 해당하는 주차 필터링
    week_keys = sorted([wk for wk in df_custody["week_key"].unique() if wk.startswith(str(year))])
    
    if not week_keys:
        print(f"[{year}년] 실행할 주차(Week) 데이터가 없습니다.")
        sys.exit(0)

    # CORE 부족 주 통계용
    short_weeks = df_issues[df_issues["issue_type"] == "CORE_COUNT_SHORT"]["date"].unique()

    # ── 3. 루프 준비 ──
    all_weights = []
    all_comparisons = []
    
    # 생존자 편향 통계용
    survivorship_excluded = 0

    phase = "1A" if year <= 2022 else "1B"
    ticker_all = pd.read_csv(TICKER_UNIVERSE)["ticker"].unique() if phase == "1B" else []

    for wk in week_keys:
        curr_custody = df_custody[df_custody["week_key"] == wk].copy()
        
        # 최신 날짜 사용
        최신_날짜 = curr_custody["date"].max()
        core_df = curr_custody[curr_custody["date"] == 최신_날짜].copy()
        core_df = core_df.sort_values("amount", ascending=False).reset_index(drop=True)
        core_count = len(core_df)
        
        # 월간 매매
        match_month = 최신_날짜[:8] + "01"
        trading_month = df_trading[df_trading["date"] == match_month].copy()
        trading_sum_map = trading_month.groupby("ticker")["weekly_sum"].sum()
        
        # Return Price (이번 주차)
        curr_price = df_price[df_price["week_key"] == wk]
        valid_prices = curr_price[curr_price["return_price"].notna()]["ticker"].tolist()
        
        if not curr_price.empty:
            out_date = curr_price["date"].iloc[0]
        else:
            out_date = 최신_날짜

        # ── 코어 세팅 ──
        core_df["segment"] = "CORE"
        core_df["rank"] = range(1, core_count + 1)
        core_df["weekly_sum"] = core_df["ticker"].map(trading_sum_map).fillna(0.0)

        # ── 새틀라이트 세팅 ──
        if phase == "1A":
            sat_candidates = trading_month[
                (~trading_month["ticker"].isin(core_df["ticker"])) &
                (trading_month["ticker"].isin(valid_prices))
            ].copy()
            sat_candidates = sat_candidates.sort_values("weekly_sum", ascending=False).head(50).reset_index(drop=True)
            sat_candidates["segment"] = "SATELLITE"
            
        else:
            # Phase 1B
            sat_pool = pd.DataFrame({"ticker": ticker_all})
            
            # 1. Core 제외
            sat_pool = sat_pool[~sat_pool["ticker"].isin(core_df["ticker"])].copy()
            # 2. Return Price 제외
            sat_pool = sat_pool[sat_pool["ticker"].isin(valid_prices)].copy()
            
            # 3. 생존자 편향 필터 (IPO 날짜)
            sat_pool = sat_pool.merge(ipo_df[["ticker", "isin", "ipo_date", "market_cap"]], on="ticker", how="left")
            sat_pool["ipo_date"] = sat_pool["ipo_date"].fillna("1900-01-01")
            surv_mask = sat_pool["ipo_date"] <= out_date
            survivorship_excluded += (~surv_mask).sum()
            sat_pool = sat_pool[surv_mask].copy()
            
            # 매매금액 삽입
            sat_pool["weekly_sum"] = sat_pool["ticker"].map(trading_sum_map)
            
            # 4. 정렬 
            # 1순위: weekly_sum 존재 여부 (True 우선) -> 실데이터로는 weekly_sum > 0 우선
            # 2순위: weekly_sum 크기
            # 3순위: market_cap (결측은 0배정됨)
            
            sat_pool["has_trading"] = sat_pool["weekly_sum"].notna() & (sat_pool["weekly_sum"] > 0)
            
            sat_pool = sat_pool.sort_values(
                by=["has_trading", "weekly_sum", "market_cap"],
                ascending=[False, False, False]
            )
            sat_candidates = sat_pool.head(50).reset_index(drop=True)
            
            sat_candidates["segment"] = np.where(sat_candidates["has_trading"], "SATELLITE", "SATELLITE_EST")
            sat_candidates["weekly_sum"] = sat_candidates["weekly_sum"].fillna(0.0)
            
            # name_en을 위해 IPO merge에서 받은 isin 살리고 이름은 yf가 안주니 그냥 빈값으로 두거나 trading에서 가져오되 없으면 ticker
            name_map = df_custody.groupby("ticker")["name_en"].last()
            sat_candidates["name_en"] = sat_candidates["ticker"].map(name_map).fillna(sat_candidates["ticker"])

        sat_count = len(sat_candidates)
        
        # 합치기
        if sat_count > 0:
            sat_candidates["rank"] = range(core_count + 1, core_count + sat_count + 1)
            sat_candidates["amount"] = 0.0 # 배분법 돌릴거임
            
            w_df = pd.concat([
                core_df[["ticker", "isin", "name_en", "amount", "segment", "rank", "weekly_sum"]],
                sat_candidates[["ticker", "isin", "name_en", "amount", "segment", "rank", "weekly_sum"]]
            ], ignore_index=True)
        else:
            w_df = core_df[["ticker", "isin", "name_en", "amount", "segment", "rank", "weekly_sum"]].copy()

        # ── 가중치 계산: Core (S_i, N_i)
        core_mask = w_df["segment"] == "CORE"
        sum_core_amount = w_df.loc[core_mask, "amount"].sum()
        w_df["S_i"] = 0.0
        w_df.loc[core_mask, "S_i"] = w_df.loc[core_mask, "amount"] / sum_core_amount
        
        sum_all_weekly_sum = w_df["weekly_sum"].sum()
        w_df["N_i"] = 0.0
        if sum_all_weekly_sum > 0:
            w_df["N_i"] = w_df["weekly_sum"] / sum_all_weekly_sum

        w_df["W_raw"] = 0.0
        w_df.loc[core_mask, "W_raw"] = 0.7 * w_df.loc[core_mask, "S_i"] + 0.3 * w_df.loc[core_mask, "N_i"]

        # ── 가중치 계산: Satellite (대칭 배분법 S_i)
        sat_mask = w_df["segment"].str.startswith("SATELLITE")
        if sat_count > 0:
            # S_6, S_N 추출. 예외처리 (만약 6미만이면, 첫번째껄 S_1로 씀)
            if core_count >= 6:
                S_6 = w_df.loc[w_df["rank"] == 6, "S_i"].values[0]
            else:
                S_6 = w_df.loc[w_df["rank"] == 1, "S_i"].values[0]
                
            S_N = w_df.loc[w_df["rank"] == core_count, "S_i"].values[0]
            
            # S_6 0 예외
            if S_6 <= 0: S_6 = 1e-6
            if S_N <= 0: S_N = 1e-6
            
            ratio_denom = (core_count - 6) if core_count > 6 else 1
            r = (S_N / S_6) ** (1.0 / ratio_denom)
            
            sat_s_i = []
            curr_s = S_N
            for _ in range(sat_count):
                sat_s_i.append(curr_s)
                curr_s *= r
            
            w_df.loc[sat_mask, "S_i"] = sat_s_i
            w_df.loc[sat_mask, "W_raw"] = 0.7 * w_df.loc[sat_mask, "S_i"] + 0.3 * w_df.loc[sat_mask, "N_i"]

        # ── 정규화 (Normalization) ──
        w_all_raw = w_df["W_raw"].sum()
        w_df["W_base"] = w_df["W_raw"] / w_all_raw
        w_df["W_i"] = w_df["W_base"].copy()  # Cap 없음!
        
        # ── 구조 정보 주입
        w_df["date"] = out_date
        w_df["satellite_method"] = "SYMMETRIC"
        w_df["cap_applied"] = "N"
        w_df["core_count"] = core_count
        w_df["satellite_count"] = sat_count
        w_df["phase"] = phase

        # ── Satellite 방식 비교 산출
        if sat_count > 0:
            total_sat_w = w_df.loc[sat_mask, "W_base"].sum()
            sum_sat_trading = w_df.loc[sat_mask, "weekly_sum"].sum()
            
            comp_df = w_df[sat_mask][["ticker", "isin"]].copy()
            comp_df["date"] = out_date
            comp_df["segment"] = w_df.loc[sat_mask, "segment"]
            comp_df["phase"] = phase
            comp_df["W_symmetric"] = w_df.loc[sat_mask, "W_base"]
            comp_df["W_equal"] = total_sat_w / sat_count
            if sum_sat_trading > 0:
                comp_df["W_trading_prop"] = total_sat_w * (w_df.loc[sat_mask, "weekly_sum"] / sum_sat_trading)
            else:
                comp_df["W_trading_prop"] = 0.0
                
            all_comparisons.append(comp_df)

        all_weights.append(w_df)

    # ── 4. 루프 종료 후 통합 ──
    new_weights = pd.concat(all_weights, ignore_index=True)
    if all_comparisons:
        new_comparisons = pd.concat(all_comparisons, ignore_index=True)
    else:
        new_comparisons = pd.DataFrame(columns=["date", "isin", "ticker", "segment", "W_symmetric", "W_equal", "W_trading_prop", "phase"])

    col_order = ["date", "isin", "ticker", "name_en", "amount", "S_i", "N_i", 
                 "W_raw", "W_base", "W_i", "segment", "satellite_method", "cap_applied", 
                 "rank", "core_count", "satellite_count", "phase"]
    new_weights = new_weights[col_order]

    # 기존 데이터 로드 후 병합 로직 (해당 연도만 드롭하고 Append)
    if OUT_WEIGHTS.exists():
        exist_weights = pd.read_csv(OUT_WEIGHTS)
        # 삭제할 연도 확인
        exist_weights["_yr"] = exist_weights["date"].str[:4]
        exist_weights = exist_weights[exist_weights["_yr"] != str(year)].drop(columns=["_yr"])
        final_weights = pd.concat([exist_weights, new_weights], ignore_index=True)
    else:
        final_weights = new_weights

    if OUT_COMPARE.exists():
        exist_comp = pd.read_csv(OUT_COMPARE)
        if "phase" not in exist_comp.columns: exist_comp["phase"] = ""
        exist_comp["_yr"] = exist_comp["date"].str[:4]
        exist_comp = exist_comp[exist_comp["_yr"] != str(year)].drop(columns=["_yr"])
        final_comp = pd.concat([exist_comp, new_comparisons], ignore_index=True)
    else:
        final_comp = new_comparisons

    # 정렬 후 저장
    final_weights = final_weights.sort_values(["date", "rank"]).reset_index(drop=True)
    final_weights.to_csv(OUT_WEIGHTS, index=False, encoding="utf-8-sig")
    
    final_comp = final_comp.sort_values(["date"]).reset_index(drop=True)
    final_comp.to_csv(OUT_COMPARE, index=False, encoding="utf-8-sig")

    # ── 5. 요약 통계 출력 ──
    print(f"\n[{year}년 완료] 행 수 추가/갱신: {len(new_weights)}줄")
    
    # 해당 연도 내에서만 통계 계산
    yr_w = final_weights[final_weights["date"].str.startswith(str(year))]
    grouped = yr_w.groupby(["date", "segment"]).size().unstack(fill_value=0)
    
    for col in ["CORE", "SATELLITE", "SATELLITE_EST"]:
        if col not in grouped.columns:
            grouped[col] = 0

    grouped["TOTAL"] = grouped["CORE"] + grouped["SATELLITE"] + grouped["SATELLITE_EST"]
    avg = grouped.mean()

    print(f"\n=== Phase {phase} 통계 ({year}) ===")
    if phase == "1A":
        print(f"연도별 평균 종목 수:")
        print(f"  {year}년: Core {avg['CORE']:.1f}개 / Satellite {avg['SATELLITE']:.1f}개 / 합계 {avg['TOTAL']:.1f}개")
    else:
        print(f"연도별 평균 종목 수:")
        print(f"  {year}년: Core {avg['CORE']:.1f}개 / SATELLITE {avg['SATELLITE']:.1f}개 / SATELLITE_EST {avg['SATELLITE_EST']:.1f}개 / 합계 {avg['TOTAL']:.1f}개")
        print(f"\n=== 생존자 편향 필터링 결과 ===")
        # 누적이라 주단위 제외 건수의 합 (종목기준 중복 포함)
        print(f"상장일 필터로 제외된 종목 수 누적: {survivorship_excluded}건 발견/제외됨")

    first_dt = yr_w["date"].min()
    w_sum = yr_w[yr_w["date"] == first_dt]["W_i"].sum()
    print(f"\n=== 검증 ===")
    print(f"첫 주({first_dt}) W_i 합계: {w_sum * 100:.2f}%")
    print(f"\n[완료] — {year}년 실행 종료")

if __name__ == "__main__":
    main()
