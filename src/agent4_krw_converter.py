import pandas as pd
import yfinance as yf
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"
INDEX_FILE = OUTPUT_DIR / "seohak100_weekly_index.csv"
OUT_FILE = OUTPUT_DIR / "seohak100_weekly_index_krw.csv"

def main():
    print("=== Agent 4: USD/KRW 환율 반영 KRW 지수 산출 시작 ===")
    
    if not INDEX_FILE.exists():
        print("마스터 인덱스 파일이 존재하지 않습니다.")
        return

    df = pd.read_csv(INDEX_FILE)
    df["date_dt"] = pd.to_datetime(df["date"])
    
    # ISO 주차 매핑
    cal = df["date_dt"].dt.isocalendar()
    df["week_key"] = cal["year"].astype(str) + "-W" + cal["week"].astype(str).str.zfill(2)
    
    # 환율 데이터 페치
    print("yfinance에서 KRW=X 데이터를 수집 중...")
    fx = yf.download("KRW=X", start="2019-12-01", end="2026-03-01", progress=False)["Close"]
    if isinstance(fx, pd.DataFrame):
        fx = fx.squeeze()
        
    fx = fx.reset_index()
    fx.columns = ["date", "fx_rate"]
    fx["date_dt"] = pd.to_datetime(fx["date"])
    
    fx_cal = fx["date_dt"].dt.isocalendar()
    fx["week_key"] = fx_cal["year"].astype(str) + "-W" + fx_cal["week"].astype(str).str.zfill(2)
    
    # 주별 최종 환율 추출
    fx_weekly = fx.groupby("week_key").last().reset_index()[["week_key", "fx_rate"]]
    
    # 머지
    df = df.merge(fx_weekly, on="week_key", how="left")
    
    # 누락된 환율이 있다면 앞선 데이터로 채움 (또는 뒷 데이터로 채움)
    df["fx_rate"] = df["fx_rate"].ffill().bfill()
    
    # 각 지표 계산
    # 1. fx_return
    df["fx_return"] = df["fx_rate"].pct_change().fillna(0.0)
    
    # 2. weekly_return_krw
    # = (1 + 자산수익률) * (1 + 환율변동) - 1
    df["weekly_return_krw"] = (1 + df["weekly_return"]) * (1 + df["fx_return"]) - 1.0
    
    # 첫째 주(베이스)의 수익률은 0.0 으로 고정
    df.loc[0, "fx_return"] = 0.0
    df.loc[0, "weekly_return_krw"] = 0.0
    
    # 3. index_point_krw
    # 체인 방식으로 연결. 최초 1000 포인트 시작
    krw_idx = []
    curr = 1000.0
    
    for i, r in df.iterrows():
        if i == 0:
            krw_idx.append(curr)
        else:
            curr = curr * (1 + r["weekly_return_krw"])
            krw_idx.append(curr)
            
    df["index_point_krw"] = krw_idx
    
    # 최종 컬럼 정리
    cols_order = [
        "date", "index_point", "weekly_return", 
        "fx_rate", "fx_return", "index_point_krw", "weekly_return_krw",
        "component_count", "core_count", "satellite_count", 
        "top3_contributors", "satellite_method", "phase"
    ]
    df = df[cols_order]
    
    # 소수점 반올림 (옵션, 기존 index_point는 그대로 둠)
    # df["fx_rate"] = df["fx_rate"].round(2)
    
    df.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")
    print(f"환율 반영 지수 저장 완료: {OUT_FILE}")
    print(f"최종 USD 지수: {df['index_point'].iloc[-1]:.2f} pt")
    print(f"최종 KRW 지수: {df['index_point_krw'].iloc[-1]:.2f} pt")

if __name__ == "__main__":
    main()
