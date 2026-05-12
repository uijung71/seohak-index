import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime

base_dir = Path(__file__).resolve().parent.parent
data_dir = base_dir / "data"
ticker_file = data_dir / "processed" / "ticker_universe.csv"
out_file = data_dir / "reference" / "ipo_dates.csv"

def main():
    print("=== IPO Date & Market Cap 수집 시작 ===")
    if not ticker_file.exists():
        print("ticker_universe.csv 파일이 없습니다. 에이전트 1을 실행하세요.")
        return

    df = pd.read_csv(ticker_file)
    tickers = df["ticker"].tolist()
    isins = df["isin"].tolist()

    out_data = []
    
    total = len(tickers)
    for i, (t, isin) in enumerate(zip(tickers, isins)):
        print(f"[{i+1}/{total}] 수집 중: {t}", end="\r")
        try:
            ticker_obj = yf.Ticker(t)
            info = ticker_obj.info
            
            # 1. 상장일 (IPO)
            ipo_epoch = info.get("firstTradeDateEpochUtc")
            if ipo_epoch:
                ipo_date = datetime.fromtimestamp(ipo_epoch).strftime('%Y-%m-%d')
            else:
                # epoch 정보가 없으면 history 첫날로
                hist = ticker_obj.history(period="max")
                if not hist.empty:
                    ipo_date = hist.index[0].strftime('%Y-%m-%d')
                else:
                    ipo_date = "1900-01-01"
            
            # 2. Market Cap (최신)
            market_cap = info.get("marketCap", 0)
            if market_cap is None:
               market_cap = 0
            
            out_data.append({
                "ticker": t,
                "isin": isin,
                "ipo_date": ipo_date,
                "market_cap": market_cap
            })
            
        except Exception as e:
            out_data.append({
                "ticker": t,
                "isin": isin,
                "ipo_date": "1900-01-01",
                "market_cap": 0
            })
            
    print("\n수집 완료. csv 저장 중...")
    out_dir = out_file.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    
    res_df = pd.DataFrame(out_data)
    res_df.to_csv(out_file, index=False, encoding="utf-8-sig")
    print(f"저장 성공: {out_file}")

if __name__ == "__main__":
    main()
