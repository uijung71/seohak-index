import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import requests
from datetime import datetime, timedelta
from pathlib import Path
import matplotlib.dates as mdates

# Configuration
API_KEY = "693abf5882dab9.42616862"
INDEX_FILE = Path("output/seohak100_daily_index.csv")
CHART_OUTPUT_DIR = Path(r"C:\Users\1\OneDrive\Documents\Drive Sync\Charts")
CHART_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Plotting style
sns.set_theme(style="whitegrid")
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['axes.unicode_minus'] = False

def fetch_benchmark(symbol, start_date):
    """Fetch benchmark index from EODHD"""
    print(f"Fetching benchmark: {symbol}...")
    url = f"https://eodhd.com/api/eod/{symbol}.INDX?api_token={API_KEY}&fmt=json&from={start_date}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            df = pd.DataFrame(resp.json())
            df['date'] = pd.to_datetime(df['date'])
            return df[['date', 'close']].rename(columns={'close': symbol})
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
    return pd.DataFrame()

def generate_charts():
    # 1. Load Seohak-100 data
    if not INDEX_FILE.exists():
        print("Index file not found.")
        return
    
    df_seohak = pd.read_csv(INDEX_FILE)
    df_seohak['date'] = pd.to_datetime(df_seohak['date'], format='mixed')
    
    # 2. Fetch Benchmarks
    start_dt = df_seohak['date'].min().strftime('%Y-%m-%d')
    ndx = fetch_benchmark("NDX", start_dt)
    spx = fetch_benchmark("GSPC", start_dt)
    kospi = fetch_benchmark("KS11", start_dt)
    
    # 3. Merge data
    df = df_seohak[['date', 'index_point_usd', 'index_point_krw']].rename(
        columns={'index_point_usd': 'Seohak-100 (USD)', 'index_point_krw': 'Seohak-100 (KRW)'}
    )
    
    for bench in [ndx, spx, kospi]:
        if not bench.empty:
            df = pd.merge(df, bench, on='date', how='left')
    
    # Fill missing values (weekends/holidays) with forward fill
    df = df.sort_values('date').ffill()
    
    # 4. Define Periods
    periods = {
        "All-Time": None,
        "1Y": 365,
        "6M": 180,
        "3M": 90,
        "1M": 30
    }
    
    latest_date = df['date'].max()
    chart_paths = {}

    for name, days in periods.items():
        if days:
            start_p = latest_date - timedelta(days=days)
            df_p = df[df['date'] >= start_p].copy()
        else:
            df_p = df.copy()
            
        if df_p.empty: continue
        
        # Normalize to 100 at the start of the period for comparison
        cols_to_compare = ['Seohak-100 (USD)', 'Seohak-100 (KRW)', 'NDX', 'GSPC', 'KS11']
        available_cols = [c for c in cols_to_compare if c in df_p.columns]
        
        df_norm = df_p.copy()
        first_row = df_p.iloc[0]
        for col in available_cols:
            df_norm[col] = (df_p[col] / first_row[col]) * 100
            
        # Plotting
        plt.figure(figsize=(12, 7))
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
        for i, col in enumerate(available_cols):
            linewidth = 3 if 'Seohak' in col else 1.5
            alpha = 1.0 if 'Seohak' in col else 0.7
            plt.plot(df_norm['date'], df_norm[col], label=col, linewidth=linewidth, alpha=alpha, color=colors[i])
            
        plt.title(f"Seohak-100 Performance Comparison ({name})", fontsize=16, fontweight='bold', pad=20)
        plt.ylabel("Normalized Value (Start = 100)", fontsize=12)
        plt.xlabel("Date", fontsize=12)
        plt.legend(loc='upper left', frameon=True, fontsize=10)
        plt.grid(True, linestyle='--', alpha=0.6)
        
        # Date formatting
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.xticks(rotation=45)
        
        # Add watermark/branding
        plt.text(0.99, 0.01, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}", 
                 transform=plt.gca().transAxes, fontsize=8, color='gray', alpha=0.5, ha='right')

        plt.tight_layout()
        
        # Save
        filename = f"Seohak100_Comparison_{name}.png"
        save_path = CHART_OUTPUT_DIR / filename
        plt.savefig(save_path, dpi=150)
        plt.close()
        
        print(f"Saved: {save_path}")
        chart_paths[name] = save_path
        
    return chart_paths

if __name__ == "__main__":
    generate_charts()
