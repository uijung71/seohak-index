"""
서학 100 AI 일일 시장분석 리포트 생성기
- Gemini API를 사용하여 당일 시장 데이터 기반 리포트 자동 생성
- 벤치마크 지수(나스닥/S&P/코스피) 자동 업데이트
- 하루 1회 파이프라인 실행 시 호출
- 결과는 output/daily_report.json에 저장
"""
import json
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from google import genai

# ── Configuration ──────────────────────────────────────────────
load_dotenv()
CLIENT = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

WEIGHTS_FILE = DATA_DIR / "processed" / "weights_daily_live.csv"
RETURNS_FILE = DATA_DIR / "processed" / "daily_returns_live.csv"
INDEX_FILE = OUTPUT_DIR / "seohak100_daily_index.csv"
TICKER_MAP_FILE = DATA_DIR / "processed" / "ticker_korean_map.csv"
BENCHMARK_FILE = DATA_DIR / "raw" / "benchmark_indices.csv"
REPORT_OUTPUT = OUTPUT_DIR / "daily_report.json"

MODEL_NAME = "gemini-2.5-flash"
EODHD_API_KEY = "693abf5882dab9.42616862"
BENCHMARK_SYMBOLS = ['NDX', 'GSPC', 'KS11']


def update_benchmarks():
    """Fetch latest benchmark index data from EODHD and update CSV."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Benchmark update...")
    if not BENCHMARK_FILE.exists():
        return
    df = pd.read_csv(BENCHMARK_FILE)
    df['date'] = pd.to_datetime(df['date'])
    from_date = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')

    for sym in BENCHMARK_SYMBOLS:
        try:
            url = f"https://eodhd.com/api/eod/{sym}.INDX?api_token={EODHD_API_KEY}&fmt=json&from={from_date}"
            data = pd.DataFrame(requests.get(url, timeout=10).json())
            data['date'] = pd.to_datetime(data['date'])
            for _, row in data.iterrows():
                mask = df['date'] == row['date']
                if mask.any():
                    df.loc[mask, sym] = row['close']
                else:
                    df = pd.concat([df, pd.DataFrame([{'date': row['date'], sym: row['close']}])], ignore_index=True)
            print(f"  {sym}: OK ({len(data)} rows)")
        except Exception as e:
            print(f"  {sym}: FAILED ({e})")

    df = df.sort_values('date').drop_duplicates('date', keep='last')
    df.to_csv(BENCHMARK_FILE, index=False)



def load_ticker_map():
    if TICKER_MAP_FILE.exists():
        df = pd.read_csv(TICKER_MAP_FILE)
        return dict(zip(df['ticker'].str.strip().str.upper(), df['name_ko']))
    return {}


def get_name(ticker, ticker_map):
    import re
    t = re.sub(r'[^A-Z0-9.]', '', str(ticker).strip().upper())
    return ticker_map.get(t, t)


def analyze_composition_changes():
    """Analyze actual index component changes vs daily rank shifts."""
    HISTORY_FILE = DATA_DIR / "processed" / "weights_history_live.csv"
    COMPONENTS_FILE = OUTPUT_DIR / "seohak100_components_since_202512.csv"
    
    if not HISTORY_FILE.exists():
        return None
    
    df_hist = pd.read_csv(HISTORY_FILE)
    dates = sorted(df_hist['date'].unique(), reverse=True)
    if len(dates) < 2:
        return None
    
    today_date = dates[0]
    # 7일 전과 가장 가까운 날짜 찾기 (비중 변화 분석용)
    target_prev = (pd.to_datetime(today_date) - timedelta(days=7)).strftime('%Y-%m-%d')
    closest_prev = min(dates[1:], key=lambda x: abs(pd.to_datetime(x) - pd.to_datetime(target_prev)))
    prev_date = closest_prev

    today_weights = df_hist[df_hist['date'] == today_date]
    prev_weights = df_hist[df_hist['date'] == prev_date]
    
    # 1. 실제 지수 구성 종목 변경 (Actual Rebalancing)
    actual_in = []
    actual_out = []
    rebal_info = "정보 없음"
    
    if COMPONENTS_FILE.exists():
        comp_df = pd.read_csv(COMPONENTS_FILE)
        comp_df['date'] = pd.to_datetime(comp_df['date']).dt.strftime('%Y-%m-%d')
        rebal_dates = sorted(comp_df['date'].unique(), reverse=True)
        
        if len(rebal_dates) >= 2:
            latest_rebal_date = rebal_dates[0]
            prev_rebal_date = rebal_dates[1]
            
            latest_members = set(comp_df[comp_df['date'] == latest_rebal_date]['ticker'].tolist())
            prev_members = set(comp_df[comp_df['date'] == prev_rebal_date]['ticker'].tolist())
            
            actual_in = list(latest_members - prev_members)
            actual_out = list(prev_members - latest_members)
            
            if today_date == latest_rebal_date:
                rebal_info = f"금일({today_date}) 공식 리밸런싱 수행됨"
            else:
                rebal_info = f"최근 리밸런싱({latest_rebal_date}) 결과 유지 중"

    # 2. 가중치 및 순위권 변화 (Rank Shifts / Market Sentiment)
    merged = pd.merge(
        today_weights[['ticker', 'weight']], 
        prev_weights[['ticker', 'weight']], 
        on='ticker', 
        how='outer', 
        suffixes=('_today', '_prev')
    ).fillna(0)
    
    merged['diff'] = merged['weight_today'] - merged['weight_prev']
    top_gainers = merged.sort_values('diff', ascending=False).head(5)
    top_losers = merged.sort_values('diff', ascending=True).head(5)
    
    # 100위권 진입/이탈 (이것은 실제 지수 편입과 다를 수 있음)
    rank_in = merged[(merged['weight_prev'] == 0) & (merged['weight_today'] > 0)]['ticker'].tolist()
    rank_out = merged[(merged['weight_today'] == 0) & (merged['weight_prev'] > 0)]['ticker'].tolist()
    
    return {
        'today': today_date,
        'prev': prev_date,
        'rebal_info': rebal_info,
        'gainers': top_gainers.to_dict('records'),
        'losers': top_losers.to_dict('records'),
        'actual_in': actual_in,
        'actual_out': actual_out,
        'rank_in': rank_in,
        'rank_out': rank_out
    }


def build_prompt(index_data, top5, top10_up, top10_down, chg_pct, ticker_map, comp_data):
    """Build the prompt with today's market data and weight shifts."""

    top5_text = ""
    top5_info = []
    for _, row in top5.iterrows():
        name = get_name(row['ticker'], ticker_map)
        ret = row.get('daily_return', 0) * 100
        top5_text += f"  - {name} ({row['ticker']}): {ret:+.2f}%\n"
        top5_info.append(f"{name}({row['ticker']})")

    top5_tickers_str = ", ".join(top5_info)

    up_text = ""
    for _, row in top10_up.iterrows():
        name = get_name(row['ticker'], ticker_map)
        ret = row['daily_return'] * 100
        up_text += f"  - {name} ({row['ticker']}): {ret:+.2f}%\n"

    down_text = ""
    for _, row in top10_down.iterrows():
        name = get_name(row['ticker'], ticker_map)
        ret = row['daily_return'] * 100
        down_text += f"  - {name} ({row['ticker']}): {ret:+.2f}%\n"

    # Composition text handling
    if comp_data:
        actual_in_text = ", ".join(comp_data['actual_in']) if comp_data['actual_in'] else "없음"
        actual_out_text = ", ".join(comp_data['actual_out']) if comp_data['actual_out'] else "없음"
        rank_in_text = ", ".join(comp_data['rank_in'][:10]) if comp_data['rank_in'] else "없음"
        rank_out_text = ", ".join(comp_data['rank_out'][:10]) if comp_data['rank_out'] else "없음"
        rebal_info = comp_data.get('rebal_info', '정보 없음')
    else:
        actual_in_text = actual_out_text = rank_in_text = rank_out_text = "데이터 부족"
        rebal_info = "데이터 부족"

    prompt = f"""당신은 서학 100 지수(한국 투자자들의 해외주식 보유 상위 100종목 기반 지수)의 전문 시장 분석가입니다.
아래 오늘의 시장 데이터와 비중 변화 데이터를 바탕으로, 한국어로 전문적인 일일 시장 분석 리포트를 작성하세요.

### 1. 지수 성과
- 서학 100 지수: {index_data['index_point_usd']:.2f} pt ({chg_pct:+.2f}%)
- 당일 지수 영향력 상위 종목:
{top5_text}

### 2. 주간 포트폴리오 비중 변화 (최근 7일 대비)
- **비중 크게 증가:**
{up_text}
- **비중 크게 감소:**
{down_text}

### 3. 지수 구성 종목 변경 현황 (공식 리밸런싱 - v5.2 규칙)
*참고: 리밸런싱은 매주 월요일에만 발생하며, 2주 연속 90위 이내(진입) 또는 110위 밖(퇴출) 조건을 충족해야 합니다.*
- **현재 상태:** {rebal_info}
- **공식 신규 편입 (IN):** {actual_in_text}
- **공식 지수 편출 (OUT):** {actual_out_text}

### 4. 실시간 수급 모니터링 (100위권 순위 변동 - 지수 종목과 무관할 수 있음)
*순위권에는 진입했으나 아직 2주 대기 조건을 채우지 못한 '예비 후보군' 성격의 종목들입니다.*
- **100위권 신규 포착:** {rank_in_text}
- **100위권 이탈:** {rank_out_text}

- 반드시 JSON 형식으로만 응답하세요.
- 키 설명:
    - headline: 리포트 제목
    - summary: 전체 시장 요약 (1단락)
    - composition_analysis: **[중요]** 지수 리밸런싱 및 수급 분석. 공식 리밸런싱(월요일)과 단순 순위 변동을 엄격히 구분하세요. 애플(AAPL) 등이 순위가 밀려도 공식 리밸런싱 전까지는 "퇴출"이라 표현하지 말고 "퇴출 후보" 등으로 설명해야 합니다.
    - top5_reasons: **반드시 딕셔너리 형식**이어야 하며, 키는 다음 5개 티커({", ".join(top5['ticker'].tolist())})여야 합니다. 값은 해당 종목의 오늘 변동 원인에 대한 전문적인 설명입니다.
    - outlook: 향후 전망 및 투자 전략
"""
    return prompt


def generate_report():
    """Generate daily AI market analysis report."""
    # Update benchmark indices first
    update_benchmarks()

    print(f"[{datetime.now().strftime('%H:%M:%S')}] AI report generation...")

    # Load data
    ticker_map = load_ticker_map()
    df_index = pd.read_csv(INDEX_FILE)
    df_index['date'] = pd.to_datetime(df_index['date'], format='mixed')
    df_index = df_index.sort_values('date')

    df_weights = pd.read_csv(WEIGHTS_FILE)
    df_weights['date'] = pd.to_datetime(df_weights['date'], format='mixed')

    df_returns = pd.read_csv(RETURNS_FILE)
    df_returns['date'] = pd.to_datetime(df_returns['date'], format='mixed')

    # Calculate change
    last = df_index.iloc[-1]
    prev = df_index.iloc[-2] if len(df_index) > 1 else last
    chg_pct = ((last['index_point_usd'] - prev['index_point_usd']) / prev['index_point_usd']) * 100

    # Get top 5 by weight with returns
    latest_w_date = df_weights['date'].max()
    top5_w = df_weights[df_weights['date'] == latest_w_date].sort_values('weight', ascending=False).head(5)

    latest_r_date = df_returns['date'].max()
    returns_latest = df_returns[df_returns['date'] == latest_r_date].drop_duplicates('ticker')

    # Merge returns into top5
    top5 = top5_w.merge(returns_latest[['ticker', 'daily_return']], on='ticker', how='left').fillna(0)

    # Universe-filtered TOP 10
    universe = df_weights[df_weights['date'] == latest_w_date]['ticker'].unique().tolist()
    filtered = returns_latest[returns_latest['ticker'].isin(universe)]
    top10_up = filtered.nlargest(10, 'daily_return')
    top10_down = filtered.nsmallest(10, 'daily_return')

    # 비중 변화 데이터 추출
    comp_data = analyze_composition_changes()

    # Build prompt and call Gemini
    prompt = build_prompt(last, top5, top10_up, top10_down, chg_pct, ticker_map, comp_data)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Gemini API 호출 중...")
    response = CLIENT.models.generate_content(model=MODEL_NAME, contents=prompt)

    # Parse response
    raw_text = response.text.strip()
    # Remove markdown code fences if present
    if raw_text.startswith("```"):
        raw_text = raw_text.split("\n", 1)[1]  # Remove first line
        if raw_text.endswith("```"):
            raw_text = raw_text[:-3]
        raw_text = raw_text.strip()

    try:
        report = json.loads(raw_text)
    except json.JSONDecodeError:
        print(f"[ERROR] JSON 파싱 실패. 원본 응답:\n{raw_text}")
        report = {
            "headline": f"서학 100 지수 {chg_pct:+.2f}% 변동",
            "summary": "AI 리포트 생성에 실패했습니다. 기본 데이터를 참고하세요.",
            "composition_analysis": "데이터 집계 중입니다.",
            "top5_reasons": {},
            "outlook": "시장 상황을 면밀히 관찰할 필요가 있습니다.",
        }

    # Add metadata
    report['generated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    report['index_date'] = last['date'].strftime('%Y-%m-%d')
    report['change_pct'] = round(chg_pct, 2)

    # Save
    REPORT_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_OUTPUT, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] [OK] 리포트 저장 완료: {REPORT_OUTPUT}")
    print(f"  헤드라인: {report.get('headline', 'N/A')}")
    return report


if __name__ == "__main__":
    generate_report()
