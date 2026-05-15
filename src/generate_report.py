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
    """Analyze weekly weight shifts and in/out stocks."""
    HISTORY_FILE = DATA_DIR / "processed" / "weights_history_live.csv"
    if not HISTORY_FILE.exists():
        return None
    
    df = pd.read_csv(HISTORY_FILE)
    dates = sorted(df['date'].unique(), reverse=True)
    if len(dates) < 2:
        return None
    
    today_date = dates[0]
    # 7일 전과 가장 가까운 날짜 찾기
    target_prev = (pd.to_datetime(today_date) - timedelta(days=7)).strftime('%Y-%m-%d')
    closest_prev = min(dates[1:], key=lambda x: abs(pd.to_datetime(x) - pd.to_datetime(target_prev)))
    prev_date = closest_prev

    today_df = df[df['date'] == today_date]
    prev_df = df[df['date'] == prev_date]
    
    # 가중치 변화 계산
    merged = pd.merge(
        today_df[['ticker', 'weight']], 
        prev_df[['ticker', 'weight']], 
        on='ticker', 
        how='outer', 
        suffixes=('_today', '_prev')
    ).fillna(0)
    
    merged['diff'] = merged['weight_today'] - merged['weight_prev']
    top_gainers = merged.sort_values('diff', ascending=False).head(5)
    top_losers = merged.sort_values('diff', ascending=True).head(5)
    
    # 신규 진입/퇴출
    in_stocks = merged[(merged['weight_prev'] == 0) & (merged['weight_today'] > 0)]['ticker'].tolist()
    out_stocks = merged[(merged['weight_today'] == 0) & (merged['weight_prev'] > 0)]['ticker'].tolist()
    
    return {
        'today': today_date,
        'prev': prev_date,
        'gainers': top_gainers.to_dict('records'),
        'losers': top_losers.to_dict('records'),
        'in': in_stocks,
        'out': out_stocks
    }


def build_prompt(index_data, top5, top10_up, top10_down, chg_pct, ticker_map, composition_text):
    """Build the prompt with today's market data and weight shifts."""

    top5_text = ""
    for _, row in top5.iterrows():
        name = get_name(row['ticker'], ticker_map)
        ret = row.get('daily_return', 0) * 100
        top5_text += f"  - {name} ({row['ticker']}): {ret:+.2f}%\n"

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

    direction = "상승" if chg_pct >= 0 else "하락"

    prompt = f"""당신은 서학 100 지수(한국 투자자들의 해외주식 보유 상위 100종목 기반 지수)의 전문 시장 분석가입니다.
아래 오늘의 시장 데이터와 비중 변화 데이터를 바탕으로, 한국어로 전문적인 일일 시장 분석 리포트를 작성하세요.

=== 오늘의 데이터 ({datetime.now().strftime('%Y-%m-%d')}) ===
서학 100 지수 변동률: {chg_pct:+.2f}% ({direction})

▶ 보유 비중 상위 5종목 및 당일 수익률:
{top5_text}
▶ 당일 상승률 TOP 10:
{up_text}
▶ 당일 하락률 TOP 10:
{down_text}

=== 주간 비중 및 수급 변화 (최근 7일 내외) ===
{composition_text}

=== 작성 규칙 ===
1. 반드시 아래 5개 섹션으로 구성하세요:
   - "headline": 한 줄 헤드라인 (지수 방향성과 핵심 동인을 포함, 30자 내외)
   - "summary": 시장 흐름 요약 (2~3문장, 지수 변동률을 언급하고, 주요 상승/하락 원인을 구체적으로 분석)
   - "composition_analysis": **가장 중요** 비중 변화 분석 (3~4문장, 비중이 크게 늘거나 줄어든 종목, 신규 진입/퇴출 종목을 바탕으로 한국 투자자들의 수급 트렌드를 전문적으로 해석)
   - "top5_reasons": 상위 5종목 각각의 등락 사유 (종목별 1문장, 구체적인 이유 포함)
   - "outlook": 향후 전망 (1~2문장, 당일 흐름을 바탕으로 한 단기 전망)

2. 반드시 아래 JSON 형식으로만 응답하세요 (다른 텍스트 없이):
{{
  "headline": "...",
  "summary": "...",
  "composition_analysis": "...",
  "top5_reasons": {{
    "TICKER1": "사유...",
    "TICKER2": "사유...",
    "TICKER3": "사유...",
    "TICKER4": "사유...",
    "TICKER5": "사유..."
  }},
  "outlook": "..."
}}

3. 추상적이거나 일반적인 표현은 금지합니다. 반드시 데이터에 기반한 구체적인 분석을 하세요.
4. 전문 금융 미디어 수준의 격식 있는 한국어를 사용하세요.
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
    composition_text = "비중 변화 데이터가 아직 충분하지 않습니다."
    if comp_data:
        composition_text = f"비교 기간: {comp_data['prev']} -> {comp_data['today']}\n"
        composition_text += "- 비중 증가 상위:\n"
        for item in comp_data['gainers']:
            composition_text += f"  * {get_name(item['ticker'], ticker_map)}: {item['weight_prev']*100:.2f}% -> {item['weight_today']*100:.2f}% ({item['diff']*100:+.3f}%p)\n"
        composition_text += "- 비중 감소 상위:\n"
        for item in comp_data['losers']:
            composition_text += f"  * {get_name(item['ticker'], ticker_map)}: {item['weight_prev']*100:.2f}% -> {item['weight_today']*100:.2f}% ({item['diff']*100:+.3f}%p)\n"
        if comp_data['in']:
            composition_text += f"- 신규 진입: {', '.join(comp_data['in'])}\n"
        if comp_data['out']:
            composition_text += f"- 퇴출 종목: {', '.join(comp_data['out'])}\n"

    # Build prompt and call Gemini
    prompt = build_prompt(df_index, top5, top10_up, top10_down, chg_pct, ticker_map, composition_text)

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
