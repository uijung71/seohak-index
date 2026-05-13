"""
서학 100 지수 실시간 대시보드 V12.0
- Full-width layout (no sidebar)
- Base-1000 normalized multi-index comparison
- 20/60-day moving averages for all indices
- Universe-filtered TOP 10 rankings
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta, timezone
import re
import json
import yfinance as yf

# ── Configuration ──────────────────────────────────────────────
st.set_page_config(page_title="서학 100 대시보드 V12.0", page_icon="⚡", layout="wide")
KST = timezone(timedelta(hours=9))

BASE = 1000                          # Normalization base for chart
REPORT_HEIGHT = 433                   # AI report card height (px)
TABLE_HEIGHT = 388                    # TOP 10 table height (px, compensates tab bar)
PORTFOLIO_HEIGHT = 530                # Portfolio section height (px)
MA_WINDOWS = [20, 60]                 # Moving average periods

# ── File Paths ─────────────────────────────────────────────────
INDEX_FILE = Path("output/seohak100_daily_index.csv")
WEIGHTS_FILE = Path("data/processed/weights_daily_live.csv")
RETURNS_FILE = Path("data/processed/daily_returns_live.csv")
BENCHMARK_FILE = Path("data/raw/benchmark_indices.csv")
REPORT_FILE = Path("output/daily_report.json")
TICKER_MAP_FILE = Path("data/processed/ticker_korean_map.csv")

# ── Period Mapping ─────────────────────────────────────────────
PERIOD_DAYS = {"최근 1개월": 30, "최근 3개월": 90, "최근 6개월": 180, "최근 1년": 365, "전체 기간": 9999}

# ── Sector Classification Rules ────────────────────────────────
SECTOR_RULES = [
    (['SOXL','TQQQ','LABU','NVDL','BITX','CONL','TSLL','FNGU','BULZ'], '🚀 고레버리지 ETF'),
    (['SOXS','SQQQ'], '📉 인버스/헤지'),
    (['NVDA','TSLA','AAPL','MSFT','AMZN','GOOG','META','AVGO','ARM','ORCL','AMD','NFLX'], '💻 빅테크 & AI'),
    (['MU','SMH','SOXX','LRCX','AMAT','ASML','INTC','QCOM','KLAC'], '🔌 반도체 장비/설계'),
    (['TLT','TMF','EDV','IBIT','BITO','GLD','GDX','SIL','IAU'], '🏦 채권/금/암호화폐'),
    (['LLY','NVO','UNH','JNJ','ABBV','MRK','PFE'], '🏥 헬스케어/바이오'),
    (['WMT','PG','KO','PEP','MCD','NKE','SBUX','DIS','HD','V','MA'], '🛒 소비재 & 금융'),
    (['NEE','XOM','CVX','AMT','PLD','O'], '🔋 에너지/리츠'),
]

# ── AI Report (loaded from daily_report.json) ─────────────────
DEFAULT_REASON = "섹터 내 견조한 펀더멘털과 시장 수급 개선에 따라 유의미한 변동성을 보였습니다."

def load_ai_report():
    """Load AI-generated daily report from JSON file."""
    if REPORT_FILE.exists():
        with open(REPORT_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

# ── Benchmark Index Config ─────────────────────────────────────
BENCHMARKS = [
    {'col': 'NDX',  'name': '나스닥 100', 'color': '#2ecc71', 'yf': '^IXIC'},
    {'col': 'GSPC', 'name': 'S&P 500',   'color': '#e74c3c', 'yf': '^GSPC'},
    {'col': 'KS11', 'name': '코스피',     'color': '#636e72', 'yf': '^KS11'},
]

# ── CSS ────────────────────────────────────────────────────────
def inject_css():
    st.markdown("""<style>
    @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css');
    * { font-family: 'Pretendard', sans-serif; }
    .main { background: #0e1117; }
    .section-header {
        font-size: 2.2rem !important; font-weight: 900 !important;
        background: linear-gradient(90deg, #00d4ff, #9b59b6);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        margin-bottom: 25px !important; margin-top: 45px !important;
    }
    .date-info { font-size: 1rem; color: #aaa; text-align: right; margin-bottom: -40px; padding-right: 15px; }

    /* ── Custom Metric Cards (replaces st.metric) ──── */
    .metric-grid {
        display: grid;
        grid-template-columns: repeat(5, 1fr);
        gap: 12px;
        margin-bottom: 10px;
    }
    .metric-card {
        background: linear-gradient(145deg, #23273a, #1e2130);
        border-radius: 16px; padding: 16px 14px;
        border: 1px solid rgba(255,255,255,0.08);
        text-align: left;
    }
    .metric-label { font-size: 0.8rem; color: #aaa; margin-bottom: 4px; white-space: nowrap; }
    .metric-value { font-size: 1.6rem; font-weight: 800; color: #fff; white-space: nowrap; }
    .metric-delta { font-size: 0.85rem; font-weight: 600; margin-top: 2px; }
    .delta-up { color: #ff4b4b; }
    .delta-down { color: #4b91ff; }

    .status-card {
        background: rgba(255,255,255,0.04); backdrop-filter: blur(12px);
        border-radius: 28px; padding: 25px; border: 1px solid rgba(255,255,255,0.1);
        line-height: 1.6; font-size: 0.95rem; color: #e0e0e0;
        height: """ + str(REPORT_HEIGHT) + """px; overflow-y: auto;
    }
    .status-card::-webkit-scrollbar { width: 6px; }
    .status-card::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.1); border-radius: 10px; }
    .report-title { font-size: 1.35rem; font-weight: 800; color: #00d4ff; margin-bottom: 12px; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 8px; }
    .sub-title { font-size: 1rem; font-weight: 700; color: #9b59b6; margin-top: 12px; margin-bottom: 5px; }
    .highlight-up { color: #ff4b4b; font-weight: 700; }
    .highlight-down { color: #4b91ff; font-weight: 700; }
    [data-testid="stDataFrame"] { font-size: 0.88rem !important; }

    /* ── Mobile Responsive ─────────────────────────── */
    @media (max-width: 768px) {
        .section-header { font-size: 1.2rem !important; margin-top: 15px !important; margin-bottom: 10px !important; }
        .date-info { font-size: 0.7rem !important; margin-bottom: -15px !important; }

        .metric-grid { grid-template-columns: repeat(3, 1fr); gap: 8px; }
        .metric-card { padding: 10px 8px; border-radius: 12px; }
        .metric-label { font-size: 0.6rem; }
        .metric-value { font-size: 1.1rem; }
        .metric-delta { font-size: 0.65rem; }

        /* Chart controls */
        [data-testid="stHorizontalBlock"] [data-testid="stCheckbox"] label span {
            font-size: 0.7rem !important;
        }

        /* Report card */
        .status-card { padding: 15px !important; border-radius: 16px !important; font-size: 0.82rem !important; height: auto !important; }
        .report-title { font-size: 1rem !important; }
        .sub-title { font-size: 0.85rem !important; }
        [data-testid="stDataFrame"] { font-size: 0.72rem !important; }
    }
    @media (max-width: 480px) {
        .section-header { font-size: 1rem !important; }
        .metric-grid { grid-template-columns: repeat(3, 1fr); gap: 6px; }
        .metric-card { padding: 8px 6px; border-radius: 10px; }
        .metric-label { font-size: 0.52rem; }
        .metric-value { font-size: 0.95rem; }
        .metric-delta { font-size: 0.58rem; }
        .status-card { font-size: 0.75rem !important; }
    }
    </style>""", unsafe_allow_html=True)


# ── Helper Functions ───────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_ticker_map():
    """Load ticker→Korean name mapping from CSV."""
    if TICKER_MAP_FILE.exists():
        df = pd.read_csv(TICKER_MAP_FILE)
        return dict(zip(df['ticker'].str.strip().str.upper(), df['name_ko']))
    return {}

def get_korean_name(ticker, ticker_map):
    """Convert ticker symbol to Korean display name."""
    t = re.sub(r'[^A-Z0-9]', '', str(ticker).split('.')[0].strip().upper())
    return ticker_map.get(t, t)

def classify_sector(ticker):
    """Classify ticker into premium sector category."""
    t = str(ticker).upper()
    for keywords, label in SECTOR_RULES:
        if any(k in t for k in keywords):
            return label
    return '📦 기타 전략 종목'

def calc_change_pct(current, previous):
    """Calculate percentage change safely."""
    return ((current - previous) / previous) * 100 if previous != 0 else 0

def normalize(series, start_ref=None):
    """Normalize series to BASE (1000) starting point."""
    ref = start_ref if start_ref is not None else (series.iloc[0] if not series.empty else 1)
    return (series / ref) * BASE

def add_moving_averages(df, columns, windows=MA_WINDOWS):
    """Add MA columns to dataframe for specified columns and windows."""
    for col in columns:
        if col in df.columns:
            for w in windows:
                df[f'{col}_ma{w}'] = df[col].rolling(window=w).mean()
    return df

def load_csv(path):
    """Load CSV with date parsing, return empty DataFrame if missing."""
    if path.exists():
        df = pd.read_csv(path)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='mixed')
        return df
    return pd.DataFrame()

@st.cache_data(ttl=600)
def get_live_indices():
    """Fetch real-time benchmark index data via yfinance."""
    result = {}
    for bm in BENCHMARKS:
        try:
            hist = yf.Ticker(bm['yf']).history(period="2d")
            if len(hist) >= 2:
                curr, prev = hist['Close'].iloc[-1], hist['Close'].iloc[-2]
                result[bm['name']] = {
                    "val": curr,
                    "delta": f"{calc_change_pct(curr, prev):+.2f}%",
                    "date": hist.index[-1].strftime('%m/%d'),
                }
        except Exception:
            result[bm['name']] = {"val": 0, "delta": "-", "date": "-"}
    return result


# ── UI Section Renderers ───────────────────────────────────────
def _metric_card_html(label, value, delta_str):
    """Generate a single metric card as HTML."""
    try:
        delta_val = float(delta_str.replace('%', '').replace('+', ''))
        delta_class = 'delta-down' if delta_val < 0 else 'delta-up'
        arrow = '▲' if delta_val >= 0 else '▼'
        delta_display = f"{arrow} {delta_str}"
    except (ValueError, AttributeError):
        delta_class = 'delta-down'
        delta_display = delta_str
    return (
        f'<div class="metric-card">'
        f'<div class="metric-label">{label}</div>'
        f'<div class="metric-value">{value}</div>'
        f'<div class="metric-delta {delta_class}">{delta_display}</div>'
        f'</div>'
    )

def render_header(last, prev, live_data):
    """Render top metrics bar using custom HTML cards."""
    chg_usd = calc_change_pct(last['index_point_usd'], prev['index_point_usd'])
    chg_krw = calc_change_pct(last['index_point_krw'], prev['index_point_krw'])
    date_str = last['date'].strftime('%m/%d')

    st.markdown(f'<div class="date-info">데이터 기준일: {last["date"].strftime("%Y-%m-%d")} | 업데이트: {datetime.now(KST).strftime("%Y-%m-%d %H:%M")}</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-header">⚡ 서학 100 지수 실시간 대시보드</div>', unsafe_allow_html=True)

    cards_html = '<div class="metric-grid">'
    cards_html += _metric_card_html(f"서학 USD ({date_str})", f"{last['index_point_usd']:,.0f}", f"{chg_usd:+.2f}%")
    cards_html += _metric_card_html(f"서학 KRW ({date_str})", f"{last['index_point_krw']:,.0f}", f"{chg_krw:+.2f}%")
    for bm in BENCHMARKS:
        d = live_data.get(bm['name'], {})
        bm_date = d.get('date', '-')
        val = d.get('val', 0)
        cards_html += _metric_card_html(f"{bm['name']} ({bm_date})", f"{val:,.0f}", d.get('delta', '-'))
    cards_html += '</div>'
    st.markdown(cards_html, unsafe_allow_html=True)


def render_chart(df_index, df_bench):
    """Render performance comparison chart with MA overlays."""
    st.markdown('<div class="section-header">📈 지수 성과 비교 및 기술적 분석</div>', unsafe_allow_html=True)

    # All controls in one row: Period | Indices | Moving Averages
    c1, c2, c3 = st.columns([1, 2, 1.5])
    with c1:
        period = st.selectbox("조회 기간", list(PERIOD_DAYS.keys()))
    with c2:
        indices = st.multiselect("비교지수", ['서학(USD)', '서학(KRW)', '나스닥', 'S&P', '코스피'],
                                 default=['서학(USD)', '나스닥'])
    with c3:
        ma_options = st.multiselect("이동평균선", ['20일선', '60일선'], default=[])

    sw_usd = '서학(USD)' in indices
    sw_krw = '서학(KRW)' in indices
    sw_ma20 = '20일선' in ma_options
    sw_ma60 = '60일선' in ma_options
    bm_switches = {
        'NDX': '나스닥' in indices,
        'GSPC': 'S&P' in indices,
        'KS11': '코스피' in indices,
    }

    cutoff = df_index['date'].max() - timedelta(days=PERIOD_DAYS[period])
    df_p = df_index[df_index['date'] >= cutoff].sort_values('date')
    if df_p.empty:
        return

    fig = go.Figure()
    seohak_traces = [
        ('index_point_usd', '서학 100 (USD)', '#00d4ff', 4, sw_usd),
        ('index_point_krw', '서학 100 (KRW)', '#9b59b6', 2, sw_krw),
    ]
    for col, name, color, width, switch in seohak_traces:
        if not switch:
            continue
        start_val = df_p[col].iloc[0]
        fig.add_trace(go.Scatter(x=df_p['date'], y=normalize(df_p[col]), name=name, line=dict(color=color, width=width)))
        if sw_ma20 and f'{col}_ma20' in df_p.columns:
            fig.add_trace(go.Scatter(x=df_p['date'], y=normalize(df_p[f'{col}_ma20'], start_val), name=f'{name} 20일', line=dict(color=color, width=1, dash='dot')))
        if sw_ma60 and f'{col}_ma60' in df_p.columns:
            fig.add_trace(go.Scatter(x=df_p['date'], y=normalize(df_p[f'{col}_ma60'], start_val), name=f'{name} 60일', line=dict(color=color, width=1, dash='dash')))

    if not df_bench.empty:
        df_bc = df_bench[df_bench['date'] >= cutoff].sort_values('date')
        for bm in BENCHMARKS:
            col = bm['col']
            if not bm_switches.get(col) or col not in df_bc.columns:
                continue
            d = df_bc.dropna(subset=[col])
            if d.empty:
                continue
            start_val = d[col].iloc[0]
            fig.add_trace(go.Scatter(x=d['date'], y=normalize(d[col]), name=bm['name'], line=dict(color=bm['color'], width=1.5)))
            if sw_ma20 and f'{col}_ma20' in d.columns:
                fig.add_trace(go.Scatter(x=d['date'], y=normalize(d[f'{col}_ma20'], start_val), name=f"{bm['name']} 20일", line=dict(color=bm['color'], width=0.8, dash='dot')))
            if sw_ma60 and f'{col}_ma60' in d.columns:
                fig.add_trace(go.Scatter(x=d['date'], y=normalize(d[f'{col}_ma60'], start_val), name=f"{bm['name']} 60일", line=dict(color=bm['color'], width=0.8, dash='dash')))

    fig.update_layout(
        template='plotly_dark', height=500, hovermode='x unified',
        margin=dict(t=20, b=20, l=20, r=20), yaxis_title="지수 성과 (Base 1000)",
        legend=dict(orientation="h", y=1.1, x=1, xanchor="right"),
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
    )
    st.plotly_chart(fig, use_container_width=True)


def render_report_and_rankings(df_weights, df_returns, chg_usd, ticker_map):
    """Render AI market analysis report and TOP 10 tables."""
    st.markdown('<div class="section-header">📝 시장분석 및 상승/하락종목 TOP10</div>', unsafe_allow_html=True)

    latest_date = df_weights['date'].max()
    universe = df_weights[df_weights['date'] == latest_date]['ticker'].unique().tolist()
    top5 = df_weights[df_weights['date'] == latest_date].sort_values('weight', ascending=False).head(5)
    returns_latest = df_returns[df_returns['date'] == df_returns['date'].max()].drop_duplicates('ticker')

    # Load AI-generated report
    ai_report = load_ai_report()

    col_left, col_right = st.columns([1.6, 1])

    # Left: AI Report
    with col_left:
        if ai_report:
            headline = f"⚡ {ai_report.get('headline', '시장 분석 리포트')}"
            summary = ai_report.get('summary', '')
            ai_reasons = ai_report.get('top5_reasons', {})
            outlook = ai_report.get('outlook', '')
        else:
            headline = f"⚡ 서학 100 지수 {chg_usd:+.2f}% 변동"
            summary = f"서학 100 지수가 전일 대비 {chg_usd:+.2f}% 변동했습니다."
            ai_reasons = {}
            outlook = "AI 리포트를 생성하려면 generate_report.py를 실행하세요."

        top5_html = ""
        for _, row in top5.iterrows():
            tick = row['ticker']
            ret = returns_latest[returns_latest['ticker'] == tick]
            rv = ret['daily_return'].iloc[0] * 100 if not ret.empty else 0
            css_class = "highlight-up" if rv >= 0 else "highlight-down"
            name = get_korean_name(tick, ticker_map)
            reason = ai_reasons.get(tick, DEFAULT_REASON)
            top5_html += f"<b>• {name} ({tick}) | <span class='{css_class}'>{rv:+.2f}%</span></b><br>"
            top5_html += f"<span style='font-size:0.92rem; color:#bbb;'>&nbsp;&nbsp;{reason}</span><br>"

        st.markdown(
            f'<div class="status-card">'
            f'<div class="report-title">{headline}</div>'
            f'<div class="sub-title">1. 시장 흐름 요약</div>{summary}'
            f'<div class="sub-title">2. 5대 핵심 보유 종목 심층 분석</div>{top5_html}'
            f'<div class="sub-title">3. 향후 전망</div>{outlook}</div>',
            unsafe_allow_html=True,
        )

    # Right: TOP 10 Tables
    with col_right:
        filtered = returns_latest[returns_latest['ticker'].isin(universe)].copy()
        filtered['종목명'] = filtered['ticker'].apply(lambda t: get_korean_name(t, ticker_map))

        def format_ranking(df_slice):
            return df_slice[['ticker', '종목명', 'daily_return']].rename(
                columns={'ticker': '티커', 'daily_return': '수익률'}
            )

        tab_up, tab_down = st.tabs(['📈 상승 TOP 10', '📉 하락 TOP 10'])
        with tab_up:
            st.dataframe(format_ranking(filtered.nlargest(10, 'daily_return')).style.format({'수익률': '{:+.2%}'}),
                         height=TABLE_HEIGHT, hide_index=True, use_container_width=True)
        with tab_down:
            st.dataframe(format_ranking(filtered.nsmallest(10, 'daily_return')).style.format({'수익률': '{:+.2%}'}),
                         height=TABLE_HEIGHT, hide_index=True, use_container_width=True)


def render_portfolio(df_weights, ticker_map):
    """Render portfolio composition pie chart and holdings table."""
    latest_date = df_weights['date'].max()
    st.markdown(
        f'<div class="section-header">🍰 포트폴리오 구성 및 비중 '
        f'<span style="font-size:1.2rem; color:#888;">({latest_date.strftime("%Y-%m-%d")} 기준)</span></div>',
        unsafe_allow_html=True,
    )
    latest = df_weights[df_weights['date'] == latest_date].copy()
    latest['sector'] = latest['ticker'].apply(classify_sector)
    latest['종목명'] = latest['ticker'].apply(lambda t: get_korean_name(t, ticker_map))

    col_pie, col_table = st.columns([1, 1.2])
    with col_pie:
        sector_weights = latest.groupby('sector')['weight'].sum().reset_index()
        fig = px.pie(sector_weights, values='weight', names='sector', hole=0.6,
                     color_discrete_sequence=px.colors.qualitative.Bold)
        fig.update_traces(textposition='inside', textinfo='percent+label', showlegend=False)
        fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', margin=dict(t=10, b=10, l=10, r=10), height=PORTFOLIO_HEIGHT)
        st.plotly_chart(fig, use_container_width=True)
    with col_table:
        top20 = latest.nlargest(20, 'weight')
        st.dataframe(
            top20[['ticker', '종목명', 'sector', 'weight']]
            .rename(columns={'ticker': '티커', 'sector': '섹터', 'weight': '비중'})
            .style.format({'비중': '{:.2%}'}),
            height=PORTFOLIO_HEIGHT, hide_index=True, use_container_width=True,
        )


# ── Main ───────────────────────────────────────────────────────
def main():
    if not INDEX_FILE.exists():
        st.error("지수 데이터 파일이 없습니다. 파이프라인을 먼저 실행하세요.")
        return

    inject_css()
    ticker_map = load_ticker_map()

    # Load data
    df_index = load_csv(INDEX_FILE).sort_values('date')
    df_weights = load_csv(WEIGHTS_FILE)
    df_returns = load_csv(RETURNS_FILE)
    df_bench = load_csv(BENCHMARK_FILE)

    # Pre-calculate moving averages
    df_index = add_moving_averages(df_index, ['index_point_usd', 'index_point_krw'])
    if not df_bench.empty:
        df_bench = df_bench.sort_values('date')
        df_bench = add_moving_averages(df_bench, [bm['col'] for bm in BENCHMARKS])

    # Derive key values
    last, prev = df_index.iloc[-1], (df_index.iloc[-2] if len(df_index) > 1 else df_index.iloc[-1])
    chg_usd = calc_change_pct(last['index_point_usd'], prev['index_point_usd'])
    live_data = get_live_indices()

    # Render sections
    render_header(last, prev, live_data)
    render_chart(df_index, df_bench)
    if not df_returns.empty and not df_weights.empty:
        render_report_and_rankings(df_weights, df_returns, chg_usd, ticker_map)
    if not df_weights.empty:
        render_portfolio(df_weights, ticker_map)


if __name__ == "__main__":
    main()
