#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
케이트렌드 US — 닥터K
서학개미 주간 매매 분석 통합 파이프라인

실행: python weekly_analysis.py [YYYYMMDD_시작] [YYYYMMDD_종료]
예시: python weekly_analysis.py 20260501 20260507
날짜 생략 시 이번 주 자동 계산

데이터 소스:
  - KSD  : Drive_sync 폴더의 KSD_YYYYMMDD.xlsx (자동 탐지)
  - 토스 : Google Sheets (공개) 또는 Drive_sync 폴더의 toss_*.xlsx
  - 주가 : EODHD API (KSD+토스 합산 종목 주간 등락률)

결과 저장:
  - OUTPUT_DIR(케이트렌드 시황 동기화 폴더)에 저장
  - Google Drive 앱이 자동으로 클라우드 동기화
  - Claude Project에서 Drive MCP로 바로 읽어옴
"""

import requests
import pandas as pd
import sys
from datetime import datetime, timedelta
from pathlib import Path
import sys

# 프로젝트 루트를 path에 추가하여 src 패키지 인식
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from src.utils_telegram import send_telegram_message

# --- Telegram Config ---
TG_TOKEN = '8720582478:AAGakD7M2_-8uoGXYSTGK-fsZmJzpxBJZRU'
TG_CHAT_ID = '8356746472'

# ============================================================
# 설정
# ============================================================

EODHD_API_KEY = "693abf5882dab9.42616862"
EXCHANGE_RATE = 1500  # $1 = 1,500원

# Google Sheets 토스 데이터
TOSS_SHEET_ID = "1eEwTeJUZBzsk5Axpytn5hpaAzfXtvdpzCYBJQpGPbcg"
TOSS_GID      = "1824379062"  # row_toss_week 시트 GID

# KSD 파일이 들어오는 Drive 동기화 폴더
DRIVE_SYNC_DIR = Path(r"C:\Users\1\OneDrive\Documents\Drive Sync")

# 분석 결과 파일 저장 위치
OUTPUT_DIR = Path(r"C:\Users\1\OneDrive\Documents\Drive Sync\Analysis_Reports")


# ============================================================
# 날짜 유틸
# ============================================================

def get_week_dates(start_str=None, end_str=None):
    if start_str and end_str:
        s = datetime.strptime(start_str, "%Y%m%d")
        e = datetime.strptime(end_str,   "%Y%m%d")
    else:
        today = datetime.today()
        s = today - timedelta(days=today.weekday())
        e = s + timedelta(days=4)
    return s, e

def fmt(dt):       return dt.strftime("%Y%m%d")
def fmtd(dt):      return dt.strftime("%Y-%m-%d")
def week_label(s): return f"{s.year}년 {s.month}월 {(s.day-1)//7+1}주차"


# ============================================================
# 1. KSD — Drive_sync 폴더에서 최신 파일 자동 로드
# ============================================================

def load_ksd(start: datetime, end: datetime) -> pd.DataFrame:
    print(f"\n[KSD] Drive_sync 폴더에서 파일 탐색 중...")
    print(f"  경로: {DRIVE_SYNC_DIR}")

    if not DRIVE_SYNC_DIR.exists():
        print(f"  ❌ 폴더 없음: {DRIVE_SYNC_DIR}")
        print(f"  → 스크립트 상단 DRIVE_SYNC_DIR 경로를 수정해주세요.")
        return pd.DataFrame()

    # 파일명 패턴 우선순위: 날짜 정확 일치 → 최신 수정 파일
    patterns = [
        f"US_Weekly_Settlement_*{fmt(end)}.xlsx",
        f"US_Weekly_Settlement_*{fmt(start)}.xlsx",
        "US_Weekly_Settlement_*.xlsx",
        "KSD_*.xlsx",
    ]
    found = None
    for pat in patterns:
        candidates = list(DRIVE_SYNC_DIR.glob(pat))
        if candidates:
            found = max(candidates, key=lambda p: p.stat().st_mtime)
            break

    if not found:
        print(f"  ❌ KSD 파일을 찾지 못했습니다. Drive_sync 내 xlsx 파일:")
        for f in sorted(DRIVE_SYNC_DIR.glob("*.xlsx"))[:10]:
            print(f"     {f.name}")
        return pd.DataFrame()

    print(f"  📂 사용 파일: {found.name}")

    try:
        xl  = pd.ExcelFile(found)
        df  = None
        for sheet in xl.sheet_names:
            tmp = pd.read_excel(found, sheet_name=sheet)
            if len(tmp) > 5:
                df = tmp
                print(f"  시트: {sheet} ({len(df)}행)")
                break
        if df is None:
            df = pd.read_excel(found)
    except Exception as e:
        print(f"  ❌ 읽기 실패: {e}")
        return pd.DataFrame()

    # 컬럼 표준화
    col_map = {}
    for c in df.columns:
        cl = str(c).lower()
        if any(x in cl for x in ["rank","순위"]):            col_map[c] = "순위"
        elif any(x in cl for x in ["isin","종목코드"]):      col_map[c] = "종목코드"
        elif any(x in cl for x in ["name","nm","종목명","stock name"]):   col_map[c] = "종목명"
        elif "net" in cl and "buy" in cl:                    col_map[c] = "순매수_달러"
        elif any(x in cl for x in ["buy","매수"]):           col_map[c] = "매수결제"
        elif any(x in cl for x in ["sell","매도"]):          col_map[c] = "매도결제"
        elif any(x in cl for x in ["total","합산"]):         col_map[c] = "합산결제"
        elif any(x in cl for x in ["country","nation","국가"]): col_map[c] = "국가"
    df = df.rename(columns=col_map)
    print(f"  [DEBUG] Columns after rename: {df.columns.tolist()}")

    for col in ["매수결제","매도결제","합산결제"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",","").str.strip(),
                errors="coerce"
            ).fillna(0)

    if "매수결제" in df.columns and "매도결제" in df.columns:
        df["순매수_달러"] = df["매수결제"] - df["매도결제"]
        df["매수_억원"]   = (df["매수결제"]   * EXCHANGE_RATE / 1e8).round(0)
        df["매도_억원"]   = (df["매도결제"]   * EXCHANGE_RATE / 1e8).round(0)
        df["합산_억원"]   = ((df["매수결제"] + df["매도결제"]) * EXCHANGE_RATE / 1e8).round(0)
        df["순매수_억원"] = (df["순매수_달러"] * EXCHANGE_RATE / 1e8).round(0)

    print(f"  ✅ {len(df)}개 종목 로드 완료")
    return df


# ============================================================
# 2. 토스 — Google Sheets 또는 Drive_sync 파일
# ============================================================

def load_toss() -> pd.DataFrame:
    print(f"\n[Toss] Google Sheets 조회 중...")
    url = (
        f"https://docs.google.com/spreadsheets/d/{TOSS_SHEET_ID}"
        f"/export?format=csv&gid={TOSS_GID}"
    )
    try:
        df = pd.read_csv(url)
        if "window" in df.columns:
            df = df[df["window"] == "1W"].copy()
        print(f"  ✅ {len(df)}행 수집 (주간)")
        return df
    except Exception as e:
        print(f"  -> Drive_sync 폴더에서 토스 파일 탐색...")

    print(f"  → Drive_sync 폴더에서 토스 파일 탐색...")
    for pat in ["*toss*.xlsx","*Toss*.xlsx","*토스*.xlsx"]:
        candidates = list(DRIVE_SYNC_DIR.glob(pat))
        if candidates:
            latest = max(candidates, key=lambda p: p.stat().st_mtime)
            print(f"  📂 {latest.name}")
            df = pd.read_excel(latest)
            if "window" in df.columns:
                df = df[df["window"] == "1W"].copy()
            print(f"  ✅ {len(df)}행 로드")
            return df

    print(f"  ❌ 토스 데이터를 찾지 못했습니다.")
    return pd.DataFrame()


# ============================================================
# 3. EODHD — KSD+토스 합산 종목 주간 등락률 일괄 조회
# ============================================================

def load_prices(tickers: list, start: datetime, end: datetime) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame()

    print(f"\n[EODHD] {len(tickers)}개 종목 주간 등락률 조회 중...")
    s_str = fmtd(start - timedelta(days=7))
    e_str = fmtd(end)
    results = []

    for i, ticker in enumerate(tickers, 1):
        if i % 20 == 0:
            print(f"  진행: {i}/{len(tickers)}")
        try:
            r = requests.get(
                f"https://eodhd.com/api/eod/{ticker}.US"
                f"?api_token={EODHD_API_KEY}&fmt=json&period=d&from={s_str}&to={e_str}",
                timeout=8
            )
            if r.status_code != 200:
                continue
            data = r.json()
            if not isinstance(data, list) or len(data) < 2:
                continue

            week = [d for d in data if fmtd(start) <= d["date"] <= e_str]
            if not week:
                continue

            # 수정 종가(adjusted_close)를 기준으로 등락률 계산
            prev  = [d for d in data if d["date"] < fmtd(start)]
            # 이전 종가 (수정 종가 사용)
            prev_close = prev[-1]["adjusted_close"] if prev else week[0]["adjusted_close"]
            # 이번 주 종가 (수정 종가 사용)
            week_close = week[-1]["adjusted_close"]
            weekly_ret = (week_close - prev_close) / prev_close * 100 if prev_close else 0

            # 시가/고가/저가도 수정 비율(factor)을 적용하여 현실화
            # factor = adjusted_close / close
            
            results.append({
                "ticker":        ticker,
                "전주종가":      round(prev_close, 2),
                "주간시가":      round(week[0]["open"] * (week[0]["adjusted_close"]/week[0]["close"]), 2) if week[0]["close"] else week[0]["open"],
                "주간종가":      round(week_close, 2),
                "주간고가":      round(max(d["high"] * (d["adjusted_close"]/d["close"]) for d in week), 2),
                "주간저가":      round(min(d["low"] * (d["adjusted_close"]/d["close"]) for d in week), 2),
                "주간등락률(%)": round(weekly_ret, 2),
                "주간거래량":    sum(d.get("volume", 0) for d in week),
            })
        except Exception:
            continue

    df = pd.DataFrame(results)
    print(f"  ✅ {len(df)}개 종목 가격 데이터 수집")
    return df


# ============================================================
# 4. 분석 — 에코시스템 탐지 + 주가 교차 분석
# ============================================================

def analyze(ksd: pd.DataFrame, toss: pd.DataFrame, prices: pd.DataFrame) -> dict:
    result = {}

    # ── KSD
    if not ksd.empty and "순매수_억원" in ksd.columns:
        s = ksd.sort_values("순매수_억원", ascending=False)
        result["ksd_top_buy"]           = s.head(10)[["종목명","순매수_억원","합산_억원"]].to_dict("records")
        result["ksd_top_sell"]          = s.tail(10)[["종목명","순매수_억원","합산_억원"]].to_dict("records")
        result["ksd_total_합산_억원"]   = round(ksd["합산_억원"].sum(), 0)
        result["ksd_total_순매수_억원"] = round(ksd["순매수_억원"].sum(), 0)

    # ── 토스
    if not toss.empty:
        amount_col = next((c for c in toss.columns if "amount" in c.lower()), None)
        ticker_col = next((c for c in toss.columns if c.lower() == "ticker"), None)
        name_col   = next((c for c in toss.columns if "name_ko" in c.lower()), None)
        if amount_col:
            toss = toss.copy()
            toss["거래대금_억원"] = pd.to_numeric(toss[amount_col], errors="coerce").fillna(0) / 1e8
            result["toss_top20"]      = toss.sort_values("거래대금_억원", ascending=False).head(20)[
                [c for c in [ticker_col, name_col, "거래대금_억원"] if c]
            ].to_dict("records")
            result["toss_total_억원"] = round(toss["거래대금_억원"].sum(), 0)
            if ticker_col and name_col:
                result["ecosystems"] = detect_ecosystems(toss, ticker_col, name_col, amount_col)

    # ── 주가 교차 분석
    if not prices.empty and not ksd.empty and "순매수_억원" in ksd.columns:
        ksd_tc = next((c for c in ksd.columns if c.lower() in ["ticker","종목코드"]), None)
        toss_tc = next((c for c in toss.columns if c.lower() == "ticker"), None) if not toss.empty else None

        # 종목코드(ISIN) → ticker 매핑: 토스 데이터의 isin 컬럼 활용
        isin_col_toss = next((c for c in toss.columns if "isin" in c.lower()), None) if not toss.empty else None
        if toss_tc and isin_col_toss and ksd_tc and "종목코드" in ksd.columns:
            isin_map = dict(zip(toss[isin_col_toss], toss[toss_tc]))
            ksd["ticker"] = ksd["종목코드"].map(isin_map)
            merged = pd.merge(
                ksd[["종목명","ticker","순매수_억원"]].dropna(subset=["ticker"]),
                prices, on="ticker", how="inner"
            )
        else:
            merged = pd.DataFrame()

        if not merged.empty:
            merged["매매신호"] = merged.apply(lambda r:
                "✅ 추격매수성공"  if r["순매수_억원"] > 0  and r["주간등락률(%)"] > 0  else
                "⚠️ 역발상매수"   if r["순매수_억원"] > 0  and r["주간등락률(%)"] < -2 else
                "✅ 차익실현성공"  if r["순매수_억원"] < 0  and r["주간등락률(%)"] > 2  else
                "⚠️ 조기청산실패" if r["순매수_억원"] < 0  and r["주간등락률(%)"] < 0  else
                "→ 중립", axis=1
            )
            result["price_cross"] = merged.sort_values("주간등락률(%)", ascending=False).to_dict("records")

        # 서학개미 미편입 강세종목
        traded = set()
        if ksd_tc and not ksd.empty:
            traded.update(ksd[ksd_tc].dropna().tolist())
        if toss_tc and not toss.empty:
            traded.update(toss[toss_tc].dropna().tolist())
        missed = prices[(prices["주간등락률(%)"] > 5) & (~prices["ticker"].isin(traded))]
        result["missed_강세종목"] = missed.sort_values("주간등락률(%)", ascending=False).to_dict("records")

    return result


def detect_ecosystems(toss, ticker_col, name_col, amount_col):
    """동일 기초자산 현물+롱ETF+인버스ETF 동시 등장 탐지 (동적 발굴)"""
    ecosystems = []
    etf_col = next((c for c in toss.columns if "is_etf" in c.lower()), None)
    lev_col = next((c for c in toss.columns if "leverage_ratio" in c.lower()), None)
    if not etf_col:
        return ecosystems

    toss = toss.copy()
    toss["_amt"] = pd.to_numeric(toss[amount_col], errors="coerce").fillna(0)
    is_etf = toss[etf_col].astype(str).str.upper().isin(["TRUE","1","YES"])
    etfs   = toss[is_etf]
    stocks = toss[~is_etf]

    seen = set()
    for _, stock in stocks.iterrows():
        base = str(stock[ticker_col]).upper()
        if base in seen:
            continue
        related = etfs[etfs[name_col].astype(str).str.upper().str.contains(base, na=False)]
        if len(related) < 2:
            continue
        seen.add(base)

        members = [{"ticker": stock[ticker_col], "name_ko": stock.get(name_col,""),
                    "거래대금_억원": round(stock["_amt"]/1e8,0), "유형":"현물"}]
        for _, etf in related.iterrows():
            lv = str(etf.get(lev_col,"")).replace(".0","") if lev_col else ""
            if lv in ["2","3"]:
                유형 = f"롱{lv}xETF"
            elif any(x in str(etf[name_col]).upper() for x in ["SHORT","BEAR","INVERSE","인버스","베어","숏"]):
                유형 = "인버스ETF"
            else:
                유형 = "ETF"
            members.append({"ticker": etf[ticker_col], "name_ko": etf.get(name_col,""),
                            "거래대금_억원": round(etf["_amt"]/1e8,0), "유형": 유형})

        total       = sum(m["거래대금_억원"] for m in members)
        has_long    = any("롱" in m["유형"] for m in members)
        has_inverse = any("인버스" in m["유형"] for m in members)
        signal      = "⚠️ 양방향 동시 베팅" if (has_long and has_inverse) else "→ 단방향 집중"
        ecosystems.append({"기초자산": base, "구성": members,
                           "합산_억원": round(total,0), "신호": signal})

    return sorted(ecosystems, key=lambda x: x["합산_억원"], reverse=True)


# ============================================================
# 5. Excel 저장 (5개 시트)
# ============================================================

def save_excel(ksd, toss, prices, analysis, start, end) -> Path:
    filename = OUTPUT_DIR / f"서학개미분석_{fmt(start)}_{fmt(end)}.xlsx"
    print(f"\n[저장] {filename}")

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        wb = writer.book

        # 시트1: 📊 요약
        ws = wb.create_sheet("📊요약")
        rows = [
            ["항목","값"],
            ["분석기간",           f"{fmtd(start)} ~ {fmtd(end)}"],
            ["주차",               week_label(start)],
            ["환율",               f"$1 = {EXCHANGE_RATE:,}원"],
            [""],
            ["── KSD ──",""],
            ["합산결제 총합(억원)", analysis.get("ksd_total_합산_억원",  "-")],
            ["순매수 합계(억원)",   analysis.get("ksd_total_순매수_억원","-")],
            [""],
            ["── 토스 ──",""],
            ["주간 거래대금(억원)", analysis.get("toss_total_억원","-")],
            [""],
            ["── 에코시스템 탐지 ──",""],
        ]
        for eco in analysis.get("ecosystems",[]):
            tks = " + ".join(f"{m['ticker']}({m['유형']})" for m in eco["구성"])
            rows.append([f"  {eco['기초자산']} {eco['신호']}", f"{tks} = {eco['합산_억원']}억원"])
        if analysis.get("missed_강세종목"):
            rows += [[""],["── 미편입 강세종목(+5%↑) ──",""]]
            for m in analysis["missed_강세종목"][:5]:
                rows.append([f"  {m['ticker']}", f"+{m['주간등락률(%)']}%  종가 ${m['주간종가']}"])
        for row in rows:
            ws.append(row)
        ws.column_dimensions["A"].width = 38
        ws.column_dimensions["B"].width = 55

        # 시트2: KSD 순매수 정렬
        if not ksd.empty and "순매수_억원" in ksd.columns:
            cols = [c for c in ["순위","종목명","종목코드","매수_억원","매도_억원","합산_억원","순매수_억원"] if c in ksd.columns]
            ksd.sort_values("순매수_억원", ascending=False)[cols].to_excel(
                writer, sheet_name="KSD_순매수정렬", index=False)

        # 시트3: 토스 거래대금
        if not toss.empty:
            toss[[c for c in toss.columns if c != "_amt"]].to_excel(
                writer, sheet_name="토스_거래대금", index=False)

        # 시트4: 주가 주간 등락률
        if not prices.empty:
            prices.sort_values("주간등락률(%)", ascending=False).to_excel(
                writer, sheet_name="주가_주간등락률", index=False)

        # 시트5: 매매 vs 주가 교차 분석
        if analysis.get("price_cross"):
            pd.DataFrame(analysis["price_cross"]).to_excel(
                writer, sheet_name="매매vs주가_교차분석", index=False)

    print(f"  ✅ {filename.resolve()}")
    
    # Telegram Notification
    try:
        tg_msg = [
            "📑 *주간 매매 분석 리포트 생성 완료*",
            f"\n📅 기간: {fmtd(start)} ~ {fmtd(end)}",
            f"📈 KSD 순매수합계: {analysis.get('ksd_total_순매수_억원','-')}억원",
            f"💰 토스 거래대금: {analysis.get('toss_total_억원','-')}억원",
            "\n🔝 *KSD 순매수 TOP3*"
        ]
        for r in analysis.get("ksd_top_buy",[])[:3]:
            tg_msg.append(f"- {r.get('종목명','?')}: +{r.get('순매수_억원',0):.0f}억")
        
        if analysis.get("ecosystems"):
            tg_msg.append("\n🔍 *주요 에코시스템*")
            for eco in analysis["ecosystems"][:2]:
                tg_msg.append(f"- [{eco['기초자산']}] {eco['신호']} ({eco['합산_억원']}억)")
        
        tg_msg.append(f"\n📁 저장: {filename.name}")
        send_telegram_message(TG_TOKEN, TG_CHAT_ID, "\n".join(tg_msg))
    except Exception as e:
        print(f"Telegram notification failed: {e}")

    return filename


# ============================================================
# 6. 터미널 요약
# ============================================================

def print_summary(analysis, start, end):
    sep = "=" * 62
    print(f"\n{sep}")
    print(f"  📊 {week_label(start)}  |  {fmtd(start)} ~ {fmtd(end)}")
    print(sep)
    print(f"  KSD 합산결제    {analysis.get('ksd_total_합산_억원',  '-'):>8}억원")
    print(f"  KSD 순매수합계  {analysis.get('ksd_total_순매수_억원','-'):>8}억원")
    print(f"  토스 거래대금   {analysis.get('toss_total_억원',       '-'):>8}억원")

    print("\n  ── KSD 순매수 TOP5 ──")
    for r in analysis.get("ksd_top_buy",[])[:5]:
        print(f"  {r.get('종목명','?'):<32} +{r.get('순매수_억원',0):>7.0f}억")

    print("\n  ── KSD 순매도 TOP5 ──")
    for r in list(reversed(analysis.get("ksd_top_sell",[])))[:5]:
        print(f"  {r.get('종목명','?'):<32}  {r.get('순매수_억원',0):>8.0f}억")

    if analysis.get("ecosystems"):
        print("\n  ── 에코시스템 탐지 ──")
        for eco in analysis["ecosystems"][:5]:
            print(f"  [{eco['기초자산']}] {eco['신호']}  합산 {eco['합산_억원']}억원")
            for m in eco["구성"]:
                print(f"    {m['ticker']:<8} {m['유형']:<12} {m['거래대금_억원']:>6.0f}억")

    if analysis.get("missed_강세종목"):
        print("\n  ── 미편입 강세종목 (+5%↑) ──")
        for m in analysis["missed_강세종목"][:5]:
            print(f"  {m['ticker']:<10} +{m['주간등락률(%)']}%  종가 ${m['주간종가']}")

    print(f"\n{sep}")
    print("  생성 파일을 Claude Project에 업로드 → '주간분석' 입력")
    print(f"{sep}\n")


# ============================================================
# 7. 메인
# ============================================================

def main():
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    args = sys.argv[1:]
    start, end = get_week_dates(
        args[0] if len(args) > 0 else None,
        args[1] if len(args) > 1 else None,
    )
    print(f"\n[START] Weekly Analysis - {week_label(start)}")
    print(f"   {fmtd(start)} ~ {fmtd(end)}")

    # OUTPUT_DIR 없으면 생성
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    ksd    = load_ksd(start, end)
    toss   = load_toss()

    # 티커 수집 (짧은 심볼만)
    tickers = set()
    for df, col_hint in [(ksd,"종목코드"),(toss,"ticker")]:
        if df.empty: continue
        tc = next((c for c in df.columns if col_hint in c.lower()), None)
        if tc:
            tickers.update(str(v) for v in df[tc].dropna() if 1 <= len(str(v)) <= 6)

    prices   = load_prices(list(tickers), start, end)
    analysis = analyze(ksd, toss, prices)
    print_summary(analysis, start, end)
    output_file = save_excel(ksd, toss, prices, analysis, start, end)

    print(f"\n  ✅ 완료!")
    print(f"  📁 저장 위치: {output_file.resolve()}")
    print(f"  ☁️  Google Drive 앱이 자동으로 동기화합니다.")
    print(f"  → Claude Project에서 '주간분석' 입력하면 바로 분석 시작됩니다.\n")


if __name__ == "__main__":
    main()
