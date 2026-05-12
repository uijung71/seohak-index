"""
src/agent6_pipeline.py
Agent 6: Integrated Pipeline (Universe Management + Data Collection)
1. 세이브로 WebSquare Top 50 리스트(보관/매매)를 가져와서 신규 종목 발굴
2. 발견된 신규 종목을 ticker_universe.csv에 자동 추가
3. 전체 유니버스(약 400+ 종목)에 대해 공식 OpenAPI로 실제 데이터 수집
"""

import os
import time
import datetime
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROC_DIR = BASE_DIR / "data" / "processed"
SNAP_DIR = RAW_DIR / "snapshots"
UNIVERSE_DATA = PROC_DIR / "ticker_universe.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)
SNAP_DIR.mkdir(parents=True, exist_ok=True)

# API Configs
WS_POST = 'https://seibro.or.kr/websquare/engine/proworks/callServletService.jsp'
OPEN_URL = "http://seibro.or.kr/OpenPlatform/callOpenAPI.jsp"
OPEN_KEY = "3d8e7ac5c28da5cc517d4f45c74efd2da5ff3ed875b4e00102bc027448469d9f"
EODHD_KEY = "693abf5882dab9.42616862"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Content-Type': 'application/xml; charset=UTF-8'
}

# ══════════════════════════════════════════════════════
# Helper Functions
# ══════════════════════════════════════════════════════

def call_websquare_top50(is_custody, target_date):
    """WebSquare API로 Top 50 리스트 호출"""
    action = "getImptFrcurStkCusRemaList" if is_custody else "getImptFrcurStkSetlAmtList"
    s_type = "1" if is_custody else "2"
    
    # 매매현황은 최근 1주일 합계(D_TYPE=3)로 조회하는 것이 지수 규칙에 유리
    d_type_tag = '<D_TYPE value="3"/>' if not is_custody else ''
    start_dt = (datetime.datetime.strptime(target_date, "%Y%m%d") - datetime.timedelta(days=6)).strftime("%Y%m%d") if not is_custody else target_date

    payload = f"""<reqParam action="{action}" task="ksd.safe.bip.cnts.OvsSec.process.OvsSecIsinPTask">
  <MENU_NO value="921"/>
  <PG_START value="1"/>
  <PG_END value="50"/>
  <START_DT value="{start_dt}"/>
  <END_DT value="{target_date}"/>
  <S_TYPE value="{s_type}"/>
  <S_COUNTRY value="US"/>
  {d_type_tag}
</reqParam>"""
    
    try:
        session = requests.Session()
        # Initial hit for session
        session.get('https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/ovsSec/BIP_CNTS10013V.xml&menuNo=921', headers={'User-Agent': HEADERS['User-Agent']})
        
        # Add referer
        headers = HEADERS.copy()
        headers['Referer'] = 'https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/ovsSec/BIP_CNTS10013V.xml&menuNo=921'
        
        resp = session.post(WS_POST, headers=headers, data=payload.encode('utf-8'), timeout=20)
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        print(f"  [Error] WebSquare Call Failed: {e}")
    return None

def parse_ws_isins(xml_text):
    """WebSquare XML에서 ISIN 및 한글명 추출"""
    isins = []
    try:
        root = ET.fromstring(xml_text)
        for res in root.findall(".//result"):
            isin_node = res.find("ISIN")
            name_node = res.find("KOR_SECN_NM")
            if isin_node is not None:
                isins.append({
                    'isin': isin_node.attrib.get('value'),
                    'name_ko': name_node.attrib.get('value') if name_node is not None else ''
                })
    except Exception:
        pass
    return isins

def get_ticker_from_isin_eodhd(isin):
    """EODHD Search API를 통해 ISIN으로부터 Ticker 검색"""
    url = f"https://eodhd.com/api/search/{isin}"
    params = {"api_token": EODHD_KEY, "fmt": "json"}
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            # 미국 거래소(US) 데이터 중 첫 번째 것 반환
            for item in data:
                if item.get('Exchange') in ['US', 'NASDAQ', 'NYSE', 'ARCA', 'BATS']:
                    return item.get('Code'), item.get('Name')
    except Exception:
        pass
    return None, None

# ══════════════════════════════════════════════════════
# Main Pipeline
# ══════════════════════════════════════════════════════

def run_pipeline():
    print("="*60)
    print("Agent 6: Integrated Universe & Data Pipeline")
    print("="*60)

    # 1. 유니버스 로드
    if UNIVERSE_DATA.exists():
        univ_df = pd.read_csv(UNIVERSE_DATA)
    else:
        univ_df = pd.DataFrame(columns=['isin', 'ticker', 'name_en', 'name_ko'])
    
    existing_isins = set(univ_df['isin'].dropna().unique())
    print(f"  Existing Universe: {len(existing_isins)} tickers")

    # 2. 날짜 스캔 (공통 사용)
    today = datetime.date.today()
    target_dt = None
    print("[1] 최신 가용 데이터 날짜 스캔 (2일~10일 전)...")
    for i in range(2, 11):
        test_dt = (today - datetime.timedelta(days=i)).strftime("%Y%m%d")
        # 간단한 OpenAPI 호출로 확인
        url = f"{OPEN_URL}?key={OPEN_KEY}&apiId=getSecnFrsecCusInfo&params=STD_DT:{test_dt},ISIN:US0378331005"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code == 200 and "<result>" in resp.text:
                target_dt = test_dt
                print(f"  [SUCCESS] Target Date: {target_dt}")
                break
        except:
            continue
    
    if not target_dt:
        print("[!] 가용 데이터를 찾지 못했습니다.")
        return

    # 3. 신규 종목 발굴 (Top 50 List)
    print("[2] 실시간 Top 50 리스트 분석 (보관/매매)...")
    new_found_count = 0
    for is_custody in [True, False]:
        xml = call_websquare_top50(is_custody, target_dt)
        if xml:
            found_items = parse_ws_isins(xml)
            for item in found_items:
                isin = item['isin']
                if isin not in existing_isins:
                    print(f"    [New Ticker Found] ISIN: {isin}, Name: {item['name_ko']}")
                    # Ticker 검색
                    ticker, name_en = get_ticker_from_isin_eodhd(isin)
                    if ticker:
                        new_row = {
                            'isin': isin,
                            'ticker': ticker,
                            'name_en': name_en,
                            'name_ko': item['name_ko']
                        }
                        univ_df = pd.concat([univ_df, pd.DataFrame([new_row])], ignore_index=True)
                        existing_isins.add(isin)
                        new_found_count += 1
                        print(f"      -> Mapped to Ticker: {ticker} ({name_en})")
                    else:
                        print(f"      [!] Failed to map Ticker for {isin}")
    
    if new_found_count > 0:
        univ_df.to_csv(UNIVERSE_DATA, index=False, encoding="utf-8-sig")
        print(f"  [UPDATE] Universe updated with {new_found_count} new tickers.")
    else:
        print("  No new tickers found in Top 50 lists.")

    # 4. 전체 유니버스 데이터 수집 (OpenAPI) - 최근 5거래일 합산
    print(f"[3] {target_dt} 기준 최근 5거래일 전체 유니버스({len(univ_df)}) 데이터 집계 시작...")
    
    # 5거래일 날짜 리스트 생성
    end_dt_obj = datetime.datetime.strptime(target_dt, "%Y%m%d")
    date_range = [(end_dt_obj - datetime.timedelta(days=i)).strftime("%Y%m%d") for i in range(7)] # 여유있게 7일치 스캔
    
    isin_list = univ_df['isin'].tolist()
    custody_results = []
    trading_results = []
    
    def fetch_isin_data(i, isin, row):
        # 1. 보관금액 (가장 최신일 기준 하나만)
        c_url = f"{OPEN_URL}?key={OPEN_KEY}&apiId=getSecnFrsecCusInfo&params=STD_DT:{target_dt},ISIN:{isin}"
        amt = 0.0
        try:
            c_resp = requests.get(c_url, headers=HEADERS, timeout=10)
            if c_resp.status_code == 200:
                root = ET.fromstring(c_resp.text)
                for res in root.findall(".//result"):
                    if res.find("NATION_CD").attrib.get("value") == "US":
                        amt = float(res.find("FRSEC_TOT_HOLD_AMT").attrib.get("value", "0"))
                        break
        except: pass
        
        # 2. 매매현황 (최근 5거래일 합산)
        total_buy, total_sell = 0.0, 0.0
        valid_days = 0
        for d in date_range:
            if valid_days >= 5: break # 5거래일 채우면 중단
            t_url = f"{OPEN_URL}?key={OPEN_KEY}&apiId=getSecnFrsecSetlInfo&params=PROC_DT:{d},ISIN:{isin}"
            try:
                t_resp = requests.get(t_url, headers=HEADERS, timeout=10)
                if t_resp.status_code == 200 and "<result>" in t_resp.text:
                    root = ET.fromstring(t_resp.text)
                    day_buy, day_sell = 0.0, 0.0
                    for res in root.findall(".//result"):
                        if res.find("NATION_CD").attrib.get("value") == "US":
                            val = float(res.find("SETL_AMT").attrib.get("value", "0"))
                            biz = res.find("INTL_BIZ_CACD").attrib.get("value")
                            if biz == "1110": day_buy += val
                            elif biz == "1120": day_sell += val
                    
                    if day_buy > 0 or day_sell > 0:
                        total_buy += day_buy
                        total_sell += day_sell
                        valid_days += 1
            except: pass
        
        return i, {
            'isin': isin, 'ticker': row['ticker'], 'name_en': row['name_en'], 'amount': amt, 'date': target_dt
        }, {
            'isin': isin, 'ticker': row['ticker'], 'name_en': row['name_en'], 'buy': total_buy, 'sell': total_sell, 'net_buy': total_buy - total_sell, 'date': target_dt
        }

    custody_results_map = {}
    trading_results_map = {}
    
    print(f"  Starting Multi-threaded Fetch (Threads: 10)...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_isin_data, i, row['isin'], row): i for i, row in univ_df.iterrows()}
        
        completed = 0
        for future in as_completed(futures):
            i, c_res, t_res = future.result()
            custody_results_map[i] = c_res
            trading_results_map[i] = t_res
            completed += 1
            if completed % 20 == 0 or completed == len(isin_list):
                print(f"  Progress: {completed}/{len(isin_list)} (Aggregated 5 days)...", flush=True)

    # Sort results back to original order
    custody_results = [custody_results_map[i] for i in range(len(isin_list))]
    trading_results = [trading_results_map[i] for i in range(len(isin_list))]

    # 5. 저장
    fmt_dt = f"{target_dt[:4]}-{target_dt[4:6]}-{target_dt[6:8]}"
    
    cust_df = pd.DataFrame(custody_results).assign(date=fmt_dt)
    trad_df = pd.DataFrame(trading_results).assign(date=fmt_dt)
    
    # 최신 데이터 업데이트 (Master)
    cust_df.to_csv(RAW_DIR / "custody_daily.csv", index=False, encoding="utf-8-sig")
    trad_df.to_csv(RAW_DIR / "trading_daily.csv", index=False, encoding="utf-8-sig")
    
    # 일간 스냅샷 저장 (History)
    snap_dt = target_dt # YYYYMMDD
    cust_df.to_csv(SNAP_DIR / f"custody_{snap_dt}.csv", index=False, encoding="utf-8-sig")
    trad_df.to_csv(SNAP_DIR / f"trading_{snap_dt}.csv", index=False, encoding="utf-8-sig")
    
    print(f"\n[DONE] 파이프라인 완료. 기준일: {fmt_dt} (스냅샷 저장 완료)")

if __name__ == "__main__":
    run_pipeline()
