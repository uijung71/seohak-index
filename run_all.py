"""
run_all.py
Master Orchestrator for Seohak-100 Daily Index Pipeline
- Runs all agents in sequence
- Collects status and potential errors
- Generates a summary report
"""

import subprocess
import sys
from pathlib import Path
import datetime
import pandas as pd
from src.utils_telegram import send_telegram_message, send_telegram_photo
from src.utils_chart import generate_charts

# --- Telegram Config ---
TG_TOKEN = '8720582478:AAGakD7M2_-8uoGXYSTGK-fsZmJzpxBJZRU'
TG_CHAT_ID = '8356746472'

BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
SCRATCH_DIR = BASE_DIR / "scratch"
OUTPUT_DIR = BASE_DIR / "output"
PROC_DIR = BASE_DIR / "data" / "processed"

def run_script(script_path, name):
    print(f"\n>>> Running {name} ({script_path.name})...")
    try:
        result = subprocess.run([sys.executable, str(script_path)], 
                                capture_output=True, 
                                text=True, 
                                check=True, 
                                encoding='utf-8', 
                                errors='replace')
        print(f"    [OK] {name} completed.")
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        print(f"    [FAIL] {name} failed with exit code {e.returncode}.")
        stdout = e.stdout if e.stdout else ""
        stderr = e.stderr if e.stderr else ""
        return False, stdout + "\n" + stderr
    except Exception as e:
        print(f"    [ERROR] Unexpected error running {name}: {str(e)}")
        return False, str(e)

def generate_tg_report(results, start_time):
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    
    msg = []
    msg.append("📊 *서학개미 100 지수 산출 리포트*")
    msg.append(f"\n📅 실행 시간: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    msg.append(f"⏱ 소요 시간: {duration.total_seconds():.1f}초")
    
    msg.append("\n✅ *단계별 수행 결과*")
    for name, success, output in results:
        status = "성공" if success else "실패"
        icon = "🟢" if success else "🔴"
        msg.append(f"{icon} {name}: {status}")
    
    # 지수 정보 추가
    INDEX_FILE = OUTPUT_DIR / "seohak100_daily_index.csv"
    if INDEX_FILE.exists():
        try:
            df_idx = pd.read_csv(INDEX_FILE)
            last_row = df_idx.iloc[-1]
            msg.append("\n📈 *최신 지수 현황*")
            msg.append(f"- 기준 날짜: {last_row['date']}")
            msg.append(f"- USD 지수: {last_row['index_point_usd']:.2f} pt")
            msg.append(f"- KRW 지수: {last_row['index_point_krw']:.2f} pt")
            change = last_row['daily_return'] * 100
            msg.append(f"- 일간 수익률: {change:+.4f}%")
        except: pass
    
    # 건강성 요약 추가
    HEALTH_FILE = PROC_DIR / "custody_health_master.csv"
    if HEALTH_FILE.exists():
        try:
            df_h = pd.read_csv(HEALTH_FILE)
            summary = df_h['status'].value_counts()
            msg.append("\n🔍 *데이터 건강성 요약*")
            for status, count in summary.items():
                status_ko = {"ACTIVE": "정상 수집", "NEVER_COLLECTED": "미수집", "DEFUNCT_CANDIDATE": "이탈 후보"}.get(status, status)
                msg.append(f"- {status_ko}: {count}개 종목")
        except: pass

    return "\n".join(msg)

def generate_report(results, start_time):
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    
    report = []
    report.append("# Seohak-100 Daily Pipeline Report")
    report.append(f"\n- **Execution Time**: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"\n- **Duration**: {duration.total_seconds():.1f} seconds")
    
    report.append("\n## Step Execution Status")
    report.append("| Step | Status | Note |")
    report.append("| :--- | :--- | :--- |")
    
    for name, success, output in results:
        status = "SUCCESS" if success else "FAILED"
        lines = [l.strip() for l in output.split('\n') if l.strip()]
        note = lines[-1] if lines else "No output"
        report.append(f"| {name} | {status} | {note} |")
    
    INDEX_FILE = OUTPUT_DIR / "seohak100_daily_index.csv"
    if INDEX_FILE.exists():
        try:
            df_idx = pd.read_csv(INDEX_FILE)
            last_row = df_idx.iloc[-1]
            report.append("\n## Latest Index Points")
            report.append(f"- **Date**: {last_row['date']}")
            report.append(f"- **USD Index**: {last_row['index_point_usd']:.2f} pt")
            report.append(f"- **KRW Index**: {last_row['index_point_krw']:.2f} pt")
            report.append(f"- **Daily Return**: {last_row['daily_return']*100:+.4f}%")
        except: pass
    
    HEALTH_FILE = PROC_DIR / "custody_health_master.csv"
    if HEALTH_FILE.exists():
        try:
            df_h = pd.read_csv(HEALTH_FILE)
            summary = df_h['status'].value_counts()
            report.append("\n## Custody Health Summary")
            for status, count in summary.items():
                report.append(f"- **{status}**: {count} tickers")
        except: pass

    report_path = OUTPUT_DIR / f"pipeline_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report))
    
    print(f"\n[REPORT GENERATED] {report_path}")
    return report_path

def main():
    start_time = datetime.datetime.now()
    scripts = [
        (SRC_DIR / "agent6_pipeline.py", "SEIBRO Data Fetch"),
        (SRC_DIR / "agent7_live_prices.py", "EODHD Price Fetch"),
        (SCRATCH_DIR / "update_fx.py", "FX Rate Update"),
        (SRC_DIR / "agent10_custody_health.py", "Custody Health Check"),
        (SRC_DIR / "agent9_live_weights.py", "Daily Weights Calculation"),
        (SRC_DIR / "agent8_live_index.py", "Daily Index Calculation")
    ]
    
    results = []
    try:
        for path, name in scripts:
            success, output = run_script(path, name)
            results.append((name, success, output))
            if not success:
                print(f"\n[CRITICAL] Pipeline halted due to failure in {name}.")
                break
    finally:
        # Always generate report even if failed
        if results:
            report_path = generate_report(results, start_time)
            try:
                with open(report_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    print("\n" + "="*60)
                    print("PIPELINE SUMMARY")
                    print("="*60)
                    # Safe print
                    print(content.encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding))
                    
                    # Telegram Notification
                    tg_msg = generate_tg_report(results, start_time)
                    send_telegram_message(TG_TOKEN, TG_CHAT_ID, tg_msg)
                    
                    # Generate Charts and Send to Telegram (1M chart)
                    try:
                        print("\n>>> Generating Comparison Charts...")
                        chart_paths = generate_charts()
                        if "1M" in chart_paths:
                            print(f"    [OK] Sending 1M Chart to Telegram: {chart_paths['1M']}")
                            send_telegram_photo(TG_TOKEN, TG_CHAT_ID, str(chart_paths["1M"]), 
                                                caption="📈 *최근 1개월 지수 비교 차트*\n(Seohak-100 vs NDX, S&P500, KOSPI)")
                    except Exception as ce:
                        print(f"Chart generation or Telegram photo failed: {ce}")
            except Exception as e: 
                print(f"Telegram report generation failed: {e}")

if __name__ == "__main__":
    main()
