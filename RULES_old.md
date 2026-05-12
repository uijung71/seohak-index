# 서학개미 100 지수 산출 규칙 (RULES.md) v3.0
# 이 파일은 모든 에이전트가 반드시 가장 먼저 읽어야 한다
# 최종 업데이트: 2026-04

---

# 0. 지수 정의 (Index Definition)

서학개미 100 지수(Seohak-100 Index)는
대한민국 개인 투자자의 미국 주식 보유 및 매매 데이터를 기반으로 구성된
수급 가중 주가 지수 (Flow + Stock Weighted Index) 이다.

지수는 다음 두 단계로 구분된다.

---

## Phase 1: Estimated Index (과거 추정 지수)

- 기간: 2020-01-06 ~ 2025-12-31
- 특징:
  - Top 50 종목: 실제 보관금액 데이터 사용
  - 51~100 종목: 대칭 배분법 기반 모델 추정
  - 매매금액: 월간 데이터를 주간으로 균등 배분한 Proxy 값

> 본 구간은 제한된 데이터 환경에서 구성된 시뮬레이션 지수이며
> 실제 과거 수급을 완전히 재현하지 않는다.
> 모든 추정 구간은 명시적으로 공개된다.

---

## Phase 2: Live Index (실제 지수)

- 기간: 2026-01-01 이후
- 특징:
  - 보관금액 + 매매금액 모두 실제 데이터 사용
  - 100% 실데이터 기반 지수
  - Phase 1과 체인 방식으로 자동 연결

---

# 1. 기준값 (Base Setting)

- 기준일: 2020-01-06
- 기준지수: 1,000 pt
- 산출 주기: 주간 (Weekly)
- 기준 요일: 월요일
  - 세이브로 데이터는 결제일 기준 D+1 반영
  - 실질 반영 시점: 직전 주 금요일 기준
  - NDX 등 외부 지수와 비교 시 반드시 동일 기준일로 맞출 것

---

# 2. 데이터 정의 (Data Specification)

## 2.1 보관 데이터 (Custody)

- 출처: 세이브로
- 파일명: data/raw/custody_weekly.csv
- 단위: 주간 (매주 월요일)
- 기준: 보관금액 상위 50 종목

컬럼:
- date: 기준일 (YYYY-MM-DD)
- isin: 종목 고유코드
- ticker: 야후 파이낸스 티커
- name_en: 종목명
- amount: 보관금액 (USD)
- price_stock: 해당 주 종목 가격 (Observed Price)

### 실제 데이터 결함 처리 — 주간 Core 종목 수 부족

실제 데이터에서 특정 주의 custody 종목 수가 50개 미만인 경우가 존재한다.
(확인된 사례: 48개 또는 49개로 수집된 주 존재)

처리 원칙:
- 수집된 종목 수가 50개 미만이어도 해당 주 지수 산출을 중단하지 않는다
- 가용한 실제 종목 수(예: 48개, 49개)를 Core로 그대로 사용한다
- 부족분(예: 2개, 1개)을 억지로 채우지 않는다
- 단, 종목 수가 40개 미만으로 떨어지는 주가 발생하면
  작업을 멈추고 사람에게 보고한다

기록 원칙:
- 50개 미만인 모든 주는 data_issues.csv에 기록
  issue_type: CORE_COUNT_SHORT
  detail: "실제 Core 종목 수 N개 (기준 50개 미달)"
- 해당 주의 Satellite 비중 계산 시
  기울기(r) 산출 기준을 실제 최하위 종목 기준으로 자동 조정
  예: Core가 48개인 경우 r = (S_48 / S_6)^(1/42)

---

## 2.2 매매 데이터 (Trading)

- 출처: 세이브로
- 파일명: data/raw/trading_monthly.csv
- 단위: 월간

컬럼:
- date: 기준일 (YYYY. M. 1 형식 → 파싱 시 해당 월 첫째날로 처리)
- isin: 종목 고유코드
- ticker: 야후 파이낸스 티커
- name_en: 종목명
- buy: 매수금액 (USD)
- sell: 매도금액 (USD)
- sum: 매매금액 합계 (USD)
- price: 해당 월 종목 가격 (Observed Price)

### 주간 변환 방식

  weekly_trading = monthly_sum / 해당 월의 월요일 수

> 본 값은 실제 주간 수급이 아닌 균등 배분된 Proxy 값이다.
> 이를 수급 가중치(N_i) 산출에 사용한다.

### 월간 가격 누락 처리 지침

실제 데이터 현황:
- trading_monthly.csv 전체 행 수: 약 3,750개
- 가격(price) 누락 행: 약 230개 (전체의 약 6.1%)
- 누락 종목 특성: 보관금액 하위권 비중이 낮은 종목에 집중

처리 원칙:
- price 컬럼이 비어있는 행은 Observed Price 용도(정합성 검증)에서만 제외
- 해당 종목의 sum(매매금액) 데이터는 유효하므로 N_i 계산에는 그대로 사용
- 단, Return Price(수익률 계산용)는 yfinance에서 별도 수집하므로
  price 누락이 지수 산출 자체를 막지는 않는다
- price 누락 종목을 별도로 제거하거나 행 전체를 삭제하지 않는다

기록 원칙:
- price 누락 행은 data_issues.csv에 일괄 기록
  issue_type: TRADING_PRICE_MISSING
  detail: "월간 매매 데이터 price 누락 — N_i 계산은 정상 진행"
- 누락 종목 목록을 에이전트 1번 실행 후 요약 출력

> 약 230개 누락 종목은 중요도가 낮은 하위 종목으로 확인되었으며
> 지수 산출에 미치는 영향이 미미하므로 제외 처리를 허용한다.
> 단, 누락 사실은 반드시 data_issues.csv에 기록한다.

---

### 수급 가중치 해석 원칙

본 지수의 수급 가중치(N_i)는 거래 강도(Activity)를 반영하며
순매수 방향성은 반영하지 않는다.
(sum = buy + sell 기반이므로 매수/매도 방향 구분 없음)

---

## 2.3 가격 데이터 (Price) — 이중 가격 체계

지수 산출에는 목적이 다른 두 종류의 가격을 구분하여 사용한다.

---

### 2.3.1 Observed Price (관측 가격)

출처:
- custody_weekly.csv의 price_stock
- trading_monthly.csv의 price

정의:
- 해당 시점 투자자가 실제로 관측한 가격
- 액분/병합 이전의 원시 가격 (Raw Price)
- corporate action이 반영되지 않은 가격일 수 있음
- 시계열 연속성이 보장되지 않음

용도:
- 보관금액(amount) 정합성 검증
- 매매금액 데이터 해석
- 이상치 탐지 및 데이터 품질 점검

---

### 2.3.2 Return Price (수익률 계산용 가격)

출처:
- 외부 가격 데이터 (yfinance Adjusted Close 기준)
- 또는 corporate action을 반영하여 정제된 내부 가격 테이블

정의:
- 액분, 병합, 티커변경 등 corporate action을 반영하여
  시계열적으로 연속 수익률 계산이 가능하도록 정리된 가격

용도:
- 종목 수익률 계산 (r_i)
- 지수 수익률 및 지수 포인트 산출

기준:
- Price Index 기준
- 배당(Dividend)은 반영하지 않음

---

### 2.3.3 가격 사용 원칙

- 보관금액 및 규모 해석 → Observed Price 사용
- 수익률 및 지수 산출 → Return Price 사용
- 수익률 계산에 Observed Price 사용 절대 금지
- 두 용도의 혼용 절대 금지

---

### 2.3.4 가격 검증 및 예외 처리

다음 두 조건을 모두 충족할 때 이상 후보로 분류한다:

  조건 1: abs(ReturnPrice 주간 변화율) > 30%
  조건 2: Observed Price와 Return Price 간 괴리 > 20%

단일 조건만 충족 시:
- 조건 1만: 고변동 종목 정상 가능 (SOXL, TSLA 등) → 기록만 수행
- 조건 2만: 데이터 지연 가능성 → 기록만 수행

두 조건 모두 충족 시:
  1. corporate action 여부 확인
  2. 이상 원인 분류:
     - 액분 / 병합
     - 티커 변경
     - 데이터 오류
  3. data/processed/data_issues.csv에 기록

---

### 2.3.5 가격 마스터 테이블

파일: data/processed/price_weekly_master.csv

생성 시점:
- 에이전트 2번(주간 수익률 계산) 실행 시 생성
- 에이전트 3번 이후 모든 작업은 이 파일을 Return Price 소스로 사용

컬럼:
- date
- isin
- ticker
- observed_price
- return_price
- price_source (SEIBRO / YFINANCE / ADJUSTED)
- event_flag (Y/N)
- validation_note

---

# 3. 종목 구성 규칙 (Universe Construction)

## 3.1 Core (1~50위)

- 해당 주 custody_weekly.csv의 amount 기준 상위 50 종목
- 실제 데이터 사용

---

## 3.2 Satellite (51~100위) — 종목 선정 기준

다음 조건을 순서대로 적용한다.

1. 해당 월 trading_monthly.csv의 sum(매매금액) 기준 내림차순 정렬
2. 해당 주 custody Top50에 포함된 종목 제외
3. 해당 주 Return Price가 없는 종목 제외
4. 위 조건을 통과한 종목을 매매금액 순으로 최대 50개 선정
5. 50개 미달 시: 가용 수만큼만 사용 (억지로 채우지 않음)

> Satellite 종목 부족은 정상이며
> 지수 구성 종목 수는 100 미만이 될 수 있다.
> 특히 초기 구간(2020년 초)은 종목 수가 적을 수 있으며 이는 정상이다.

---

## 3.3 Satellite 비중 산정 방식 — 대칭 배분법 (기본)

핵심 원칙: 비중(W)은 반드시 비율(%) 단위로 계산한다. amount(금액)를 직접 사용하지 않는다.

Step 1: Core 50위 종목의 자산 비중(S_50) 산출

  S_50 = amount_rank50 / Σ(amount_전체 Core 종목)

Step 2: 6위~실제 최하위 Core 종목 기울기(r) 계산
  (1~5위 슈퍼캡 제외 — 포함 시 기울기 과대 왜곡)
  (Core 종목 수가 N개인 경우 일반화 공식 적용)

  Core가 50개인 정상 주:
    S_6  = amount_rank6  / Σ(amount_전체 Core 종목)
    r = (S_50 / S_6)^(1/44)

  Core가 N개인 부족 주 (N < 50):
    S_6  = amount_rank6  / Σ(amount_전체 Core 종목)
    S_N  = amount_rankN  / Σ(amount_전체 Core 종목)
    r = (S_N / S_6)^(1/(N-6))
    → 분모가 (N-6)으로 자동 조정됨

Step 3: Satellite 비중 순차 계산

  W_50 = S_50
  W_51 = W_50 × r
  W_52 = W_51 × r
  W_53 = W_52 × r
  ... W_100까지 반복

Step 4: 전체 합계 100% 정규화 (Normalization) 필수

  모든 W_i 합산 후 각 W_i를 합계로 나누어 정규화

---

## 3.4 Satellite 모델 검증 (필수)

다음 3가지 방식을 병렬 계산하여 validation_report.md에 비교 결과 출력한다.

1. 대칭 배분법 (기본 채택)
2. 동일 가중 (1/N)
3. 매매금액 비례

모델 선택 기준:
- 기본 채택 방식: 대칭 배분법
- 단, 대칭 배분법 결과가 동일 가중 대비 전 기간 누적 수익률 괴리가
  20% 이상 발생 시 사람이 검토 후 최종 결정

---

# 4. 가중치 계산 (Weighting)

## 4.1 자산 가중치 (S_i)

  S_i = amount_i / Σ(amount_전체종목)

---

## 4.2 수급 가중치 (N_i)

  N_i = weekly_trading_i / Σ(weekly_trading_전체종목)

(월간 sum을 해당 월 주 수로 나눈 값 사용)

---

## 4.3 최종 가중치

  W_i = 0.7 × S_i + 0.3 × N_i

가중치 고정 원칙:
- W_i(t)는 t 시점 시작 시점(월요일) 기준으로 고정된다
- 해당 주 동안 W_i는 변하지 않는다
- 다음 주 가중치는 다음 주 월요일 데이터로 새로 산출한다

---

## 4.4 비중 제한 (Cap)

Cap을 적용하지 않는다.

본 지수는 한국 투자자의 실제 보유 비중을
그대로 반영하는 것을 원칙으로 한다.
특정 종목의 비중이 높더라도 실제 수급을
그대로 지수에 반영한다.

> Cap 미적용 근거:
> 서학개미 100 지수는 한국 개인투자자의
> 실제 포트폴리오 성과를 추적하는 지수이며
> NDX, S&P500 대비 실제 초과/미달 수익률을
> 정확히 측정하는 것이 목적이다.

---

# 5. 수익률 계산 (Return Calculation)

## 5.1 종목 수익률

  r_i(t) = (ReturnPrice_t - ReturnPrice_(t-1)) / ReturnPrice_(t-1)

- 반드시 Return Price 사용
- Observed Price 사용 절대 금지

---

## 5.2 지수 수익률

  R_t = Σ (W_i × r_i)

해당 주의 가중치(W_i)와 종목 수익률(r_i)을 곱하여 합산

---

## 5.3 지수 포인트 — 체인 수익률 방식

본 지수는 체인 수익률(Chain Return) 방식으로 산출한다.

  I_t = I_(t-1) × (1 + R_t)

체인 방식 채택 이유:
- 매주 가중치가 변동하는 구조에 적합
- 종목 교체 시 자연스러운 연속성 유지
- 별도 Divisor 관리 불필요

> Divisor 시스템은 사용하지 않는다.
> 체인 방식과 Divisor 방식은 구조적으로 중복되므로
> 혼용 시 지수 왜곡이 발생한다.

---

## 5.4 수익률 계산 기준

- 가격 기준: 주간 종가 (Close-to-Close)
- 구간:
  - t 시점: 해당 주 월요일 종가
  - t-1 시점: 직전 주 월요일 종가

---

## 5.5 Price Index 원칙

본 지수는 Price Index로 정의한다.

- 배당은 반영하지 않는다
- corporate action 중 다음만 반영:
  - 액분 (Split)
  - 병합 (Reverse Split)
  - 티커 변경
  - 인수합병

> NDX(나스닥 100)도 Price Index 기준이므로
> 비교 시 동일 기준이 적용된다.

---

# 6. 리밸런싱 규칙 (Rebalancing)

- 산출 주기: 주간
- 기준 요일: 월요일

가중치 기준 시점:
→ 해당 주 월요일 세이브로 데이터 (실질: 직전 주 금요일 D+1)

수익률 적용 구간:
→ 해당 주 월요일 종가 → 다음 주 월요일 종가

NDX 비교 시 동일 기준일 맞춤 필수

---

# 7. 종목 이벤트 처리 (Corporate Actions)

Corporate action은 Return Price 기준으로 수익률 연속성을 유지하도록 처리한다.
모든 이벤트는 corporate_actions_reference.csv를 기준으로 검증한다.

---

## 7.1 A형 — 완전 상장폐지

- 마지막 유효 Return Price 기준으로 수익률 반영
- 다음 주부터 해당 종목 제거
- operation_log.csv에 'DELISTED' 이벤트로 기록

---

## 7.2 B형 — 인수합병 (M&A)

현금 인수:
- 인수가격을 마지막 Return Price로 사용
- 해당 시점 수익률 반영 후 제거

주식 교환:
- 교환 비율에 따라 신규 종목으로 amount 승계
- Return Price 기준으로 연속성 유지
- 신규 종목이 이미 바스켓 내 있으면 amount 합산
- 신규 종목이 바스켓 밖이면 Satellite 편입 여부 검토

---

## 7.3 C형 — 티커 변경

- ISIN 기준으로 동일 종목으로 간주
- Return Price 시계열 연결
- 수익률 영향 없음
- data_issues.csv에 변경 이력 기록

---

## 7.4 D형 — 액분 / 병합 (Split / Reverse Split)

- Return Price는 자동 조정된 가격 사용 → 수익률 왜곡 없음
- Observed Price는 변경하지 않음 (당시 가격 기록 유지)
- corporate_actions_reference.csv에 ratio 기록 필수

---

## 7.5 E형 — 데이터 누락

1~2주 누락:
- Return Price Carry-forward
- r_i = 0% 처리

3주 이상 연속 누락:
- 해당 종목 제거
- 마지막 유효 수익률까지만 반영

레버리지 ETF 포함 모든 종목에 동일 기준 적용
누락 처리된 종목은 data_issues.csv에 기록

---

## 7.6 Corporate Action Reference 테이블

파일: data/reference/corporate_actions_reference.csv

파일 관리 방식:
- 사전 입력: 이미 알려진 이벤트(예: NVDA 2024년 액분 등)는 사람이 미리 입력
- 자동 탐지: 에이전트 2번이 가격 이상 조건(2.3.4) 충족 발견 시 후보 항목 추가
             단, event_type은 사람이 최종 확인 후 확정

컬럼:
- isin
- ticker
- event_date
- event_type (SPLIT / REVERSE_SPLIT / MERGER_CASH / MERGER_STOCK / DELISTED / TICKER_CHANGE)
- ratio
- old_ticker
- new_ticker
- note

---

## 7.7 이벤트 검증 규칙

- 가격 이상 조건(2.3.4) 충족 시 반드시 corporate action 여부 확인
- 설명 불가능한 가격 변화는 data_issues.csv 기록
- 중요 이벤트는 validation_report.md에 요약

---

# 8. 데이터 정합성 규칙

## 8.1 가격 데이터 원칙

2.3항 이중 가격 체계를 반드시 따를 것.
- 보관금액 및 규모 해석 → Observed Price
- 수익률 및 지수 산출 → Return Price
- 두 용도의 혼용 절대 금지

---

## 8.2 Look-ahead Bias 방지 (필수)

- 가중치 계산 시 해당 시점 이전 데이터만 사용
- 미래 시점의 보관금액, 매매금액, 가격 데이터 사용 절대 금지
- 특히 Satellite 종목 선정 시 해당 주 기준 데이터만 참조

---

# 9. 지수 연속성 — 체인 방식 (Chain Return)

본 지수는 체인 수익률 방식으로 연속성을 유지한다. (5.3항 참조)

  I_t = I_(t-1) × (1 + R_t)

Divisor 시스템은 사용하지 않는다.

종목 교체, 상장폐지 등 이벤트 발생 시:
- 해당 주 R_t 계산 시 잔존 종목 기준으로 W_i 재정규화 후 적용
- 별도 제수 조정 불필요
- 이벤트 이력은 operation_log.csv에 기록

operation_log.csv 컬럼:
- date
- event_type (REBALANCE / DELISTED / MERGER / TICKER_CHANGE / SPLIT)
- isin_out
- isin_in
- note

---

# 10. 검증 (Validation)

에이전트 5번 실행 시 다음 항목을 반드시 점검하고
output/validation_report.md를 작성한다.

## 10.1 품질 검증

- 지수 음수 여부
- 주간 변동 ±30% 이상 구간 목록 (원인 추정 포함)
- 종목 수 전주 대비 10개 이상 급변하는 주간
- Return Price와 Observed Price 이상 조건(2.3.4) 충족 종목 목록
- Core 종목 수 50개 미만인 주 목록 및 실제 종목 수
- 월간 매매 가격 누락 종목 수 및 비율 요약

## 10.2 구조 검증

- Top5 비중 추이
- 레버리지 ETF 비중 및 Cap 적용 빈도
- Core vs Satellite 기여도 비교
- Satellite 종목 수 추이 (부족 구간 명시)

## 10.3 성과 검증

- NDX(^NDX) 동일 기간 주간 데이터와 누적 수익률 비교표
- 상관계수(Correlation)
- 최대 낙폭(MDD) 비교
- 연도별 수익률 비교표
- 초과 수익 발생 구간과 원인 종목 명시

## 10.4 모델 민감도

- Satellite 3가지 방식별 지수 결과 비교
- 결과 괴리가 20% 이상이면 사람 검토 필요 플래그 표시

## 10.5 Corporate Action 검증

- 처리된 이벤트 목록 및 수익률 영향 요약
- 미처리 가격 이상치 잔존 여부 확인

---

# 11. 지수 연결 (Phase 1 → Phase 2 Transition)

- 연결 시점: 2025-12-31 → 2026-01-01
- 체인 방식으로 자동 연결:
  I_2026-01-01 = I_2025-12-31 × (1 + R_첫주)
- 2026-01-01 시점에서 W_i는 실제 데이터로 재산출
- 계산 방식 변경 없음
- "스케일 조정" 없음 — 체인 방식이 연속성을 보장함
- Phase 전환 이벤트를 operation_log.csv에 기록

---

# 12. 출력 (Output)

## 12.1 주간 지수 파일

파일: output/seohak100_weekly_index.csv

컬럼:
- date
- index_point
- weekly_return
- component_count (실제 사용된 종목 수)
- core_count (Core 종목 수)
- satellite_count (Satellite 종목 수)
- top3_contributors (상위 3개 기여 종목)
- satellite_method (사용된 Satellite 방식 명시)
- phase (1 또는 2)

## 12.2 운영 로그

파일: output/operation_log.csv (9항 참조)

## 12.3 데이터 이슈

파일: data/processed/data_issues.csv

컬럼:
- date
- isin
- ticker
- issue_type (PRICE_GAP / CARRY_FORWARD / DELISTED_CANDIDATE /
             TICKER_CHANGE / SPLIT_DETECTED /
             CORE_COUNT_SHORT / TRADING_PRICE_MISSING)
- detail

## 12.4 가격 마스터 테이블

파일: data/processed/price_weekly_master.csv (2.3.5항 참조)

## 12.5 Corporate Action 참조 테이블

파일: data/reference/corporate_actions_reference.csv (7.6항 참조)

## 12.6 검증 보고서

파일: output/validation_report.md

---

# 13. 핵심 원칙 (Principles)

1. 완벽한 재현보다 일관성을 우선한다
2. 모든 추정은 명시적으로 공개한다
3. 동일한 룰을 전 기간에 적용한다
4. Look-ahead bias를 절대 허용하지 않는다
5. 수익률 계산에는 반드시 Return Price만 사용한다
6. Observed Price와 Return Price를 혼용하지 않는다
7. 비중(W)은 반드시 비율(%) 단위로 계산한다. amount(금액)를 직접 비중으로 사용하지 않는다
8. 지수 연속성은 체인 방식으로 유지한다. Divisor 방식과 혼용하지 않는다
9. 실제 데이터 전환 이후(Phase 2)는 공식 지수로 간주한다
10. 에이전트는 판단이 필요한 경우 작업을 멈추고 사람에게 보고한다

---

# 14. 파일 구조 참조 (File Structure)

seohak-index/
├── data/
│   ├── raw/
│   │   ├── custody_weekly.csv
│   │   └── trading_monthly.csv
│   ├── processed/
│   │   ├── custody_weekly_clean.csv
│   │   ├── trading_monthly_clean.csv
│   │   ├── price_weekly_master.csv       ← 에이전트 2번 생성
│   │   ├── weekly_returns.csv
│   │   ├── weights_weekly.csv
│   │   ├── ticker_universe.csv
│   │   ├── delisted_candidates.csv
│   │   └── data_issues.csv
│   └── reference/
│       └── corporate_actions_reference.csv  ← 사람 사전 입력 + 에이전트 보완
├── output/
│   ├── seohak100_weekly_index.csv
│   ├── operation_log.csv
│   └── validation_report.md
└── RULES.md

---

# 15. v3 변경 이력 (Changelog)

## v1.0 → v2.0
- 이중 가격 체계(Observed/Return) 도입
- 액분/병합(D형) 이벤트 추가
- Corporate Action Reference 테이블 추가

## v2.0 → v3.0
- [버그 수정] Satellite 비중 산정 단위 오류 수정
  amount(금액)를 직접 W로 사용하던 오류를 S_i(비율) 기반으로 수정
- [구조 변경] Divisor 시스템 제거, 체인 수익률 방식으로 통일
- [개선] 가격 괴리 탐지 기준을 단일 조건에서 이중 조건으로 강화
  (ReturnPrice 변화 >30% AND 괴리 >20% 동시 충족 시 이상 후보)
- [추가] 수급 가중치 해석 원칙 명시 (거래 강도 기반, 방향성 미반영)
- [추가] W_i 고정 원칙 명시 (주 시작 시점 고정, 주중 불변)
- [추가] 레버리지 ETF Cap 적용 대상 명시
- [수정] Phase 전환 방식을 "스케일 조정"에서 "체인 자동 연결"로 명확화
- [추가] 출력 파일에 core_count, satellite_count 컬럼 추가

## v3.0 → v3.1
- [추가] 2.1 주간 Core 종목 수 부족 처리 지침
  (48~49개 수집 주 존재 확인 → 가용 종목 수로 산출, 40개 미만 시 보고)
- [추가] 2.2 월간 매매 가격 누락 처리 지침
  (3,750행 중 약 230행 누락 → N_i 계산은 정상 진행, 누락 기록만 수행)
- [추가] 3.3 Core 부족 주의 기울기(r) 일반화 공식
  (N개 Core 시 r = (S_N / S_6)^(1/(N-6)) 자동 적용)
- [추가] data_issues issue_type에 CORE_COUNT_SHORT, TRADING_PRICE_MISSING 추가
- [추가] 10.1 검증 항목에 데이터 결함 통계 추가

---
