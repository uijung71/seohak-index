# 서학개미 100 지수 산출 규칙 (RULES.md) v4.0
# 이 파일은 모든 에이전트가 반드시 가장 먼저 읽어야 한다
# 최종 업데이트: 2026-04

---

# 0. 지수 정의 (Index Definition)

서학개미 100 지수(Seohak-100 Index)는
대한민국 개인 투자자의 미국 주식 보유 및 매매 데이터를 기반으로 구성된
수급 가중 주가 지수 (Flow + Stock Weighted Index) 이다.

지수는 다음 세 단계로 구분된다.

---

## Phase 1A: Early Estimated Index (초기 추정 지수)

- 기간: 2020-01-06 ~ 2022-12-31
- 특징:
  - Core (1~50위): 실제 보관금액 데이터 사용
  - Satellite (51~N위): trading 데이터 기반 실제 선정
  - 종목 수: 평균 65~70개 (데이터 가용성 반영)
  - 서학개미 투자 초기 실제 포트폴리오 성격

> 본 구간은 데이터 한계로 인해 100종목을 채우지 않는다.
> 실제 수급 데이터만 사용하며 추정을 최소화한다.
> 모든 추정 구간은 명시적으로 공개된다.

---

## Phase 1B: Expanded Estimated Index (확장 추정 지수)

- 기간: 2023-01-01 ~ 2025-12-31
- 특징:
  - Core (1~50위): 실제 보관금액 데이터 사용
  - Satellite (51~100위): 종목 풀(~400개) 기반 선정 + 대칭 배분법 보관금액 추정
  - 종목 수: 항상 100개 유지
  - 생존자 편향 방지: 해당 주 기준 상장일 필터링 적용

> 서학개미 투자 성숙기에 해당하며 100종목 풀을 완성한다.
> 51~100위 보관금액은 추정값이며 이를 명시적으로 공개한다.

---

## Phase 2: Live Index (실제 지수)

- 기간: 2026-01-01 이후
- 특징:
  - 보관금액 + 매매금액 모두 실제 데이터 사용
  - 실제 보관 상위 100종목 기반
  - 100% 실데이터 기반 지수
  - Phase 1B와 체인 방식으로 자동 연결

---

# 1. 기준값 (Base Setting)

- 기준일: 2020-01-06
- 기준지수: 1,000 pt
- 산출 주기: 주간 (Weekly)
- 기준 요일: 월요일
  - 세이브로 데이터는 결제일 기준 D+1 반영
  - 실질 반영 시점: 직전 주 금요일 기준
  - 날짜 매칭: ISO week number 기준 (휴일로 인한 날짜 밀림 처리)
  - NDX 등 외부 지수와 비교 시 반드시 동일 기준일로 맞출 것

---

# 2. 데이터 정의 (Data Specification)

## 2.1 보관 데이터 (Custody)

- 출처: 세이브로
- 파일명: data/raw/custody_weekly.csv
- 단위: 주간 (매주 월요일 기준, ISO week 매칭)
- 기준: 보관금액 상위 50 종목

컬럼:
- date: 기준일 (YYYY-MM-DD, 휴일 시 다음 영업일 가능)
- isin: 종목 고유코드
- ticker: 야후 파이낸스 티커
- name_en: 종목명
- amount: 보관금액 (USD)
- price_stock: 해당 주 종목 가격 (Observed Price)

### 날짜 매칭 원칙

custody date가 월요일이 아닌 경우(한국/미국 휴일로 인한 밀림):
- ISO week number + 연도 기준으로 해당 주차를 매칭
- 같은 주차에 데이터가 2개 이상이면 최신 날짜 사용
- 절대 월요일 날짜로만 매칭하지 않는다

### Core 종목 수 부족 처리

실제 데이터에서 특정 주의 custody 종목 수가 50개 미만인 경우:
- 가용한 실제 종목 수(N개)를 Core로 그대로 사용
- 40개 미만 시 작업 중단 후 사람에게 보고
- data_issues.csv에 CORE_COUNT_SHORT로 기록

---

## 2.2 매매 데이터 (Trading)

- 출처: 세이브로
- 파일명: data/raw/trading_monthly.csv
- 단위: 월간

컬럼:
- date: 기준일 (YYYY. M. 1 형식 → 연월(YYYY-MM) 기준으로 매칭)
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

### 월간 가격 누락 처리

- price 누락 행은 삭제하지 않고 유지
- sum(매매금액)은 유효하므로 N_i 계산에 그대로 사용
- data_issues.csv에 TRADING_PRICE_MISSING으로 기록

### 수급 가중치 해석 원칙

본 지수의 수급 가중치(N_i)는 거래 강도(Activity)를 반영하며
순매수 방향성은 반영하지 않는다.
(sum = buy + sell 기반이므로 매수/매도 방향 구분 없음)

---

## 2.3 종목 풀 (Ticker Universe)

- 파일명: data/processed/ticker_universe.csv
- 구성: custody + trading에 등장한 전체 종목
- 실제 유효 종목 수: 약 364개 (상장폐지/합병 제외)
- 용도: Phase 1B의 Satellite 후보 풀

### 상장일(IPO Date) 데이터

- 파일명: data/reference/ipo_dates.csv
- 용도: Phase 1B에서 생존자 편향 방지
- 에이전트 3B 실행 전 yfinance로 수집
- 컬럼: ticker, isin, ipo_date

---

## 2.4 가격 데이터 (Price) — 이중 가격 체계

### Observed Price (관측 가격)
- 출처: custody price_stock, trading price
- 용도: 보관금액 정합성 검증, 이상치 탐지
- 특징: corporate action 미반영, 시계열 연속성 미보장

### Return Price (수익률 계산용 가격)
- 출처: yfinance Adjusted Close
- 용도: 종목 수익률 계산 (r_i), 지수 산출
- 기준: Price Index (배당 미반영)
- 수익률 계산에 Observed Price 사용 절대 금지

### 가격 이상 탐지 기준 (이중 조건)

다음 두 조건을 모두 충족할 때 이상 후보로 분류:
- 조건 1: abs(ReturnPrice 주간 변화율) > 30%
- 조건 2: Observed vs Return 괴리 > 20%

단일 조건만 충족 시 기록만 수행 (고변동 정상 가능)

### 가격 마스터 테이블

파일: data/processed/price_weekly_master.csv
생성: 에이전트 2번 실행 시
이후 모든 에이전트는 이 파일을 Return Price 소스로 사용

---

# 3. 종목 구성 규칙 (Universe Construction)

## 3.1 Phase별 종목 구성 방식

```
Phase 1A (2020~2022): 실제 데이터 기반
  Core:      custody 보관 상위 50개 (실제)
  Satellite: trading 전체에서 Core 제외 후
             매매금액 순 상위 (최대 50개)
  총 종목:   평균 65~70개

Phase 1B (2023~2025): 풀 기반 확장
  Core:      custody 보관 상위 50개 (실제)
  Satellite: 종목 풀(~364개)에서 Core 제외 후
             trading 매매금액 순 + 상장일 필터링
             보관금액은 대칭 배분법으로 추정
  총 종목:   항상 100개

Phase 2 (2026~): 완전 실데이터
  Core:      실제 보관 상위 100개
  Satellite: 없음 (Core가 100개)
  총 종목:   100개
```

---

## 3.2 Phase 1A Satellite 선정 (2020~2022)

다음 조건을 순서대로 적용한다:

1. trading 전체 데이터에서 Core 종목 제외
2. Return Price 없는 종목 제외
3. 나머지를 weekly_sum 기준 내림차순 정렬
4. 상위 최대 50개 선정
5. 50개 미달 시 가용 수만큼만 사용

> Phase 1A는 실제 데이터만 사용하므로
> 종목 수가 65~70개여도 정상이다.
> 억지로 채우지 않는다.

---

## 3.3 Phase 1B Satellite 선정 (2023~2025)

### Step 1: 후보 풀 구성

```
전체 종목 풀 (~364개)
- Core 종목 제외
- Return Price 없는 종목 제외
- 해당 주 기준 상장일(ipo_date) 이전 종목만 포함
  (생존자 편향 방지)
→ 후보 풀 확정
```

### Step 2: 우선순위 정렬

```
후보 풀을 다음 기준으로 정렬:
1순위: trading weekly_sum 있는 종목 (매매금액 내림차순)
2순위: trading 데이터 없는 종목 (yfinance 시가총액 내림차순)
→ 상위 50개 선정
```

### Step 3: 보관금액 추정 (대칭 배분법)

핵심 원칙: 비중(W)은 반드시 비율(%) 단위로 계산한다.

```
Core 실제 데이터로 기울기(r) 계산:

정상 주 (Core = 50개):
  S_6  = amount_rank6  / Σ(amount_전체 Core)
  S_50 = amount_rank50 / Σ(amount_전체 Core)
  r = (S_50 / S_6)^(1/44)

Core 부족 주 (Core = N개):
  S_6  = amount_rank6  / Σ(amount_전체 Core)
  S_N  = amount_rankN  / Σ(amount_전체 Core)
  r = (S_N / S_6)^(1/(N-6))

Satellite 추정 비중:
  W_sat_1 = S_N (Core 최하위와 동일한 값에서 시작)
  W_sat_2 = W_sat_1 × r
  W_sat_3 = W_sat_2 × r
  ... Satellite 50개까지 반복

전체 정규화: 모든 W_i 합산 후 100%로 정규화
```

### Satellite 3가지 방식 병렬 검증

1. SYMMETRIC: 대칭 배분법 (기본 채택)
2. EQUAL: 동일 가중 (1/N)
3. TRADING_PROP: 매매금액 비례

결과를 weights_satellite_comparison.csv로 저장
대칭 배분법 vs 동일 가중 괴리 20% 이상 시 사람 검토 필요

---

## 3.4 Phase 전환 처리

### Phase 1A → Phase 1B 전환 (2022-12-31 → 2023-01-01)

```
체인 방식으로 자동 연결
I_2023-01-01 = I_2022-12-31 × (1 + R_첫주)
종목 수가 65~70개 → 100개로 증가하지만
체인 방식이므로 지수값 단절 없음
operation_log.csv에 PHASE_1A_TO_1B 이벤트로 기록
```

### Phase 1B → Phase 2 전환 (2025-12-31 → 2026-01-01)

```
체인 방식으로 자동 연결
W_i는 실제 데이터로 재산출
계산 방식 변경 없음
operation_log.csv에 PHASE_1B_TO_2 이벤트로 기록
```

---

# 4. 가중치 계산 (Weighting)

## 4.1 자산 가중치 (S_i)

  S_i = amount_i / Σ(amount_전체종목)

Phase 1B Satellite:
  S_i = 대칭 배분법 추정값 사용
  segment 컬럼에 SATELLITE_EST로 표시

---

## 4.2 수급 가중치 (N_i)

  N_i = weekly_trading_i / Σ(weekly_trading_전체종목)

Phase 1B Satellite 중 trading 데이터 없는 종목:
  N_i = 0 으로 처리
  W_i = S_i (보관금액 추정값만 반영)

---

## 4.3 최종 가중치

  W_i = 0.7 × S_i + 0.3 × N_i

가중치 고정 원칙:
- W_i(t)는 해당 주 월요일 기준으로 고정
- 해당 주 동안 변하지 않음
- 다음 주 월요일에 새로 산출

---

## 4.4 비중 제한 (Cap)

Cap을 적용하지 않는다.

본 지수는 한국 투자자의 실제 보유 비중을
그대로 반영하는 것을 원칙으로 한다.

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

  R_t = Σ(W_i × r_i)

---

## 5.3 지수 포인트 — 체인 수익률 방식

  I_t = I_(t-1) × (1 + R_t)

Divisor 시스템은 사용하지 않는다.
체인 방식과 Divisor 방식의 혼용은 지수 왜곡을 유발한다.

---

## 5.4 수익률 계산 기준

- 가격 기준: 주간 종가 (Close-to-Close)
- t 시점: 해당 주 월요일 종가
- t-1 시점: 직전 주 월요일 종가

---

## 5.5 Price Index 원칙

- 배당은 반영하지 않는다
- Corporate action 반영 항목: 액분, 병합, 티커변경, 인수합병
- NDX도 Price Index 기준 → 비교 시 동일 기준 적용

---

# 6. 리밸런싱 규칙 (Rebalancing)

- 산출 주기: 주간
- 기준 요일: 월요일 (ISO week 기준 매칭)
- 가중치 기준: 해당 주 월요일 세이브로 데이터 (실질: 직전 주 금요일 D+1)
- 수익률 적용: 해당 주 월요일 종가 → 다음 주 월요일 종가
- NDX 비교 시 동일 기준일 맞춤 필수

---

# 7. 종목 이벤트 처리 (Corporate Actions)

Corporate action은 Return Price 기준으로 수익률 연속성을 유지한다.
모든 이벤트는 corporate_actions_reference.csv 기준으로 검증한다.

## 7.1 A형 — 완전 상장폐지
- 마지막 유효 Return Price로 수익률 반영 후 제거
- operation_log.csv에 DELISTED 기록

## 7.2 B형 — 인수합병
현금 인수: 인수가격을 마지막 Return Price로 처리 후 제거
주식 교환: 교환 비율로 신규 종목 amount 승계

## 7.3 C형 — 티커 변경
- ISIN 기준 연결, 수익률 영향 없음

## 7.4 D형 — 액분 / 병합
- Return Price 자동 조정 → 수익률 왜곡 없음
- Observed Price는 변경하지 않음

## 7.5 E형 — 데이터 누락
- 1~2주: Carry-forward, r_i = 0%
- 3주 이상: 제거

## 7.6 Corporate Action Reference 테이블

파일: data/reference/corporate_actions_reference.csv
- 사전 입력: 알려진 이벤트는 사람이 미리 입력
- 자동 탐지: 에이전트 2가 이상 조건 발견 시 후보 추가

---

# 8. 데이터 정합성 규칙

## 8.1 가격 데이터 원칙
- 보관금액 해석 → Observed Price
- 수익률 산출 → Return Price
- 혼용 절대 금지

## 8.2 Look-ahead Bias 방지 (필수)
- 가중치 계산 시 해당 시점 이전 데이터만 사용
- Phase 1B 상장일 필터링: 해당 주 기준 상장 전 종목 제외
- 미래 데이터 사용 절대 금지

---

# 9. 지수 연속성 — 체인 방식

  I_t = I_(t-1) × (1 + R_t)

Divisor 시스템 사용하지 않음.
종목 교체 시 잔존 종목 기준 W_i 재정규화 후 R_t 계산.

operation_log.csv 컬럼:
- date
- event_type (REBALANCE / DELISTED / MERGER / TICKER_CHANGE /
             SPLIT / PHASE_1A_TO_1B / PHASE_1B_TO_2)
- isin_out
- isin_in
- note

---

# 10. 검증 (Validation)

## 10.1 품질 검증
- 지수 음수 여부
- 주간 변동 ±30% 이상 구간 (원인 추정 포함)
- 종목 수 전주 대비 10개 이상 급변 주간
- Core 종목 수 50개 미만 주 목록
- 월간 매매 가격 누락 통계
- Phase 1A → 1B 전환 시점 종목 수 변화 확인

## 10.2 구조 검증
- Top5 비중 추이
- 레버리지 ETF 비중
- Core vs Satellite 기여도
- Phase 1B SATELLITE_EST 종목 비중 합계 추이

## 10.3 성과 검증
- NDX(^NDX) 동일 기간 누적 수익률 비교
- 상관계수(Correlation)
- 최대 낙폭(MDD) 비교
- 연도별 수익률 비교표
- Phase별 성과 분리 비교

## 10.4 모델 민감도
- Satellite 3가지 방식 비교 (Phase 1B 구간)
- 괴리 20% 이상 시 사람 검토 필요 플래그

## 10.5 생존자 편향 검증 (Phase 1B)
- 상장일 필터링으로 제외된 종목 목록
- 제외 종목이 지수에 포함됐을 경우 가상 수익률 비교

---

# 11. 지수 연결 (Phase Transitions)

## Phase 1A → 1B (2022-12-31 → 2023-01-01)
- 체인 방식 자동 연결
- 종목 수 65~70개 → 100개 증가 (지수값 단절 없음)
- operation_log에 기록

## Phase 1B → 2 (2025-12-31 → 2026-01-01)
- 체인 방식 자동 연결
- W_i 실제 데이터로 재산출
- operation_log에 기록

---

# 12. 출력 (Output)

## 12.1 주간 지수 파일

파일: output/seohak100_weekly_index.csv

컬럼:
- date
- index_point
- weekly_return
- component_count
- core_count
- satellite_count
- top3_contributors
- satellite_method
- phase (1A / 1B / 2)

## 12.2 운영 로그
파일: output/operation_log.csv

## 12.3 데이터 이슈
파일: data/processed/data_issues.csv

issue_type 목록:
- PRICE_GAP
- CARRY_FORWARD
- DELISTED_CANDIDATE
- TICKER_CHANGE
- SPLIT_DETECTED
- CORE_COUNT_SHORT
- TRADING_PRICE_MISSING
- SATELLITE_CARRIED_FORWARD
- CUSTODY_DATA_MISSING

## 12.4 가격 마스터 테이블
파일: data/processed/price_weekly_master.csv

## 12.5 Corporate Action 참조
파일: data/reference/corporate_actions_reference.csv

## 12.6 IPO 날짜 참조
파일: data/reference/ipo_dates.csv

## 12.7 검증 보고서
파일: output/validation_report.md

---

# 13. 핵심 원칙 (Principles)

1. 완벽한 재현보다 일관성을 우선한다
2. 모든 추정은 명시적으로 공개한다
3. 동일한 룰을 각 Phase 내 전 기간에 적용한다
4. Look-ahead bias를 절대 허용하지 않는다
5. 수익률 계산에는 반드시 Return Price만 사용한다
6. Observed Price와 Return Price를 혼용하지 않는다
7. 비중(W)은 반드시 비율(%) 단위로 계산한다
8. 지수 연속성은 체인 방식으로 유지한다. Divisor 방식과 혼용하지 않는다
9. Phase 1A는 실제 데이터 우선, 억지로 종목을 채우지 않는다
10. Phase 1B는 생존자 편향 방지를 위해 상장일 필터링을 반드시 적용한다
11. Cap을 적용하지 않는다. 실제 수급 비중을 그대로 반영한다
12. 실제 데이터 전환 이후(Phase 2)는 공식 지수로 간주한다
13. 에이전트는 판단이 필요한 경우 작업을 멈추고 사람에게 보고한다

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
│   │   ├── price_weekly_master.csv       ← 에이전트 2 생성
│   │   ├── weekly_returns.csv
│   │   ├── weights_weekly.csv
│   │   ├── weights_satellite_comparison.csv
│   │   ├── ticker_universe.csv
│   │   ├── delisted_candidates.csv
│   │   └── data_issues.csv
│   └── reference/
│       ├── corporate_actions_reference.csv
│       └── ipo_dates.csv                 ← 에이전트 3B 실행 전 수집
├── output/
│   ├── seohak100_weekly_index.csv
│   ├── operation_log.csv
│   └── validation_report.md
└── RULES.md

---

# 15. 변경 이력 (Changelog)

## v1.0 → v2.0
- 이중 가격 체계(Observed/Return) 도입
- 액분/병합(D형) 이벤트 추가
- Corporate Action Reference 테이블 추가

## v2.0 → v3.0
- Satellite 비중 단위 오류 수정 (amount→비율)
- Divisor 시스템 제거, 체인 방식 통일
- 가격 괴리 탐지 이중 조건 강화
- 수급 가중치 방향성 미반영 원칙 명시
- W_i 주중 고정 원칙 명시

## v3.0 → v3.1
- 주간 Core 종목 수 부족 처리 지침 추가
- 월간 매매 가격 누락 처리 지침 추가
- Core 부족 주 기울기 일반화 공식 추가

## v3.1 → v4.0 (현재)
- Phase 구조 변경: Phase 1 → Phase 1A + Phase 1B + Phase 2
- Phase 1A (2020~2022): 실제 데이터만, 65~70개 운영
- Phase 1B (2023~2025): 종목 풀 기반 100종목 완성
- Cap 제거: 실제 수급 비중 그대로 반영
- ISO week 날짜 매칭 원칙 추가 (휴일 밀림 처리)
- 생존자 편향 방지: 상장일 필터링 원칙 추가
- ipo_dates.csv 참조 파일 추가
- Phase 전환 이벤트 operation_log 기록 추가
- phase 컬럼 1A/1B/2로 세분화

---
