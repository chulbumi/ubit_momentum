# 업비트 Open API 상세 명세

> 업비트 개발자 센터: https://docs.upbit.com/kr/reference/

---

## 목차

1. [API 개요](#1-api-개요)
2. [인증](#2-인증)
3. [요청 수 제한 (Rate Limits)](#3-요청-수-제한-rate-limits)
4. [REST API 사용 및 에러 안내](#4-rest-api-사용-및-에러-안내)
5. [WebSocket 사용 및 에러 안내](#5-websocket-사용-및-에러-안내)
6. [주요 REST API 엔드포인트](#6-주요-rest-api-엔드포인트)
7. [주요 WebSocket 구독 항목](#7-주요-websocket-구독-항목)
8. [DEPRECATED API](#8-deprecated-api)
9. [마켓별 호가 단위 및 주문 정책](#9-마켓별-호가-단위-및-주문-정책)
10. [변경 이력 및 유의사항](#10-변경-이력-및-유의사항)

---

## 1. API 개요

### 1.1 API 분류

업비트 API는 크게 두 가지 카테고리로 분류됩니다:

- **Quotation API (시세 조회)**
  - 공개(Public) API로 인증 없이 사용 가능
  - 현재가, 캔들, 체결 내역, 호가 등 시세 정보 조회
  
- **Exchange API (거래 및 자산 관리)**
  - 개인 API Key를 사용한 인증 필수
  - 주문 생성/취소/조회, 자산 조회, 입출금 관리 등

### 1.2 연동 방식

- **REST API**: 필요 시점에 요청하여 응답을 받는 방식
- **WebSocket**: 연결 후 실시간 스트림 데이터 수신 (스냅샷 + 실시간 업데이트)

### 1.3 기본 엔드포인트

- REST API: `https://api.upbit.com/v1`
- WebSocket (Public): `wss://api.upbit.com/websocket/v1`
- WebSocket (Private): `wss://api.upbit.com/websocket/v1/private`

---

## 2. 인증

### 2.1 API Key 발급

1. 업비트 로그인 → "Open API 관리" 페이지 진입
2. 이용약관 동의
3. 사용 기능 선택
   - 자산 조회
   - 주문 조회
   - 주문하기
   - 출금하기
   - 출금 조회
4. 허용 IP 등록 (선택 사항, 등록하지 않으면 전체 허용)
5. 2채널 인증 완료
6. **Secret Key는 발급 시 한 번만 표시되므로 안전하게 보관 필수**

### 2.2 JWT 인증 방식

Exchange API 및 Private WebSocket 사용 시 JWT Bearer 토큰 필요:

```
Authorization: Bearer <JWT_TOKEN>
```

#### JWT 토큰 생성 방법

```python
import jwt
import hashlib
import uuid
from urllib.parse import urlencode

access_key = "YOUR_ACCESS_KEY"
secret_key = "YOUR_SECRET_KEY"

payload = {
    'access_key': access_key,
    'nonce': str(uuid.uuid4()),
}

# 쿼리 파라미터가 있는 경우
query = {
    'market': 'KRW-BTC',
}
query_string = urlencode(query).encode()

m = hashlib.sha512()
m.update(query_string)
query_hash = m.hexdigest()

payload['query_hash'] = query_hash
payload['query_hash_alg'] = 'SHA512'

jwt_token = jwt.encode(payload, secret_key)
```

---

## 3. 요청 수 제한 (Rate Limits)

### 3.1 REST API 제한

| API 그룹 | 제한 | 비고 |
|---------|------|------|
| Quotation (시세 조회) | 초당 10회 | 그룹별 제한 |
| Exchange (주문 생성/취소) | 초당 8회 | 계정 단위 |
| 주문 일괄 취소 | 2초당 1회 | DELETE /v1/orders/open |
| 기타 Exchange API | 초당 30회 | - |

### 3.2 WebSocket 제한

| 항목 | 제한 |
|------|------|
| 연결 요청 | 초당 5회 |
| 메시지 요청 (구독) | 초당 5회, 분당 100회 |

### 3.3 Origin 헤더 제한

- Origin 헤더가 포함된 요청 (브라우저 환경): **10초당 1회**로 제한
- 시세 조회 및 WebSocket 연결에 적용

### 3.4 잔여 요청 확인

HTTP 응답 헤더의 `Remaining-Req` 필드에서 잔여 요청 가능 횟수 확인 가능:

```
Remaining-Req: group=market; min=573; sec=9
```

---

## 4. REST API 사용 및 에러 안내

### 4.1 기본 요구사항

- **TLS 버전**: TLS 1.2 이상 (TLS 1.3 권장)
- **Content-Type**: `application/json; charset=utf-8`
- **POST 요청**: JSON 형식만 지원 (Form-urlencoded 방식은 2022년 3월 1일부터 지원 종료)

### 4.2 URL 인코딩

GET/DELETE 요청의 쿼리 파라미터는 URL 인코딩 필수:

```python
# 올바른 예
import urllib.parse
params = {'market': 'KRW-BTC', 'state': 'wait'}
query_string = urllib.parse.urlencode(params)
# market=KRW-BTC&state=wait
```

**주의사항**:
- 특수문자(`:`, `+`, `/` 등)는 반드시 인코딩
- 배열 파라미터(`[]`)의 대괄호는 인코딩 대상에서 제외 (Exchange API)

### 4.3 HTTP 상태 코드 및 에러

| 상태 코드 | 에러 코드 예시 | 원인 | 해결 방법 |
|-----------|----------------|------|-----------|
| 200 OK | - | 정상 처리 | - |
| 201 Created | - | 생성 완료 (주문 등) | - |
| 400 Bad Request | `create_ask_error`<br>`create_bid_error`<br>`insufficient_funds_ask`<br>`insufficient_funds_bid`<br>`under_min_total_ask`<br>`under_min_total_bid`<br>`validation_error` | 파라미터 오류<br>잔고 부족<br>최소 주문 금액 미달 | 파라미터 확인<br>잔고 확인<br>주문 정책 준수 |
| 401 Unauthorized | `invalid_query_payload`<br>`jwt_verification`<br>`expired_access_key`<br>`nonce_used`<br>`no_authorization_ip`<br>`no_authorization_token`<br>`out_of_scope` | JWT 인증 실패<br>API Key 만료<br>IP 허용 목록 미등록<br>권한 없는 기능 요청 | JWT 재생성<br>API Key 확인<br>IP 등록 확인<br>권한 설정 확인 |
| 404 Not Found | - | 존재하지 않는 리소스 | 경로/UUID 확인 |
| 429 Too Many Requests | - | Rate Limit 초과 | 요청 빈도 감소 |
| 418 I'm a teapot | - | 반복적인 제한 위반 | 차단 기간 대기 |
| 500 Internal Server Error | - | 서버 내부 오류 | 재시도 또는 공지 확인 |

### 4.4 에러 응답 형식

```json
{
  "error": {
    "name": "insufficient_funds_ask",
    "message": "매도 가능 금액이 부족합니다."
  }
}
```

### 4.5 압축 지원

Quotation API는 gzip 압축 지원:

```
Accept-Encoding: gzip
```

---

## 5. WebSocket 사용 및 에러 안내

### 5.1 연결

```javascript
// Public WebSocket
const ws = new WebSocket('wss://api.upbit.com/websocket/v1');

// Private WebSocket (인증 필요)
const ws = new WebSocket('wss://api.upbit.com/websocket/v1/private');
```

### 5.2 요청 메시지 구조

WebSocket 메시지는 JSON Array 형식:

```json
[
  {"ticket": "unique-ticket-id"},
  {"type": "ticker", "codes": ["KRW-BTC", "KRW-ETH"]},
  {"format": "SIMPLE"}
]
```

#### 메시지 구성 요소

1. **Ticket Object** (필수)
```json
{"ticket": "unique-uuid"}
```

2. **Data Type Object** (필수, 복수 가능)
```json
{
  "type": "ticker",
  "codes": ["KRW-BTC"],
  "is_only_snapshot": false,
  "is_only_realtime": false
}
```

3. **Format Object** (선택)
```json
{"format": "SIMPLE"}
```

### 5.3 Format 옵션

| Format | 설명 |
|--------|------|
| `DEFAULT` | 전체 필드명 사용 |
| `SIMPLE` | 축약된 필드명 사용 |
| `JSON_LIST` | 리스트 형태 (전체 필드명) |
| `SIMPLE_LIST` | 리스트 형태 (축약 필드명) |

### 5.4 연결 유지

- **Idle Timeout**: 120초 동안 송수신 없으면 자동 종료
- **연결 유지 방법**: PING/PONG 또는 "PING" 메시지 전송

```javascript
setInterval(() => {
  ws.send("PING");
}, 60000); // 60초마다 PING 전송
```

### 5.5 WebSocket 에러 코드

| 에러 코드 | 설명 |
|-----------|------|
| `INVALID_AUTH` | 인증 정보 오류 |
| `WRONG_FORMAT` | 메시지 형식 오류 |
| `NO_TICKET` | Ticket 누락 |
| `NO_TYPE` | Type 누락 |
| `NO_CODES` | Codes 누락 (필요한 경우) |
| `INVALID_PARAM` | 잘못된 파라미터 |

### 5.6 에러 응답 형식

```json
{
  "error": {
    "name": "INVALID_AUTH",
    "message": "인증 정보가 올바르지 않습니다."
  }
}
```

---

## 6. 주요 REST API 엔드포인트

### 6.1 Quotation API (공개)

#### 6.1.1 페어(Trading Pairs)

**페어 목록 조회**
```
GET /v1/market/all
```

업비트에서 거래 가능한 모든 마켓(페어) 정보를 조회합니다.

**파라미터**:
- `is_details` (boolean, optional): 상세 정보 포함 여부 (기본값: false)

**Rate Limit**: 초당 10회 (IP 단위, 마켓 그룹 공유)

**기본 응답 예시**:
```json
[
  {
    "market": "KRW-BTC",
    "korean_name": "비트코인",
    "english_name": "Bitcoin",
    "market_warning": "NONE"
  },
  {
    "market": "KRW-ETH",
    "korean_name": "이더리움",
    "english_name": "Ethereum",
    "market_warning": "CAUTION"
  }
]
```

**상세 정보 포함 응답 예시** (`is_details=true`):
```json
[
  {
    "market": "KRW-BTC",
    "korean_name": "비트코인",
    "english_name": "Bitcoin",
    "market_warning": "NONE",
    "market_event": {
      "warning": false,
      "caution": false
    }
  }
]
```

**응답 필드 설명**:
- `market`: 마켓 코드 (예: KRW-BTC)
- `korean_name`: 한글 명칭
- `english_name`: 영문 명칭
- `market_warning`: 유의 종목 여부 (`NONE`, `CAUTION` 등)
- `market_event`: 시장 이벤트 정보 (상장, 투자유의 등)

**변경 이력**:
- 2024-11-20: `market_event` 필드 추가, `market_warning` 필드 필수 여부 변경
- 2024-02-22: 페어별 시장 경보(`market_warning`) 조회 기능 추가

#### 6.1.2 캔들(OHLCV)

**초(Second) 캔들 조회**
```
GET /v1/candles/seconds
```

**지원 단위**: 1초 (단일 단위 지원)

**파라미터**:
- `market` (string, required): 마켓 코드 (예: KRW-BTC)
- `to` (string, optional): 마지막 캔들 시각 (ISO 8601 형식)
- `count` (integer, optional): 캔들 개수 (최대 200, 기본값 1)

**Rate Limit**: 초당 10회 (IP 단위, 캔들 그룹 공유)

**분(Minute) 캔들 조회**
```
GET /v1/candles/minutes/{unit}
```

**지원 단위**: 1, 3, 5, 10, 15, 30, 60, 240

**파라미터**:
- `market` (string, required): 마켓 코드 (예: KRW-BTC)
- `to` (string, optional): 마지막 캔들 시각 (ISO 8601 형식)
  - 예: `2024-01-01T00:00:00+09:00`
- `count` (integer, optional): 캔들 개수 (최대 200, 기본값 1)

**Rate Limit**: 초당 10회 (IP 단위, 캔들 그룹 공유)

**일(Day) 캔들 조회**
```
GET /v1/candles/days
```

**파라미터**:
- `market` (string, required): 마켓 코드
- `to` (string, optional): 마지막 캔들 시각 (ISO 8601 형식)
- `count` (integer, optional): 캔들 개수 (최대 200, 기본값 1)
- `converting_price_unit` (string, optional): 종가 환산 화폐 단위 (KRW로 변환)

**주(Week) 캔들 조회**
```
GET /v1/candles/weeks
```

**파라미터**:
- `market` (string, required): 마켓 코드
- `to` (string, optional): 마지막 캔들 시각 (ISO 8601 형식)
- `count` (integer, optional): 캔들 개수 (최대 200, 기본값 1)

**월(Month) 캔들 조회**
```
GET /v1/candles/months
```

**파라미터**:
- `market` (string, required): 마켓 코드
- `to` (string, optional): 마지막 캔들 시각 (ISO 8601 형식)
- `count` (integer, optional): 캔들 개수 (최대 200, 기본값 1)

**연(Year) 캔들 조회**
```
GET /v1/candles/years
```

**파라미터**:
- `market` (string, required): 마켓 코드
- `to` (string, optional): 마지막 캔들 시각 (ISO 8601 형식)
- `count` (integer, optional): 캔들 개수 (최대 200, 기본값 1)

**캔들 공통 응답 예시**:
```json
[
  {
    "market": "KRW-BTC",
    "candle_date_time_utc": "2024-01-01T00:00:00",
    "candle_date_time_kst": "2024-01-01T09:00:00",
    "opening_price": 50000000,
    "high_price": 51000000,
    "low_price": 49000000,
    "trade_price": 50500000,
    "timestamp": 1704067200000,
    "candle_acc_trade_price": 123456789.0,
    "candle_acc_trade_volume": 2.5,
    "unit": 1
  }
]
```

**응답 필드 설명**:
- `market`: 마켓 코드
- `candle_date_time_utc`: 캔들 기준 시각 (UTC)
- `candle_date_time_kst`: 캔들 기준 시각 (KST)
- `opening_price`: 시가
- `high_price`: 고가
- `low_price`: 저가
- `trade_price`: 종가
- `timestamp`: 마지막 틱 타임스탬프 (밀리초)
- `candle_acc_trade_price`: 누적 거래 금액
- `candle_acc_trade_volume`: 누적 거래량
- `unit`: 캔들 단위 (분/초 캔들의 경우)
- `prev_closing_price`: 전일 종가 (일 캔들 이상)
- `change_price`: 전일 대비 가격 변화량 (일 캔들 이상)
- `change_rate`: 전일 대비 등락률 (일 캔들 이상)

#### 6.1.3 체결(Trade)

**최근 체결 내역 조회**
```
GET /v1/trades/ticks
```

파라미터:
- `market` (string, required): 마켓 코드
- `to` (string, optional): 마지막 체결 시각
- `count` (integer, optional): 체결 개수 (최대 500, 기본값 1)
- `cursor` (string, optional): 페이지네이션 커서
- `days_ago` (integer, optional): 최근 며칠 전 데이터 (최대 7)

응답 예시:
```json
[
  {
    "market": "KRW-BTC",
    "trade_date_utc": "2024-01-01",
    "trade_time_utc": "00:00:00",
    "timestamp": 1704067200000,
    "trade_price": 50000000,
    "trade_volume": 0.1,
    "prev_closing_price": 49500000,
    "change_price": 500000,
    "ask_bid": "BID",
    "sequential_id": 1234567890
  }
]
```

#### 6.1.4 현재가(Ticker)

**페어 단위 현재가 조회**
```
GET /v1/ticker
```

특정 마켓의 현재가 정보를 조회합니다.

**파라미터**:
- `markets` (string, required): 마켓 코드 (쉼표로 구분, 최대 100개)
  - 예: `KRW-BTC,KRW-ETH`

**Rate Limit**: 초당 10회 (IP 단위, 현재가 그룹 공유)

**마켓 단위 현재가 조회**
```
GET /v1/ticker/all
```

특정 기준 통화(KRW, BTC, USDT 등)의 모든 마켓 현재가를 한 번에 조회합니다.

**파라미터**:
- `quote_currencies` (string, optional): 기준 통화 (쉼표로 구분)
  - 예: `KRW`, `BTC`, `USDT`
  - 미지정 시 전체 마켓 조회

**Rate Limit**: 초당 10회 (IP 단위, 현재가 그룹 공유)

**현재가 공통 응답 예시**:
```json
[
  {
    "market": "KRW-BTC",
    "trade_date": "20240101",
    "trade_time": "000000",
    "trade_date_kst": "20240101",
    "trade_time_kst": "090000",
    "trade_timestamp": 1704067200000,
    "opening_price": 50000000,
    "high_price": 51000000,
    "low_price": 49000000,
    "trade_price": 50500000,
    "prev_closing_price": 49500000,
    "change": "RISE",
    "change_price": 1000000,
    "change_rate": 0.0202020202,
    "signed_change_price": 1000000,
    "signed_change_rate": 0.0202020202,
    "trade_volume": 0.1,
    "acc_trade_price": 123456789000,
    "acc_trade_price_24h": 234567890000,
    "acc_trade_volume": 2500,
    "acc_trade_volume_24h": 5000,
    "highest_52_week_price": 60000000,
    "highest_52_week_date": "2023-12-01",
    "lowest_52_week_price": 30000000,
    "lowest_52_week_date": "2023-06-01",
    "timestamp": 1704067200000
  }
]
```

**응답 필드 설명**:
- `market`: 마켓 코드
- `trade_date`: 최근 거래 일자 (UTC)
- `trade_time`: 최근 거래 시각 (UTC)
- `trade_date_kst`: 최근 거래 일자 (KST)
- `trade_time_kst`: 최근 거래 시각 (KST)
- `trade_timestamp`: 최근 거래 타임스탬프 (밀리초)
- `opening_price`: 시가
- `high_price`: 고가
- `low_price`: 저가
- `trade_price`: 현재가
- `prev_closing_price`: 전일 종가
- `change`: 전일 대비 변화
  - `RISE`: 상승
  - `EVEN`: 보합
  - `FALL`: 하락
- `change_price`: 전일 대비 가격 변화량 (절대값)
- `change_rate`: 전일 대비 등락률 (절대값)
- `signed_change_price`: 전일 대비 가격 변화량 (부호 포함)
- `signed_change_rate`: 전일 대비 등락률 (부호 포함)
- `trade_volume`: 최근 거래량
- `acc_trade_price`: 당일 누적 거래대금 (UTC 0시 기준)
- `acc_trade_price_24h`: 24시간 누적 거래대금
- `acc_trade_volume`: 당일 누적 거래량 (UTC 0시 기준)
- `acc_trade_volume_24h`: 24시간 누적 거래량
- `highest_52_week_price`: 52주 최고가
- `highest_52_week_date`: 52주 최고가 달성일
- `lowest_52_week_price`: 52주 최저가
- `lowest_52_week_date`: 52주 최저가 달성일
- `timestamp`: 타임스탬프 (밀리초)

#### 6.1.5 호가(Orderbook)

**호가 조회**
```
GET /v1/orderbook
```

현재 호가 정보를 조회합니다.

**파라미터**:
- `markets` (string, required): 마켓 코드 (쉼표로 구분, 최대 100개)
  - 예: `KRW-BTC` 또는 `KRW-BTC,KRW-ETH`
- `level` (integer, optional): 호가 레벨 (기본값: 0)
  - `0`: 전체 호가 (15호가)
  - `1~5`: 해당 레벨까지의 호가

**Rate Limit**: 초당 10회 (IP 단위, 호가 그룹 공유)

**응답 예시**:
```json
[
  {
    "market": "KRW-BTC",
    "timestamp": 1704067200000,
    "total_ask_size": 10.5,
    "total_bid_size": 12.3,
    "orderbook_units": [
      {
        "ask_price": 50100000,
        "bid_price": 50000000,
        "ask_size": 1.5,
        "bid_size": 2.3
      }
    ],
    "level": 0
  }
]
```

**응답 필드 설명**:
- `market`: 마켓 코드
- `timestamp`: 호가 생성 시각 (밀리초)
- `total_ask_size`: 총 매도 호가 수량
- `total_bid_size`: 총 매수 호가 수량
- `orderbook_units`: 호가 리스트 (배열)
  - `ask_price`: 매도 호가
  - `bid_price`: 매수 호가
  - `ask_size`: 매도 호가 수량
  - `bid_size`: 매수 호가 수량
- `level`: 조회된 호가 레벨

**호가 정책 조회**
```
GET /v1/orderbook/policy
```

마켓별 호가 정책 정보를 조회합니다.

**파라미터**:
- `markets` (string, required): 마켓 코드 (쉼표로 구분)

**응답 예시**:
```json
[
  {
    "market": "KRW-BTC",
    "order_sides": ["ask", "bid"],
    "price_unit_policies": [
      {
        "range_from": "0",
        "range_to": "10",
        "tick_size": "0.01"
      },
      {
        "range_from": "10",
        "range_to": "100",
        "tick_size": "0.1"
      },
      {
        "range_from": "2000000",
        "range_to": null,
        "tick_size": "1000"
      }
    ]
  }
]
```

**응답 필드 설명**:
- `market`: 마켓 코드
- `order_sides`: 지원하는 주문 방향
- `price_unit_policies`: 호가 단위 정책 배열
  - `range_from`: 가격 구간 시작
  - `range_to`: 가격 구간 종료 (null이면 무제한)
  - `tick_size`: 해당 구간의 호가 단위

### 6.2 Exchange API (인증 필요)

#### 6.2.1 자산(Asset)

**계정 잔고 조회**
```
GET /v1/accounts
```

응답 예시:
```json
[
  {
    "currency": "KRW",
    "balance": "1000000.0",
    "locked": "0.0",
    "avg_buy_price": "0",
    "avg_buy_price_modified": false,
    "unit_currency": "KRW"
  },
  {
    "currency": "BTC",
    "balance": "0.5",
    "locked": "0.1",
    "avg_buy_price": "45000000",
    "avg_buy_price_modified": false,
    "unit_currency": "KRW"
  }
]
```

#### 6.2.2 주문(Order)

**주문 가능 정보 조회**
```
GET /v1/orders/chance
```

파라미터:
- `market` (string, required): 마켓 코드

응답 예시:
```json
{
  "bid_fee": "0.0005",
  "ask_fee": "0.0005",
  "maker_bid_fee": "0.0005",
  "maker_ask_fee": "0.0005",
  "market": {
    "id": "KRW-BTC",
    "name": "BTC/KRW",
    "order_types": ["limit", "price", "market"],
    "order_sides": ["ask", "bid"],
    "bid": {
      "currency": "KRW",
      "price_unit": null,
      "min_total": "5000"
    },
    "ask": {
      "currency": "BTC",
      "price_unit": null,
      "min_total": "5000"
    },
    "max_total": "1000000000",
    "state": "active"
  },
  "bid_account": {
    "currency": "KRW",
    "balance": "1000000.0",
    "locked": "0.0",
    "avg_buy_price": "0",
    "avg_buy_price_modified": false,
    "unit_currency": "KRW"
  },
  "ask_account": {
    "currency": "BTC",
    "balance": "0.5",
    "locked": "0.1",
    "avg_buy_price": "45000000",
    "avg_buy_price_modified": false,
    "unit_currency": "KRW"
  }
}
```

**주문 생성**
```
POST /v1/orders
```

실제 주문을 생성합니다.

**요청 본문**:
```json
{
  "market": "KRW-BTC",
  "side": "bid",
  "volume": "0.01",
  "price": "50000000",
  "ord_type": "limit",
  "identifier": "custom-order-id"
}
```

**파라미터**:
- `market` (string, required): 마켓 코드 (예: KRW-BTC)
- `side` (string, required): 주문 종류
  - `bid`: 매수
  - `ask`: 매도
- `ord_type` (string, required): 주문 타입
  - `limit`: 지정가 주문
  - `price`: 시장가 주문 (매수, 금액 기준)
  - `market`: 시장가 주문 (매도, 수량 기준)
  - `best`: 최유리 주문
- `volume` (string, conditional): 주문 수량
  - 지정가 주문 시 필수
  - 시장가 매도 주문 시 필수
- `price` (string, conditional): 주문 가격
  - 지정가 주문 시 필수
  - 시장가 매수 주문 시 필수 (총 주문 금액)
- `identifier` (string, optional): 사용자 정의 주문 식별자
  - 최대 40자
  - 한번 사용된 값은 재사용 불가
  - 2024-10-18 이후 지원
- `time_in_force` (string, optional): 주문 유효 시간 정책
  - `ioc`: Immediate or Cancel (즉시 체결 또는 취소)
  - `fok`: Fill or Kill (전량 체결 또는 취소)
  - `post_only`: Post Only (Maker 주문만 허용, `ord_type=limit`일 때만 사용 가능)
  - 주의: `post_only`는 `smp_type`과 함께 사용 불가
- `smp_type` (string, optional): Self-Match Prevention (자전거래 방지)
  - `cancel_maker`: Maker 주문 취소
  - `cancel_taker`: Taker 주문 취소
  - `reduce`: 수량 감소

**Rate Limit**: 초당 8회 (계정 단위)

**응답 예시**:
```json
{
  "uuid": "cdd92199-2897-4e14-9448-f923320408ad",
  "side": "bid",
  "ord_type": "limit",
  "price": "50000000.0",
  "state": "wait",
  "market": "KRW-BTC",
  "created_at": "2024-01-01T00:00:00+09:00",
  "volume": "0.01",
  "remaining_volume": "0.01",
  "reserved_fee": "25.0",
  "remaining_fee": "25.0",
  "paid_fee": "0.0",
  "locked": "500025.0",
  "executed_volume": "0.0",
  "trades_count": 0,
  "identifier": "custom-order-id"
}
```

**응답 필드 설명**:
- `uuid`: 주문 UUID
- `side`: 주문 종류 (bid/ask)
- `ord_type`: 주문 타입
- `price`: 주문 가격
- `state`: 주문 상태
  - `wait`: 체결 대기
  - `watch`: 예약 주문 대기
  - `done`: 전체 체결 완료
  - `cancel`: 주문 취소
- `market`: 마켓 코드
- `created_at`: 주문 생성 시각
- `volume`: 주문 수량
- `remaining_volume`: 미체결 수량
- `reserved_fee`: 수수료로 예약된 금액
- `remaining_fee`: 남은 수수료
- `paid_fee`: 지불된 수수료
- `locked`: 묶여있는 금액
- `executed_volume`: 체결된 수량
- `trades_count`: 체결 건수
- `identifier`: 사용자 정의 식별자

**주문 생성 테스트**
```
POST /v1/orders/test
```

실제 주문을 생성하지 않고 주문 가능 여부만 테스트합니다. 잔고가 차감되지 않습니다.

**요청 본문**: 주문 생성 API와 동일

**응답**: 주문 생성 API와 동일한 형식이지만 실제 주문은 생성되지 않음

**Rate Limit**: 초당 8회 (계정 단위, 주문 그룹 공유)

**개별 주문 조회**
```
GET /v1/order
```

특정 주문의 상세 정보를 조회합니다.

**파라미터**:
- `uuid` (string, conditional): 주문 UUID
- `identifier` (string, conditional): 사용자 정의 주문 식별자
- 둘 중 하나는 필수

**Rate Limit**: 초당 30회 (계정 단위)

**주문 목록 조회**
```
GET /v1/orders
```

주문 목록을 조회합니다. 다양한 필터 조건을 지원합니다.

**파라미터**:
- `market` (string, optional): 마켓 코드
- `state` (string, optional): 주문 상태
  - `wait`: 체결 대기
  - `watch`: 예약 주문
  - `done`: 전체 체결 완료
  - `cancel`: 주문 취소
  - `states[]`와 동시 사용 불가
- `states` (array, optional): 주문 상태 배열 (여러 상태 동시 조회)
  - `state`와 동시 사용 불가
- `uuids` (array, optional): 주문 UUID 배열
- `identifiers` (array, optional): 사용자 정의 식별자 배열
- `limit` (integer, optional): 페이지당 개수 (최대 100, 기본값: 100)

**⚠️ 지원되지 않는 파라미터**:
- ~~`page`~~: 문서에 명시되어 있으나 실제로는 작동하지 않음
- ~~`order_by`~~: 문서에 명시되어 있으나 실제로는 작동하지 않음

**Rate Limit**: 초당 30회 (계정 단위)

**참고**: 페이지네이션이 필요한 경우 `/v1/orders/closed` 엔드포인트 사용 권장

**id로 주문 목록 조회**
```
GET /v1/orders/uuids
```

UUID 또는 identifier 배열로 여러 주문을 한 번에 조회합니다.

**파라미터**:
- `uuids` (array, optional): 주문 UUID 배열 (최대 100개)
- `identifiers` (array, optional): 사용자 정의 식별자 배열 (최대 100개)
- 둘 중 하나는 필수

**Rate Limit**: 초당 30회 (계정 단위)

**체결 대기 주문 목록 조회**
```
GET /v1/orders/open
```

미체결 주문(wait, watch 상태)만 조회합니다.

**파라미터**:
- `market` (string, optional): 마켓 코드
- `page` (integer, optional): 페이지 번호
- `limit` (integer, optional): 페이지당 개수 (최대 100)
- `order_by` (string, optional): 정렬 기준 (asc/desc)

**Rate Limit**: 초당 30회 (계정 단위)

**종료 주문 목록 조회**
```
GET /v1/orders/closed
```

종료된 주문(done, cancel 상태)만 조회합니다.

**파라미터**:
- `market` (string, optional): 마켓 코드
- `state` (string, optional): 주문 상태 (done 또는 cancel)
  - `states[]`와 동시 사용 불가 (둘 중 하나만 사용)
- `states` (array, optional): 주문 상태 배열 (예: `['done','cancel']`)
  - `state`와 동시 사용 불가 (둘 중 하나만 사용)
- `start_time` (string, optional): 조회 시작 시각
  - **ISO 8601 형식** (예: `2024-03-13T00:00:00+09:00`) 또는
  - **밀리초 타임스탬프** (13자리, 예: `1710259200000`) 지원
  - ⚠️ **주의**: ISO 8601 형식 사용 시 JWT 쿼리 서명 오류 발생 가능 → **밀리초 타임스탬프 사용 권장**
- `end_time` (string, optional): 조회 종료 시각
  - **ISO 8601 형식** 또는 **밀리초 타임스탬프** 지원
  - ⚠️ **주의**: `start_time`과 함께 사용 시 **최대 7일 범위**만 조회 가능
  - 하나만 지정 시 해당 시점으로부터 ±7일 범위로 해석됨
- `limit` (integer, optional): 조회 개수 (최대 1000, 기본값: 100)
- `order_by` (string, optional): 정렬 기준
  - `asc`: 오름차순 (과거순)
  - `desc`: 내림차순 (최신순, 기본값)

**기본 동작** (파라미터 없이 호출 시):
- 최근 7일 이내의 종료된 주문(done, cancel)
- 최신순 정렬(desc)
- 최대 100개 반환

**Rate Limit**: 초당 30회 (계정 단위)

**중요 제약사항**:
1. `state`와 `states[]`는 동시에 사용할 수 없음
2. `start_time` + `end_time` 조합 시 최대 7일 범위 제한
3. ISO 8601 형식보다 **밀리초 타임스탬프 사용 권장** (JWT 서명 오류 방지)
4. 장기간 데이터 조회 시 7일 단위로 분할하여 요청 필요

**개별 주문 취소**
```
DELETE /v1/order
```

특정 주문을 취소합니다.

**파라미터**:
- `uuid` (string, conditional): 주문 UUID
- `identifier` (string, conditional): 사용자 정의 주문 식별자
- 둘 중 하나는 필수

**Rate Limit**: 초당 8회 (계정 단위, 주문 그룹 공유)

**id로 주문 목록 취소**
```
DELETE /v1/orders/uuids
```

여러 주문을 한 번에 취소합니다.

**파라미터**:
- `uuids` (array, optional): 주문 UUID 배열 (최대 100개)
- `identifiers` (array, optional): 사용자 정의 식별자 배열 (최대 100개)
- 둘 중 하나는 필수

**Rate Limit**: 초당 8회 (계정 단위, 주문 그룹 공유)

**주문 일괄 취소**
```
DELETE /v1/orders/open
```

미체결 주문을 일괄 취소합니다.

**파라미터**:
- `market` (string, optional): 마켓 코드
  - 지정 시: 해당 마켓의 미체결 주문만 취소
  - 미지정 시: 전체 마켓의 미체결 주문 취소

**Rate Limit**: 2초당 1회 (계정 단위)

**주의사항**:
- 이 API는 매우 강력한 기능이므로 신중하게 사용해야 합니다
- Rate Limit이 매우 엄격하므로 남용하지 마세요

**취소 후 재주문**
```
POST /v1/orders/cancel_and_new
```

기존 주문을 취소하고 새로운 주문을 생성합니다. 두 작업이 원자적(atomic)으로 처리됩니다.

**요청 본문**:
```json
{
  "prev_order_uuid": "existing-order-uuid",
  "market": "KRW-BTC",
  "side": "bid",
  "volume": "0.01",
  "price": "51000000",
  "ord_type": "limit",
  "identifier": "new-order-id"
}
```

**파라미터**:
- `prev_order_uuid` (string, conditional): 취소할 주문의 UUID
- `prev_order_identifier` (string, conditional): 취소할 주문의 사용자 정의 식별자
- 둘 중 하나는 필수
- `market` (string, required): 마켓 코드
- `side` (string, required): 주문 종류 (bid/ask)
- `ord_type` (string, required): 주문 타입 (limit/price/market/best)
- `volume` (string, conditional): 주문 수량
- `price` (string, conditional): 주문 가격
- `identifier` (string, optional): 새 주문의 사용자 정의 식별자
- `time_in_force` (string, optional): 주문 유효 시간 정책
- `smp_type` (string, optional): Self-Match Prevention 타입

**Rate Limit**: 초당 8회 (계정 단위, 주문 그룹 공유)

**응답 예시**:
```json
{
  "uuid": "new-order-uuid",
  "side": "bid",
  "ord_type": "limit",
  "price": "51000000.0",
  "state": "wait",
  "market": "KRW-BTC",
  "created_at": "2024-01-01T00:00:00+09:00",
  "volume": "0.01",
  "remaining_volume": "0.01",
  "reserved_fee": "25.5",
  "remaining_fee": "25.5",
  "paid_fee": "0.0",
  "locked": "510025.5",
  "executed_volume": "0.0",
  "trades_count": 0,
  "identifier": "new-order-id"
}
```

**주의사항**:
- 이전 주문이 취소되지 않으면 새 주문도 생성되지 않습니다
- 이전 주문이 이미 체결 중이거나 완료된 경우 에러가 발생합니다
- 취소와 생성이 원자적으로 처리되므로 중간 상태가 발생하지 않습니다

#### 6.2.3 출금(Withdrawal)

**출금 가능 정보 조회**
```
GET /v1/withdraws/chance
```

특정 화폐의 출금 가능 정보를 조회합니다.

**파라미터**:
- `currency` (string, required): 화폐 코드 (예: BTC, ETH, KRW)

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
{
  "member_level": {
    "security_level": 3,
    "fee_level": 0,
    "email_verified": true,
    "identity_auth_verified": true,
    "bank_account_verified": true,
    "kakao_pay_auth_verified": false,
    "locked": false,
    "wallet_locked": false
  },
  "currency": {
    "code": "BTC",
    "withdraw_fee": "0.0005",
    "is_coin": true,
    "wallet_state": "working",
    "wallet_support": ["deposit", "withdraw"]
  },
  "account": {
    "currency": "BTC",
    "balance": "10.0",
    "locked": "3.5",
    "avg_buy_price": "8042000",
    "avg_buy_price_modified": false,
    "unit_currency": "KRW"
  },
  "withdraw_limit": {
    "currency": "BTC",
    "minimum": null,
    "onetime": null,
    "daily": "10.0"
  }
}
```

**응답 필드 설명**:
- `member_level`: 회원 등급 정보
  - `security_level`: 보안 등급
  - `fee_level`: 수수료 등급
  - `email_verified`: 이메일 인증 여부
  - `identity_auth_verified`: 실명 인증 여부
  - `bank_account_verified`: 계좌 인증 여부
  - `locked`: 계정 잠금 여부
  - `wallet_locked`: 지갑 잠금 여부
- `currency`: 화폐 정보
  - `code`: 화폐 코드
  - `withdraw_fee`: 출금 수수료
  - `is_coin`: 디지털 자산 여부
  - `wallet_state`: 지갑 상태 (working, withdraw_only, deposit_only, paused, unsupported)
  - `wallet_support`: 지원 기능 배열
- `account`: 계좌 정보
- `withdraw_limit`: 출금 한도
  - `minimum`: 최소 출금 금액
  - `onetime`: 1회 출금 한도
  - `daily`: 1일 출금 한도

**출금 허용 주소 목록 조회**
```
GET /v1/withdraws/coin_addresses
```

등록된 출금 허용 주소 목록을 조회합니다.

**파라미터**: 없음

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
[
  {
    "currency": "BTC",
    "withdraw_address": "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
    "secondary_address": null
  },
  {
    "currency": "XRP",
    "withdraw_address": "rN7n7otQDd6FczFgLdSqtcsAUxDkw6fzRH",
    "secondary_address": "123456789"
  }
]
```

**디지털 자산 출금 요청**
```
POST /v1/withdraws/coin
```

디지털 자산 출금을 요청합니다.

**요청 본문**:
```json
{
  "currency": "BTC",
  "amount": "0.1",
  "address": "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
  "secondary_address": "",
  "transaction_type": "default"
}
```

**파라미터**:
- `currency` (string, required): 화폐 코드
- `amount` (string, required): 출금 수량
- `address` (string, required): 출금 주소
- `secondary_address` (string, optional): 2차 주소 (XRP, XLM, EOS, STEEM 등)
- `transaction_type` (string, optional): 출금 유형
  - `default`: 일반 출금 (기본값)
  - `internal`: 업비트 회원간 출금

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
{
  "type": "withdraw",
  "uuid": "9f432943-54e0-40b7-825f-b6fec8b42b79",
  "currency": "BTC",
  "txid": null,
  "state": "processing",
  "created_at": "2024-01-01T00:00:00+09:00",
  "done_at": null,
  "amount": "0.1",
  "fee": "0.0005",
  "transaction_type": "default"
}
```

**원화 출금 요청**
```
POST /v1/withdraws/krw
```

원화 출금을 요청합니다.

**요청 본문**:
```json
{
  "amount": "10000"
}
```

**파라미터**:
- `amount` (string, required): 출금 금액

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
{
  "type": "withdraw",
  "uuid": "9f432943-54e0-40b7-825f-b6fec8b42b79",
  "currency": "KRW",
  "txid": "xxxxxxxx",
  "state": "processing",
  "created_at": "2024-01-01T00:00:00+09:00",
  "done_at": null,
  "amount": "10000",
  "fee": "0",
  "transaction_type": "default"
}
```

**개별 출금 조회**
```
GET /v1/withdraw
```

특정 출금의 상세 정보를 조회합니다.

**파라미터**:
- `uuid` (string, conditional): 출금 UUID
- `txid` (string, conditional): 트랜잭션 ID
- `currency` (string, conditional): 화폐 코드
- 셋 중 하나는 필수

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
{
  "type": "withdraw",
  "uuid": "9f432943-54e0-40b7-825f-b6fec8b42b79",
  "currency": "BTC",
  "txid": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "state": "done",
  "created_at": "2024-01-01T00:00:00+09:00",
  "done_at": "2024-01-01T00:10:00+09:00",
  "amount": "0.1",
  "fee": "0.0005",
  "transaction_type": "default"
}
```

**응답 필드 설명**:
- `type`: 입출금 타입 (withdraw)
- `uuid`: 출금 UUID
- `currency`: 화폐 코드
- `txid`: 트랜잭션 ID (블록체인 해시)
- `state`: 출금 상태
  - `processing`: 처리 중
  - `done`: 완료
  - `failed`: 실패
  - `cancelled`: 취소됨
  - `rejected`: 거부됨
- `created_at`: 출금 신청 시각
- `done_at`: 출금 완료 시각
- `amount`: 출금 금액/수량
- `fee`: 출금 수수료
- `transaction_type`: 출금 유형

**출금 목록 조회**
```
GET /v1/withdraws
```

출금 목록을 조회합니다.

**파라미터**:
- `currency` (string, optional): 화폐 코드
- `state` (string, optional): 출금 상태
- `uuids` (array, optional): 출금 UUID 배열
- `txids` (array, optional): 트랜잭션 ID 배열
- `limit` (integer, optional): 페이지당 개수 (최대 100, 기본값 100)
- `page` (integer, optional): 페이지 번호 (기본값 1)
- `order_by` (string, optional): 정렬 기준 (asc/desc, 기본값 desc)

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
[
  {
    "type": "withdraw",
    "uuid": "9f432943-54e0-40b7-825f-b6fec8b42b79",
    "currency": "BTC",
    "txid": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "state": "done",
    "created_at": "2024-01-01T00:00:00+09:00",
    "done_at": "2024-01-01T00:10:00+09:00",
    "amount": "0.1",
    "fee": "0.0005",
    "transaction_type": "default"
  }
]
```

**디지털 자산 출금 취소 요청**
```
DELETE /v1/withdraws/coin
```

대기 중인 디지털 자산 출금을 취소합니다.

**파라미터**:
- `uuid` (string, required): 출금 UUID

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
{
  "type": "withdraw",
  "uuid": "9f432943-54e0-40b7-825f-b6fec8b42b79",
  "currency": "BTC",
  "txid": null,
  "state": "cancelled",
  "created_at": "2024-01-01T00:00:00+09:00",
  "done_at": "2024-01-01T00:05:00+09:00",
  "amount": "0.1",
  "fee": "0.0005",
  "transaction_type": "default"
}
```

**주의사항**:
- 출금 주소는 사전에 업비트 웹사이트에서 등록해야 합니다
- `processing` 상태의 출금만 취소 가능합니다
- 원화 출금은 취소가 불가능합니다

#### 6.2.4 입금(Deposit)

**디지털 자산 입금 가능 정보 조회**
```
GET /v1/deposits/coin_addresses
```

전체 디지털 자산의 입금 주소 목록을 조회합니다.

**파라미터**: 없음

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
[
  {
    "currency": "BTC",
    "deposit_address": "3EusRwybuZUhVDeHL7gh3HSLmbhLcy7NqD",
    "secondary_address": null
  },
  {
    "currency": "XRP",
    "deposit_address": "rN7n7otQDd6FczFgLdSqtcsAUxDkw6fzRH",
    "secondary_address": "123456789"
  }
]
```

**입금 주소 생성 요청**
```
POST /v1/deposits/generate_coin_address
```

새로운 입금 주소를 생성합니다.

**요청 본문**:
```json
{
  "currency": "BTC"
}
```

**파라미터**:
- `currency` (string, required): 화폐 코드

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
{
  "success": true,
  "message": "BTC 입금주소를 생성중입니다."
}
```

**주의사항**:
- 입금 주소 생성은 비동기로 처리됩니다
- 생성 완료까지 수 분이 소요될 수 있습니다
- 생성 완료 후 개별 입금 주소 조회 API로 확인 가능합니다

**개별 입금 주소 조회**
```
GET /v1/deposits/coin_address
```

특정 화폐의 입금 주소를 조회합니다.

**파라미터**:
- `currency` (string, required): 화폐 코드

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
{
  "currency": "BTC",
  "deposit_address": "3EusRwybuZUhVDeHL7gh3HSLmbhLcy7NqD",
  "secondary_address": null
}
```

**응답 필드 설명**:
- `currency`: 화폐 코드
- `deposit_address`: 입금 주소
- `secondary_address`: 2차 주소 (XRP, XLM, EOS, STEEM 등의 경우 Destination Tag/Memo)

**입금 주소 목록 조회**
```
GET /v1/deposits/coin_addresses
```

전체 디지털 자산의 입금 주소 목록을 조회합니다. (디지털 자산 입금 가능 정보 조회와 동일)

**원화 입금**
```
POST /v1/deposits/krw
```

원화 입금을 신청합니다.

**요청 본문**:
```json
{
  "amount": "10000"
}
```

**파라미터**:
- `amount` (string, required): 입금 금액

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
{
  "type": "deposit",
  "uuid": "94332e99-3a87-4a35-ad98-28b0c969f830",
  "currency": "KRW",
  "txid": "xxxxxxxx",
  "state": "processing",
  "created_at": "2024-01-01T00:00:00+09:00",
  "done_at": null,
  "amount": "10000",
  "fee": "0",
  "transaction_type": "default"
}
```

**개별 입금 조회**
```
GET /v1/deposit
```

특정 입금의 상세 정보를 조회합니다.

**파라미터**:
- `uuid` (string, conditional): 입금 UUID
- `txid` (string, conditional): 트랜잭션 ID
- `currency` (string, conditional): 화폐 코드
- 셋 중 하나는 필수

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
{
  "type": "deposit",
  "uuid": "94332e99-3a87-4a35-ad98-28b0c969f830",
  "currency": "BTC",
  "txid": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "state": "accepted",
  "created_at": "2024-01-01T00:00:00+09:00",
  "done_at": "2024-01-01T00:10:00+09:00",
  "amount": "0.1",
  "fee": "0",
  "transaction_type": "default",
  "confirmations": 6
}
```

**응답 필드 설명**:
- `type`: 입출금 타입 (deposit)
- `uuid`: 입금 UUID
- `currency`: 화폐 코드
- `txid`: 트랜잭션 ID (블록체인 해시)
- `state`: 입금 상태
  - `submitting`: 처리 중
  - `submitted`: 처리 완료
  - `almost_accepted`: 입금 대기 중
  - `accepted`: 입금 완료
  - `rejected`: 거부됨
  - `cancelled`: 취소됨
  - `travel_rule_suspected`: 트래블룰 검증 필요
  - `refunding`: 환불 진행 중
  - `refunded`: 환불 완료
- `created_at`: 입금 신청 시각
- `done_at`: 입금 완료 시각
- `amount`: 입금 금액/수량
- `fee`: 입금 수수료
- `transaction_type`: 입금 유형
- `confirmations`: 블록체인 컨펌 수

**입금 목록 조회**
```
GET /v1/deposits
```

입금 목록을 조회합니다.

**파라미터**:
- `currency` (string, optional): 화폐 코드
- `state` (string, optional): 입금 상태
- `uuids` (array, optional): 입금 UUID 배열
- `txids` (array, optional): 트랜잭션 ID 배열
- `limit` (integer, optional): 페이지당 개수 (최대 100, 기본값 100)
- `page` (integer, optional): 페이지 번호 (기본값 1)
- `order_by` (string, optional): 정렬 기준 (asc/desc, 기본값 desc)

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
[
  {
    "type": "deposit",
    "uuid": "94332e99-3a87-4a35-ad98-28b0c969f830",
    "currency": "BTC",
    "txid": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "state": "accepted",
    "created_at": "2024-01-01T00:00:00+09:00",
    "done_at": "2024-01-01T00:10:00+09:00",
    "amount": "0.1",
    "fee": "0",
    "transaction_type": "default"
  }
]
```

##### 트래블룰 검증

**계정주 확인 서비스 지원 거래소 목록 조회**
```
GET /v1/deposits/travel_rule_vasp_list
```

트래블룰 검증을 지원하는 거래소(VASP) 목록을 조회합니다.

**파라미터**: 없음

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
[
  {
    "vasp_name": "Binance",
    "vasp_code": "BINANCE"
  },
  {
    "vasp_name": "Coinbase",
    "vasp_code": "COINBASE"
  }
]
```

**입금 UUID로 계정주 검증 요청**
```
POST /v1/deposits/travel_rule_verification
```

입금 UUID를 사용하여 계정주 검증을 요청합니다.

**요청 본문**:
```json
{
  "deposit_uuid": "94332e99-3a87-4a35-ad98-28b0c969f830"
}
```

**파라미터**:
- `deposit_uuid` (string, required): 입금 UUID

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
{
  "success": true,
  "message": "계정주 검증이 요청되었습니다."
}
```

**입금 TxID로 계정주 검증 요청**
```
POST /v1/deposits/travel_rule_verification_by_txid
```

트랜잭션 ID를 사용하여 계정주 검증을 요청합니다.

**요청 본문**:
```json
{
  "txid": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "currency": "BTC"
}
```

**파라미터**:
- `txid` (string, required): 트랜잭션 ID
- `currency` (string, required): 화폐 코드

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
{
  "success": true,
  "message": "계정주 검증이 요청되었습니다."
}
```

**트래블룰 검증 주의사항**:
- 트래블룰은 자금세탁방지(AML) 규제의 일환입니다
- 특정 금액 이상의 디지털 자산 입금 시 송금인 정보 확인이 필요할 수 있습니다
- `travel_rule_suspected` 상태의 입금은 검증 완료 후 `accepted`로 변경됩니다
- 검증이 완료되지 않으면 입금이 환불될 수 있습니다

#### 6.2.5 서비스 정보(Service)

**입출금 서비스 상태 조회**
```
GET /v1/status/wallet
```

디지털 자산별 입출금 서비스 상태를 조회합니다.

**파라미터**: 없음

**Rate Limit**: 초당 30회 (IP 단위)

**응답 예시**:
```json
[
  {
    "currency": "BTC",
    "wallet_state": "working",
    "block_state": "normal",
    "block_height": 700000,
    "block_updated_at": "2024-01-01T00:00:00+09:00"
  },
  {
    "currency": "ETH",
    "wallet_state": "withdraw_only",
    "block_state": "delayed",
    "block_height": 15000000,
    "block_updated_at": "2024-01-01T00:00:00+09:00"
  }
]
```

**응답 필드 설명**:
- `currency`: 화폐 코드
- `wallet_state`: 지갑 상태
  - `working`: 정상 (입출금 모두 가능)
  - `withdraw_only`: 출금만 가능
  - `deposit_only`: 입금만 가능
  - `paused`: 일시 중지
  - `unsupported`: 지원 종료
- `block_state`: 블록 상태
  - `normal`: 정상
  - `delayed`: 지연
  - `inactive`: 비활성
- `block_height`: 현재 블록 높이
- `block_updated_at`: 블록 정보 업데이트 시각

**API Key 목록 조회**
```
GET /v1/api_keys
```

보유 중인 API Key 목록을 조회합니다.

**파라미터**: 없음

**Rate Limit**: 초당 30회 (계정 단위)

**응답 예시**:
```json
[
  {
    "access_key": "xxxxxxxxxxxxxxxxxxxxxxxx",
    "expire_at": "2024-12-31T23:59:59+09:00"
  }
]
```

**응답 필드 설명**:
- `access_key`: Access Key
- `expire_at`: 만료 시각

**주의사항**:
- Secret Key는 조회되지 않습니다
- Secret Key는 발급 시에만 확인 가능하므로 안전하게 보관해야 합니다

---

## 7. 주요 WebSocket 구독 항목

### 7.1 현재가 (Ticker)

**요청 예시**:
```json
[
  {"ticket": "test-ticker"},
  {"type": "ticker", "codes": ["KRW-BTC", "KRW-ETH"]},
  {"format": "SIMPLE"}
]
```

**응답 필드 (SIMPLE 포맷)**:
```json
{
  "ty": "ticker",
  "cd": "KRW-BTC",
  "op": 50000000,
  "hp": 51000000,
  "lp": 49000000,
  "tp": 50500000,
  "pcp": 49500000,
  "c": "RISE",
  "cp": 1000000,
  "cr": 0.0202020202,
  "scp": 1000000,
  "scr": 0.0202020202,
  "tv": 0.1,
  "atv": 2500,
  "atv24h": 5000,
  "atp": 123456789000,
  "atp24h": 234567890000,
  "tdt": "20240101",
  "ttm": "000000",
  "ttms": 1704067200000,
  "tms": 1704067200000,
  "st": "SNAPSHOT"
}
```

**주요 필드 설명**:
- `ty` (type): 데이터 타입
- `cd` (code): 마켓 코드
- `op` (opening_price): 시가
- `hp` (high_price): 고가
- `lp` (low_price): 저가
- `tp` (trade_price): 현재가
- `pcp` (prev_closing_price): 전일 종가
- `c` (change): 전일 대비 (`RISE`, `EVEN`, `FALL`)
- `cp` (change_price): 전일 대비 가격
- `cr` (change_rate): 전일 대비 등락률
- `atv24h` (acc_trade_volume_24h): 24시간 누적 거래량
- `atp24h` (acc_trade_price_24h): 24시간 누적 거래대금
- `st` (stream_type): `SNAPSHOT` 또는 `REALTIME`

### 7.2 체결 (Trade)

**요청 예시**:
```json
[
  {"ticket": "test-trade"},
  {"type": "trade", "codes": ["KRW-BTC"]}
]
```

**응답 필드**:
```json
{
  "type": "trade",
  "code": "KRW-BTC",
  "timestamp": 1704067200000,
  "trade_date": "2024-01-01",
  "trade_time": "00:00:00",
  "trade_timestamp": 1704067200000,
  "trade_price": 50000000,
  "trade_volume": 0.1,
  "ask_bid": "BID",
  "prev_closing_price": 49500000,
  "change": "RISE",
  "change_price": 500000,
  "sequential_id": 1234567890,
  "stream_type": "REALTIME"
}
```

### 7.3 호가 (Orderbook)

**요청 예시**:
```json
[
  {"ticket": "test-orderbook"},
  {"type": "orderbook", "codes": ["KRW-BTC"], "level": 5}
]
```

**응답 필드**:
```json
{
  "type": "orderbook",
  "code": "KRW-BTC",
  "timestamp": 1704067200000,
  "total_ask_size": 10.5,
  "total_bid_size": 12.3,
  "orderbook_units": [
    {
      "ask_price": 50100000,
      "bid_price": 50000000,
      "ask_size": 1.5,
      "bid_size": 2.3
    }
  ],
  "level": 5,
  "stream_type": "SNAPSHOT"
}
```

### 7.4 캔들 (Candle)

**요청 예시**:
```json
[
  {"ticket": "test-candle"},
  {"type": "candle.minute.1", "codes": ["KRW-BTC"]}
]
```

**지원 단위**:
- `candle.second.1`, `candle.second.3`, `candle.second.5`, `candle.second.10`, `candle.second.30`, `candle.second.60`
- `candle.minute.1`, `candle.minute.3`, `candle.minute.5`, `candle.minute.10`, `candle.minute.15`, `candle.minute.30`, `candle.minute.60`, `candle.minute.240`
- `candle.day`, `candle.week`, `candle.month`

**응답 필드**:
```json
{
  "type": "candle.minute.1",
  "code": "KRW-BTC",
  "opening_price": 50000000,
  "high_price": 51000000,
  "low_price": 49000000,
  "trade_price": 50500000,
  "timestamp": 1704067200000,
  "candle_acc_trade_price": 123456789.0,
  "candle_acc_trade_volume": 2.5,
  "candle_date_time_utc": "2024-01-01T00:00:00",
  "candle_date_time_kst": "2024-01-01T09:00:00",
  "stream_type": "SNAPSHOT"
}
```

### 7.5 내 주문 및 체결 (MyOrder)

**요청 예시** (Private WebSocket):
```json
[
  {"ticket": "test-myorder"},
  {"type": "myOrder"}
]
```

**응답 필드**:
```json
{
  "type": "myOrder",
  "order_uuid": "cdd92199-2897-4e14-9448-f923320408ad",
  "market": "KRW-BTC",
  "order_type": "limit",
  "order_side": "bid",
  "price": "50000000",
  "state": "wait",
  "created_at": "2024-01-01T00:00:00+09:00",
  "volume": "0.01",
  "remaining_volume": "0.01",
  "reserved_fee": "25.0",
  "remaining_fee": "25.0",
  "paid_fee": "0.0",
  "locked": "500025.0",
  "executed_volume": "0.0",
  "trade_count": 0,
  "identifier": "custom-order-id",
  "trade_uuid": "trade-uuid-here",
  "trade_timestamp": 1704067200000,
  "trade_price": "50000000",
  "trade_volume": "0.01",
  "trade_fee": "25.0",
  "is_maker": true,
  "timestamp": 1704067200000,
  "stream_type": "REALTIME"
}
```

**주요 필드 설명**:
- `order_uuid`: 주문 UUID
- `state`: 주문 상태 (`wait`, `watch`, `done`, `cancel`)
- `trade_uuid`: 체결 UUID (체결 발생 시)
- `trade_timestamp`: 체결 시각 (체결 발생 시)
- `is_maker`: Maker 여부
- `identifier`: 사용자 정의 식별자 (2024-10-18 이후 생성된 주문만)

### 7.6 내 자산 (MyAsset)

**요청 예시** (Private WebSocket):
```json
[
  {"ticket": "test-myasset"},
  {"type": "myAsset"}
]
```

**응답 필드**:
```json
{
  "type": "myAsset",
  "asset_uuid": "asset-uuid-here",
  "assets": [
    {
      "currency": "KRW",
      "balance": "1000000.0",
      "locked": "0.0",
      "avg_buy_price": "0",
      "avg_buy_price_modified": false,
      "unit_currency": "KRW"
    },
    {
      "currency": "BTC",
      "balance": "0.5",
      "locked": "0.1",
      "avg_buy_price": "45000000",
      "avg_buy_price_modified": false,
      "unit_currency": "KRW"
    }
  ],
  "asset_timestamp": 1704067200000,
  "timestamp": 1704067200000,
  "stream_type": "SNAPSHOT"
}
```

### 7.7 구독 중인 스트림 목록 조회

**요청 예시**:
```json
"LIST_SUBSCRIPTIONS"
```

**응답 예시**:
```json
{
  "type": "LIST_SUBSCRIPTIONS",
  "subscriptions": [
    {
      "type": "ticker",
      "codes": ["KRW-BTC", "KRW-ETH"]
    },
    {
      "type": "trade",
      "codes": ["KRW-BTC"]
    }
  ]
}
```

---

## 8. DEPRECATED API

### 8.1 호가 모아보기 단위 조회

```
GET /v1/orderbook/units
```

**상태**: DEPRECATED (지원 종료 예정)

**대체 API**: `/v1/orderbook/policy` 사용 권장

이 API는 호가 단위 정보를 조회하는 기능이었으나, 현재는 `/v1/orderbook/policy` API로 대체되었습니다.

**파라미터**:
- `markets` (string, required): 마켓 코드 (쉼표로 구분)

**기존 응답 예시**:
```json
[
  {
    "market": "KRW-BTC",
    "orderbook_units": [
      {
        "unit": "1000"
      }
    ]
  }
]
```

**마이그레이션 가이드**:

기존 코드:
```python
# DEPRECATED
response = requests.get('https://api.upbit.com/v1/orderbook/units?markets=KRW-BTC')
```

새로운 코드:
```python
# 권장
response = requests.get('https://api.upbit.com/v1/orderbook/policy?markets=KRW-BTC')
```

새로운 API는 더 상세한 호가 단위 정책 정보를 제공합니다:
- 가격 구간별 호가 단위 (`price_unit_policies`)
- 각 구간의 시작/종료 가격 (`range_from`, `range_to`)
- 해당 구간의 호가 단위 (`tick_size`)

---

## 9. 마켓별 호가 단위 및 주문 정책

### 9.1 원화(KRW) 마켓

#### 호가 단위

원화 마켓의 호가 단위는 가격 구간에 따라 다릅니다:

| 가격 구간 | 호가 단위 |
|-----------|----------|
| 0원 이상 ~ 10원 미만 | 0.01원 |
| 10원 이상 ~ 100원 미만 | 0.1원 |
| 100원 이상 ~ 1,000원 미만 | 1원 |
| 1,000원 이상 ~ 10,000원 미만 | 5원 |
| 10,000원 이상 ~ 100,000원 미만 | 10원 |
| 100,000원 이상 ~ 500,000원 미만 | 50원 |
| 500,000원 이상 ~ 1,000,000원 미만 | 100원 |
| 1,000,000원 이상 ~ 2,000,000원 미만 | 500원 |
| 2,000,000원 이상 | 1,000원 |

#### 최소 주문 금액

- **최소 주문 금액**: 5,000원
- 주문 금액 = 주문 가격 × 주문 수량

#### 예시

```python
# 비트코인 가격이 50,000,000원일 때
price = 50000000  # 호가 단위: 1,000원
volume = 0.0001   # 최소 주문 금액: 5,000원

# 올바른 주문 가격
valid_prices = [50000000, 50001000, 50002000]  # 1,000원 단위

# 잘못된 주문 가격
invalid_prices = [50000500, 50000100]  # 호가 단위 미준수
```

### 9.2 USDT 마켓

#### 호가 단위

USDT 마켓의 호가 단위는 디지털 자산 가격 구간에 따라 다릅니다:

| 가격 구간 | 호가 단위 |
|-----------|----------|
| 0 USDT 이상 ~ 0.1 USDT 미만 | 0.00001 USDT |
| 0.1 USDT 이상 ~ 1 USDT 미만 | 0.0001 USDT |
| 1 USDT 이상 ~ 10 USDT 미만 | 0.001 USDT |
| 10 USDT 이상 ~ 100 USDT 미만 | 0.01 USDT |
| 100 USDT 이상 ~ 1,000 USDT 미만 | 0.1 USDT |
| 1,000 USDT 이상 | 1 USDT |

#### 최소 주문 금액

- **최소 주문 금액**: 0.5 USDT
- 주문 금액 = 주문 가격 × 주문 수량

### 9.3 BTC 마켓

#### 호가 단위

BTC 마켓의 호가 단위는 디지털 자산 가격 구간에 따라 다릅니다:

| 가격 구간 | 호가 단위 |
|-----------|----------|
| 0 BTC 이상 ~ 0.001 BTC 미만 | 0.00000001 BTC |
| 0.001 BTC 이상 ~ 0.01 BTC 미만 | 0.0000001 BTC |
| 0.01 BTC 이상 ~ 0.1 BTC 미만 | 0.000001 BTC |
| 0.1 BTC 이상 ~ 1 BTC 미만 | 0.00001 BTC |
| 1 BTC 이상 ~ 10 BTC 미만 | 0.0001 BTC |
| 10 BTC 이상 | 0.001 BTC |

#### 최소 주문 금액

- **최소 주문 금액**: 0.00005 BTC

---

## 10. 변경 이력 및 유의사항

### 10.1 주요 변경 사항

#### 2024-10-18
- **identifier 필드 추가**: 주문 생성 시 사용자 정의 식별자 지정 가능
- REST 주문 API 및 WebSocket myOrder 응답에 `identifier` 필드 포함
- 2024-10-18 이후 생성된 주문만 identifier 제공

#### 2024-08-07
- **WebSocket 구독 목록 조회 기능 추가**: `LIST_SUBSCRIPTIONS` 메시지로 현재 구독 중인 스트림 확인 가능

#### 2024-05-29
- **MyOrder WebSocket 응답 개선**: 체결 정보 추가
  - `trade_uuid`: 체결 UUID
  - `trade_timestamp`: 체결 시각
  - `is_maker`: Maker 여부
- **myTrade 스트림 지원 종료 예정**: myOrder 사용 권장

#### 2024-03-08
- **ticker WebSocket 응답 개선**: `acc_trade_price_24h`, `acc_trade_volume_24h` 필드가 null이 아닌 정상 값으로 제공

#### 2022-03-01
- **POST 요청 Form-urlencoded 방식 지원 종료**: JSON 형식만 지원

### 10.2 주요 유의사항

#### 인증 관련
- Secret Key는 발급 시 한 번만 표시되므로 안전하게 보관
- JWT 토큰의 nonce는 재사용 불가 (매 요청마다 새로운 UUID 생성)
- API Key 권한 설정 확인 (`out_of_scope` 에러 방지)
- 허용 IP 설정 시 해당 IP에서만 접근 가능

#### Rate Limit 관련
- 잔여 요청 수는 `Remaining-Req` 헤더로 확인
- 주문 일괄 취소는 2초당 1회로 제한되므로 주의
- Origin 헤더가 있는 요청은 10초당 1회로 제한 (브라우저 환경)

#### WebSocket 관련
- 120초 idle 시 자동 종료되므로 PING 메시지로 연결 유지
- Private WebSocket은 JWT 인증 필요
- myAsset 타입에는 codes 파라미터 사용 불가

#### 주문 관련
- 최소 주문 금액: 5,000원
- 주문 가격은 호가 단위에 맞춰야 함
- identifier는 최대 40자까지 지정 가능
- 2024-10-18 이전 주문은 identifier 필드 없음

#### 출금 관련
- 출금 주소는 사전에 등록 필요
- transaction_type: `default` (일반 출금), `internal` (업비트 내부 출금)

#### 기타
- TLS 1.2 이상 필수 (TLS 1.3 권장)
- 특수문자는 URL 인코딩 필수
- gzip 압축은 Quotation API에서만 지원

---

## 참고 자료

- [업비트 개발자 센터](https://docs.upbit.com/kr/reference/)
- [API 인증 가이드](https://docs.upbit.com/kr/reference/api-overview)
- [요청 수 제한 가이드](https://docs.upbit.com/kr/docs/user-request-guide)
- [에러 코드 목록](https://docs.upbit.com/kr/docs/api-%EC%A3%BC%EC%9A%94-%EC%97%90%EB%9F%AC-%EC%BD%94%EB%93%9C-%EB%AA%A9%EB%A1%9D)

---

**작성일**: 2026-01-17  
**API 버전**: v1.6.1
