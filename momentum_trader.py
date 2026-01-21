#!/usr/bin/env python3
"""
=================================================================================
🚀 달리는 말에 올라타는 모멘텀 트레이딩 봇 (Momentum Riding Strategy)
=================================================================================

전략 개요:
- 거시적 관점: 일/주/월 캔들로 전반적인 시장 추세 분석
- 미시적 관점: 분/초 캔들로 실시간 모멘텀 감지 및 진입
- 빠른 상승 시 시장가 매수, 지속적인 익절가 조정
- 하락 전환 시 빠른 익절/손절

핵심 원칙:
1. 전체 하락장에서는 관망
2. 상승 모멘텀 감지 시 빠르게 진입
3. 수익 보호를 위한 트레일링 스탑
4. 손실 최소화를 위한 빠른 손절
"""

import os
import sys
import time
import json
import uuid
import asyncio
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from collections import deque
from urllib.parse import urlencode, unquote

import jwt
import requests
import websockets
from dotenv import load_dotenv
import redis

class RedisLogHandler(logging.Handler):
    def __init__(self, redis_client, channel="[MON]:LOGS"):
        super().__init__()
        self.redis_client = redis_client
        self.channel = channel

    def emit(self, record):
        try:
            msg = self.format(record)
            if self.redis_client:
                self.redis_client.publish(self.channel, msg)
        except Exception:
            self.handleError(record)

# =================================================================================
# 📊 전략 파라미터 (Strategy Parameters) - 여기서 조절 가능
# =================================================================================

# === 투자 설정 ===
MAX_INVESTMENT = 1000000         # 최대 투자금 (원) - 1천만원으로 상향
MIN_ORDER_AMOUNT = 5_000            # 최소 주문 금액 (업비트 최소금액 5,000원 + 버퍼)
TRADING_FEE_RATE = 0.0005           # 거래 수수료 (0.05% = 0.0005)
REDIS_PASSWORD = "zC0ZQOY0oJPz/NQ/tqTkpG1OEEXHCx5gJrTLFCg1rlw="
TRADING_FEE_RATE = 0.0005           # 거래 수수료 (0.05% = 0.0005)

# === BTC 중심 시장 분석 (BTC-Centric Market Analysis) ===
BTC_MARKET = "KRW-BTC"              # 비트코인 마켓 (시장 중심 지표)
BTC_TREND_THRESHOLD = -0.005        # BTC 하락 임계값 (-0.5% 이하면 시장 위험)
BTC_BULLISH_THRESHOLD = 0.003       # BTC 상승 임계값 (+0.3% 이상이면 시장 안정)
BTC_CHECK_INTERVAL = 60             # BTC 추세 체크 주기 (초)
BTC_DOWNTREND_BUY_BLOCK = False      # BTC 하락 시 매수 금지 (True: 적용, False: 미적용)

# === 거시적 분석 (Macro Analysis) - 전체 시장 추세 ===
MACRO_LOOKBACK_DAYS = 7             # 일봉 분석 기간 (일)
MACRO_MIN_CHANGE_RATE = -0.015      # 전체 하락장 판단 기준 (-1.5% 이하면 관망) - 강화
MACRO_BULLISH_THRESHOLD = 0.015     # 상승장 판단 기준 (+1.5% 이상) - 강화
MACRO_UPDATE_INTERVAL = 300         # 거시 분석 갱신 주기 (초)

# === 미시적 분석 (Micro Analysis) - 진입 신호 (대폭 강화) ===
MOMENTUM_WINDOW = 20                # 모멘텀 계산 윈도우 (캔들 개수) - 20분으로 확대
MOMENTUM_THRESHOLD = 0.012          # 진입 모멘텀 기준 (1.2% 상승률) - 가짜 신호 필터링 강화
VOLUME_SPIKE_RATIO = 3.0            # 거래량 급등 배율 (평균 대비 3배) - 수급 확인 강화
CONSECUTIVE_UP_CANDLES = 6          # 연속 상승 캔들 개수 - 6개로 강화

# === 초봉 분석 (Second Candle Analysis) - 실시간 변화 감지 ===
SECOND_CANDLE_UNIT = 5              # 초봉 단위 (1, 3, 5, 10, 30, 60 중 선택)
SECOND_MOMENTUM_WINDOW = 15         # 초봉 모멘텀 윈도우 (개수) - 확대
SECOND_MOMENTUM_THRESHOLD = 0.002   # 초봉 모멘텀 기준 (0.2%) - 강화
SECOND_RAPID_RISE_THRESHOLD = 0.006 # 급등 판단 기준 (0.6%/5초) - 노이즈 제거 강화

# === 단타 전문가 기법 (Pro Scalping) 파라미터 ===
SHORT_TREND_WINDOW = 20             # 단기 추세 확인 (20분) - 확대
SHORT_MOMENTUM_THRESHOLD = 0.008    # 단기 급반등 기준 (20분 내 0.8% 이상) - 강화
VOL_INTENSITY_THRESHOLD = 2.5       # 수급 집중도 (평균 대비 2.5배 이상)
BREAKOUT_VELOCITY = 0.0015          # 분당 가격 가속도 (0.15%/min) - 강화

# === 다중 타임프레임 분석 (Multi-Timeframe Analysis) - 핵심 개선 ===
MTF_ENABLED = True                  # 다중 타임프레임 분석 활성화
MTF_5M_MIN_CANDLES = 24             # 5분봉 최소 필요 개수 (24개 = 2시간)
MTF_15M_MIN_CANDLES = 12            # 15분봉 최소 필요 개수 (12개 = 3시간)
MTF_5M_TREND_THRESHOLD = 0.002      # 5분봉 상승 추세 기준 (0.2%)
MTF_15M_TREND_THRESHOLD = 0.001     # 15분봉 상승/횡보 기준 (0.1%, 하락이 아니면 OK)
MTF_5M_EARLY_STAGE_MAX = 0.025      # 5분봉 상승 초기 단계 최대치 (2.5% 이하여야 초기)
MTF_VOLUME_CONFIRMATION = 1.5       # 5분봉 거래량 확인 배율 (평균 대비)
MTF_STRICT_MODE = True              # 엄격 모드 (15분봉 하락 시 무조건 차단)

# === 익절/손절 설정 (핵심 개선) ===
INITIAL_STOP_LOSS = 0.025           # 초기 손절선 (2.5%) - 빈번한 손절 방지
DYNAMIC_STOP_LOSS_ENABLED = True    # 동적 손절선 활성화 (변동성 기반)
DYNAMIC_STOP_LOSS_MIN = 0.018       # 동적 손절 최소 (1.8%)
DYNAMIC_STOP_LOSS_MAX = 0.035       # 동적 손절 최대 (3.5%)
TRAILING_STOP_ACTIVATION = 0.012    # 트레일링 스탑 활성화 기준 (+1.2% 수익 시) - 빨리 활성화
TRAILING_STOP_DISTANCE = 0.008      # 트레일링 스탑 거리 (0.8% - 고점 대비) - 타이트하게
TRAILING_MIN_PROFIT = 0.005         # 트레일링 시 최소 수익 보장 (0.5%)
TAKE_PROFIT_TARGET = 0.025          # 목표 수익률 (2.5% - 상향)
MAX_HOLDING_TIME = 21600            # 최대 보유 시간 (초, 6시간으로 연장)

# === 리스크 관리 (강화) ===
MAX_TRADES_PER_HOUR = 20             # 시간당 최대 거래 횟수 - 20회로 제한 (과거래 방지)
COOL_DOWN_AFTER_LOSS = 600          # 손절 후 대기 시간 (초) - 10분으로 강화
CONSECUTIVE_LOSS_COOLDOWN = 1200    # 연속 손절 시 추가 대기 (20분)
MIN_PRICE_STABILITY = 0.008         # 최소 가격 안정성 (급등락 필터) - 강화

# === 시스템 설정 ===
# MARKET: 빈 배열([]) 이면 거래대금 상위 TOP_MARKET_COUNT개 자동 선정
#         지정된 마켓이 있으면 해당 마켓만 트레이딩
# MARKET = ["KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-AXS" ]  # 빈 배열: 자동 선정, 예: ["KRW-BTC", "KRW-ETH"]
MARKET = [] 
MARKET_UPDATE_INTERVAL = 600        # 마켓 목록 갱신 주기 (10분) - 자동 모드에서만 사용
TOP_MARKET_COUNT = 20               # 거래대금 상위 20개 선정 (집중도 상향)
CANDLE_UNIT = 1                     # 분봉 단위 (1분)
LOG_LEVEL = logging.INFO            # 로그 레벨
DRY_RUN = True                      # 테스트 모드 (True: 실제 거래 X)
USE_SECOND_CANDLES = True           # 초봉 사용 여부
BALANCE_REPORT_INTERVAL = 60        # 잔고 리포트 주기 (초, 1분)

# === 거래 기록 설정 ===
TRADE_LOG_FILE = "logs/trades.csv"  # 거래 기록 파일 경로

# =================================================================================
# 🔧 시스템 설정
# =================================================================================

# 환경변수 로드
load_dotenv()

# API 키 설정
ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY")
SECRET_KEY = os.getenv("UPBIT_SECRET_KEY")

# API 엔드포인트
REST_BASE_URL = "https://api.upbit.com/v1"
WS_PUBLIC_URL = "wss://api.upbit.com/websocket/v1"
WS_PRIVATE_URL = "wss://api.upbit.com/websocket/v1/private"

# 로깅 설정
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class Color:
    """ANSI 색상 코드"""
    RESET = "\033[0m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"

class UpbitAPI:
    """업비트 REST API 클라이언트"""
    
    def __init__(self, access_key: str, secret_key: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.session = requests.Session()
        
    def _generate_jwt(self, query: Optional[Dict] = None, query_string: Optional[str] = None) -> str:
        """JWT 토큰 생성"""
        payload = {
            'access_key': self.access_key,
            'nonce': str(uuid.uuid4()),
        }
        
        if query_string:
             # 이미 생성된 쿼리 스트링이 있는 경우 (그대로 사용)
            m = hashlib.sha512()
            m.update(query_string.encode())
            payload['query_hash'] = m.hexdigest()
            payload['query_hash_alg'] = 'SHA512'
        elif query:
            # 딕셔너리로 넘겨받은 경우 (unquote 적용하여 표준 준수)
            q_str = unquote(urlencode(query)).encode()
            m = hashlib.sha512()
            m.update(q_str)
            payload['query_hash'] = m.hexdigest()
            payload['query_hash_alg'] = 'SHA512'
            
        return jwt.encode(payload, self.secret_key)
    
    def _request(self, method: str, endpoint: str, 
                 params: Optional[Dict] = None, 
                 data: Optional[Dict] = None) -> Dict:
        """API 요청 수행"""
        url = f"{REST_BASE_URL}{endpoint}"
        headers = {}
        
        if method == 'GET' or method == 'DELETE':
            if params:
                # GET/DELETE 요청:
                # 1. URL용: 인코딩된 쿼리 스트링 (예: time=...%3A... / states%5B%5D=done&states%5B%5D=cancel)
                # doseq=True: 리스트 파라미터 처리 (states[]=['done', 'cancel'] -> states[]=done&states[]=cancel)
                query_string = urlencode(params, doseq=True)
                # 2. 해시용: 디코딩된 쿼리 스트링 (예: time=...:...) - Upbit 표준
                hash_string = unquote(query_string)
                
                token = self._generate_jwt(query_string=hash_string)
                headers['Authorization'] = f"Bearer {token}"
                url = f"{url}?{query_string}"
            else:
                token = self._generate_jwt()
                headers['Authorization'] = f"Bearer {token}"
        elif params or data:
            # POST 데이터 처리
            token = self._generate_jwt(query=params or data)
            headers['Authorization'] = f"Bearer {token}"
        elif endpoint.startswith('/orders') or endpoint == '/accounts':
            token = self._generate_jwt()
            headers['Authorization'] = f"Bearer {token}"
            
        for attempt in range(4):
            try:
                if method == 'GET':
                    # URL에 이미 쿼리 스트링이 포함되어 있으므로 params=None
                    response = self.session.get(url, headers=headers)
                elif method == 'POST':
                    headers['Content-Type'] = 'application/json; charset=utf-8'
                    response = self.session.post(url, json=data, headers=headers)
                elif method == 'DELETE':
                    # URL에 이미 쿼리 스트링이 포함되어 있으므로 params=None
                    response = self.session.delete(url, headers=headers)
                else:
                    raise ValueError(f"Unsupported method: {method}")
                
                # Rate Limit handling
                if response.status_code == 429:
                    wait_time = 0.5 * (2 ** attempt)
                    logger.warning(f"API 요청 빈도 제한(429). {wait_time}초 대기 후 재시도... ({attempt+1}/3)")
                    time.sleep(wait_time)
                    continue
                    
                response.raise_for_status()
                # 요청 간 최소 간격 유지 (Throttling) - Rate Limit 방지 강화
                time.sleep(0.2)
                return response.json()
                
            except requests.exceptions.RequestException as e:
                # 429가 아닌 다른 오류나 마지막 재시도 실패 시
                if attempt == 3 or (hasattr(e, 'response') and e.response is not None and e.response.status_code != 429):
                    logger.error(f"API 요청 실패: {e}")
                    logger.error(f"요청 정보: endpoint={endpoint}, params={params}, data={data}")
                    if hasattr(e, 'response') and e.response:
                         logger.error(f"응답: {e.response.text}")
                    raise
                # 429 외의 일시적 오류일 수도 있으나, 여기서는 429 처리 위주로 구성
                # 만약 RequestException이 response를 포함하고 429라면 위 loop에서 처리됨(status_code check가 먼저)
                # 하지만 raise_for_status()에서 예외 발생 시 여기로 오므로, 429 체크 필요
                if hasattr(e, 'response') and e.response is not None and e.response.status_code == 429:
                     wait_time = 0.5 * (2 ** attempt)
                     logger.warning(f"API 요청 빈도 제한(429) - 예외 발생. {wait_time}초 대기 후 재시도... ({attempt+1}/3)")
                     time.sleep(wait_time)
                     continue
                raise
            
    def get_accounts(self) -> List[Dict]:
        """계정 잔고 조회"""
        return self._request('GET', '/accounts')
    
    def get_ticker(self, markets: str) -> List[Dict]:
        """현재가 조회"""
        return self._request('GET', '/ticker', params={'markets': markets})
    
    def get_candles_minutes(self, market: str, unit: int = 1, 
                           count: int = 200, to: Optional[str] = None) -> List[Dict]:
        """분봉 조회"""
        params = {'market': market, 'count': count}
        if to:
            params['to'] = to
        return self._request('GET', f'/candles/minutes/{unit}', params=params)
    
    def get_candles_days(self, market: str, count: int = 200) -> List[Dict]:
        """일봉 조회"""
        return self._request('GET', '/candles/days', 
                           params={'market': market, 'count': count})
    
    def get_candles_weeks(self, market: str, count: int = 10) -> List[Dict]:
        """주봉 조회"""
        return self._request('GET', '/candles/weeks',
                           params={'market': market, 'count': count})
    
    def get_candles_months(self, market: str, count: int = 6) -> List[Dict]:
        """월봉 조회"""
        return self._request('GET', '/candles/months',
                           params={'market': market, 'count': count})
    
    def get_candles_seconds(self, market: str, count: int = 60, 
                           to: Optional[str] = None) -> List[Dict]:
        """초봉 조회 (1초봉 고정, unit 파라미터 없음)"""
        params = {'market': market, 'count': count}
        if to:
            params['to'] = to
        return self._request('GET', '/candles/seconds', params=params)
    
    def get_orderbook(self, markets: str) -> List[Dict]:
        """호가 조회"""
        return self._request('GET', '/orderbook', params={'markets': markets})
    
    def place_order(self, market: str, side: str, ord_type: str,
                   volume: Optional[str] = None, price: Optional[str] = None,
                   identifier: Optional[str] = None) -> Dict:
        """주문 생성"""
        data = {
            'market': market,
            'side': side,
            'ord_type': ord_type
        }
        if volume:
            data['volume'] = volume
        if price:
            data['price'] = price
        if identifier:
            data['identifier'] = identifier
            
        return self._request('POST', '/orders', data=data)
    
    def cancel_order(self, uuid_val: str) -> Dict:
        """주문 취소"""
        return self._request('DELETE', '/order', params={'uuid': uuid_val})
    
    def get_order(self, uuid_val: str) -> Dict:
        """주문 조회"""
        return self._request('GET', '/order', params={'uuid': uuid_val})
    
    def get_orders_chance(self, market: str) -> Dict:
        """주문 가능 정보 조회"""
        return self._request('GET', '/orders/chance', params={'market': market})

    def get_all_markets(self) -> List[Dict]:
        """모든 마켓 코드 조회"""
        return self._request('GET', '/market/all')

    def get_closed_orders(self, market: str, limit: int = 100, start_time: Optional[str] = None, end_time: Optional[str] = None, states: Optional[List[str]] = None) -> List[Dict]:
        """종료된 주문 조회 (최근 체결 내역 확인용)
        - start_time: 조회 시작 시각 (예: '2023-01-01T00:00:00+09:00')
        - end_time: 조회 끝 시각
        - limit: 요청 개수 (1~1000)
        - states: 조회할 주문 상태 리스트 (예: ['done', 'cancel']) - 미지정 시 API 기본값(done, cancel 모두)
        """
        params = {
            'market': market,
            'order_by': 'desc',
            'limit': limit
        }
        
        # states 파라미터 처리 (리스트인 경우 반복 파라미터로 변환 필요하지만 requests가 처리해줌, 단 Upbit는 states[] 키 필요)
        if states:
            params['states[]'] = states
        else:
             # 명시적으로 지정하지 않으면 done만 가져오는게 아니라, done/cancel 모두 가져오기 위해 states[] 파라미터 사용 권장
             params['states[]'] = ['done', 'cancel']

        if start_time:
            params['start_time'] = start_time
        if end_time:
            params['end_time'] = end_time
            
        return self._request('GET', '/orders/closed', params=params)


class TradingState:
    """거래 상태 관리 (개선된 버전)"""
    
    def __init__(self, market: str = "Unknown"):
        self.market = market
        self.position = None              # 현재 포지션 정보
        self.entry_price = 0.0            # 진입 가격
        self.entry_time = None            # 진입 시간
        self.highest_price = 0.0          # 보유 중 최고가
        self.stop_loss_price = 0.0        # 손절가
        self.take_profit_price = 0.0      # 익절가
        self.trailing_active = False      # 트레일링 스탑 활성화 여부
        self.dynamic_stop_loss_rate = INITIAL_STOP_LOSS  # 동적 손절율
        
        # 거래 기록
        self.trades_today = []            # 오늘 거래 기록
        self.last_trade_time = None       # 마지막 거래 시간
        self.last_loss_time = None        # 마지막 손절 시간
        
        # === 연속 손실 추적 (신규) ===
        self.consecutive_losses = 0       # 연속 손실 횟수
        self.last_exit_price = 0.0        # 마지막 청산 가격 (재진입 방지용)
        self.recent_loss_count = 0        # 최근 1시간 내 손실 횟수
        
        # 수익 추적
        self.total_profit = 0.0           # 총 수익
        self.total_trades = 0             # 총 거래 횟수
        self.winning_trades = 0           # 수익 거래
        self.losing_trades = 0            # 손실 거래
        
    def has_position(self) -> bool:
        """포지션 보유 여부"""
        return self.position is not None
    
    def can_trade(self) -> bool:
        """거래 가능 여부 확인 (강화된 버전)"""
        now = datetime.now()
        
        # 시간당 거래 횟수 제한
        hour_ago = now - timedelta(hours=1)
        recent_trades = [t for t in self.trades_today 
                        if t['time'] > hour_ago]
        if len(recent_trades) >= MAX_TRADES_PER_HOUR:
            return False
        
        # 최근 손실 횟수 업데이트
        recent_losses = [t for t in self.trades_today 
                        if t['time'] > hour_ago and t['type'] in ['stop_loss']]
        self.recent_loss_count = len(recent_losses)
            
        # 손절 후 쿨다운 (기본)
        if self.last_loss_time:
            cooldown_end = self.last_loss_time + timedelta(seconds=COOL_DOWN_AFTER_LOSS)
            if now < cooldown_end:
                return False
            
            # 연속 손실 시 추가 쿨다운 (2회 이상 연속 손실 시)
            if self.consecutive_losses >= 2:
                extended_cooldown = self.last_loss_time + timedelta(seconds=CONSECUTIVE_LOSS_COOLDOWN)
                if now < extended_cooldown:
                    return False
        
        # 최근 1시간 내 3회 이상 손실 시 추가 대기
        if self.recent_loss_count >= 3:
            return False
                
        return True
    
    def reset_consecutive_losses(self):
        """연속 손실 카운터 초기화 (수익 거래 시)"""
        self.consecutive_losses = 0
    
    def record_trade(self, trade_type: str, amount: float, 
                    price: float, profit: float = 0.0):
        """거래 기록 (연속 손실 추적 포함)"""
        trade = {
            'time': datetime.now(),
            'type': trade_type,
            'amount': amount,
            'price': price,
            'profit': profit
        }
        self.trades_today.append(trade)
        self.last_trade_time = trade['time']
        self.total_trades += 1
        
        if trade_type in ['take_profit', 'trailing_stop', 'time_exit']:
            if profit > 0:
                self.winning_trades += 1
                self.reset_consecutive_losses()  # 수익 시 연속 손실 리셋
            else:
                self.losing_trades += 1
                self.consecutive_losses += 1  # 손실인 경우에도 카운트
            self.total_profit += profit
            self.last_exit_price = price  # 청산 가격 기록
            
        if trade_type == 'stop_loss':
            self.last_loss_time = trade['time']
            self.losing_trades += 1
            self.consecutive_losses += 1  # 연속 손실 증가
            self.total_profit += profit
            self.last_exit_price = price  # 청산 가격 기록


class MarketAnalyzer:
    """시장 분석기 - 전문가 관점의 종합 분석"""
    
    def __init__(self, api: UpbitAPI, market: str):
        self.api = api
        self.market = market
        self.macro_trend = None           # 거시 추세 (bullish/bearish/neutral)
        self.macro_score = 0.0            # 거시 점수
        self.last_macro_update = None     # 마지막 거시 분석 시간
        
        # 캔들 데이터 캐시 (다양한 시간대)
        self.minute_candles = deque(maxlen=200)      # 1분봉
        self.minute5_candles = deque(maxlen=100)     # 5분봉
        self.minute15_candles = deque(maxlen=50)     # 15분봉
        self.second_candles = deque(maxlen=120)      # 초봉 캐시 (최근 2분)
        self.volume_history = deque(maxlen=100)
        self.second_volume_history = deque(maxlen=60)
        
        # ==== 체결 데이터 (Trade) - 매수/매도 세력 분석 ====
        self.recent_trades = deque(maxlen=500)        # 최근 체결 내역
        self.bid_volume_1m = 0.0                      # 최근 1분간 매수 체결량
        self.ask_volume_1m = 0.0                      # 최근 1분간 매도 체결량
        self.bid_volume_5m = 0.0                      # 최근 5분간 매수 체결량
        self.ask_volume_5m = 0.0                      # 최근 5분간 매도 체결량
        self.trade_count_1m = {'bid': 0, 'ask': 0}    # 최근 1분간 체결 건수
        self.last_trade_update = None
        
        # ==== 가격 피로도/심리 지표 ====
        self.price_history = deque(maxlen=300)        # 가격 히스토리 (5분간)
        self.volatility = 0.0                         # 현재 변동성 (표준편차)
        self.rsi_value = 50.0                         # RSI 유사 지표 (0-100)
        self.fatigue_score = 0.0                      # 급등 피로도 (0-100, 높을수록 조정 가능성)
        self.momentum_exhaustion = False              # 모멘텀 소진 여부
        
        # ==== 호가 데이터 (매수/매도 벽 분석) ====
        self.orderbook = {
            'total_ask_size': 0.0,
            'total_bid_size': 0.0,
            'units': [],
            'spread': 0.0,                 # 스프레드 (매도호가 - 매수호가)
            'spread_rate': 0.0,            # 스프레드 비율
            'bid_depth_ratio': 0.0,        # 매수벽 깊이 비율
            'imbalance': 0.0,              # 호가 불균형 (-1 ~ 1, 양수면 매수 우위)
        }
        
        # ==== 종합 시장 심리 ====
        self.market_sentiment = 'neutral'  # bullish/bearish/neutral
        self.sentiment_score = 50.0        # 시장 심리 점수 (0-100)
        
    def analyze_macro(self) -> Dict:
        """시장 추세 분석 (초단기/중단기/거시 하이브리드)"""
        try:
            # 1. 초단기 분석 (15분봉/30분봉) - 전문가 기법 적용
            time.sleep(0.1)
            m15 = self.api.get_candles_minutes(self.market, unit=15, count=2)
            m15_change = (m15[0]['trade_price'] - m15[1]['trade_price']) / m15[1]['trade_price'] if len(m15) >= 2 else 0
            
            time.sleep(0.1)
            m30 = self.api.get_candles_minutes(self.market, unit=30, count=2)
            m30_change = (m30[0]['trade_price'] - m30[1]['trade_price']) / m30[1]['trade_price'] if len(m30) >= 2 else 0

            # 2. 중단기 분석 (1시간/4시간)
            time.sleep(0.1)
            h1 = self.api.get_candles_minutes(self.market, unit=60, count=2)
            h1_change = (h1[0]['trade_price'] - h1[1]['trade_price']) / h1[1]['trade_price'] if len(h1) >= 2 else 0

            time.sleep(0.1)
            h4 = self.api.get_candles_minutes(self.market, unit=240, count=2)
            h4_change = (h4[0]['trade_price'] - h4[1]['trade_price']) / h4[1]['trade_price'] if len(h4) >= 2 else 0
            
            # 3. 일봉 (대세 확인)
            time.sleep(0.1)
            daily = self.api.get_candles_days(self.market, count=2)
            daily_change = (daily[0]['trade_price'] - daily[1]['trade_price']) / daily[1]['trade_price'] if len(daily) >= 2 else 0

            # 종합 점수 계산 (초단위/분단위 기법 적용 가중치)
            # 15분(40%) + 30분(25%) + 1시간(15%) + 4시간(15%) + 1일(5%)
            score = m15_change * 0.4 + m30_change * 0.25 + h1_change * 0.15 + h4_change * 0.15 + daily_change * 0.05
            
            # [전문가 기법] 단기 수급 급전환 감지 (Aggressive Entry)
            # 15분간 0.5% 이상 상승하면 거시 추세와 무시하고 단타 기회로 판단
            short_squeeze = m15_change >= SHORT_MOMENTUM_THRESHOLD
            
            if score < MACRO_MIN_CHANGE_RATE and not short_squeeze:
                trend = 'bearish'
                can_trade = False
            elif score > MACRO_BULLISH_THRESHOLD or short_squeeze:
                trend = 'bullish'
                can_trade = True
            else:
                trend = 'neutral'
                can_trade = True
            
            self.macro_trend = trend
            self.macro_score = score
            self.last_macro_update = datetime.now()
            
            result = {
                'trend': trend,
                'score': score,
                'can_trade': can_trade,
                'm15_change': m15_change,
                'short_squeeze': short_squeeze
            }
            
            log_msg = f"[{self.market}] 📊 추세 분석 | {trend} | 15m:{m15_change*100:+.2f}% 1h:{h1_change*100:+.2f}% 일:{daily_change*100:+.2f}%"
            if short_squeeze:
                log_msg += " | 🔥 단기 수급 폭발(Short Squeeze) 감지"
            logger.info(log_msg)
            
            return result
            
        except Exception as e:
            logger.error(f"거시 분석 오류: {e}")
            return {'trend': 'neutral', 'score': 0, 'can_trade': True}
    
    def update_candles(self, candles: List[Dict]):
        """1분봉 데이터 업데이트"""
        for candle in reversed(candles):  # 시간순 정렬
            self.minute_candles.append(candle)
            self.volume_history.append(candle['candle_acc_trade_volume'])
    
    def update_candles_5m(self, candles: List[Dict]):
        """5분봉 데이터 업데이트"""
        for candle in reversed(candles):  # 시간순 정렬
            self.minute5_candles.append(candle)
    
    def update_candles_15m(self, candles: List[Dict]):
        """15분봉 데이터 업데이트"""
        for candle in reversed(candles):  # 시간순 정렬
            self.minute15_candles.append(candle)
            
    def update_second_candles(self, candles: List[Dict]):
        """초봉 데이터 업데이트"""
        for candle in reversed(candles):  # 시간순 정렬
            self.second_candles.append(candle)
            self.second_volume_history.append(candle['candle_acc_trade_volume'])
            
    def update_candle_from_ws(self, data: Dict, type_key: str):
        """WebSocket 캔들 데이터 업데이트 - 다양한 시간대 지원"""
        # WS 데이터 포맷을 REST API 포맷으로 변환
        candle = {
            'market': data.get('code') or data.get('cd'),
            'candle_date_time_kst': data.get('candle_date_time_kst') or data.get('cdttmk'),
            'opening_price': data.get('opening_price') or data.get('op'),
            'high_price': data.get('high_price') or data.get('hp'),
            'low_price': data.get('low_price') or data.get('lp'),
            'trade_price': data.get('trade_price') or data.get('tp'),
            'candle_acc_trade_volume': data.get('candle_acc_trade_volume') or data.get('catv'),
        }
        
        # 1분봉 (candle.1m)
        if type_key == 'candle.1m':
            if self.minute_candles and self.minute_candles[-1]['candle_date_time_kst'] == candle['candle_date_time_kst']:
                self.minute_candles[-1] = candle
                if self.volume_history:
                    self.volume_history[-1] = candle['candle_acc_trade_volume']
            else:
                self.minute_candles.append(candle)
                self.volume_history.append(candle['candle_acc_trade_volume'])
        
        # 5분봉 (candle.5m)
        elif type_key == 'candle.5m':
            if self.minute5_candles and self.minute5_candles[-1]['candle_date_time_kst'] == candle['candle_date_time_kst']:
                self.minute5_candles[-1] = candle
            else:
                self.minute5_candles.append(candle)
        
        # 15분봉 (candle.15m)
        elif type_key == 'candle.15m':
            if self.minute15_candles and self.minute15_candles[-1]['candle_date_time_kst'] == candle['candle_date_time_kst']:
                self.minute15_candles[-1] = candle
            else:
                self.minute15_candles.append(candle)
                
        # 초봉 (candle.1s)
        elif type_key == 'candle.1s':
            if self.second_candles and self.second_candles[-1]['candle_date_time_kst'] == candle['candle_date_time_kst']:
                self.second_candles[-1] = candle
                if self.second_volume_history:
                    self.second_volume_history[-1] = candle['candle_acc_trade_volume']
            else:
                self.second_candles.append(candle)
                self.second_volume_history.append(candle['candle_acc_trade_volume'])
    
    
    def update_orderbook_from_ws(self, data: Dict):
        """호가 데이터 업데이트 - 스프레드 및 불균형 분석 포함"""
        self.orderbook['total_ask_size'] = data.get('total_ask_size') or data.get('tas', 0.0)
        self.orderbook['total_bid_size'] = data.get('total_bid_size') or data.get('tbs', 0.0)
        
        # units 처리 (약어로 올 수 있음)
        units = data.get('orderbook_units') or data.get('obu')
        if units:
            unit_list = []
            for u in units:
                unit_list.append({
                    'ask_price': u.get('ask_price') or u.get('ap'),
                    'bid_price': u.get('bid_price') or u.get('bp'),
                    'ask_size': u.get('ask_size') or u.get('as'),
                    'bid_size': u.get('bid_size') or u.get('bs'),
                })
            self.orderbook['units'] = unit_list
            
            # 스프레드 계산 (최상위 호가 기준)
            if unit_list:
                best_ask = unit_list[0]['ask_price']
                best_bid = unit_list[0]['bid_price']
                if best_ask and best_bid:
                    self.orderbook['spread'] = best_ask - best_bid
                    self.orderbook['spread_rate'] = (best_ask - best_bid) / best_bid if best_bid > 0 else 0
            
            # 호가 불균형 계산 (-1 ~ 1)
            total_ask = self.orderbook['total_ask_size']
            total_bid = self.orderbook['total_bid_size']
            if total_ask + total_bid > 0:
                self.orderbook['imbalance'] = (total_bid - total_ask) / (total_bid + total_ask)
            
            # 상위 5호가 매수벽 깊이 비율
            if len(unit_list) >= 5:
                top5_bid = sum(u['bid_size'] for u in unit_list[:5] if u['bid_size'])
                top5_ask = sum(u['ask_size'] for u in unit_list[:5] if u['ask_size'])
                if top5_ask > 0:
                    self.orderbook['bid_depth_ratio'] = top5_bid / top5_ask
    
    def update_trade_from_ws(self, data: Dict):
        """체결 데이터 업데이트 - 매수/매도 세력 분석"""
        trade = {
            'timestamp': data.get('trade_timestamp') or data.get('ttms', 0),
            'price': data.get('trade_price') or data.get('tp', 0),
            'volume': data.get('trade_volume') or data.get('tv', 0),
            'ask_bid': data.get('ask_bid') or data.get('ab', 'BID'),  # BID: 매수체결, ASK: 매도체결
            'sequential_id': data.get('sequential_id') or data.get('sid', 0),
        }
        
        self.recent_trades.append(trade)
        self.last_trade_update = datetime.now()
        
        # 가격 히스토리 업데이트 (RSI 및 변동성 계산용)
        self.price_history.append({
            'price': trade['price'],
            'timestamp': trade['timestamp']
        })
        
        # 1분간, 5분간 체결량 집계 업데이트
        self._update_volume_aggregates()
        
        # RSI 및 피로도 업데이트
        self._update_technical_indicators()
    
    def _update_volume_aggregates(self):
        """체결량 집계 업데이트 (1분/5분)"""
        now_ts = datetime.now().timestamp() * 1000  # 밀리초
        one_min_ago = now_ts - 60 * 1000
        five_min_ago = now_ts - 5 * 60 * 1000
        
        bid_1m = ask_1m = 0.0
        bid_5m = ask_5m = 0.0
        bid_count = ask_count = 0
        
        for trade in self.recent_trades:
            ts = trade['timestamp']
            vol = trade['volume']
            is_bid = trade['ask_bid'] == 'BID'
            
            if ts >= one_min_ago:
                if is_bid:
                    bid_1m += vol
                    bid_count += 1
                else:
                    ask_1m += vol
                    ask_count += 1
            
            if ts >= five_min_ago:
                if is_bid:
                    bid_5m += vol
                else:
                    ask_5m += vol
        
        self.bid_volume_1m = bid_1m
        self.ask_volume_1m = ask_1m
        self.bid_volume_5m = bid_5m
        self.ask_volume_5m = ask_5m
        self.trade_count_1m = {'bid': bid_count, 'ask': ask_count}
    
    def _update_technical_indicators(self):
        """RSI, 변동성, 피로도 등 기술 지표 업데이트"""
        if len(self.price_history) < 14:
            return
        
        prices = [p['price'] for p in list(self.price_history)[-60:]]  # 최근 60틱
        
        # === RSI 계산 (14-period 유사 방식) ===
        gains = []
        losses = []
        for i in range(1, min(15, len(prices))):
            change = prices[-i] - prices[-i-1]
            if change > 0:
                gains.append(change)
            else:
                losses.append(abs(change))
        
        avg_gain = sum(gains) / 14 if gains else 0.0001
        avg_loss = sum(losses) / 14 if losses else 0.0001
        
        if avg_loss > 0:
            rs = avg_gain / avg_loss
            self.rsi_value = 100 - (100 / (1 + rs))
        else:
            self.rsi_value = 100 if avg_gain > 0 else 50
        
        # === 변동성 계산 (표준편차) ===
        if len(prices) >= 20:
            import statistics
            self.volatility = statistics.stdev(prices[-20:]) / statistics.mean(prices[-20:])
        
        # === 급등 피로도 계산 ===
        self._update_fatigue_score(prices)
    
    def _update_fatigue_score(self, prices: List[float]):
        """급등 피로도 계산 - 급등 후 조정 가능성 평가"""
        if len(prices) < 30:
            self.fatigue_score = 0
            return
        
        current = prices[-1]
        
        # 5분 전 가격 대비 상승률
        price_5m_ago = prices[-min(30, len(prices))]
        change_5m = (current - price_5m_ago) / price_5m_ago if price_5m_ago > 0 else 0
        
        # 1. 급격한 상승률에 따른 피로도 (5% 이상 상승 시 높은 피로도)
        rate_fatigue = min(100, abs(change_5m) * 1000)  # 1% = 10점
        
        # 2. RSI가 70 이상이면 과매수 → 피로도 증가
        rsi_fatigue = 0
        if self.rsi_value >= 70:
            rsi_fatigue = (self.rsi_value - 70) * 3  # 70 초과분 * 3
        elif self.rsi_value >= 80:
            rsi_fatigue = 30 + (self.rsi_value - 80) * 5  # 더 가파르게
        
        # 3. 거래량 스파이크 후 급감하면 모멘텀 소진
        volume_fatigue = 0
        if len(self.minute_candles) >= 3:
            recent_vols = [c['candle_acc_trade_volume'] for c in list(self.minute_candles)[-3:]]
            if len(recent_vols) == 3:
                # 이전 거래량 대비 현재 거래량이 급감하면 소진
                if recent_vols[1] > 0 and recent_vols[2] / recent_vols[1] < 0.5:
                    volume_fatigue = 20
                    self.momentum_exhaustion = True
                else:
                    self.momentum_exhaustion = False
        
        # 4. 매도 우위 전환 감지
        sell_pressure = 0
        if self.bid_volume_1m + self.ask_volume_1m > 0:
            sell_ratio = self.ask_volume_1m / (self.bid_volume_1m + self.ask_volume_1m)
            if sell_ratio > 0.6:  # 60% 이상 매도체결
                sell_pressure = (sell_ratio - 0.5) * 100
        
        # 종합 피로도
        self.fatigue_score = min(100, rate_fatigue + rsi_fatigue + volume_fatigue + sell_pressure)
    
    def analyze_market_sentiment(self) -> Dict:
        """종합 시장 심리 분석 - 전문가 관점"""
        analysis = {
            'sentiment': 'neutral',
            'score': 50.0,
            'buy_pressure': 0.0,
            'sell_pressure': 0.0,
            'fatigue': self.fatigue_score,
            'volatility': self.volatility,
            'rsi': self.rsi_value,
            'orderbook_imbalance': self.orderbook.get('imbalance', 0),
            'reasons': [],
            'warnings': [],
        }
        
        score = 50.0  # 중립 기준
        
        # === 1. 매수/매도 체결량 비율 분석 ===
        total_vol_1m = self.bid_volume_1m + self.ask_volume_1m
        if total_vol_1m > 0:
            buy_ratio = self.bid_volume_1m / total_vol_1m
            analysis['buy_pressure'] = buy_ratio
            analysis['sell_pressure'] = 1 - buy_ratio
            
            if buy_ratio >= 0.65:
                score += 15
                analysis['reasons'].append(f"매수 체결 우위 ({buy_ratio*100:.1f}%)")
            elif buy_ratio >= 0.55:
                score += 8
                analysis['reasons'].append(f"매수 소폭 우위 ({buy_ratio*100:.1f}%)")
            elif buy_ratio <= 0.35:
                score -= 15
                analysis['warnings'].append(f"매도 체결 우위 ({(1-buy_ratio)*100:.1f}%)")
            elif buy_ratio <= 0.45:
                score -= 8
                analysis['warnings'].append(f"매도 소폭 우위 ({(1-buy_ratio)*100:.1f}%)")
        
        # === 2. 호가창 불균형 분석 ===
        imbalance = self.orderbook.get('imbalance', 0)
        if imbalance >= 0.3:
            score += 10
            analysis['reasons'].append(f"매수벽 우위 (불균형:{imbalance:.2f})")
        elif imbalance <= -0.3:
            score -= 10
            analysis['warnings'].append(f"매도벽 우위 (불균형:{imbalance:.2f})")
        
        # === 3. RSI 과매수/과매도 분석 ===
        if self.rsi_value >= 80:
            score -= 20
            analysis['warnings'].append(f"🚨 극심한 과매수 (RSI:{self.rsi_value:.1f})")
        elif self.rsi_value >= 70:
            score -= 10
            analysis['warnings'].append(f"⚠️ 과매수 구간 (RSI:{self.rsi_value:.1f})")
        elif self.rsi_value <= 20:
            score += 15
            analysis['reasons'].append(f"과매도 반등 가능 (RSI:{self.rsi_value:.1f})")
        elif self.rsi_value <= 30:
            score += 8
            analysis['reasons'].append(f"과매도 구간 (RSI:{self.rsi_value:.1f})")
        
        # === 4. 급등 피로도 분석 ===
        if self.fatigue_score >= 60:
            score -= 25
            analysis['warnings'].append(f"🔥 급등 피로도 높음 ({self.fatigue_score:.1f}) - 조정 가능성")
        elif self.fatigue_score >= 40:
            score -= 12
            analysis['warnings'].append(f"급등 피로감 ({self.fatigue_score:.1f})")
        
        # === 5. 모멘텀 소진 체크 ===
        if self.momentum_exhaustion:
            score -= 15
            analysis['warnings'].append("📉 모멘텀 소진 - 거래량 급감")
        
        # === 6. 변동성 체크 ===
        if self.volatility >= 0.02:  # 2% 이상 변동성
            score -= 5
            analysis['warnings'].append(f"높은 변동성 ({self.volatility*100:.2f}%)")
        
        # 최종 점수 및 심리 결정
        score = max(0, min(100, score))
        analysis['score'] = score
        
        if score >= 65:
            analysis['sentiment'] = 'bullish'
        elif score <= 35:
            analysis['sentiment'] = 'bearish'
        else:
            analysis['sentiment'] = 'neutral'
        
        self.market_sentiment = analysis['sentiment']
        self.sentiment_score = score
        
        return analysis
    
    def analyze_multi_timeframe(self, current_price: float) -> Dict:
        """다중 타임프레임 분석 - 5분봉/15분봉으로 진입 타이밍 검증
        
        핵심 목표:
        1. 상승 '초기' 단계인지 확인 (고점 추격 방지)
        2. 중기 추세(15분봉)가 하락이 아닌지 확인
        3. 5분봉 거래량을 통해 수급 확인
        """
        result = {
            'valid_entry': True,  # 진입 허용 여부
            'stage': 'unknown',   # early(초기), mid(중반), late(후반), exhausted(소진)
            'trend_5m': 'neutral',
            'trend_15m': 'neutral',
            'change_5m': 0.0,
            'change_15m': 0.0,
            'volume_confirmed': False,
            'reasons': [],
            'warnings': [],
        }
        
        # MTF 비활성화 시 항상 허용
        if not MTF_ENABLED:
            result['reasons'].append("MTF 분석 비활성화")
            return result
        
        # === 1. 5분봉 분석 ===
        if len(self.minute5_candles) >= MTF_5M_MIN_CANDLES:
            candles_5m = list(self.minute5_candles)[-MTF_5M_MIN_CANDLES:]
            
            # 5분봉 전체 변화율 (시작 ~ 현재)
            start_price = candles_5m[0]['opening_price']
            change_5m = (current_price - start_price) / start_price if start_price > 0 else 0
            result['change_5m'] = change_5m
            
            # 최근 5분봉 2개의 추세
            recent_5m_change = (candles_5m[-1]['trade_price'] - candles_5m[-2]['trade_price']) / candles_5m[-2]['trade_price'] if candles_5m[-2]['trade_price'] > 0 else 0
            
            # 5분봉 추세 판단
            if change_5m >= MTF_5M_TREND_THRESHOLD and recent_5m_change >= 0:
                result['trend_5m'] = 'bullish'
                result['reasons'].append(f"5분봉 상승 추세 ({change_5m*100:.2f}%)")
            elif change_5m <= -MTF_5M_TREND_THRESHOLD:
                result['trend_5m'] = 'bearish'
                result['warnings'].append(f"5분봉 하락 추세 ({change_5m*100:.2f}%)")
            else:
                result['trend_5m'] = 'neutral'
            
            # 상승 단계 판단 (핵심!)
            if change_5m >= MTF_5M_EARLY_STAGE_MAX:
                # 이미 2.5% 이상 상승 = 후반/소진 단계
                result['stage'] = 'late'
                result['warnings'].append(f"⚠️ 상승 후반 ({change_5m*100:.2f}%) - 고점 추격 위험")
                result['valid_entry'] = False
            elif change_5m >= MTF_5M_TREND_THRESHOLD:
                # 0.2% ~ 2.5% 상승 = 초기~중반
                if change_5m <= 0.01:  # 1% 이하
                    result['stage'] = 'early'
                    result['reasons'].append(f"✅ 상승 초기 ({change_5m*100:.2f}%)")
                else:
                    result['stage'] = 'mid'
                    result['reasons'].append(f"📈 상승 중반 ({change_5m*100:.2f}%)")
            else:
                result['stage'] = 'neutral'
            
            # 5분봉 거래량 확인
            if len(candles_5m) >= 3:
                avg_vol = sum(c['candle_acc_trade_volume'] for c in candles_5m[:-1]) / (len(candles_5m) - 1)
                current_vol = candles_5m[-1]['candle_acc_trade_volume']
                if avg_vol > 0 and current_vol >= avg_vol * MTF_VOLUME_CONFIRMATION:
                    result['volume_confirmed'] = True
                    result['reasons'].append(f"거래량 확인 ({current_vol/avg_vol:.1f}x)")
                elif avg_vol > 0 and current_vol < avg_vol * 0.7:
                    result['warnings'].append(f"거래량 감소 ({current_vol/avg_vol:.1f}x)")
        else:
            result['warnings'].append(f"5분봉 데이터 부족 ({len(self.minute5_candles)}/{MTF_5M_MIN_CANDLES})")
        
        # === 2. 15분봉 분석 ===
        if len(self.minute15_candles) >= MTF_15M_MIN_CANDLES:
            candles_15m = list(self.minute15_candles)[-MTF_15M_MIN_CANDLES:]
            
            # 15분봉 전체 변화율
            start_price_15m = candles_15m[0]['opening_price']
            change_15m = (current_price - start_price_15m) / start_price_15m if start_price_15m > 0 else 0
            result['change_15m'] = change_15m
            
            # 15분봉 추세 판단
            if change_15m >= MTF_15M_TREND_THRESHOLD:
                result['trend_15m'] = 'bullish'
                result['reasons'].append(f"15분봉 상승 ({change_15m*100:.2f}%)")
            elif change_15m <= -MTF_15M_TREND_THRESHOLD:
                result['trend_15m'] = 'bearish'
                result['warnings'].append(f"🚫 15분봉 하락 ({change_15m*100:.2f}%)")
                if MTF_STRICT_MODE:
                    result['valid_entry'] = False
            else:
                result['trend_15m'] = 'neutral'
                result['reasons'].append(f"15분봉 횡보 ({change_15m*100:.2f}%)")
        else:
            result['warnings'].append(f"15분봉 데이터 부족 ({len(self.minute15_candles)}/{MTF_15M_MIN_CANDLES})")
        
        # === 3. 추가 필터: 직전 캔들 음봉 연속 체크 ===
        if len(self.minute5_candles) >= 3:
            recent_3 = list(self.minute5_candles)[-3:]
            down_count = sum(1 for c in recent_3 if c['trade_price'] < c['opening_price'])
            if down_count >= 2:
                result['warnings'].append(f"최근 5분봉 {down_count}개 음봉")
                if down_count == 3:
                    result['valid_entry'] = False
                    result['warnings'].append("🚫 3연속 음봉 - 진입 차단")
        
        return result

    
    def detect_momentum(self, current_price: float) -> Dict:
        """모멘텀 감지 (분봉 기반 - 가속도 및 수급 интенсив성 분석)"""
        if len(self.minute_candles) < MOMENTUM_WINDOW:
            return {'signal': False, 'strength': 0, 'reason': '데이터 부족', 'price_change': 0, 'volume_ratio': 0}
        
        recent = list(self.minute_candles)[-MOMENTUM_WINDOW:]
        
        # 1. 가격 변화율 (전체 윈도우)
        price_change = (current_price - recent[0]['opening_price']) / recent[0]['opening_price']
        
        # 2. 가격 가속도 (Velocity) - 최근 3분간의 변화
        velocity = (current_price - recent[-3]['opening_price']) / 3 if len(recent) >= 3 else 0
        velocity_pct = velocity / recent[-3]['opening_price'] if len(recent) >= 3 else 0
        
        # 3. 거래량 수급 분석
        avg_volume = sum(self.volume_history) / len(self.volume_history) if self.volume_history else 0
        recent_volume = recent[-1]['candle_acc_trade_volume']
        volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 0
        
        # 4. 연속 상승 캔들
        up_count = 0
        for i in range(1, len(recent)):
            if recent[i]['trade_price'] > recent[i-1]['trade_price']:
                up_count += 1
            else:
                up_count = 0
        
        # [전문가 판단 로직 - 호가 분석 추가]
        # 매수벽이 매도벽보다 두터우면 긍정적
        bid_ask_ratio = 1.0
        if self.orderbook['total_ask_size'] > 0:
            bid_ask_ratio = self.orderbook['total_bid_size'] / self.orderbook['total_ask_size']
        
        orderbook_ok = bid_ask_ratio >= 0.8 # 매수세가 어느정도 받쳐줌
        
        momentum_ok = price_change >= MOMENTUM_THRESHOLD
        volume_ok = volume_ratio >= VOLUME_SPIKE_RATIO
        velocity_ok = velocity_pct >= BREAKOUT_VELOCITY
        consecutive_ok = up_count >= CONSECUTIVE_UP_CANDLES
        
        # 강도 계산 (수급 및 가속도에 가중치)
        strength = 0
        if momentum_ok: strength += 30
        if volume_ok: strength += (volume_ratio / VOLUME_SPIKE_RATIO) * 20
        if velocity_ok: strength += (velocity_pct / BREAKOUT_VELOCITY) * 30
        if consecutive_ok: strength += 20
        if bid_ask_ratio > 1.2: strength += 10 # 매수 우위 보너스
        
        strength = min(strength, 100)
        
        # 최종 신호: 모멘텀이 있고 (거래량이 터지거나 가속도가 붙었을 때) + 호가창 확인
        signal = momentum_ok and (volume_ok or velocity_ok or consecutive_ok) and orderbook_ok
        
        reason = []
        if velocity_ok: reason.append(f"가속도↑({velocity_pct*100:.2f}%)")
        if volume_ok: reason.append(f"수급집중({volume_ratio:.1f}x)")
        if momentum_ok: reason.append(f"모멘텀({price_change*100:.2f}%)")
        if not orderbook_ok: reason.append(f"호가불안({bid_ask_ratio:.2f})")
        
        return {
            'signal': signal,
            'strength': strength,
            'price_change': price_change,
            'velocity': velocity_pct,
            'volume_ratio': volume_ratio,
            'up_count': up_count,
            'reason': ' / '.join(reason) if reason else '조건 미충족'
        }
    
    def detect_second_momentum(self, current_price: float) -> Dict:
        """초봉 기반 실시간 모멘텀 감지 (더 빠른 반응)"""
        if len(self.second_candles) < SECOND_MOMENTUM_WINDOW:
            return {'signal': False, 'strength': 0, 'reason': '초봉 데이터 부족', 'rapid_rise': False}
        
        recent = list(self.second_candles)[-SECOND_MOMENTUM_WINDOW:]
        
        # 1. 초단위 가격 모멘텀
        sec_price_change = (current_price - recent[0]['opening_price']) / recent[0]['opening_price']
        
        # 2. 급등 감지 (최근 5초 내 급격한 상승)
        if len(recent) >= 2:
            rapid_change = (current_price - recent[-2]['opening_price']) / recent[-2]['opening_price']
        else:
            rapid_change = 0
        rapid_rise = rapid_change >= SECOND_RAPID_RISE_THRESHOLD
        
        # 3. 연속 상승 초봉 확인
        sec_up_count = 0
        for i in range(1, len(recent)):
            if recent[i]['trade_price'] > recent[i-1]['trade_price']:
                sec_up_count += 1
            else:
                sec_up_count = 0
        
        # 4. 초봉 거래량 급등
        avg_sec_volume = sum(self.second_volume_history) / len(self.second_volume_history) if self.second_volume_history else 0
        recent_sec_volume = recent[-1]['candle_acc_trade_volume']
        sec_volume_ratio = recent_sec_volume / avg_sec_volume if avg_sec_volume > 0 else 0
        
        # 시그널 판단
        sec_momentum_ok = sec_price_change >= SECOND_MOMENTUM_THRESHOLD
        sec_volume_ok = sec_volume_ratio >= VOLUME_SPIKE_RATIO
        
        signal = (sec_momentum_ok and sec_volume_ok) or rapid_rise
        
        # 강도 계산
        strength = 0
        if sec_momentum_ok:
            strength += sec_price_change / SECOND_MOMENTUM_THRESHOLD * 25
        if sec_volume_ok:
            strength += (sec_volume_ratio - 1) * 15
        if rapid_rise:
            strength += rapid_change / SECOND_RAPID_RISE_THRESHOLD * 30
        strength = min(strength, 100)
        
        reason = []
        if sec_momentum_ok:
            reason.append(f"초봉모멘텀 {sec_price_change*100:.3f}%")
        if rapid_rise:
            reason.append(f"🚀급등 {rapid_change*100:.3f}%")
        if sec_volume_ok:
            reason.append(f"초봉거래량 {sec_volume_ratio:.1f}배")
        if sec_up_count >= 3:
            reason.append(f"연속상승초 {sec_up_count}개")
        
        return {
            'signal': signal,
            'strength': strength,
            'price_change': sec_price_change,
            'rapid_change': rapid_change,
            'rapid_rise': rapid_rise,
            'volume_ratio': sec_volume_ratio,
            'up_count': sec_up_count,
            'reason': ' / '.join(reason) if reason else '조건 미충족'
        }
    
    def detect_combined_momentum(self, current_price: float) -> Dict:
        """분봉 + 초봉 + 다중 타임프레임(5분/15분) 결합 모멘텀 감지
        
        개선된 진입 로직 (v3.1 강화):
        1. 1분봉/초봉으로 모멘텀 신호 감지
        2. 5분봉/15분봉으로 상승 초기 단계인지 확인
        3. 고점 추격 방지 (상승 후반/소진 단계 진입 차단)
        4. 호가 불균형 필터 (매도벽 강하면 차단)
        5. 최소 신호 강도 체크
        """
        minute_result = self.detect_momentum(current_price)
        
        # 초봉 사용 안함이면 분봉만 사용, MTF는 별도 처리
        if not USE_SECOND_CANDLES or len(self.second_candles) < SECOND_MOMENTUM_WINDOW:
            second_result = {'signal': False, 'strength': 0, 'rapid_rise': False, 'reason': '초봉 미사용'}
        else:
            second_result = self.detect_second_momentum(current_price)
        
        # === 다중 타임프레임 분석 (핵심 개선) ===
        mtf_result = self.analyze_multi_timeframe(current_price)
        
        # 분봉 기본조건 + 초봉 확인으로 정밀도 향상
        # 케이스 1: 분봉 신호 O + 초봉 확인 = 강력한 신호
        # 케이스 2: 분봉 신호 X + 초봉 급등 = 빠른 진입 기회 (조건 강화!)
        
        combined_signal = False
        combined_strength = 0
        reasons = []
        mtf_blocked = False
        
        # === 0단계: 호가 불균형 사전 필터 (v3.1 추가) ===
        orderbook_imbalance = self.orderbook.get('imbalance', 0)
        if orderbook_imbalance <= -0.3:
            # 매도벽이 30% 이상 강하면 진입 차단
            return {
                'signal': False,
                'strength': 0,
                'minute_signal': minute_result['signal'],
                'second_signal': second_result.get('signal', False),
                'rapid_rise': second_result.get('rapid_rise', False),
                'mtf_valid': mtf_result['valid_entry'],
                'mtf_stage': mtf_result.get('stage', 'unknown'),
                'mtf_trend_5m': mtf_result.get('trend_5m', 'neutral'),
                'mtf_trend_15m': mtf_result.get('trend_15m', 'neutral'),
                'mtf_blocked': True,
                'reason': f'🚫 호가불균형 차단 (매도우위:{orderbook_imbalance:.2f})'
            }
        
        # === 1단계: 기존 1분봉/초봉 신호 확인 ===
        if minute_result['signal'] and second_result.get('signal', False):
            # 둘 다 신호: 매우 강력
            combined_signal = True
            combined_strength = min(100, minute_result['strength'] * 0.6 + second_result['strength'] * 0.4)
            reasons.append(minute_result['reason'])
            reasons.append(second_result['reason'])
            
        elif second_result.get('rapid_rise', False):
            # 초봉 급등만 감지: 빠른 진입 (조건 대폭 강화!)
            # v3.1: 분봉 조건 90% 충족 + MTF 상승 추세 필요
            has_minute_support = minute_result['price_change'] > MOMENTUM_THRESHOLD * 0.9
            has_bullish_trend = mtf_result.get('trend_5m') == 'bullish' or mtf_result.get('trend_15m') == 'bullish'
            
            if has_minute_support and has_bullish_trend:
                combined_signal = True
                combined_strength = second_result['strength']
                reasons.append(f"⚡빠른진입: {second_result['reason']}")
            elif has_minute_support:
                # 분봉 지지만 있고 MTF 상승이 아니면 강도 대폭 하락
                combined_signal = True
                combined_strength = second_result['strength'] * 0.5  # 50% 감소
                reasons.append(f"⚠️ 약한진입: {second_result['reason']} (MTF 미확인)")
                
        elif minute_result['signal']:
            # 분봉 신호만: 일반 진입
            combined_signal = True
            combined_strength = minute_result['strength'] * 0.8
            reasons.append(minute_result['reason'])
        
        # === 2단계: MTF 필터 적용 (핵심!) ===
        if combined_signal and MTF_ENABLED:
            # MTF 분석 결과에 따른 진입 차단/허용
            if not mtf_result['valid_entry']:
                combined_signal = False
                mtf_blocked = True
                reasons.append(f"🚫 MTF 차단: {' | '.join(mtf_result['warnings'])}")
            else:
                # 상승 단계에 따른 강도 조정
                stage = mtf_result.get('stage', 'unknown')
                if stage == 'early':
                    combined_strength = min(100, combined_strength * 1.2)  # 초기 단계 보너스
                    reasons.append(f"🎯 상승초기 진입")
                elif stage == 'mid':
                    combined_strength = combined_strength * 0.9  # 중반은 약간 할인
                    reasons.append(f"📈 상승중반")
                elif stage == 'late':
                    combined_signal = False  # 후반 진입 차단
                    mtf_blocked = True
                    reasons.append(f"🚫 상승후반 - 진입차단")
                elif stage == 'neutral' or stage == 'unknown':
                    # v3.1: MTF 중립/미확인 시 추가 조건 적용
                    combined_strength = combined_strength * 0.7  # 30% 감소
                    if combined_strength < 70:  # 중립 시 강도 70 이상 필요
                        combined_signal = False
                        mtf_blocked = True
                        reasons.append(f"🚫 MTF 중립 + 강도 부족 ({combined_strength:.0f}<70)")
                    else:
                        reasons.append(f"⚠️ MTF중립 (강도:{combined_strength:.0f})")
                
                # 거래량 확인 보너스
                if mtf_result['volume_confirmed']:
                    combined_strength = min(100, combined_strength + 10)
                
                # 15분봉 추세 보너스/패널티
                if mtf_result['trend_15m'] == 'bullish':
                    combined_strength = min(100, combined_strength + 5)
                elif mtf_result['trend_15m'] == 'bearish':
                    combined_strength = max(0, combined_strength - 15)
                    if MTF_STRICT_MODE:
                        combined_signal = False
                        mtf_blocked = True
                        reasons.append(f"🚫 15분봉 하락추세")
        
        # === 3단계: 최소 신호 강도 체크 (v3.1 추가) ===
        MIN_SIGNAL_STRENGTH = 60
        if combined_signal and combined_strength < MIN_SIGNAL_STRENGTH:
            combined_signal = False
            mtf_blocked = True
            reasons.append(f"🚫 최소 강도 미달 ({combined_strength:.0f}<{MIN_SIGNAL_STRENGTH})")
        
        return {
            'signal': combined_signal,
            'strength': combined_strength,
            'minute_signal': minute_result['signal'],
            'second_signal': second_result.get('signal', False),
            'rapid_rise': second_result.get('rapid_rise', False),
            'mtf_valid': mtf_result['valid_entry'],
            'mtf_stage': mtf_result.get('stage', 'unknown'),
            'mtf_trend_5m': mtf_result.get('trend_5m', 'neutral'),
            'mtf_trend_15m': mtf_result.get('trend_15m', 'neutral'),
            'mtf_blocked': mtf_blocked,
            'reason': ' | '.join(reasons) if reasons else '조건 미충족'
        }


class MomentumTrader:
    """모멘텀 트레이딩 봇"""
    
    def __init__(self):
        self.access_key = ACCESS_KEY
        self.secret_key = SECRET_KEY
        self.api = UpbitAPI(ACCESS_KEY, SECRET_KEY)
        
        # Redis 초기화
        try:
            self.redis = redis.Redis(host='localhost', port=6379, db=0, password=REDIS_PASSWORD, decode_responses=True)
            self.redis.ping()
            logger.info("✅ Redis 연결 성공")
            
            # Redis Log Handler 추가
            redis_handler = RedisLogHandler(self.redis)
            redis_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
            logging.getLogger().addHandler(redis_handler)
        except Exception as e:
            logger.error(f"Redis 연결 실패: {e}")
            self.redis = None
        
        # 동적 관리
        self.markets = []  
        self.states = {}     # {market: TradingState}
        self.analyzers = {}  # {market: MarketAnalyzer}
        self.assets = {}     # {currency: {balance, locked, avg_buy_price}}
        
        self.current_prices = {} 
        self.last_price_updates = {}
        
        self.running = True
        
        # 자산 및 주문 (WebSocket 업데이트)
        self.active_orders = {} 
        
        # === BTC 중심 시장 분석 ===
        self.btc_trend = 'neutral'          # BTC 추세 (bullish/bearish/neutral)
        self.btc_change_rate = 0.0          # BTC 1시간 변화율
        self.last_btc_check = None          # 마지막 BTC 체크 시간
        self.market_safe = True             # 시장 안전 여부 (BTC 기반)
        
        # === 누적 수익 추적 (전체) ===
        self.cumulative_profit = 0.0        # 누적 수익 (원)
        self.cumulative_trades = 0          # 누적 거래 횟수
        self.cumulative_wins = 0            # 누적 수익 거래
        self.cumulative_losses = 0          # 누적 손실 거래
        self.start_time = datetime.now()    # 봇 시작 시간
        
        # 거래 로그 파일 초기화
        self._init_trade_log()
        
        # 초기 자산 로딩
        try:
             accounts = self.api.get_accounts()
             for acc in accounts:
                 cur = acc['currency']
                 self.assets[cur] = {
                     'balance': float(acc['balance']),
                     'locked': float(acc['locked']),
                     'avg_buy_price': float(acc['avg_buy_price'])
                 }
        except Exception as e:
            logger.error(f"초기 자산 로딩 실패: {e}")
    
    def _init_trade_log(self):
        """거래 로그 파일 초기화"""
        import os
        log_dir = os.path.dirname(TRADE_LOG_FILE)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # 파일이 없으면 헤더 작성
        if not os.path.exists(TRADE_LOG_FILE):
            with open(TRADE_LOG_FILE, 'w', encoding='utf-8') as f:
                f.write("timestamp,market,type,price,trade_value,volume,profit,profit_rate,cumulative_profit,reason\n")
            logger.info(f"📝 거래 로그 파일 생성: {TRADE_LOG_FILE}")
    
    def _log_trade(self, market: str, trade_type: str, price: float, amount: float, 
                   volume: float = 0, profit: float = 0, profit_rate: float = 0, reason: str = ""):
        """거래 내역을 파일에 기록"""
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(TRADE_LOG_FILE, 'a', encoding='utf-8') as f:
                f.write(f"{timestamp},{market},{trade_type},{price:.2f},{amount:.2f},{volume:.8f},{profit:.2f},{profit_rate:.4f},{self.cumulative_profit:.2f},{reason}\n")
        except Exception as e:
            logger.error(f"거래 로그 기록 실패: {e}")

    async def _update_top_markets(self):
        """거래대금 상위 종목으로 마켓 리스트 갱신
        - MARKET이 빈 배열이면: 자동으로 TOP_MARKET_COUNT개 선정
        - MARKET이 지정되어 있으면: 해당 마켓만 사용
        """
        try:
            # === 수동 마켓 지정 모드 ===
            if MARKET and len(MARKET) > 0:
                # 지정된 마켓만 사용 (초기화 시 1회만 실행)
                if not self.markets:
                    new_markets = MARKET.copy()
                    logger.info(f"🎯 수동 마켓 지정 모드: {len(new_markets)}개 종목")
                    logger.info(f"   마켓: {new_markets}")
                    
                    # 마켓 초기화
                    for market in new_markets:
                        if market not in self.states:
                            self.states[market] = TradingState(market)
                        if market not in self.analyzers:
                            self.analyzers[market] = MarketAnalyzer(self.api, market)
                            
                        try:
                            self.analyzers[market].analyze_macro()
                            
                            # 1분봉 로드
                            candles = self.api.get_candles_minutes(market, CANDLE_UNIT, 200)
                            self.analyzers[market].update_candles(candles)
                            
                            # 5분봉 로드
                            candles_5m = self.api.get_candles_minutes(market, 5, 100)
                            self.analyzers[market].update_candles_5m(candles_5m)
                            
                            # 15분봉 로드
                            candles_15m = self.api.get_candles_minutes(market, 15, 50)
                            self.analyzers[market].update_candles_15m(candles_15m)
                            
                            # 초봉 로드
                            sec_candles = self.api.get_candles_seconds(market, 120)
                            self.analyzers[market].update_second_candles(sec_candles)
                            
                            self.last_price_updates[market] = None
                            logger.info(f"[{market}] 초기 데이터 로드 완료 (1분:{len(candles)} 5분:{len(candles_5m)} 15분:{len(candles_15m)} 초:{len(sec_candles)})")
                            
                        except Exception as e:
                            logger.error(f"[{market}] 초기 데이터 로딩 실패: {e}")
                    
                    self.markets = new_markets
                return  # 수동 모드에서는 갱신 없음
            
            # === 자동 마켓 선정 모드 ===
            # 1. 모든 KRW 마켓 조회
            all_markets = self.api.get_all_markets()
            krw_markets = [m['market'] for m in all_markets if m['market'].startswith('KRW-')]
            
            # 2. 현재가 및 거래대금 조회
            tickers = []
            chunk_size = 100
            for i in range(0, len(krw_markets), chunk_size):
                chunk = krw_markets[i:i+chunk_size]
                if not chunk: break
                tickers.extend(self.api.get_ticker(','.join(chunk)))
                time.sleep(0.1) # Rate limit safe
            
            # 3. 24시간 거래대금 기준 정렬
            sorted_tickers = sorted(tickers, key=lambda x: x['acc_trade_price_24h'], reverse=True)
            top_markets = [t['market'] for t in sorted_tickers[:TOP_MARKET_COUNT]]
            
            # 4. 현재 보유 중인 종목은 무조건 포함
            held_markets = []
            for market, state in self.states.items():
                if state.has_position():
                    held_markets.append(market)
            
            # 5. 최종 마켓 리스트 병합 (중복 제거)
            new_markets = list(set(top_markets + held_markets))
            
            # 6. 변경 사항 적용
            added_markets = [m for m in new_markets if m not in self.markets]
            removed_markets = [m for m in self.markets if m not in new_markets]
            
            if added_markets or removed_markets:
                logger.info(f"🔄 마켓 리스트 갱신 (총 {len(new_markets)}개)")
                if added_markets:
                    logger.info(f"   ➕ 추가: {added_markets}")
                if removed_markets:
                    logger.info(f"   ➖ 제외: {removed_markets}")
                
                # 추가된 마켓 초기화
                for market in added_markets:
                    if market not in self.states:
                        self.states[market] = TradingState(market)
                    if market not in self.analyzers:
                        self.analyzers[market] = MarketAnalyzer(self.api, market)
                        
                    # 초기 데이터 로딩 (캔들, 거시분석)
                    try:
                        self.analyzers[market].analyze_macro()
                        
                        # 1분봉 로드
                        candles = self.api.get_candles_minutes(market, CANDLE_UNIT, 200)
                        self.analyzers[market].update_candles(candles)
                        
                        # 5분봉 로드
                        candles_5m = self.api.get_candles_minutes(market, 5, 100)
                        self.analyzers[market].update_candles_5m(candles_5m)
                        
                        # 15분봉 로드
                        candles_15m = self.api.get_candles_minutes(market, 15, 50)
                        self.analyzers[market].update_candles_15m(candles_15m)
                        
                        # 초봉 로드
                        sec_candles = self.api.get_candles_seconds(market, 120)
                        self.analyzers[market].update_second_candles(sec_candles)
                        
                        self.last_price_updates[market] = None
                        logger.info(f"[{market}] 초기 데이터 로드 완료")
                        
                    except Exception as e:
                        logger.error(f"[{market}] 초기 데이터 로딩 실패: {e}")

                self.markets = new_markets
                
        except Exception as e:
            logger.error(f"마켓 리스트 갱신 실패: {e}")

    async def _market_update_loop(self):
        """주기적으로 마켓 리스트 갱신"""
        while self.running:
            try:
                await self._update_top_markets()
            except Exception as e:
                logger.error(f"마켓 업데이트 루프 오류: {e}")
            
            await asyncio.sleep(MARKET_UPDATE_INTERVAL)

    async def start(self):
        """트레이딩 봇 시작"""
        logger.info("=" * 60)
        logger.info("🚀 모멘텀 트레이딩 봇 시작 (BTC 중심 전략)")
        
        # 1. 마켓 리스트 구성 (가장 먼저 실행)
        await self._update_top_markets()
        
        if not self.markets:
             logger.error("거래 가능한 마켓이 없습니다. 종료합니다.")
             return

        logger.info(f"   타겟 마켓: {len(self.markets)}개 종목 (Top {TOP_MARKET_COUNT} + 보유)")
        logger.info(f"   최대 투자금: {MAX_INVESTMENT:,}원")
        logger.info(f"   테스트 모드: {'ON' if DRY_RUN else 'OFF'}")
        logger.info(f"   📊 BTC 중심 시장 분석: 활성화")
        logger.info(f"   📝 거래 기록 파일: {TRADE_LOG_FILE}")
        logger.info("=" * 60)
        
        # 2. 초기 BTC 추세 확인
        await self._check_btc_trend()
        
        # 3. 초기 잔고 확인
        self._check_balance()
        
        # 4. 기 보유 종목에 대한 상태 동기화
        self._sync_state_with_balance()
        
        self.running = True
        
        try:
            await asyncio.gather(
                self._public_ws_monitor(),
                self._private_ws_monitor(),
                self._trading_loop(),
                self._macro_update_loop(),
                self._balance_report_loop(),
                self._market_update_loop(),
                self._btc_monitor_loop()  # BTC 추세 모니터링 추가
            )
        except KeyboardInterrupt:
            logger.info("사용자에 의해 중단됨")
        except Exception as e:
            logger.error(f"봇 오류: {e}")
        finally:
            self.running = False
            self._print_summary()
    
    async def _check_btc_trend(self):
        """BTC 추세 확인 (시장 중심 지표)"""
        try:
            # BTC 1시간봉으로 추세 확인
            h1_candles = self.api.get_candles_minutes(BTC_MARKET, unit=60, count=2)
            if len(h1_candles) >= 2:
                btc_change = (h1_candles[0]['trade_price'] - h1_candles[1]['trade_price']) / h1_candles[1]['trade_price']
                self.btc_change_rate = btc_change
                
                # 추세 판단
                if btc_change <= BTC_TREND_THRESHOLD:
                    self.btc_trend = 'bearish'
                    # BTC_DOWNTREND_BUY_BLOCK이 True일 때만 매수 금지
                    self.market_safe = not BTC_DOWNTREND_BUY_BLOCK
                elif btc_change >= BTC_BULLISH_THRESHOLD:
                    self.btc_trend = 'bullish'
                    self.market_safe = True
                else:
                    self.btc_trend = 'neutral'
                    self.market_safe = True  # neutral에서는 거래 허용
                
                self.last_btc_check = datetime.now()
                
                # 로그 출력
                trend_emoji = "🟢" if self.btc_trend == 'bullish' else ("🔴" if self.btc_trend == 'bearish' else "🟡")
                safe_status = "✅ 진입가능" if self.market_safe else "⛔ 진입중단"
                block_status = "[BTC차단:ON]" if BTC_DOWNTREND_BUY_BLOCK else "[BTC차단:OFF]"
                logger.info(f"[{BTC_MARKET}] {trend_emoji} BTC 추세: {self.btc_trend.upper()} | "
                          f"1시간 변화: {Color.YELLOW}{btc_change*100:+.2f}%{Color.RESET} | {safe_status} {block_status}")
                
        except Exception as e:
            logger.error(f"BTC 추세 확인 오류: {e}")
            # 오류 시에도 안전하게 처리
            self.market_safe = True  # 오류 시 거래 허용 (보수적)
    
    async def _btc_monitor_loop(self):
        """BTC 추세 주기적 모니터링"""
        while self.running:
            await asyncio.sleep(BTC_CHECK_INTERVAL)
            try:
                await self._check_btc_trend()
            except Exception as e:
                logger.error(f"BTC 모니터링 루프 오류: {e}")

    async def _balance_report_loop(self):
        """주기적인 잔고 및 보유 종목 리포트"""
        while self.running:
            await asyncio.sleep(BALANCE_REPORT_INTERVAL)
            try:
                # 잔고 확인은 API 호출이 포함되므로 별도로 로그 처리
                logger.info("=" * 40)
                logger.info("📋 정기 보유 종목 및 잔고 리포트")
                # Blocking IO를 Executor에서 실행
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._check_balance)
                logger.info("=" * 40)
            except Exception as e:
                logger.error(f"리포트 루프 오류: {e}")
    
    def _update_redis_ticker(self, market: str, price: float):
        """Redis에 시세 정보 업데이트"""
        if not self.redis: return
        try:
            key = f"[MON]:TICKER:{market}"
            mapping = {
                "price": str(price),
                "timestamp": str(datetime.now())
            }
            # 마켓 분석 정보도 함께
            if market in self.analyzers:
                an = self.analyzers[market]
                
                # m1_change
                m1_change = 0.0
                if an.minute_candles:
                    last_c = an.minute_candles[-1]
                    op = last_c.get('opening_price', 0)
                    if op > 0: m1_change = (price - op) / op * 100
                
                # buy_ratio
                buy_ratio = 50.0
                vol = an.bid_volume_1m + an.ask_volume_1m
                if vol > 0: buy_ratio = an.bid_volume_1m / vol * 100
                
                mapping.update({
                    "rsi": f"{an.rsi_value:.1f}",
                    "fatigue": f"{an.fatigue_score:.1f}",
                    "sentiment": an.market_sentiment,
                    "m1_change": f"{m1_change:.2f}",
                    "buy_ratio": f"{buy_ratio:.1f}"
                })
            
            self.redis.hset(key, mapping=mapping)
        except Exception: pass

    def _update_redis_status(self):
        """Redis에 봇 상태 및 자산 정보 업데이트"""
        if not self.redis: return
        try:
            # 1. 자산 정보
            assets_json = json.dumps(self.assets)
            self.redis.set("[MON]:ASSETS", assets_json)
            
            # 2. 보유 종목 상태
            positions = {}
            for market, state in self.states.items():
                if state.has_position():
                    positions[market] = {
                        'entry_price': state.entry_price,
                        'current_price': self.current_prices.get(market, 0),
                        'profit_rate': (self.current_prices.get(market, 0) - state.entry_price) / state.entry_price * 100 if state.entry_price > 0 else 0,
                        'volume': state.position.get('volume', 0)
                    }
            self.redis.set("[MON]:POSITIONS", json.dumps(positions))
            
            # 3. 누적 수익
            summary = {
                'profit': self.cumulative_profit,
                'trades': self.cumulative_trades,
                'wins': self.cumulative_wins
            }
            self.redis.set("[MON]:SUMMARY", json.dumps(summary))
            
        except Exception: pass

    def _check_balance(self):
        """잔고 확인 (WebSocket 데이터 기반)"""
        try:
            # KRW 잔고 표시
            if 'KRW' in self.assets:
                krw = self.assets['KRW']
                balance = krw['balance']
                locked = krw['locked']
                logger.info(f"💰 KRW 잔고: {Color.YELLOW}{balance:,.0f}원{Color.RESET} (주문가능: {Color.YELLOW}{balance-locked:,.0f}원{Color.RESET})")
            
            # 보유 자산별 평가금액 계산
            total_valuation = 0.0
            
            for currency, asset in self.assets.items():
                if currency == 'KRW':
                    continue
                    
                balance = asset['balance']
                locked = asset['locked']
                total_balance = balance + locked
                
                if total_balance <= 0:
                    continue
                    
                avg_buy_price = asset.get('avg_buy_price', 0.0)
                
                # 현재가 조회 (KRW 마켓 가정)
                market_code = f"KRW-{currency}"
                current_price = self.current_prices.get(market_code, 0.0)
                
                # 현재가가 없으면 평단가로 대쳐 (보수적 평가)
                if current_price == 0:
                    current_price = avg_buy_price
                
                valuation = total_balance * current_price
                total_valuation += valuation
                
                # 수익률 계산
                profit_rate = 0.0
                if avg_buy_price > 0:
                     profit_rate = (current_price - avg_buy_price) / avg_buy_price * 100
                
                # 수익률 색상
                pnl_color = Color.GREEN if profit_rate >= 0 else Color.RED
                
                logger.info(f"🪙 {Color.BOLD}{currency}{Color.RESET} | "
                          f"보유: {Color.YELLOW}{total_balance:,.8f}{Color.RESET} | "
                          f"평단: {Color.YELLOW}{avg_buy_price:,.0f}원{Color.RESET} | "
                          f"현재: {Color.YELLOW}{current_price:,.0f}원{Color.RESET} | "
                          f"평가: {Color.YELLOW}{valuation:,.0f}원{Color.RESET} ({pnl_color}{profit_rate:+.2f}%{Color.RESET})")
                          
            logger.info(f"💵 총 자산 추정: {Color.YELLOW}{self.assets.get('KRW', {}).get('balance', 0) + total_valuation:,.0f}원{Color.RESET}")
            
            # Redis 업데이트
            self._update_redis_status()
            
        except Exception as e:
            logger.error(f"잔고 확인 실패: {e}")
    
    async def _public_ws_monitor(self):
        """WebSocket (Public) - 실시간 시세, 호가, 체결, 캔들"""
        while self.running:
            try:
                async with websockets.connect(WS_PUBLIC_URL, ping_interval=60, ping_timeout=30) as ws:
                    codes = self.markets
                    
                    # 구독: ticker, trade, orderbook, candle (1s, 1m, 5m, 15m)
                    subscribe = [
                        {"ticket": f"momentum-pub-{uuid.uuid4()}"},
                        {"type": "ticker", "codes": codes, "isOnlyRealtime": True},
                        {"type": "trade", "codes": codes, "isOnlyRealtime": True},
                        {"type": "orderbook", "codes": codes, "isOnlyRealtime": True},
                        {"type": "candle.1s", "codes": codes},    # 초봉
                        {"type": "candle.1m", "codes": codes},    # 1분봉
                        {"type": "candle.5m", "codes": codes},    # 5분봉
                        {"type": "candle.15m", "codes": codes},   # 15분봉
                        {"format": "DEFAULT"}
                    ]
                    
                    await ws.send(json.dumps(subscribe))
                    logger.info(f"📡 Public WebSocket 연결됨 ({len(codes)}개 마켓) - ticker + trade + orderbook + 초/1분/5분/15분봉")
                    
                    # PING 타이머
                    last_ping = time.time()
                    
                    while self.running:
                        try:
                            # PING 전송 (60초마다)
                            if time.time() - last_ping > 60:
                                await ws.send("PING")
                                last_ping = time.time()
                            
                            # 메시지 수신
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            
                            if msg == "PONG":
                                continue
                            
                            data = json.loads(msg)
                            
                            # 에러 응답 처리
                            if 'error' in data:
                                err_name = data['error'].get('name', 'UNKNOWN')
                                err_msg = data['error'].get('message', '')
                                logger.error(f"WebSocket 에러: {err_name} - {err_msg}")
                                continue
                            
                            type_val = data.get('type') or data.get('ty')
                            if not type_val:  # type 없는 경우
                                continue
                            
                            code = data.get('cd') or data.get('code')  # 마켓 코드 (KRW-BTC 등)
                            
                            if code and code in self.markets:
                                if type_val == 'ticker':
                                    self.current_prices[code] = data.get('trade_price') or data.get('tp')
                                    self.last_price_updates[code] = datetime.now()
                                    self._update_redis_ticker(code, self.current_prices[code])
                                    
                                elif type_val == 'trade':
                                    # 체결 데이터 - 가격 업데이트 + 매수/매도 세력 분석
                                    self.current_prices[code] = data.get('trade_price') or data.get('tp', self.current_prices.get(code, 0))
                                    self.last_price_updates[code] = datetime.now()
                                    self._update_redis_ticker(code, self.current_prices[code])
                                    # 체결 데이터를 Analyzer에 전달 (매수/매도 분석용)
                                    self.analyzers[code].update_trade_from_ws(data)
                                    
                                elif type_val == 'orderbook':
                                    # 호가 데이터 - 매수벽/매도벽 분석
                                    self.analyzers[code].update_orderbook_from_ws(data)
                                    if self.redis:
                                        self.redis.set(f"[MON]:ORDERBOOK:{code}", json.dumps(data))
                                
                                elif type_val.startswith('candle.'):
                                    # 캔들 데이터 (1s, 1m, 5m, 15m 등)
                                    self.analyzers[code].update_candle_from_ws(data, type_val)
                                
                        except asyncio.TimeoutError:
                            await ws.send("PING")
                            last_ping = time.time()
                            
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"Public WebSocket 연결 끊김 (code:{e.code}), 재연결 시도...")
                await asyncio.sleep(3)
            except Exception as e:
                logger.error(f"Public WebSocket 오류: {e}")
                await asyncio.sleep(5)

    async def _private_ws_monitor(self):
        """WebSocket (Private) - 내 주문, 자산"""
        # JWT 토큰 생성
        token = self.api._generate_jwt()
        headers = {'Authorization': f'Bearer {token}'}
        
        while self.running:
            try:
                async with websockets.connect(WS_PRIVATE_URL, additional_headers=headers) as ws:
                    # 구독 요청 (myOrder, myAsset)
                    subscribe = [
                        {"ticket": f"momentum-priv-{uuid.uuid4()}"},
                        {"type": "myOrder", "codes": self.markets}, # 마켓 지정 가능하면 지정
                        {"type": "myAsset"},
                        {"format": "DEFAULT"}
                    ]
                    await ws.send(json.dumps(subscribe))
                    logger.info("🔐 Private WebSocket 연결됨 - 주문/자산 모니터링")
                    
                    last_ping = time.time()
                    
                    while self.running:
                        # 토큰 만료 갱신 필요 시 재연결 로직은 복잡하므로, 끊어지면 다시 연결하도록 유도
                        # (일반적으로 JWT 유효기간 내에 동작하거나, 끊어지면 다시 headers 생성해서 연결)
                        
                        try:
                            if time.time() - last_ping > 60:
                                await ws.send("PING")
                                last_ping = time.time()
                                
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            if msg == "PONG": continue
                            
                            data = json.loads(msg)
                            type_val = data.get('type') or data.get('ty')
                            
                            if type_val == 'myAsset':
                                # 자산 업데이트
                                assets = data.get('assets') or data.get('ast')
                                for asset in assets:
                                    cur = asset.get('currency') or asset.get('cu')
                                    self.assets[cur] = {
                                        'balance': float(asset.get('balance') or asset.get('b')),
                                        'locked': float(asset.get('locked') or asset.get('l')),
                                        'avg_buy_price': float(asset.get('avg_buy_price') or asset.get('abp'))
                                    }
                                    
                            elif type_val == 'myOrder':
                                # 주문 상태 업데이트
                                uid = data.get('uuid') or data.get('uid')
                                state = data.get('state') or data.get('s')
                                
                                if state in ['wait', 'watch']:
                                    self.active_orders[uid] = data
                                elif state in ['done', 'cancel']:
                                    if uid in self.active_orders:
                                        del self.active_orders[uid]
                                        
                        except asyncio.TimeoutError:
                            await ws.send("PING")
                            last_ping = time.time()
                            
            except websockets.exceptions.ConnectionClosed:
                logger.warning("Private WebSocket 연결 끊김, 재연결 시도...")
                # 재연결 시 토큰 갱신
                token = self.api._generate_jwt()
                headers = {'Authorization': f'Bearer {token}'}
                await asyncio.sleep(3)
            except Exception as e:
                logger.error(f"Private WebSocket 오류: {e}")
                await asyncio.sleep(5)
    
    async def _trading_loop(self):
        """메인 트레이딩 루프"""
        # 가격 데이터 수신 대기
        await asyncio.sleep(5)
        last_status_log = 0
        
        while self.running:
            try:
                # === BTC 안전 체크 (시장 중심 지표) ===
                if not self.market_safe:
                    # BTC가 하락 중이면 신규 진입 중단 (기존 포지션은 관리)
                    for market in self.markets:
                        state = self.states[market]
                        if state.has_position():
                            await self._manage_position(market)
                    await asyncio.sleep(1)
                    continue
                
                # 모든 마켓에 대해 반복
                for market in self.markets:
                    current_price = self.current_prices.get(market, 0)
                    if current_price <= 0:
                        continue
                    
                    analyzer = self.analyzers[market]
                    state = self.states[market]
                    
                    # 거시 분석 결과 확인
                    if analyzer.macro_trend == 'bearish':
                        if not state.has_position():
                             # 하락장에서는 관망 (로그는 너무 자주 찍히지 않게 조절 필요)
                            continue
                    
                    if state.has_position():
                        # 포지션 관리
                        await self._manage_position(market)
                    else:
                        # 진입 기회 탐색
                        await self._find_entry(market)
                    
                # 30초마다 분석 상태 로그 + 누적 수익률
                now = time.time()
                if now - last_status_log >= 30:
                    last_status_log = now
                    
                    # === 누적 수익률 출력 ===
                    runtime = datetime.now() - self.start_time
                    runtime_str = str(runtime).split('.')[0]  # 소수점 제거
                    profit_color = Color.GREEN if self.cumulative_profit >= 0 else Color.RED
                    logger.info(f"💰 누적 수익: {profit_color}{self.cumulative_profit:+,.0f}원{Color.RESET} | "
                              f"거래: {self.cumulative_trades}회 (승:{self.cumulative_wins}/패:{self.cumulative_losses}) | "
                              f"실행시간: {runtime_str}")
                    
                    for market in self.markets:
                        price = self.current_prices.get(market, 0)
                        if price <= 0: continue
                        
                        analyzer = self.analyzers[market]
                        # 상세 분석 정보 수집
                        min_result = analyzer.detect_momentum(price)
                        sec_result = analyzer.detect_second_momentum(price) if USE_SECOND_CANDLES else {}
                        
                        min_change = min_result.get('price_change', 0) * 100
                        vol_ratio = min_result.get('volume_ratio', 0)
                        sec_change = sec_result.get('price_change', 0) * 100 if sec_result else 0
                        
                        # 심리 분석 정보 추가
                        rsi = analyzer.rsi_value
                        fatigue = analyzer.fatigue_score
                        sentiment = analyzer.market_sentiment
                        
                        # 매수/매도 비율
                        total_vol = analyzer.bid_volume_1m + analyzer.ask_volume_1m
                        buy_ratio = analyzer.bid_volume_1m / total_vol * 100 if total_vol > 0 else 50
                        
                        sentiment_emoji = "🟢" if sentiment == 'bullish' else ("🔴" if sentiment == 'bearish' else "🟡")
                        
                        logger.info(f"[{market}] 📊 {price:,.0f}원 | "
                                  f"분봉:{min_change:+.2f}% | "
                                  f"RSI:{rsi:.0f} 피로:{fatigue:.0f} | "
                                  f"매수:{buy_ratio:.0f}% | {sentiment_emoji}{sentiment}")
                
                await asyncio.sleep(1)  # 1초마다 체크
                
            except Exception as e:
                logger.error(f"트레이딩 루프 오류: {e}")
                await asyncio.sleep(5)
    
    async def _macro_update_loop(self):
        """거시 분석 주기적 업데이트"""
        while self.running:
            await asyncio.sleep(MACRO_UPDATE_INTERVAL)
            try:
                for market in self.markets:
                    self.analyzers[market].analyze_macro()
                    await asyncio.sleep(1.0) # 마켓 간 딜레이
            except Exception as e:
                logger.error(f"거시 분석 업데이트 오류: {e}")
    
    async def _find_entry(self, market: str):
        """진입 기회 탐색 - 전문가 관점의 종합 분석"""
        state = self.states[market]
        if not state.can_trade():
            return
            
        analyzer = self.analyzers[market]
        current_price = self.current_prices[market]
        
        # === 재진입 방지 (손절 후 동일 가격대 재진입 차단) ===
        if state.last_exit_price > 0 and state.consecutive_losses > 0:
            # 마지막 청산가 대비 2% 이상 하락해야 재진입 허용
            min_reentry_price = state.last_exit_price * 0.98
            if current_price > min_reentry_price:
                if int(time.time()) % 30 == 0:  # 30초에 한번 로그
                    logger.debug(f"[{market}] ⏳ 재진입 대기 - 현재가({current_price:,.0f}) > 재진입가({min_reentry_price:,.0f})")
                return
        
        try:
            # 캔들 데이터 부족하면 대기
            if len(analyzer.minute_candles) < MOMENTUM_WINDOW:
                logger.debug(f"[{market}] 캔들 데이터 수집 중... ({len(analyzer.minute_candles)}/{MOMENTUM_WINDOW})")
                return

            if USE_SECOND_CANDLES and len(analyzer.second_candles) < SECOND_MOMENTUM_WINDOW:
                 return
            
            # ==== 1단계: 종합 시장 심리 분석 (전문가 관점) ====
            sentiment = analyzer.analyze_market_sentiment()
            
            # 강력한 매도 우위/피로도 높음 → 진입 차단
            if sentiment['sentiment'] == 'bearish':
                # 상세 경고 로그 (10초에 1번만)
                if int(time.time()) % 10 == 0:
                    warnings = ' | '.join(sentiment.get('warnings', []))
                    logger.debug(f"[{market}] 🚫 진입 차단 - 부정적 심리 (점수:{sentiment['score']:.0f})")
                    if warnings:
                        logger.debug(f"   경고: {warnings}")
                return
            
            # 피로도가 높으면 신중하게 접근 (진입 조건 강화) - 임계값 강화
            high_fatigue = sentiment['fatigue'] >= 35  # 40 -> 35로 강화
            overbought = sentiment['rsi'] >= 65  # 70 -> 65로 강화
            very_overbought = sentiment['rsi'] >= 75  # 극심한 과매수
            
            # ==== 2단계: 모멘텀 감지 (분봉 + 초봉) ====
            momentum = analyzer.detect_combined_momentum(current_price)
            
            if not momentum['signal']:
                return
            
            # ==== 3단계: 피로도/과매수 시 추가 필터링 (강화) ====
            if very_overbought:
                # RSI 75 이상: 진입 차단
                logger.info(f"[{market}] 🚫 극심한 과매수 (RSI:{sentiment['rsi']:.0f}) - 진입 차단")
                return
                
            if high_fatigue or overbought:
                # 피로도 높거나 과매수이면, 더 강력한 신호만 허용 (75로 강화)
                if momentum['strength'] < 75:
                    if int(time.time()) % 15 == 0:
                        logger.info(f"[{market}] ⚠️ 신호 감지되었으나 피로도/과매수로 신중 대기 | "
                                  f"피로도:{sentiment['fatigue']:.0f} RSI:{sentiment['rsi']:.0f} 강도:{momentum['strength']:.0f}")
                    return
                
                # 매도 우위라면 진입 차단 (50%로 강화)
                if sentiment['sell_pressure'] > 0.50:
                    logger.info(f"[{market}] ⚠️ 매도 우위 전환 감지 - 진입 보류 (매도비율:{sentiment['sell_pressure']*100:.1f}%)")
                    return
            
            # ==== 4단계: 모멘텀 소진 체크 ====
            if analyzer.momentum_exhaustion:
                logger.info(f"[{market}] 📉 모멘텀 소진 - 거래량 급감으로 진입 보류")
                return
            
            # ==== 최종: 진입 신호 확정 ====
            rapid_indicator = "🚀" if momentum.get('rapid_rise') else "🎯"
            sentiment_info = f"심리:{sentiment['sentiment']}({sentiment['score']:.0f})"
            trade_ratio_info = f"매수:{sentiment['buy_pressure']*100:.0f}%/매도:{sentiment['sell_pressure']*100:.0f}%"
            
            # MTF 정보 추가
            mtf_stage = momentum.get('mtf_stage', 'unknown')
            mtf_stage_icon = {'early': '🟢초기', 'mid': '🟡중반', 'late': '🔴후반', 'neutral': '⚪중립'}.get(mtf_stage, '❓')
            mtf_trend_info = f"5m:{momentum.get('mtf_trend_5m', '-')} 15m:{momentum.get('mtf_trend_15m', '-')}"
            
            logger.info(f"[{Color.BOLD}{market}{Color.RESET}] {rapid_indicator} 진입 신호 확정!")
            logger.info(f"   {momentum['reason']}")
            logger.info(f"   강도:{Color.MAGENTA}{momentum['strength']:.1f}{Color.RESET} | {sentiment_info} | {trade_ratio_info}")
            logger.info(f"   RSI:{sentiment['rsi']:.1f} | 피로도:{sentiment['fatigue']:.1f} | 호가불균형:{sentiment['orderbook_imbalance']:.2f}")
            logger.info(f"   📊 MTF: {mtf_stage_icon} | {mtf_trend_info}")
            
            await self._execute_buy(market)
                
        except Exception as e:
            logger.error(f"[{market}] 진입 탐색 오류: {e}")
    
    async def _execute_buy(self, market: str):
        """매수 실행"""
        try:
            # 사용 가능 금액 확인 (Memory Cache 사용)
            krw_balance = self.assets.get('KRW', {'balance': 0})['balance']
             
            # 투자금 계산 (최대 투자금과 잔고 중 작은 값)
            # 여러 마켓이므로 자산 배분을 고려해야 하지만, 일단 단순하게 MAX_INVESTMENT 사용
            # 실전에서는 자산 배분 로직이 필요할 수 있음
            invest_amount = min(MAX_INVESTMENT, krw_balance * 0.99)  # 99%만 사용 (수수료 대비)
            
            if invest_amount < MIN_ORDER_AMOUNT:
                logger.warning(f"잔고 부족: {Color.YELLOW}{krw_balance:,.0f}원{Color.RESET}")
                return
            
            current_price = self.current_prices[market]
            
            if DRY_RUN:
                logger.info(f"[{Color.BOLD}{market}{Color.RESET}] 🛒 [테스트] 시장가 매수 | 금액: {Color.YELLOW}{invest_amount:,.0f}원{Color.RESET} | "
                          f"현재가: {Color.YELLOW}{current_price:,.0f}원{Color.RESET}")
                # 테스트 모드에서는 가상 포지션 생성
                state = self.states[market]
                state.position = {
                    'side': 'bid',
                    'price': current_price,
                    'amount': invest_amount,
                    'volume': invest_amount / current_price
                }
            else:
                # 실제 시장가 매수
                result = self.api.place_order(
                    market=market,
                    side='bid',
                    ord_type='price',  # 시장가 매수
                    price=str(int(invest_amount))
                )
                logger.info(f"[{Color.BOLD}{market}{Color.RESET}] 🛒 시장가 매수 주문 요청 | UUID: {result['uuid']} | "
                          f"금액: {Color.YELLOW}{invest_amount:,.0f}원{Color.RESET}")
                
                # 체결 대기 (Polling 제거 -> WebSocket myOrder로 확인해야 정확하지만)
                # 시장가 주문은 거의 즉시 체결되므로, 여기서는 잠시 대기 후 state 업데이트를 기다림
                # 더 완벽한 구현은 _private_ws_monitor에서 체결 메시지를 받아서 처리하는 것임.
                # 편의상 여기서는 1초 대기 후 포지션 강제 설정 (실전에서는 myOrder 이벤트 핸들러 연동 권장)
                
                await asyncio.sleep(1.0)
                
                # 가상 체결 처리 (WebSocket 지연 고려하여 보수적 접근)
                # 실제로는 WebSocket에서 체결 메시지가 오면 state 업데이트됨
                
                executed_price = current_price # 보수적 가정
                
                state = self.states[market]
                state.position = {
                    'uuid': result['uuid'],
                    'side': 'bid',
                    'price': executed_price,
                    'amount': invest_amount,
                    'volume': invest_amount / executed_price 
                }
            
            state = self.states[market]
            if state.position:
                state.entry_price = state.position['price']
                state.entry_time = datetime.now()
                state.highest_price = state.entry_price
                
                # === 동적 손절선 계산 (변동성 기반) ===
                analyzer = self.analyzers[market]
                if DYNAMIC_STOP_LOSS_ENABLED and analyzer.volatility > 0:
                    # 변동성에 따라 손절선 조정 (최소 ~ 최대 범위 내)
                    volatility_factor = min(analyzer.volatility * 10, 1.0)  # 0 ~ 1로 정규화
                    dynamic_stop = DYNAMIC_STOP_LOSS_MIN + (DYNAMIC_STOP_LOSS_MAX - DYNAMIC_STOP_LOSS_MIN) * volatility_factor
                    state.dynamic_stop_loss_rate = max(DYNAMIC_STOP_LOSS_MIN, min(dynamic_stop, DYNAMIC_STOP_LOSS_MAX))
                else:
                    state.dynamic_stop_loss_rate = INITIAL_STOP_LOSS
                
                state.stop_loss_price = state.entry_price * (1 - state.dynamic_stop_loss_rate)
                state.take_profit_price = state.entry_price * (1 + TAKE_PROFIT_TARGET)
                state.trailing_active = False
                
                state.record_trade('buy', invest_amount, state.entry_price)
                
                # 거래 로그 파일에 기록
                volume = state.position.get('volume', 0)
                self._log_trade(market, 'BUY', state.entry_price, invest_amount, volume, reason="진입")
                
                # 지표 요약
                analyzer = self.analyzers[market]
                rsi = analyzer.rsi_value
                fatigue = analyzer.fatigue_score
                
                m1_change = 0
                if analyzer.minute_candles:
                    last_candle = list(analyzer.minute_candles)[-1]
                    open_p = last_candle['opening_price']
                    if open_p > 0:
                        m1_change = (state.entry_price - open_p) / open_p * 100

                buy_ratio = 50
                total_vol = analyzer.bid_volume_1m + analyzer.ask_volume_1m
                if total_vol > 0:
                    buy_ratio = analyzer.bid_volume_1m / total_vol * 100
                
                stat_msg = f"1분:{m1_change:+.2f}% | RSI:{rsi:.0f} | 피로:{fatigue:.0f} | 매수:{buy_ratio:.0f}%"

                logger.info(f"[{Color.BOLD}{market}{Color.RESET}] ✅ 매수 체결 | 가격: {Color.YELLOW}{state.entry_price:,.0f}원{Color.RESET} | "
                          f"매수금액: {Color.YELLOW}{invest_amount:,.0f}원{Color.RESET} | "
                          f"손절가: {Color.RED}{state.stop_loss_price:,.0f}원{Color.RESET} | "
                          f"익절가: {Color.GREEN}{state.take_profit_price:,.0f}원{Color.RESET} | "
                          f"{stat_msg}")
                
        except Exception as e:
            logger.error(f"[{market}] 매수 실행 오류: {e}")
    
    async def _manage_position(self, market: str):
        """포지션 관리 (익절/손절 판단) - 개선된 버전"""
        state = self.states[market]
        if not state.has_position():
            return
            
        current = self.current_prices[market]
        entry = state.entry_price
        profit_rate = (current - entry) / entry
        
        # 최고가 업데이트
        if current > state.highest_price:
            state.highest_price = current
            
            # 트레일링 스탑 활성화 확인
            if profit_rate >= TRAILING_STOP_ACTIVATION and not state.trailing_active:
                state.trailing_active = True
                # 최소 수익 보장선 설정 (매입가 + 최소 수익률)
                min_profit_price = entry * (1 + TRAILING_MIN_PROFIT)
                state.stop_loss_price = max(state.stop_loss_price, min_profit_price)
                logger.info(f"[{Color.BOLD}{market}{Color.RESET}] 📊 트레일링 스탑 활성화 | "
                          f"수익률: {Color.GREEN}{profit_rate*100:.2f}%{Color.RESET} | "
                          f"최소 수익 보장: {Color.YELLOW}{TRAILING_MIN_PROFIT*100:.1f}%{Color.RESET}")
            
            # 트레일링 스탑 가격 업데이트
            if state.trailing_active:
                new_stop = current * (1 - TRAILING_STOP_DISTANCE)
                # 최소 수익 보장선보다 높을 때만 업데이트
                min_profit_price = entry * (1 + TRAILING_MIN_PROFIT)
                new_stop = max(new_stop, min_profit_price)
                
                if new_stop > state.stop_loss_price:
                    old_stop = state.stop_loss_price
                    state.stop_loss_price = new_stop
                    logger.debug(f"[{Color.BOLD}{market}{Color.RESET}] 🔄 트레일링 스탑 갱신: {old_stop:,.0f} → {Color.RED}{new_stop:,.0f}원{Color.RESET}")
        
        # 매도 조건 체크
        sell_reason = None
        
        # 1. 손절선 도달 (트레일링 스탑 발동 포함)
        if current <= state.stop_loss_price:
            if state.trailing_active:
                sell_reason = 'trailing_stop'
            else:
                sell_reason = 'stop_loss'
        
        # 2. 목표 수익률 도달 시 → 바로 익절하지 않고 트레일링 스탑 강화
        elif profit_rate >= TAKE_PROFIT_TARGET:
            if not state.trailing_active:
                # 트레일링 스탑 활성화
                state.trailing_active = True
                # 손절선을 최소 수익 보장선으로 올림
                min_profit_price = entry * (1 + TRAILING_MIN_PROFIT)
                state.stop_loss_price = max(entry, min_profit_price)
                logger.info(f"[{market}] 🎯 목표 수익률 {TAKE_PROFIT_TARGET*100:.1f}% 도달! "
                          f"트레일링 활성화 (최소 수익 보장: {TRAILING_MIN_PROFIT*100:.1f}%)")
            # 계속 상승 추세 추적 (바로 익절하지 않음)
        
        # 3. 최대 보유 시간 초과
        elif state.entry_time:
            holding_time = (datetime.now() - state.entry_time).total_seconds()
            if holding_time >= MAX_HOLDING_TIME:
                sell_reason = 'time_exit'
        
        if sell_reason:
            await self._execute_sell(market, sell_reason)
        else:
            # 상태 로깅 (10초마다)
            if int(time.time()) % 10 == 0:
                pnl = profit_rate * 100
                pnl_color = Color.GREEN if pnl >= 0 else Color.RED
                volume = state.position.get('volume', 0)
                
                # 평가금액 계산 (수량 × 현재가)
                eval_amount = volume * current
                # 매수금액 계산 (수량 × 매수가)
                buy_amount = volume * entry
                # 수익금 계산
                profit_amount = eval_amount - buy_amount
                profit_color = Color.GREEN if profit_amount >= 0 else Color.RED
                
                # 익절가 (트레일링 활성화 시)
                take_profit_info = ""
                if state.trailing_active:
                    # 트레일링 스탑 거리로 익절가 계산 (고점 대비)
                    take_profit_price = state.highest_price * (1 - TRAILING_STOP_DISTANCE)
                    take_profit_info = f" | 익절가: {Color.GREEN}{take_profit_price:,.0f}원{Color.RESET}"
                
                logger.info(f"[{Color.BOLD}{market}{Color.RESET}] 📈 보유 중 | 수량: {Color.CYAN}{volume:,.4f}{Color.RESET} | "
                          f"매수가: {Color.YELLOW}{entry:,.0f}원{Color.RESET} | 현재가: {Color.YELLOW}{current:,.0f}원{Color.RESET} | "
                          f"평가금액: {Color.CYAN}{eval_amount:,.0f}원{Color.RESET}")
                logger.info(f"   수익률: {pnl_color}{pnl:+.2f}%{Color.RESET} | "
                          f"수익금: {profit_color}{profit_amount:+,.0f}원{Color.RESET} | "
                          f"손절가: {Color.RED}{state.stop_loss_price:,.0f}원{Color.RESET}{take_profit_info}")
    
    
    def _sync_state_with_balance(self):
        """보유 종목에 대한 상태 동기화 (재시작 시)"""
        logger.info("♻️ 기존 보유 종목 상태 동기화 중...")
        
        for market in self.markets:
            currency = market.split('-')[1]
            asset = self.assets.get(currency)
            
            if not asset:
                continue
                
            balance = asset['balance'] + asset['locked']
            # 최소 거래 금액(5000원) 이상 가치가 있는지 대략 확인 (평단가 기준)
            avg_price = asset.get('avg_buy_price', 0)
            if balance * avg_price < 5000:
                continue

            # 이미 상태가 있으면 스킵
            if self.states[market].has_position():
                continue
                
            logger.info(f"[{market}] 보유 물량 감지 (수량: {balance}, 평단: {avg_price}) - 상태 복구 시도")
            
            try:
                # 최근 주문 조회 (최대 1달간, 7일씩 끊어서 조회)
                last_buy = None
                
                # 현재 시간부터 12주(3개월) 전까지 1주 단위로 조회
                current_cursor = datetime.now()
                
                for i in range(12):
                    # 조회 기간 설정 (끝: current_cursor, 시작: -7일)
                    end_str = current_cursor.isoformat(timespec='seconds') + "+09:00"
                    start_dt = current_cursor - timedelta(days=7)
                    start_str = start_dt.isoformat(timespec='seconds') + "+09:00"
                    
                    orders = self.api.get_closed_orders(market, limit=1000, start_time=start_str, end_time=end_str)
                    logger.info(f"[{market}] 주문 조회 ({i+1}/4주전): {len(orders)}개 ({start_str} ~ {end_str})")
                    
                    for order in orders:
                        # 체결 가격 계산 (시장가 주문은 price 필드가 없을 수 있음)
                        exec_price = order.get('price')
                        if not exec_price:
                            if float(order.get('executed_volume', 0)) > 0:
                                exec_price = float(order.get('executed_funds', 0)) / float(order.get('executed_volume'))
                            else:
                                exec_price = 0
                                
                        logger.info(f"  📜 주문내역: {order['created_at']} | {order['side']} | {exec_price} | {order.get('uuid')} | {order['state']}")
                        
                        # 매수(bid)이고 체결량이 있는 주문 (done 또는 cancel)
                        # 시장가 매수는 잔량이 남으면 cancel 상태가 될 수 있음
                        if order['side'] == 'bid' and float(order.get('executed_volume', 0)) > 0:
                            last_buy = order
                            if not last_buy.get('price'):
                                last_buy['price'] = exec_price # 값을 채워넣음
                            break
                    
                    if last_buy:
                        break
                        
                    # 못 찾았으면 다음 루프를 위해 커서를 7일 전으로 이동
                    current_cursor = start_dt
                    # API 호출 제한 고려 잠시 대기
                    time.sleep(0.1)
                
                state = self.states[market]
                
                if last_buy:
                    # 최근 매수 내역이 있으면 그것을 기준으로 설정
                    # 주의: 평단가는 이동평균이므로 실제 마지막 매수가와 다를 수 있음. 
                    # 로직상 평단가를 기준으로 수익률 계산하는 것이 맞음.
                    entry_price = float(asset['avg_buy_price']) 
                    # API 시간(Aware)을 로컬 시간(Naive)으로 변환하여 통일
                    entry_dt_aware = datetime.fromisoformat(last_buy['created_at'].replace('Z', '+00:00'))
                    entry_time = entry_dt_aware.astimezone().replace(tzinfo=None)
                    
                    logger.info(f"[{market}] 최근 매수 내역 발견: {last_buy['created_at']} (매수가: {last_buy.get('price', 0)})")
                else:
                    # 매수 내역을 못 찾으면 (너무 오래됨) 현재 시간과 평단가로 설정
                    entry_price = float(asset['avg_buy_price'])
                    entry_time = datetime.now()
                    logger.warning(f"[{market}] 매수 내역을 찾을 수 없어 평단가 기준으로 초기화합니다.")

                # 포지션 상태 복구
                state.position = {
                    'side': 'bid',
                    'price': entry_price,
                    'amount': balance * entry_price,
                    'volume': balance
                }
                state.entry_price = entry_price
                state.entry_time = entry_time
                state.highest_price = entry_price # 일단 평단가로 초기화 (이후 시세 업데이트 시 변경됨)
                
                # 손절/익절가 재설정 (현재 평단가 기준)
                state.stop_loss_price = entry_price * (1 - INITIAL_STOP_LOSS)
                state.take_profit_price = entry_price * (1 + TAKE_PROFIT_TARGET)
                state.trailing_active = False
                
                logger.info(f"[{Color.BOLD}{market}{Color.RESET}] ✅ 상태 복구 완료 | 진입가: {Color.YELLOW}{entry_price:,.0f}원{Color.RESET} | "
                          f"수량: {Color.YELLOW}{balance:,.8f}{Color.RESET} | "
                          f"손절가: {Color.RED}{state.stop_loss_price:,.0f}원{Color.RESET}")
                
            except Exception as e:
                logger.error(f"[{market}] 상태 동기화 실패: {e}")

    async def _execute_sell(self, market: str, reason: str):
        """매도 실행"""
        state = self.states[market]
        if not state.has_position():
            return
            
        try:
            currency = market.split('-')[1]
            current_price = self.current_prices[market]
            
            if DRY_RUN:
                volume = state.position.get('volume', 0)
                executed_price = current_price
                logger.info(f"[{market}] 💵 [테스트] 시장가 매도 | 사유: {reason} | "
                          f"가격: {executed_price:,.0f}원")
            else:
                # 실제 잔고 조회 (가장 최신 정보 사용)
                try:
                    accounts = self.api.get_accounts()
                    actual_balance = 0.0
                    for acc in accounts:
                        if acc['currency'] == currency:
                            actual_balance = float(acc['balance'])
                            break
                except Exception as e:
                    logger.warning(f"[{market}] 잔고 조회 실패, 캐시된 값 사용: {e}")
                    actual_balance = state.position.get('volume', 0)
                
                # 최소 주문 수량 체크 및 수량 결정
                tracked_volume = state.position.get('volume', 0)
                
                # 실제 잔고가 있으면 그것을 사용, 없으면 트래킹된 값 사용
                if actual_balance > 0:
                    volume = actual_balance
                    if abs(volume - tracked_volume) / max(tracked_volume, 0.00001) > 0.01:
                        logger.warning(f"[{market}] 잔고 불일치 감지 | 트래킹: {tracked_volume:.8f} vs 실제: {volume:.8f}")
                else:
                    volume = tracked_volume
                
                # 최소 주문 금액 체크
                order_value = volume * current_price
                if order_value < 5000:  # 최소 주문 금액 5000원
                    logger.warning(f"[{market}] 매도 주문 금액이 최소 금액(5000원) 미만: {order_value:,.0f}원")
                    # 포지션 정리 (잔고 부족으로 매도 불가)
                    state.position = None
                    state.trailing_active = False
                    return
                
                if volume <= 0:
                    logger.error(f"[{market}] 매도할 수량이 없음 (volume: {volume})")
                    state.position = None
                    state.trailing_active = False
                    return
                
                logger.info(f"[{market}] 매도 시도 | 수량: {volume:.8f} | 현재가: {current_price:,.0f}원 | 예상금액: {order_value:,.0f}원")
                
                # 실제 시장가 매도
                result = self.api.place_order(
                    market=market,
                    side='ask',
                    ord_type='market',  # 시장가 매도
                    volume=str(volume)
                )
                logger.info(f"[{market}] 💵 시장가 매도 주문 요청 | UUID: {result['uuid']} | 사유: {reason}")
                
                 # Polling 제거
                await asyncio.sleep(1.0)
                
                executed_price = current_price

            
            # 수익 계산
            buy_amount = state.position.get('amount', 0)
            sell_amount = volume * executed_price
            fee = (buy_amount + sell_amount) * TRADING_FEE_RATE
            profit = sell_amount - buy_amount - fee
            profit_rate = profit / buy_amount * 100 if buy_amount > 0 else 0
            
            # 상태 기록
            state.record_trade(reason, sell_amount, executed_price, profit)
            
            # === 누적 수익 업데이트 ===
            self.cumulative_profit += profit
            self.cumulative_trades += 1
            if profit >= 0:
                self.cumulative_wins += 1
            else:
                self.cumulative_losses += 1
            
            # 거래 로그 파일에 기록
            self._log_trade(market, 'SELL', executed_price, sell_amount, volume, profit, profit_rate/100, reason)
            
            # 포지션 정리
            state.position = None
            state.trailing_active = False
            
            emoji = "🎉" if profit >= 0 else "📉"
            pnl_color = Color.GREEN if profit >= 0 else Color.RED
            cum_color = Color.GREEN if self.cumulative_profit >= 0 else Color.RED
            # 지표 요약
            analyzer = self.analyzers[market]
            rsi = analyzer.rsi_value
            fatigue = analyzer.fatigue_score
            
            m1_change = 0
            if analyzer.minute_candles:
                last_candle = list(analyzer.minute_candles)[-1]
                open_p = last_candle['opening_price']
                if open_p > 0:
                    m1_change = (executed_price - open_p) / open_p * 100
            
            buy_ratio = 50
            total_vol = analyzer.bid_volume_1m + analyzer.ask_volume_1m
            if total_vol > 0:
                buy_ratio = analyzer.bid_volume_1m / total_vol * 100
            
            stat_msg = f"1분:{m1_change:+.2f}% | RSI:{rsi:.0f} | 피로:{fatigue:.0f} | 매수:{buy_ratio:.0f}%"

            logger.info(f"[{Color.BOLD}{market}{Color.RESET}] {emoji} 매도 완료 | 사유: {reason} | "
                       f"매도금액: {Color.YELLOW}{sell_amount:,.0f}원{Color.RESET} | "
                       f"수익: {pnl_color}{profit:+,.0f}원 ({profit_rate:+.2f}%){Color.RESET} | "
                       f"매도가: {Color.YELLOW}{executed_price:,.0f}원{Color.RESET}")
            logger.info(f"   📊 판단기준: {stat_msg}")
            logger.info(f"💰 누적 수익: {cum_color}{self.cumulative_profit:+,.0f}원{Color.RESET} | "
                       f"총 {self.cumulative_trades}회 거래 (승:{self.cumulative_wins}/패:{self.cumulative_losses})")
            
        except Exception as e:
            logger.error(f"[{market}] 매도 실행 오류: {e}")
    
    def _print_summary(self):
        """거래 요약 출력 (전체)"""
        total_trades = 0
        winning_trades = 0
        losing_trades = 0
        total_profit = 0.0
        
        runtime = datetime.now() - self.start_time
        runtime_str = str(runtime).split('.')[0]
        
        logger.info("=" * 60)
        logger.info("📊 전체 거래 요약")
        logger.info(f"   실행 시간: {runtime_str}")
        logger.info("=" * 60)
        
        for market in self.markets:
            state = self.states[market]
            if state.total_trades > 0:  # 거래가 있는 마켓만 출력
                logger.info(f"--- {market} ---")
                logger.info(f"   거래: {state.total_trades}회 (승:{state.winning_trades}/패:{state.losing_trades})")
                logger.info(f"   수익: {state.total_profit:+,.0f}원")
            
            total_trades += state.total_trades
            winning_trades += state.winning_trades
            losing_trades += state.losing_trades
            total_profit += state.total_profit
            
        logger.info("-" * 60)
        logger.info(f"   총 거래 횟수: {total_trades}회")
        win_rate = (winning_trades / max(total_trades, 1) * 100)
        logger.info(f"   전체 승률: {win_rate:.1f}%")
        logger.info(f"   총 수익: {total_profit:+,.0f}원")
        logger.info(f"   누적 수익 (세션): {self.cumulative_profit:+,.0f}원")
        logger.info("=" * 60)


async def main():
    """메인 함수"""
    trader = MomentumTrader()
    await trader.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n프로그램 종료")
