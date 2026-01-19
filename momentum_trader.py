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

# =================================================================================
# 📊 전략 파라미터 (Strategy Parameters) - 여기서 조절 가능
# =================================================================================

# === 투자 설정 ===
MAX_INVESTMENT = 1_000_000          # 최대 투자금 (원)
MIN_ORDER_AMOUNT = 5_000            # 최소 주문 금액 (업비트 최소금액 5,000원 + 버퍼)
TRADING_FEE_RATE = 0.0005           # 거래 수수료 (0.05% = 0.0005)

# === 거시적 분석 (Macro Analysis) - 전체 시장 추세 ===
MACRO_LOOKBACK_DAYS = 7             # 일봉 분석 기간 (일)
MACRO_MIN_CHANGE_RATE = -0.02       # 전체 하락장 판단 기준 (-2% 이하면 관망)
MACRO_BULLISH_THRESHOLD = 0.01     # 상승장 판단 기준 (+1% 이상)
MACRO_UPDATE_INTERVAL = 300         # 거시 분석 갱신 주기 (초)

# === 미시적 분석 (Micro Analysis) - 진입 신호 ===
MOMENTUM_WINDOW = 15                # 모멘텀 계산 윈도우 (캔들 개수) - 추세 신뢰도 강화
MOMENTUM_THRESHOLD = 0.006          # 진입 모멘텀 기준 (0.6% 상승률) - 가짜 신호 필터링
VOLUME_SPIKE_RATIO = 2.0            # 거래량 급등 배율 (평균 대비) - 수급 확인 강화
CONSECUTIVE_UP_CANDLES = 4          # 연속 상승 캔들 개수 - 추세 지속성 확인

# === 초봉 분석 (Second Candle Analysis) - 실시간 변화 감지 ===
SECOND_CANDLE_UNIT = 5              # 초봉 단위 (1, 3, 5, 10, 30, 60 중 선택)
SECOND_MOMENTUM_WINDOW = 12         # 초봉 모멘텀 윈도우 (개수)
SECOND_MOMENTUM_THRESHOLD = 0.001   # 초봉 모멘텀 기준 (0.1% - 더 민감)
SECOND_RAPID_RISE_THRESHOLD = 0.002 # 급등 판단 기준 (0.2%/5초)

# === 단타 전문가 기법 (Pro Scalping) 파라미터 ===
SHORT_TREND_WINDOW = 15             # 단기 추세 확인 (15분)
SHORT_MOMENTUM_THRESHOLD = 0.005    # 단기 급반등 기준 (15분 내 0.5% 이상)
VOL_INTENSITY_THRESHOLD = 2.0       # 수급 집중도 (평균 대비 2배 이상)
BREAKOUT_VELOCITY = 0.001           # 분당 가격 가속도 (0.1%/min)

# === 익절/손절 설정 ===
INITIAL_STOP_LOSS = 0.015           # 초기 손절선 (1.5%) - 변동성 고려 완화
TRAILING_STOP_ACTIVATION = 0.01     # 트레일링 스탑 활성화 기준 (+1.0% 수익 시)
TRAILING_STOP_DISTANCE = 0.008      # 트레일링 스탑 거리 (0.8% - 고점 대비)
TAKE_PROFIT_TARGET = 0.01           # 목표 수익률 (1% - 트레일링으로 더 추적)
MAX_HOLDING_TIME = 10800             # 최대 보유 시간 (초, 3시간)

# === 리스크 관리 ===
MAX_TRADES_PER_HOUR = 10            # 시간당 최대 거래 횟수
COOL_DOWN_AFTER_LOSS = 180          # 손절 후 대기 시간 (초)
MIN_PRICE_STABILITY = 0.005         # 최소 가격 안정성 (급등락 필터)

# === 시스템 설정 ===
# MARKET 상수는 초기값 또는 백업용으로 사용. 실제로는 동적으로 로드됨.
MARKET = ["KRW-BTC"]  
MARKET_UPDATE_INTERVAL = 600        # 마켓 목록 갱신 주기 (10분)
TOP_MARKET_COUNT = 30               # 거래대금 상위 N개 선정
CANDLE_UNIT = 1                     # 분봉 단위 (1분)
LOG_LEVEL = logging.INFO            # 로그 레벨
DRY_RUN = True                      # 테스트 모드 (True: 실제 거래 X)
USE_SECOND_CANDLES = True           # 초봉 사용 여부
BALANCE_REPORT_INTERVAL = 30       # 잔고 리포트 주기 (초, 5분)

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
    """거래 상태 관리"""
    
    def __init__(self, market: str = "Unknown"):
        self.market = market
        self.position = None              # 현재 포지션 정보
        self.entry_price = 0.0            # 진입 가격
        self.entry_time = None            # 진입 시간
        self.highest_price = 0.0          # 보유 중 최고가
        self.stop_loss_price = 0.0        # 손절가
        self.take_profit_price = 0.0      # 익절가
        self.trailing_active = False      # 트레일링 스탑 활성화 여부
        
        # 거래 기록
        self.trades_today = []            # 오늘 거래 기록
        self.last_trade_time = None       # 마지막 거래 시간
        self.last_loss_time = None        # 마지막 손절 시간
        
        # 수익 추적
        self.total_profit = 0.0           # 총 수익
        self.total_trades = 0             # 총 거래 횟수
        self.winning_trades = 0           # 수익 거래
        self.losing_trades = 0            # 손실 거래
        
    def has_position(self) -> bool:
        """포지션 보유 여부"""
        return self.position is not None
    
    def can_trade(self) -> bool:
        """거래 가능 여부 확인"""
        now = datetime.now()
        
        # 시간당 거래 횟수 제한
        hour_ago = now - timedelta(hours=1)
        recent_trades = [t for t in self.trades_today 
                        if t['time'] > hour_ago]
        if len(recent_trades) >= MAX_TRADES_PER_HOUR:
            return False
            
        # 손절 후 쿨다운
        if self.last_loss_time:
            cooldown_end = self.last_loss_time + timedelta(seconds=COOL_DOWN_AFTER_LOSS)
            if now < cooldown_end:
                return False
                
        return True
    
    def record_trade(self, trade_type: str, amount: float, 
                    price: float, profit: float = 0.0):
        """거래 기록"""
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
            else:
                self.losing_trades += 1
            self.total_profit += profit
            
        if trade_type == 'stop_loss':
            self.last_loss_time = trade['time']
            self.losing_trades += 1
            self.total_profit += profit


class MarketAnalyzer:
    """시장 분석기"""
    
    def __init__(self, api: UpbitAPI, market: str):
        self.api = api
        self.market = market
        self.macro_trend = None           # 거시 추세 (bullish/bearish/neutral)
        self.macro_score = 0.0            # 거시 점수
        self.last_macro_update = None     # 마지막 거시 분석 시간
        
        # 캔들 데이터 캐시
        self.minute_candles = deque(maxlen=200)
        self.second_candles = deque(maxlen=120)  # 초봉 캐시 (최근 2분)
        self.volume_history = deque(maxlen=100)
        self.volume_history = deque(maxlen=100)
        self.second_volume_history = deque(maxlen=60)
        
        # 호가 데이터 (매수/매도 잔량 합계)
        self.orderbook = {
            'total_ask_size': 0.0,
            'total_bid_size': 0.0,
            'units': []
        }
        
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
        """분봉 데이터 업데이트"""
        for candle in reversed(candles):  # 시간순 정렬
            self.minute_candles.append(candle)
            self.volume_history.append(candle['candle_acc_trade_volume'])
            
    def update_second_candles(self, candles: List[Dict]):
        """초봉 데이터 업데이트"""
        for candle in reversed(candles):  # 시간순 정렬
            self.second_candles.append(candle)
            self.second_volume_history.append(candle['candle_acc_trade_volume'])
            
    def update_candle_from_ws(self, data: Dict, type_key: str):
        """WebSocket 캔들 데이터 업데이트"""
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
        
        if type_key == 'candle.1m':
            # 마지막 캔들이 업데이트 된 것이면 교체, 새로운 분이면 추가
            if self.minute_candles and self.minute_candles[-1]['candle_date_time_kst'] == candle['candle_date_time_kst']:
                self.minute_candles[-1] = candle
                # Volume history도 업데이트 필요
                if self.volume_history:
                    self.volume_history[-1] = candle['candle_acc_trade_volume']
            else:
                self.minute_candles.append(candle)
                self.volume_history.append(candle['candle_acc_trade_volume'])
                
        elif type_key == 'candle.1s':
            # 초봉 캐시 업데이트
             if self.second_candles and self.second_candles[-1]['candle_date_time_kst'] == candle['candle_date_time_kst']:
                self.second_candles[-1] = candle
                if self.second_volume_history:
                    self.second_volume_history[-1] = candle['candle_acc_trade_volume']
             else:
                self.second_candles.append(candle)
                self.second_volume_history.append(candle['candle_acc_trade_volume'])
    
    
    def update_orderbook_from_ws(self, data: Dict):
        """호가 데이터 업데이트"""
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
        """분봉 + 초봉 결합 모멘텀 감지"""
        minute_result = self.detect_momentum(current_price)
        
        # 초봉 사용 안함이면 분봉만 반환
        if not USE_SECOND_CANDLES or len(self.second_candles) < SECOND_MOMENTUM_WINDOW:
            return minute_result
        
        second_result = self.detect_second_momentum(current_price)
        
        # 분봉 기본조건 + 초봉 확인으로 정밀도 향상
        # 케이스 1: 분봉 신호 O + 초봉 확인 = 강력한 신호
        # 케이스 2: 분봉 신호 X + 초봉 급등 = 빠른 진입 기회
        
        combined_signal = False
        combined_strength = 0
        reasons = []
        
        if minute_result['signal'] and second_result['signal']:
            # 둘 다 신호: 매우 강력
            combined_signal = True
            combined_strength = min(100, minute_result['strength'] * 0.6 + second_result['strength'] * 0.4)
            reasons.append(minute_result['reason'])
            reasons.append(second_result['reason'])
            
        elif second_result['rapid_rise']:
            # 초봉 급등만 감지: 빠른 진입 (분봉 조건 완화)
            if minute_result['price_change'] > MOMENTUM_THRESHOLD * 0.5:  # 분봉 조건 50%만 충족해도 OK
                combined_signal = True
                combined_strength = second_result['strength']
                reasons.append(f"⚡빠른진입: {second_result['reason']}")
                
        elif minute_result['signal']:
            # 분봉 신호만: 일반 진입
            combined_signal = True
            combined_strength = minute_result['strength'] * 0.8
            reasons.append(minute_result['reason'])
        
        return {
            'signal': combined_signal,
            'strength': combined_strength,
            'minute_signal': minute_result['signal'],
            'second_signal': second_result['signal'],
            'rapid_rise': second_result.get('rapid_rise', False),
            'reason': ' | '.join(reasons) if reasons else '조건 미충족'
        }


class MomentumTrader:
    """모멘텀 트레이딩 봇"""
    
    def __init__(self):
        self.access_key = ACCESS_KEY
        self.secret_key = SECRET_KEY
        self.api = UpbitAPI(ACCESS_KEY, SECRET_KEY)
        
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

    async def _update_top_markets(self):
        """거래대금 상위 종목으로 마켓 리스트 갱신"""
        try:
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
                        candles = self.api.get_candles_minutes(market, CANDLE_UNIT, 200)
                        self.analyzers[market].update_candles(candles)
                        
                        if USE_SECOND_CANDLES:
                            sec_candles = self.api.get_candles_seconds(market, SECOND_MOMENTUM_WINDOW * 2)
                            self.analyzers[market].update_second_candles(sec_candles)
                            
                        self.last_price_updates[market] = None
                        
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
        logger.info("🚀 모멘텀 트레이딩 봇 시작")
        
        # 1. 마켓 리스트 구성 (가장 먼저 실행)
        await self._update_top_markets()
        
        if not self.markets:
             logger.error("거래 가능한 마켓이 없습니다. 종료합니다.")
             return

        logger.info(f"   타겟 마켓: {len(self.markets)}개 종목 (Top {TOP_MARKET_COUNT} + 보유)")
        logger.info(f"   최대 투자금: {MAX_INVESTMENT:,}원")
        logger.info(f"   테스트 모드: {'ON' if DRY_RUN else 'OFF'}")
        logger.info("=" * 60)
        
        # 2. 초기 잔고 확인
        self._check_balance()
        
        # 3. 기 보유 종목에 대한 상태 동기화
        self._sync_state_with_balance()
        
        self.running = True
        
        try:
            await asyncio.gather(
                self._public_ws_monitor(),
                self._private_ws_monitor(),
                self._trading_loop(),
                self._macro_update_loop(),
                self._balance_report_loop(),
                self._market_update_loop()
            )
        except KeyboardInterrupt:
            logger.info("사용자에 의해 중단됨")
        except Exception as e:
            logger.error(f"봇 오류: {e}")
        finally:
            self.running = False
            self._print_summary()

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
    
    def _check_balance(self):
        """잔고 확인 (WebSocket 데이터 기반)"""
        try:
            # KRW 잔고 표시
            if 'KRW' in self.assets:
                krw = self.assets['KRW']
                balance = krw['balance']
                locked = krw['locked']
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
            
        except Exception as e:
            logger.error(f"잔고 확인 실패: {e}")
    
    async def _public_ws_monitor(self):
        """WebSocket (Public) - 실시간 시세, 호가, 체결, 캔들"""
        while self.running:
            try:
                async with websockets.connect(WS_PUBLIC_URL) as ws:
                    # 구독 요청
                    # 모든 마켓 구독
                    codes = self.markets
                    subscribe = [
                        {"ticket": f"momentum-pub-{uuid.uuid4()}"},
                        {"type": "ticker", "codes": codes},
                        {"type": "trade", "codes": codes},
                        {"type": "orderbook", "codes": codes}, # 호가 구독
                        {"type": "candle.1m", "codes": codes},
                        {"format": "DEFAULT"}
                    ]
                    
                    if USE_SECOND_CANDLES:
                         subscribe.insert(4, {"type": "candle.1s", "codes": codes}) 
                    
                    await ws.send(json.dumps(subscribe))
                    logger.info(f"📡 Public WebSocket 연결됨 ({len(codes)}개 마켓)")
                    
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
                            
                            type_val = data.get('type') or data.get('ty')
                            if not type_val: # type 없는 경우
                                 continue
                            
                            code = data.get('cd') # 마켓 코드 (KRW-BTC 등)
                            if not code:
                                code = data.get('code')
                            
                            if code in self.markets:
                                if type_val == 'ticker':
                                    self.current_prices[code] = data.get('trade_price') or data.get('tp')
                                    self.last_price_updates[code] = datetime.now()
                                    
                                elif type_val == 'trade':
                                    self.current_prices[code] = data.get('trade_price', self.current_prices.get(code, 0))
                                    self.last_price_updates[code] = datetime.now()
                                
                                elif type_val == 'candle.1m' or type_val == 'candle.1s':
                                    self.analyzers[code].update_candle_from_ws(data, type_val)
                                    
                                elif type_val == 'orderbook':
                                    self.analyzers[code].update_orderbook_from_ws(data)
                                
                        except asyncio.TimeoutError:
                            await ws.send("PING")
                            last_ping = time.time()
                            
            except websockets.exceptions.ConnectionClosed:
                logger.warning("Public WebSocket 연결 끊김, 재연결 시도...")
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
                    
                # 30초마다 분석 상태 로그
                now = time.time()
                if now - last_status_log >= 30:
                    last_status_log = now
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
                        
                        logger.info(f"[{market}] 📊 {price:,.0f}원 | "
                                  f"분봉:{min_change:+.2f}% 초봉:{sec_change:+.3f}% | "
                                  f"거래량:{vol_ratio:.1f}배 | 강도:{min_result['strength']:.0f}")
                
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
        """진입 기회 탐색 (분봉 + 초봉 결합 분석)"""
        state = self.states[market]
        if not state.can_trade():
            return
            
        analyzer = self.analyzers[market]
        current_price = self.current_prices[market]
        
        try:
            # REST API 호출 제거하고 캐시된 캔들 사용 (WebSocket으로 업데이트됨)
            # 캔들 데이터 부족하면 대기
            if len(analyzer.minute_candles) < MOMENTUM_WINDOW:
                logger.debug(f"[{market}] 캔들 데이터 수집 중... ({len(analyzer.minute_candles)}/{MOMENTUM_WINDOW})")
                return

            if USE_SECOND_CANDLES and len(analyzer.second_candles) < SECOND_MOMENTUM_WINDOW:
                 return
            
            # 결합 모멘텀 감지 (분봉 + 초봉)
            momentum = analyzer.detect_combined_momentum(current_price)
            
            if momentum['signal']:
                rapid_indicator = "🚀" if momentum.get('rapid_rise') else "🎯"
                logger.info(f"[{Color.BOLD}{market}{Color.RESET}] {rapid_indicator} 진입 신호 감지! | {momentum['reason']} | "
                          f"강도: {Color.MAGENTA}{momentum['strength']:.1f}{Color.RESET}")
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
                state.stop_loss_price = state.entry_price * (1 - INITIAL_STOP_LOSS)
                state.take_profit_price = state.entry_price * (1 + TAKE_PROFIT_TARGET)
                state.trailing_active = False
                
                state.record_trade('buy', invest_amount, state.entry_price)
                
                logger.info(f"[{Color.BOLD}{market}{Color.RESET}] ✅ 매수 체결 | 가격: {Color.YELLOW}{state.entry_price:,.0f}원{Color.RESET} | "
                          f"손절가: {Color.RED}{state.stop_loss_price:,.0f}원{Color.RESET} | "
                          f"익절가: {Color.GREEN}{state.take_profit_price:,.0f}원{Color.RESET}")
                
        except Exception as e:
            logger.error(f"[{market}] 매수 실행 오류: {e}")
    
    async def _manage_position(self, market: str):
        """포지션 관리 (익절/손절 판단)"""
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
                logger.info(f"[{Color.BOLD}{market}{Color.RESET}] 📊 트레일링 스탑 활성화 | 수익률: {Color.GREEN}{profit_rate*100:.2f}%{Color.RESET}")
            
            # 트레일링 스탑 가격 업데이트
            if state.trailing_active:
                new_stop = current * (1 - TRAILING_STOP_DISTANCE)
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
                # 손절선을 매입가로 올림 (손실 없이 청산 보장)
                state.stop_loss_price = entry
                logger.info(f"[{market}] 🎯 목표 수익률 {TAKE_PROFIT_TARGET*100:.1f}% 도달! "
                          f"트레일링 활성화 (손절가→매입가: {entry:,.0f}원)")
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
                logger.info(f"[{Color.BOLD}{market}{Color.RESET}] 📈 보유 중 | 현재가: {Color.YELLOW}{current:,.0f}원{Color.RESET} | "
                          f"수익률: {pnl_color}{pnl:+.2f}%{Color.RESET} | "
                          f"손절가: {Color.RED}{state.stop_loss_price:,.0f}원{Color.RESET}")
    
    
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
            volume = state.position.get('volume', 0)
            current_price = self.current_prices[market]
            
            if DRY_RUN:
                executed_price = current_price
                logger.info(f"[{market}] 💵 [테스트] 시장가 매도 | 사유: {reason} | "
                          f"가격: {executed_price:,.0f}원")
            else:
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
            
            # 포지션 정리
            state.position = None
            state.trailing_active = False
            
            emoji = "🎉" if profit >= 0 else "📉"
            pnl_color = Color.GREEN if profit >= 0 else Color.RED
            logger.info(f"[{Color.BOLD}{market}{Color.RESET}] {emoji} 매도 완료 | 사유: {reason} | "
                       f"수익: {pnl_color}{profit:+,.0f}원 ({profit_rate:+.2f}%){Color.RESET} | "
                       f"매도가: {Color.YELLOW}{executed_price:,.0f}원{Color.RESET}")
            
        except Exception as e:
            logger.error(f"[{market}] 매도 실행 오류: {e}")
    
    def _print_summary(self):
        """거래 요약 출력 (전체)"""
        total_trades = 0
        winning_trades = 0
        losing_trades = 0
        total_profit = 0.0
        
        logger.info("=" * 60)
        logger.info("📊 전체 거래 요약")
        logger.info("=" * 60)
        
        for market in self.markets:
            state = self.states[market]
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
