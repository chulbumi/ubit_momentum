import hashlib
import jwt
import requests
import time
import uuid
from typing import Optional, Dict, List
from urllib.parse import urlencode, unquote

from config import REST_BASE_URL, logger

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
        """종료된 주문 조회 (최근 체결 내역 확인용)"""
        params = {
            'market': market,
            'order_by': 'desc',
            'limit': limit
        }
        
        # states 파라미터 처리
        if states:
            params['states[]'] = states
        else:
             params['states[]'] = ['done', 'cancel']

        if start_time:
            params['start_time'] = start_time
        if end_time:
            params['end_time'] = end_time
            
        return self._request('GET', '/orders/closed', params=params)

    def get_candles_minutes_extended(self, market: str, unit: int, total_count: int = 600) -> List[Dict]:
        """다중 페이지 분봉 조회"""
        all_candles = []
        remaining = total_count
        to_time = None
        
        while remaining > 0:
            fetch_count = min(remaining, 200)
            
            try:
                candles = self.get_candles_minutes(market, unit, fetch_count, to_time)
                if not candles:
                    break
                    
                all_candles.extend(candles)
                remaining -= len(candles)
                
                if candles:
                    oldest_candle = candles[-1]
                    to_time = oldest_candle.get('candle_date_time_utc') or oldest_candle.get('candle_date_time_kst')
                
                time.sleep(0.15)
                
            except Exception as e:
                logger.warning(f"[{market}] 캔들 확장 로드 실패 (unit={unit}, 현재 {len(all_candles)}개): {e}")
                break
        
        all_candles.reverse()
        return all_candles
    
    def buy_market_order(self, market: str, price: float) -> Dict:
        """시장가 매수 (price는 주문 금액 KRW)"""
        return self.place_order(market, 'bid', 'price', price=str(price), identifier=str(uuid.uuid4()))

    def sell_market_order(self, market: str, volume: float) -> Dict:
         """시장가 매도 (volume은 주문 수량)"""
         return self.place_order(market, 'ask', 'market', volume=str(volume), identifier=str(uuid.uuid4()))
