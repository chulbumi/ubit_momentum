import os
import logging
import statistics
from datetime import datetime
from collections import deque
from typing import Dict, List, Optional

from config import *
from api import UpbitAPI

class MarketAnalyzer:
    """시장 분석기 - 전문가 관점의 종합 분석"""
    
    def __init__(self, api: UpbitAPI, market: str):
        self.api = api
        self.market = market
        self.macro_trend = None           # 거시 추세 (bullish/bearish/neutral)
        self.macro_score = 0.0            # 거시 점수
        self.last_macro_update = None     # 마지막 거시 분석 시간
        
        # 캔들 데이터 캐시 (다양한 시간대 - v3.2 확장)
        self.minute_candles = deque(maxlen=200)       # 1분봉 (3시간 20분)
        self.minute5_candles = deque(maxlen=600)      # 5분봉 (50시간 = 약 2일)
        self.minute15_candles = deque(maxlen=400)     # 15분봉 (100시간 = 약 4일)
        self.second_candles = deque(maxlen=120)       # 초봉 캐시 (최근 2분)
        self.volume_history = deque(maxlen=200)
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
        
    def load_candles_from_disk(self, unit: int) -> List[Dict]:
        """디스크에서 캔들 데이터 로드 (CSV)"""
        try:
            filename = f"{DATA_DIR}/{self.market}_{unit}m.csv"
            if not os.path.exists(filename):
                return []
            
            import pandas as pd
            df = pd.read_csv(filename)
            # DataFrame을 dict 리스트로 변환
            candles = df.to_dict('records')
            return candles
        except Exception as e:
            logger.error(f"[{self.market}] CSV 로드 실패({unit}m): {e}")
            return []

    def save_candles_to_disk(self, unit: int, candles: deque):
        """디스크에 캔들 데이터 저장 (CSV)"""
        try:
            if not os.path.exists(DATA_DIR):
                os.makedirs(DATA_DIR, exist_ok=True)
                
            filename = f"{DATA_DIR}/{self.market}_{unit}m.csv"
            # deque -> list -> DataFrame 변환
            data_to_save = list(candles)
            
            import pandas as pd
            df = pd.DataFrame(data_to_save)
            df.to_csv(filename, index=False, encoding='utf-8')
        except Exception as e:
            logger.error(f"[{self.market}] 캔들 저장 실패({unit}m): {e}")

    def append_candle_to_disk(self, unit: int, candle: Dict):
        """단일 캔들을 디스크에 추가 (실시간 기록용)"""
        try:
            if not os.path.exists(DATA_DIR):
                os.makedirs(DATA_DIR, exist_ok=True)
                
            filename = f"{DATA_DIR}/{self.market}_{unit}m.csv"
            
            import pandas as pd
            # 단일 캔들을 DataFrame으로 변환
            df = pd.DataFrame([candle])
            
            # 파일이 존재하면 append, 없으면 새로 생성
            if os.path.exists(filename):
                df.to_csv(filename, mode='a', header=False, index=False, encoding='utf-8')
            else:
                df.to_csv(filename, mode='w', header=True, index=False, encoding='utf-8')
        except Exception as e:
            # 실시간 기록 실패는 치명적이지 않으므로 warning 레벨
            logger.warning(f"[{self.market}] 실시간 캔들 기록 실패({unit}m): {e}")

    def initialize_candles_smart(self, unit: int, max_count: int, deque_obj: deque):
        """로컬 데이터 로드 + API 부족분 요청 (스마트 초기화)"""
        try:
            # 1. 로컬 로드
            local_candles = self.load_candles_from_disk(unit)
            
            if not local_candles:
                candles = self.api.get_candles_minutes_extended(self.market, unit, max_count)
                deque_obj.extend(candles)
                self.save_candles_to_disk(unit, deque_obj)
                # logger.info(f"[{self.market}] {unit}분봉: 전체 API 로드 ({len(candles)}개)")
                return

            # 2. 갭 계산 (API 최신 캔들 기준)
            last_local_candle = local_candles[-1]
            last_local_ts_str = last_local_candle.get('candle_date_time_utc', '')
            
            # API로 최신 캔들 1개를 가져와서 현재 시점을 파악 (시스템 시간 의존 제거)
            latest_api_candles = self.api.get_candles_minutes(self.market, unit, 1)
            if not latest_api_candles:
                deque_obj.extend(local_candles)
                return
                
            latest_api_ts_str = latest_api_candles[0].get('candle_date_time_utc', '')
            
            gap_count = max_count
            
            try:
                last_local_time = datetime.strptime(last_local_ts_str, "%Y-%m-%dT%H:%M:%S")
                latest_api_time = datetime.strptime(latest_api_ts_str, "%Y-%m-%dT%H:%M:%S")
                
                diff_minutes = (latest_api_time - last_local_time).total_seconds() / 60.0
                gap_count = int(diff_minutes / unit) + 2 # 여유분
                if gap_count < 0: gap_count = 0
            except Exception as e:
                # logger.warning(f"[{self.market}] 시간 파싱 오류: {e}")
                gap_count = max_count

            # 3. 갭 메우기
            if gap_count >= max_count:
                # 갭이 너무 크면 전체 다시 로드
                candles = self.api.get_candles_minutes_extended(self.market, unit, max_count)
                deque_obj.extend(candles)
                # logger.info(f"[{self.market}] {unit}분봉: 재로드 (갭 큼: {gap_count}개)")
            elif gap_count > 0:
                # 갭만큼 요청
                fetch_count = min(gap_count, 200) 
                new_candles = self.api.get_candles_minutes(self.market, unit, fetch_count)
                new_candles.reverse() 
                
                last_local_ts = local_candles[-1]['candle_date_time_utc']
                to_append = [c for c in new_candles if c['candle_date_time_utc'] > last_local_ts]
                
                combined = local_candles + to_append
                if len(combined) > max_count:
                    combined = combined[-max_count:]
                
                deque_obj.extend(combined)
                # logger.info(f"[{self.market}] {unit}분봉: 스마트 로드 (+{len(to_append)}개)")
            else:
                deque_obj.extend(local_candles)
                # logger.info(f"[{self.market}] {unit}분봉: 최신 상태")
                
            # 저장 업데이트
            self.save_candles_to_disk(unit, deque_obj)
            
        except Exception as e:
            logger.error(f"[{self.market}] 스마트 초기화 실패({unit}m): {e}")
            candles = self.api.get_candles_minutes_extended(self.market, unit, max_count)
            deque_obj.extend(candles)

    def analyze_macro(self) -> Dict:
        """시장 추세 분석"""
        try:
            # 1. 15분봉 변화율
            m15_change = 0
            if len(self.minute15_candles) >= 2:
                m15_start = self.minute15_candles[-2]['trade_price']
                m15_current = self.minute15_candles[-1]['trade_price']
                m15_change = (m15_current - m15_start) / m15_start if m15_start > 0 else 0
            
            # 2. 30분봉 변화율
            m30_change = 0
            if len(self.minute5_candles) >= 7:
                m30_start = self.minute5_candles[-7]['trade_price']
                m30_current = self.minute5_candles[-1]['trade_price']
                m30_change = (m30_current - m30_start) / m30_start if m30_start > 0 else 0

            # 3. 1시간봉 변화율
            h1_change = 0
            if len(self.minute5_candles) >= 13:
                h1_start = self.minute5_candles[-13]['trade_price']
                h1_current = self.minute5_candles[-1]['trade_price']
                h1_change = (h1_current - h1_start) / h1_start if h1_start > 0 else 0

            # 4. 4시간 추세
            h4_change = 0
            if len(self.minute5_candles) >= 48:
                h4_start = self.minute5_candles[-48]['trade_price']
                h4_current = self.minute5_candles[-1]['trade_price']
                h4_change = (h4_current - h4_start) / h4_start if h4_start > 0 else 0
            
            # 5. 5분봉 변화율
            m5_change = 0
            if len(self.minute5_candles) >= 2:
                m5_start = self.minute5_candles[-2]['trade_price']
                m5_current = self.minute5_candles[-1]['trade_price']
                m5_change = (m5_current - m5_start) / m5_start if m5_start > 0 else 0
            
            # 1분봉 일관성 체크
            m1_consistency_count = 0
            m1_changes = []
            if len(self.minute_candles) >= 5:
                for i in range(-5, 0):
                    if i == -5: continue
                    prev_price = self.minute_candles[i-1]['trade_price']
                    curr_price = self.minute_candles[i]['trade_price']
                    change = (curr_price - prev_price) / prev_price if prev_price > 0 else 0
                    m1_changes.append(change)
                    if change > 0:
                        m1_consistency_count += 1
            
            # V자 반등 패턴 감지
            long_downtrend = False
            if len(self.minute15_candles) >= 12:
                last_12_candles = list(self.minute15_candles)[-12:]
                max_price_3h = max(candle['high_price'] for candle in last_12_candles)
                current_price = self.minute15_candles[-1]['trade_price']
                drop_from_high = (current_price - max_price_3h) / max_price_3h if max_price_3h > 0 else 0
                
                if drop_from_high <= -0.015:
                    max_rise_in_3h = 0
                    for i in range(1, len(last_12_candles)):
                        prev = last_12_candles[i-1]['trade_price']
                        curr = last_12_candles[i]['trade_price']
                        rise = (curr - prev) / prev if prev > 0 else 0
                        max_rise_in_3h = max(max_rise_in_3h, rise)
                    if max_rise_in_3h < 0.01:
                        long_downtrend = True
            
            v_reversal_detected = False
            if V_REVERSAL_ENABLED and len(m1_changes) >= 4 and long_downtrend:
                first_half = m1_changes[:2]
                second_half = m1_changes[2:]
                first_half_drop = sum(first_half)
                second_half_rise = sum(second_half)
                
                if (first_half_drop <= V_REVERSAL_MIN_DROP and 
                    second_half_rise >= V_REVERSAL_MIN_RISE):
                    v_reversal_detected = True
            
            # 변동성 체크
            m1_volatility = 0
            volatility_ok = True
            if len(m1_changes) >= 3:
                m1_volatility = statistics.stdev(m1_changes) if len(m1_changes) > 1 else 0
                volatility_ok = (m1_volatility <= VOLATILITY_MAX_STDDEV)
            
            # 6. 일봉 변화율
            daily_change = 0
            if len(self.minute5_candles) >= 288:
                daily_start = self.minute5_candles[-288]['trade_price']
                daily_current = self.minute5_candles[-1]['trade_price']
                daily_change = (daily_current - daily_start) / daily_start if daily_start > 0 else 0
            
            # 7. 3일 추세
            daily_3d_change = 0
            if len(self.minute5_candles) >= 576:
                d3_start = self.minute5_candles[-576]['trade_price']
                d3_current = self.minute5_candles[-1]['trade_price']
                daily_3d_change = (d3_current - d3_start) / d3_start if d3_start > 0 else 0

            long_term_bearish = False
            block_reason = None
            
            buy_pressure = 0.5
            if self.bid_volume_1m + self.ask_volume_1m > 0:
                buy_pressure = self.bid_volume_1m / (self.bid_volume_1m + self.ask_volume_1m)
            
            m1_consistent = (m1_consistency_count >= STRONG_MOMENTUM_1M_CONSISTENCY_MIN)
            v_reversal_ok = (not V_REVERSAL_ENABLED or v_reversal_detected)
            
            strong_short_momentum = (
                m5_change >= STRONG_SHORT_MOMENTUM_5M_THRESHOLD and 
                m1_consistent and
                volatility_ok and
                v_reversal_ok and
                h4_change > STRONG_SHORT_MOMENTUM_H4_MIN and
                buy_pressure >= STRONG_MOMENTUM_BUY_PRESSURE_MIN and
                self.fatigue_score <= STRONG_MOMENTUM_FATIGUE_MAX
            )
            
            if LONG_TERM_FILTER_ENABLED and not strong_short_momentum:
                if daily_3d_change <= DAILY_BEARISH_THRESHOLD and DAILY_BEARISH_BLOCK:
                    long_term_bearish = True
                    block_reason = f"일봉 하락추세 ({daily_3d_change*100:.2f}% / 3일)"
                
                if h4_change <= H4_BEARISH_THRESHOLD and H4_BEARISH_BLOCK:
                    long_term_bearish = True
                    block_reason = block_reason or f"4시간봉 하락 ({h4_change*100:.2f}%)"
            elif strong_short_momentum and LONG_TERM_FILTER_ENABLED:
                logger.info(f"[{self.market}] 🚀 단기 급등 감지 (5m:{m5_change*100:+.2f}% 1m일관:{m1_consistency_count}/3 4h:{h4_change*100:+.2f}% 매수:{buy_pressure*100:.1f}% 피로:{self.fatigue_score:.1f}) - 장기하락 차단 예외 적용")
            
            score = m15_change * 0.20 + m30_change * 0.15 + h1_change * 0.20 + h4_change * 0.25 + daily_change * 0.20
            
            short_squeeze = m15_change >= 0.015 # Config missing SHORT_MOMENTUM_THRESHOLD used elsewhere?
            
            if long_term_bearish:
                trend = 'bearish'
                can_trade = False
                if short_squeeze and IGNORE_SHORT_SQUEEZE_IN_DOWNTREND:
                    logger.warning(f"[{self.market}] 🚫 하락장 반등 무시 | {block_reason} | Short Squeeze 신호 차단")
            elif score < MACRO_MIN_CHANGE_RATE and not short_squeeze:
                trend = 'bearish'
                can_trade = False
            elif score > MACRO_BULLISH_THRESHOLD or (short_squeeze and not long_term_bearish):
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
                'm5_change': m5_change,
                'm1_consistency': m1_consistency_count,
                'm15_change': m15_change,
                'h4_change': h4_change,
                'daily_change': daily_change,
                'daily_3d_change': daily_3d_change,
                'short_squeeze': short_squeeze,
                'long_term_bearish': long_term_bearish,
                'block_reason': block_reason,
                'strong_short_momentum': strong_short_momentum,
                'buy_pressure': buy_pressure,
                'fatigue_score': self.fatigue_score
            }
            self.macro_result = result
            
            m5_color = Color.RED if m5_change >= 0 else Color.BLUE
            m15_color = Color.RED if m15_change >= 0 else Color.BLUE
            h4_color = Color.RED if h4_change >= 0 else Color.BLUE
            d_color = Color.RED if daily_change >= 0 else Color.BLUE
            d3_color = Color.RED if daily_3d_change >= 0 else Color.BLUE

            log_msg = (f"[{self.market:<11}] 📊 추세 분석 | {trend:<7} | "
                      f"5m:{m5_color}{m5_change*100:>+6.2f}%{Color.RESET} "
                      f"15m:{m15_color}{m15_change*100:>+6.2f}%{Color.RESET} "
                      f"4h:{h4_color}{h4_change*100:>+6.2f}%{Color.RESET} "
                      f"일:{d_color}{daily_change*100:>+6.2f}%{Color.RESET} "
                      f"3일:{d3_color}{daily_3d_change*100:>+6.2f}%{Color.RESET}")

            if long_term_bearish:
                log_msg += f" | 🚫 장기하락 차단"
            elif strong_short_momentum:
                log_msg += f" | 🚀 단기 급등 (예외 허용, 1m일관:{m1_consistency_count}/3, 매수:{buy_pressure*100:.0f}%)"
            elif short_squeeze:
                log_msg += " | 🔥 Short Squeeze"
            logger.info(log_msg)
            
            return result
            
        except Exception as e:
            logger.error(f"거시 분석 오류: {e}")
            return {'trend': 'neutral', 'score': 0, 'can_trade': True, 'long_term_bearish': False}
    
    def update_candles(self, candles: List[Dict]):
        """1분봉 데이터 업데이트 (실시간 디스크 기록 포함)"""
        for candle in reversed(candles):  # 시간순 정렬
            self.minute_candles.append(candle)
            self.volume_history.append(candle['candle_acc_trade_volume'])
            # 실시간으로 디스크에 기록
            self.append_candle_to_disk(1, candle)
    
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
            if self.minute_candles and self.minute_candles[-1]['candle_date_time_kst'] == candle['candle_date_time_kst']:
                self.minute_candles[-1] = candle
                if self.volume_history:
                    self.volume_history[-1] = candle['candle_acc_trade_volume']
            else:
                self.minute_candles.append(candle)
                self.volume_history.append(candle['candle_acc_trade_volume'])
        
        elif type_key == 'candle.5m':
            if self.minute5_candles and self.minute5_candles[-1]['candle_date_time_kst'] == candle['candle_date_time_kst']:
                self.minute5_candles[-1] = candle
            else:
                self.minute5_candles.append(candle)
        
        elif type_key == 'candle.15m':
            if self.minute15_candles and self.minute15_candles[-1]['candle_date_time_kst'] == candle['candle_date_time_kst']:
                self.minute15_candles[-1] = candle
            else:
                self.minute15_candles.append(candle)
                
        elif type_key == 'candle.1s':
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
            
            if unit_list:
                best_ask = unit_list[0]['ask_price']
                best_bid = unit_list[0]['bid_price']
                if best_ask and best_bid:
                    self.orderbook['spread'] = best_ask - best_bid
                    self.orderbook['spread_rate'] = (best_ask - best_bid) / best_bid if best_bid > 0 else 0
            
            total_ask = self.orderbook['total_ask_size']
            total_bid = self.orderbook['total_bid_size']
            if total_ask + total_bid > 0:
                self.orderbook['imbalance'] = (total_bid - total_ask) / (total_bid + total_ask)
            
            if len(unit_list) >= 5:
                top5_bid = sum(u['bid_size'] for u in unit_list[:5] if u['bid_size'])
                top5_ask = sum(u['ask_size'] for u in unit_list[:5] if u['ask_size'])
                if top5_ask > 0:
                    self.orderbook['bid_depth_ratio'] = top5_bid / top5_ask
    
    def update_trade_from_ws(self, data: Dict):
        """체결 데이터 업데이트"""
        trade = {
            'timestamp': data.get('trade_timestamp') or data.get('ttms', 0),
            'price': data.get('trade_price') or data.get('tp', 0),
            'volume': data.get('trade_volume') or data.get('tv', 0),
            'ask_bid': data.get('ask_bid') or data.get('ab', 'BID'),
            'sequential_id': data.get('sequential_id') or data.get('sid', 0),
        }
        
        self.recent_trades.append(trade)
        self.last_trade_update = datetime.now()
        
        self.price_history.append({
            'price': trade['price'],
            'timestamp': trade['timestamp']
        })
        
        self._update_volume_aggregates()
        self._update_technical_indicators()
    
    def _update_volume_aggregates(self):
        """체결량 집계 업데이트"""
        now_ts = datetime.now().timestamp() * 1000
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
        """기술 지표 업데이트"""
        if len(self.price_history) < 14:
            return
        
        prices = [p['price'] for p in list(self.price_history)[-60:]]
        
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
        
        if len(prices) >= 20:
            import statistics
            self.volatility = statistics.stdev(prices[-20:]) / statistics.mean(prices[-20:])
        
        self._update_fatigue_score(prices)
    
    def _update_fatigue_score(self, prices: List[float]):
        """급등 피로도 계산"""
        if len(prices) < 30:
            self.fatigue_score = 0
            return
        
        current = prices[-1]
        
        price_5m_ago = prices[-min(30, len(prices))]
        change_5m = (current - price_5m_ago) / price_5m_ago if price_5m_ago > 0 else 0
        
        rate_fatigue = min(100, abs(change_5m) * 1000)
        
        rsi_fatigue = 0
        if self.rsi_value >= 70:
            rsi_fatigue = (self.rsi_value - 70) * 3
        elif self.rsi_value >= 80:
            rsi_fatigue = 30 + (self.rsi_value - 80) * 5
        
        volume_fatigue = 0
        if len(self.minute_candles) >= 3:
            recent_vols = [c['candle_acc_trade_volume'] for c in list(self.minute_candles)[-3:]]
            if len(recent_vols) == 3:
                if recent_vols[1] > 0 and recent_vols[2] / recent_vols[1] < 0.5:
                    volume_fatigue = 20
                    self.momentum_exhaustion = True
                else:
                    self.momentum_exhaustion = False
        
        sell_pressure = 0
        if self.bid_volume_1m + self.ask_volume_1m > 0:
            sell_ratio = self.ask_volume_1m / (self.bid_volume_1m + self.ask_volume_1m)
            if sell_ratio > 0.6:
                sell_pressure = (sell_ratio - 0.5) * 100
        
        self.fatigue_score = min(100, rate_fatigue + rsi_fatigue + volume_fatigue + sell_pressure)
    
    def analyze_market_sentiment(self) -> Dict:
        """종합 시장 심리 분석"""
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
        
        score = 50.0
        
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
        
        imbalance = self.orderbook.get('imbalance', 0)
        if imbalance >= 0.3:
            score += 10
            analysis['reasons'].append(f"매수벽 우위 (불균형:{imbalance:.2f})")
        elif imbalance <= -0.3:
            score -= 10
            analysis['warnings'].append(f"매도벽 우위 (불균형:{imbalance:.2f})")
        
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
        
        if self.fatigue_score >= 60:
            score -= 25
            analysis['warnings'].append(f"🔥 급등 피로도 높음 ({self.fatigue_score:.1f}) - 조정 가능성")
        elif self.fatigue_score >= 40:
            score -= 12
            analysis['warnings'].append(f"급등 피로감 ({self.fatigue_score:.1f})")
        
        if self.momentum_exhaustion:
            score -= 15
            analysis['warnings'].append("📉 모멘텀 소진 - 거래량 급감")
        
        if self.volatility >= 0.02:
            score -= 5
            analysis['warnings'].append(f"높은 변동성 ({self.volatility*100:.2f}%)")
        
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
        """다중 타임프레임 분석"""
        result = {
            'valid_entry': True,
            'stage': 'unknown',
            'trend_5m': 'neutral',
            'trend_15m': 'neutral',
            'change_5m': 0.0,
            'change_15m': 0.0,
            'volume_confirmed': False,
            'reasons': [],
            'warnings': [],
        }
        
        if not MTF_ENABLED:
            result['reasons'].append("MTF 분석 비활성화")
            return result
        
        if self.macro_trend == 'bearish':
            result['valid_entry'] = False
            result['warnings'].append("🚫 거시 추세 하락 (일봉/4시간봉) - 진입 차단")
            return result
        
        # 1. 5분봉 분석
        if len(self.minute5_candles) >= MTF_5M_MIN_CANDLES:
            candles_5m = list(self.minute5_candles)
            
            ma15 = 0
            ma50 = 0
            
            if len(candles_5m) >= 15:
                ma15 = sum(c['trade_price'] for c in candles_5m[-15:]) / 15
            
            if len(candles_5m) >= 50:
                ma50 = sum(c['trade_price'] for c in candles_5m[-50:]) / 50
                
            is_downtrend = False
            if ma15 > 0 and ma50 > 0 and ma15 < ma50:
                is_downtrend = True
                
            disparity = 0
            if ma15 > 0:
                disparity = (current_price - ma15) / ma15
            
            if is_downtrend:
                last_candle = candles_5m[-1]
                is_bullish_candle = last_candle['trade_price'] > last_candle['opening_price']
                
                avg_vol = 0
                if len(candles_5m) >= 4:
                    avg_vol = sum(c['candle_acc_trade_volume'] for c in candles_5m[-4:-1]) / 3
                current_vol = last_candle['candle_acc_trade_volume']
                is_volume_spike = avg_vol > 0 and current_vol >= avg_vol * 1.5
                
                if disparity < -0.015:
                    if is_bullish_candle and is_volume_spike:
                        result['reasons'].append(f"📉 낙폭과대+거래량실린반등 (이격:{disparity*100:.1f}%)")
                    elif is_bullish_candle:
                         result['warnings'].append(f"⚠️ 거래량 부족한 반등 (이격:{disparity*100:.1f}%)")
                    else:
                         result['warnings'].append(f"⚠️ 하락가속화 (이격:{disparity*100:.1f}%)")
                else:
                    result['valid_entry'] = False
                    result['warnings'].append(f"🚫 하락추세 진행중 (이격부족:{disparity*100:.1f}%)")
            
            elif ma15 > 0 and ma50 > 0:
                if disparity < 0:
                    result['reasons'].append(f"눌림목 구간 (이격도:{disparity*100:.1f}%)")

            # 기존 분석 로직
            candles_recent = candles_5m[-MTF_5M_MIN_CANDLES:]
            start_price = candles_recent[0]['opening_price']
            change_5m = (current_price - start_price) / start_price if start_price > 0 else 0
            result['change_5m'] = change_5m
            
            recent_5m_change = (candles_recent[-1]['trade_price'] - candles_recent[-2]['trade_price']) / candles_recent[-2]['trade_price'] if len(candles_recent) >= 2 and candles_recent[-2]['trade_price'] > 0 else 0
            
            if change_5m >= MTF_5M_TREND_THRESHOLD and recent_5m_change >= 0:
                result['trend_5m'] = 'bullish'
                result['reasons'].append(f"5분봉 상승 추세 ({change_5m*100:.2f}%)")
            elif change_5m <= -MTF_5M_TREND_THRESHOLD:
                result['trend_5m'] = 'bearish'
                if is_downtrend and disparity < -0.015:
                     result['reasons'].append(f"하락 중 반등 가능성")
                else:
                     result['warnings'].append(f"5분봉 하락 추세 ({change_5m*100:.2f}%)")
            else:
                result['trend_5m'] = 'neutral'
            
            if change_5m >= MTF_5M_EARLY_STAGE_MAX:
                result['stage'] = 'late'
                result['warnings'].append(f"⚠️ 상승 후반 ({change_5m*100:.2f}%) - 고점 추격 위험")
                result['valid_entry'] = False
            elif change_5m >= MTF_5M_TREND_THRESHOLD:
                if change_5m <= 0.008:
                    result['stage'] = 'early'
                    result['reasons'].append(f"✅ 상승 초기 ({change_5m*100:.2f}%)")
                else:
                    result['stage'] = 'mid'
                    result['reasons'].append(f"📈 상승 중반 ({change_5m*100:.2f}%)")
            else:
                result['stage'] = 'neutral'
            
            if len(candles_recent) >= 3:
                avg_vol = sum(c['candle_acc_trade_volume'] for c in candles_recent[:-1]) / (len(candles_recent) - 1)
                current_vol = candles_recent[-1]['candle_acc_trade_volume']
                if avg_vol > 0 and current_vol >= avg_vol * MTF_VOLUME_CONFIRMATION:
                    result['volume_confirmed'] = True
                    result['reasons'].append(f"거래량 확인 ({current_vol/avg_vol:.1f}x)")
                elif avg_vol > 0 and current_vol < avg_vol * 0.7:
                    result['warnings'].append(f"거래량 감소 ({current_vol/avg_vol:.1f}x)")
        else:
            result['warnings'].append(f"5분봉 데이터 부족 ({len(self.minute5_candles)}/{MTF_5M_MIN_CANDLES})")
        
        # 2. 15분봉 분석
        if len(self.minute15_candles) >= MTF_15M_MIN_CANDLES:
            candles_15m = list(self.minute15_candles)[-MTF_15M_MIN_CANDLES:]
            start_price_15m = candles_15m[0]['opening_price']
            change_15m = (current_price - start_price_15m) / start_price_15m if start_price_15m > 0 else 0
            result['change_15m'] = change_15m
            
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
        
        # 3. 추가 필터
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
        """모멘텀 감지"""
        if len(self.minute_candles) < MOMENTUM_WINDOW:
            return {'signal': False, 'strength': 0, 'reason': '데이터 부족', 'price_change': 0, 'volume_ratio': 0}
        
        recent = list(self.minute_candles)[-MOMENTUM_WINDOW:]
        price_change = (current_price - recent[0]['opening_price']) / recent[0]['opening_price']
        
        velocity = (current_price - recent[-3]['opening_price']) / 3 if len(recent) >= 3 else 0
        velocity_pct = velocity / recent[-3]['opening_price'] if len(recent) >= 3 else 0
        
        avg_volume = sum(self.volume_history) / len(self.volume_history) if self.volume_history else 0
        recent_volume = recent[-1]['candle_acc_trade_volume']
        volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 0
        
        up_count = 0
        for i in range(1, len(recent)):
            if recent[i]['trade_price'] > recent[i-1]['trade_price']:
                up_count += 1
            else:
                up_count = 0
        
        bid_ask_ratio = 1.0
        if self.orderbook['total_ask_size'] > 0:
            bid_ask_ratio = self.orderbook['total_bid_size'] / self.orderbook['total_ask_size']
        
        orderbook_ok = bid_ask_ratio >= 0.8
        
        momentum_ok = price_change >= MOMENTUM_THRESHOLD
        volume_ok = volume_ratio >= VOLUME_SPIKE_RATIO
        velocity_ok = velocity_pct >= BREAKOUT_VELOCITY
        consecutive_ok = up_count >= CONSECUTIVE_UP_CANDLES
        
        strength = 0
        if momentum_ok: strength += 30
        if volume_ok: strength += (volume_ratio / VOLUME_SPIKE_RATIO) * 20
        if velocity_ok: strength += (velocity_pct / BREAKOUT_VELOCITY) * 30
        if consecutive_ok: strength += 20
        if bid_ask_ratio > 1.2: strength += 10
        strength = min(strength, 100)
        
        signal = momentum_ok and (volume_ok or velocity_ok or consecutive_ok) and orderbook_ok
        
        reason = []
        if velocity_ok: reason.append(f"가속도({velocity_pct*100:.2f}%)")
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
        """초봉 모멘텀 감지"""
        if len(self.second_candles) < SECOND_MOMENTUM_WINDOW:
            return {'signal': False, 'strength': 0, 'reason': '초봉 데이터 부족', 'rapid_rise': False}
        
        recent = list(self.second_candles)[-SECOND_MOMENTUM_WINDOW:]
        sec_price_change = (current_price - recent[0]['opening_price']) / recent[0]['opening_price']
        
        if len(recent) >= 2:
            rapid_change = (current_price - recent[-2]['opening_price']) / recent[-2]['opening_price']
        else:
            rapid_change = 0
        rapid_rise = rapid_change >= SECOND_RAPID_RISE_THRESHOLD
        
        sec_up_count = 0
        for i in range(1, len(recent)):
            if recent[i]['trade_price'] > recent[i-1]['trade_price']:
                sec_up_count += 1
            else:
                sec_up_count = 0
        
        avg_sec_volume = sum(self.second_volume_history) / len(self.second_volume_history) if self.second_volume_history else 0
        recent_sec_volume = recent[-1]['candle_acc_trade_volume']
        sec_volume_ratio = recent_sec_volume / avg_sec_volume if avg_sec_volume > 0 else 0
        
        sec_momentum_ok = sec_price_change >= SECOND_MOMENTUM_THRESHOLD
        sec_volume_ok = sec_volume_ratio >= VOLUME_SPIKE_RATIO
        
        signal = (sec_momentum_ok and sec_volume_ok) or rapid_rise
        
        strength = 0
        if sec_momentum_ok:
            strength += sec_price_change / SECOND_MOMENTUM_THRESHOLD * 25
        if sec_volume_ok:
            strength += (sec_volume_ratio - 1) * 15
        if rapid_rise:
            strength += rapid_change / SECOND_RAPID_RISE_THRESHOLD * 30
        strength = min(strength, 100)
        
        reason = []
        if sec_momentum_ok: reason.append(f"초봉모멘텀 {sec_price_change*100:.3f}%")
        if rapid_rise: reason.append(f"🚀급등 {rapid_change*100:.3f}%")
        if sec_volume_ok: reason.append(f"초봉거래량 {sec_volume_ratio:.1f}배")
        if sec_up_count >= 3: reason.append(f"연속상승초 {sec_up_count}개")
        
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
        """결합 모멘텀 감지"""
        minute_result = self.detect_momentum(current_price)
        
        if not USE_SECOND_CANDLES or len(self.second_candles) < SECOND_MOMENTUM_WINDOW:
            second_result = {'signal': False, 'strength': 0, 'rapid_rise': False, 'reason': '초봉 미사용'}
        else:
            second_result = self.detect_second_momentum(current_price)
        
        mtf_result = self.analyze_multi_timeframe(current_price)
        
        combined_signal = False
        combined_strength = 0
        reasons = []
        mtf_blocked = False
        
        orderbook_imbalance = self.orderbook.get('imbalance', 0)
        if orderbook_imbalance <= -0.3:
            return {
                'signal': False, 'strength': 0, 'minute_signal': minute_result['signal'],
                'second_signal': second_result.get('signal', False), 'rapid_rise': second_result.get('rapid_rise', False),
                'mtf_valid': mtf_result['valid_entry'], 'mtf_stage': mtf_result.get('stage', 'unknown'),
                'mtf_blocked': True, 'reason': f'🚫 호가불균형 차단 (매도우위:{orderbook_imbalance:.2f})'
            }
        
        if minute_result['signal'] and second_result.get('signal', False):
            combined_signal = True
            combined_strength = min(100, minute_result['strength'] * 0.6 + second_result['strength'] * 0.4)
            reasons.append(minute_result['reason'])
            reasons.append(second_result['reason'])
            
        elif second_result.get('rapid_rise', False):
            has_minute_support = minute_result['price_change'] > MOMENTUM_THRESHOLD * 0.9
            has_bullish_trend = mtf_result.get('trend_5m') == 'bullish' or mtf_result.get('trend_15m') == 'bullish'
            
            if has_minute_support and has_bullish_trend:
                combined_signal = True
                combined_strength = second_result['strength']
                reasons.append(f"⚡빠른진입: {second_result['reason']}")
            elif has_minute_support:
                combined_signal = True
                combined_strength = second_result['strength'] * 0.5
                reasons.append(f"⚠️ 약한진입: {second_result['reason']} (MTF 미확인)")
                
        elif minute_result['signal']:
            combined_signal = True
            combined_strength = minute_result['strength'] * 0.8
            reasons.append(minute_result['reason'])

        if not combined_signal:
            trend_bullish = mtf_result.get('trend_5m') == 'bullish' and mtf_result.get('trend_15m') in ['bullish', 'neutral']
            total_vol_5m = self.bid_volume_5m + self.ask_volume_5m
            buy_ratio_5m = (self.bid_volume_5m / total_vol_5m * 100) if total_vol_5m > 0 else 50
            strong_buying = buy_ratio_5m >= 55.0
            active_rising = minute_result.get('price_change', 0) >= 0.003
            
            if trend_bullish and strong_buying and active_rising:
                combined_signal = True
                combined_strength = 60
                reasons.append(f"📈 추세추종: 상승세(5m:{mtf_result.get('change_5m',0)*100:.2f}%) + 매수세({buy_ratio_5m:.0f}%)")
        
        if combined_signal and MTF_ENABLED:
            if mtf_result.get('trend_5m') == 'bearish':
                combined_signal = False
                mtf_blocked = True
                reasons.append(f"🚫 5분봉 하락추세 ({mtf_result.get('change_5m',0)*100:.2f}%)")
            
            elif len(self.minute5_candles) >= 3:
                recent_5m_changes = []
                for i in range(-3, 0):
                    if abs(i) <= len(self.minute5_candles):
                        curr = self.minute5_candles[i]['trade_price']
                        prev = self.minute5_candles[i-1]['trade_price']
                        change = (curr - prev) / prev if prev > 0 else 0
                        recent_5m_changes.append(change)
                
                if len(recent_5m_changes) >= 2:
                    last_momentum = recent_5m_changes[-1]
                    prev_momentum = recent_5m_changes[-2]
                    if prev_momentum > 0.003 and last_momentum < prev_momentum * 0.5:
                        combined_signal = False
                        mtf_blocked = True
                        reasons.append(f"🚫 5분봉 모멘텀 약화 ({prev_momentum*100:.2f}% → {last_momentum*100:.2f}%)")
            
            elif minute_result.get('price_change', 0) >= MTF_MAX_1M_CHANGE:
                combined_signal = False
                mtf_blocked = True
                reasons.append(f"🚫 1분봉 과도한 급등 ({minute_result.get('price_change',0)*100:.2f}%) - 고점 위험")
            
            elif not mtf_result['valid_entry']:
                combined_signal = False
                mtf_blocked = True
                reasons.append(f"🚫 MTF 차단: {' | '.join(mtf_result['warnings'])}")
            else:
                stage = mtf_result.get('stage', 'unknown')
                if (stage == 'neutral' or stage == 'unknown') and combined_strength < 80:
                    combined_signal = False 
                    mtf_blocked = True
                    reasons.append(f"⚪ MTF 중립 - 강도 부족 ({combined_strength:.1f}<80)")
                elif stage == 'early':
                    combined_strength = min(100, combined_strength * 1.2)
                    reasons.append(f"🎯 상승초기 진입")
                elif stage == 'mid':
                    combined_strength = combined_strength * 0.85
                    if combined_strength < 90:
                        combined_signal = False
                        mtf_blocked = True
                        reasons.append(f"🚫 상승중반 강도부족 ({combined_strength:.1f}<90) - 타이밍 늦음")
                    else:
                        reasons.append(f"📈 상승중반")
                elif stage == 'late':
                    combined_signal = False
                    mtf_blocked = True
                    reasons.append(f"🚫 상승후반 - 진입차단")
                
                if combined_signal:
                    if mtf_result['volume_confirmed']:
                        combined_strength = min(100, combined_strength + 10)
                    if mtf_result['trend_15m'] == 'bullish':
                        combined_strength = min(100, combined_strength + 5)
                    elif mtf_result['trend_15m'] == 'bearish':
                        combined_strength = max(0, combined_strength - 20)
                        if MTF_STRICT_MODE:
                            combined_signal = False
                            mtf_blocked = True
                            reasons.append(f"🚫 15분봉 하락추세")
        
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
