import os
import sys
import time
import json
import uuid
import asyncio
import threading
import queue
import logging
import websockets
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import deque

from prompt_toolkit import PromptSession
from prompt_toolkit.patch_stdout import patch_stdout

from config import *
from api import UpbitAPI
from state import TradingState
from analyzer import MarketAnalyzer

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
        self.user_cmd_queue = queue.Queue()
        
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
        """거래대금 상위 종목으로 마켓 리스트 갱신"""
        try:
            # === 수동 마켓 지정 모드 ===
            if MARKET and len(MARKET) > 0:
                if not self.markets:
                    new_markets = MARKET.copy()
                    logger.info(f"🎯 수동 마켓 지정 모드: {len(new_markets)}개 종목")
                    logger.info(f"   마켓: {new_markets}")
                    
                    for market in new_markets:
                        if market not in self.states:
                            self.states[market] = TradingState(market)
                        if market not in self.analyzers:
                            self.analyzers[market] = MarketAnalyzer(self.api, market)
                            
                        try:
                            self.analyzers[market].initialize_candles_smart(CANDLE_UNIT, 200, self.analyzers[market].minute_candles)
                            
                            self.analyzers[market].volume_history.clear()
                            for candle in self.analyzers[market].minute_candles:
                                self.analyzers[market].volume_history.append(candle['candle_acc_trade_volume'])

                            self.analyzers[market].initialize_candles_smart(5, 600, self.analyzers[market].minute5_candles)
                            self.analyzers[market].initialize_candles_smart(15, 400, self.analyzers[market].minute15_candles)
                            
                            sec_candles = self.api.get_candles_seconds(market, 120)
                            self.analyzers[market].update_second_candles(sec_candles)
                            
                            self.analyzers[market].analyze_macro()
                            self.last_price_updates[market] = None
                            logger.info(f"[{market:<11}] 초기 데이터 로드 완료")
                            
                        except Exception as e:
                            logger.error(f"[{market}] 초기 데이터 로딩 실패: {e}")
                    
                    self.markets = new_markets
                return
            
            # === 자동 마켓 선정 모드 ===
            all_markets = self.api.get_all_markets()
            krw_markets = [m['market'] for m in all_markets if m['market'].startswith('KRW-')]
            
            tickers = []
            chunk_size = 100
            for i in range(0, len(krw_markets), chunk_size):
                chunk = krw_markets[i:i+chunk_size]
                if not chunk: break
                tickers.extend(self.api.get_ticker(','.join(chunk)))
                time.sleep(0.1)
            
            sorted_tickers = sorted(tickers, key=lambda x: x['acc_trade_price_24h'], reverse=True)
            top_markets = [t['market'] for t in sorted_tickers[:TOP_MARKET_COUNT]]
            
            held_markets = []
            for market, state in self.states.items():
                if state.has_position():
                    held_markets.append(market)
            
            new_markets = list(set(top_markets + held_markets))
            
            added_markets = [m for m in new_markets if m not in self.markets]
            removed_markets = [m for m in self.markets if m not in new_markets]
            
            if added_markets or removed_markets:
                logger.info(f"🔄 마켓 리스트 갱신 (총 {len(new_markets)}개)")
                if added_markets: logger.info(f"   ➕ 추가: {added_markets}")
                if removed_markets: logger.info(f"   ➖ 제외: {removed_markets}")
                
                for market in added_markets:
                    if market not in self.states:
                        self.states[market] = TradingState(market)
                    if market not in self.analyzers:
                        self.analyzers[market] = MarketAnalyzer(self.api, market)
                        
                    try:
                        self.analyzers[market].initialize_candles_smart(CANDLE_UNIT, 200, self.analyzers[market].minute_candles)
                        
                        self.analyzers[market].volume_history.clear()
                        for candle in self.analyzers[market].minute_candles:
                            self.analyzers[market].volume_history.append(candle['candle_acc_trade_volume'])

                        self.analyzers[market].initialize_candles_smart(5, 600, self.analyzers[market].minute5_candles)
                        self.analyzers[market].initialize_candles_smart(15, 400, self.analyzers[market].minute15_candles)
                        
                        sec_candles = self.api.get_candles_seconds(market, 120)
                        self.analyzers[market].update_second_candles(sec_candles)
                        
                        self.analyzers[market].analyze_macro()
                        self.last_price_updates[market] = None
                        logger.info(f"[{market:<11}] 초기 데이터 로드 완료")
                        
                    except Exception as e:
                        logger.error(f"[{market}] 초기 데이터 로딩 실패: {e}")

                self.markets = new_markets
                
        except Exception as e:
            logger.error(f"마켓 리스트 갱신 실패: {e}")

    def start_command_listener(self):
        """별도 스레드에서 사용자 입력 대기"""
        def listen():
            session = PromptSession()
            while self.running:
                try:
                    with patch_stdout(raw=True):
                        command = session.prompt("USER_CMD> ")
                        if command:
                            self.user_cmd_queue.put(command.strip())
                except (EOFError, KeyboardInterrupt):
                    logger.info("❌ 커맨드 리스너 종료")
                    break
                except Exception as e:
                    print(f"Command Error: {e}")
                    time.sleep(1)
        cmd_thread = threading.Thread(target=listen, daemon=True)
        cmd_thread.start()

    async def process_user_command(self, cmd_line: str):
        """사용자 명령어 처리"""
        try:
            parts = cmd_line.strip().split()
            if not parts: return
            cmd = parts[0].lower()
            
            if cmd in ['/exit', '/quit', 'exit', 'quit']:
                logger.info("🛑 사용자 종료 명령 수신")
                self.running = False
                return

            if cmd == '/help':
                print("\\n=== 명령어 목록 ===")
                print("/buy <종목> <금액> : 시장가 매수")
                print("/sell <종목>        : 시장가 전량 매도")
                print("/status, /my      : 보유 자산 및 수익 현황")
                print("/price <종목>     : 현재가 조회")
                print("/trend <종목>     : 추세 분석 결과 조회")
                print("/stoploss <종목> <가격> : 손절가 수동 지정")
                print("/tp <종목> <가격>       : 익절가 수동 지정")
                print("==================\\n")
                return

            if cmd == '/status' or cmd == '/my':
                balance_krw = 0
                total_asset = 0
                if 'KRW' in self.assets:
                    balance_krw = self.assets['KRW']['balance']
                    total_asset += balance_krw
                
                for market, state in self.states.items():
                    if state.has_position():
                        currency = market.split('-')[1]
                        if currency in self.assets:
                            amount = self.assets[currency]['balance'] + self.assets[currency]['locked']
                            price = self.current_prices.get(market, 0)
                            val = amount * price
                            total_asset += val
                            if val > 5000:
                                avg = self.assets[currency]['avg_buy_price']
                                pnl = (price - avg) / avg * 100 if avg > 0 else 0
                                logger.info(f"   🪙 {currency:<4} | 평가:{val:,.0f}원 ({pnl:+.2f}%) | 평단:{avg:,.0f} 현재:{price:,.0f}")
                
                logger.info(f"💰 총 자산: {total_asset:,.0f}원 (KRW: {balance_krw:,.0f}원)")
                logger.info(f"   현재 수익: {self.cumulative_profit:,.0f}원 (승:{self.cumulative_wins} 패:{self.cumulative_losses})")
                return

            if cmd == '/buy':
                if len(parts) < 3:
                    logger.warning("사용법: /buy <종목> <금액>")
                    return
                coin = parts[1].upper().replace('KRW-', '')
                market = f"KRW-{coin}"
                try: amount_krw = float(parts[2])
                except ValueError: return
                
                logger.info(f"🛒 [사용자 매수] {market} {amount_krw:,.0f}원 주문 시도")
                if market not in self.states: self.states[market] = TradingState(market)
                
                if DRY_RUN:
                    logger.info(f"🧪 [Simulation] 매수 체결 가정: {market}")
                else:
                    self.api.buy_market_order(market, amount_krw)
                return

            if cmd == '/sell':
                if len(parts) < 2:
                    logger.warning("사용법: /sell <종목>")
                    return
                coin = parts[1].upper().replace('KRW-', '')
                market = f"KRW-{coin}"
                logger.info(f"📉 [사용자 매도] {market} 전량 매도 시도")
                if DRY_RUN:
                    logger.info(f"🧪 [Simulation] 매도 체결 가정")
                    if market in self.states: self.states[market].position = None
                else:
                    await self._execute_sell(market, "사용자 강제 청산")
                return

            if cmd == '/trend':
                 if len(parts) < 2: return
                 coin = parts[1].upper().replace('KRW-', '')
                 market = f"KRW-{coin}"
                 if market in self.analyzers:
                     self.analyzers[market].analyze_macro()
                     res = self.analyzers[market].macro_result or {}
                     trend = self.analyzers[market].macro_trend
                     logger.info(f"📊 {market} 추세: {trend}")
                     logger.info(f"   변화율: 5m({res.get('m5_change',0)*100:+.2f}%) 15m({res.get('m15_change',0)*100:+.2f}%)")
                 else:
                     logger.warning(f"분석 데이터 없음: {market}")
                 return
            
            if cmd == '/price':
                 if len(parts) < 2: return
                 coin = parts[1].upper().replace('KRW-', '')
                 market = f"KRW-{coin}"
                 if market in self.current_prices:
                     logger.info(f"💰 {market}: {self.current_prices[market]:,.0f}원")
                 return

        except Exception as e:
            logger.error(f"명령어 처리 중 오류: {e}")

    async def _check_commands(self):
        """사용자 커맨드 큐 모니터링"""
        while self.running:
            try:
                try:
                    cmd = self.user_cmd_queue.get_nowait()
                    await self.process_user_command(cmd)
                except queue.Empty:
                    await asyncio.sleep(0.1)
                    continue
            except Exception as e:
                logger.error(f"커맨드 처리 루프 오류: {e}")
                await asyncio.sleep(1)

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
        logger.info("🚀 모멘텀 트레이딩 봇 시작 (Refactored)")
        
        self.start_command_listener()
        await self._update_top_markets()
        
        if not self.markets:
             logger.error("거래 가능한 마켓이 없습니다. 종료합니다.")
             return

        logger.info(f"   타겟 마켓: {len(self.markets)}개 종목")
        logger.info(f"   테스트 모드: {'ON' if DRY_RUN else 'OFF'}")
        
        await self._check_btc_trend()
        self._check_balance()
        self._sync_state_with_balance()
        
        self.running = True
        try:
            await asyncio.gather(
                self._public_ws_monitor(),
                self._private_ws_monitor(),
                self._trading_loop(),
                self._macro_update_loop(),
                self._check_commands(),
                self._balance_report_loop(),
                self._market_update_loop(),
                self._btc_monitor_loop()
            )
        except KeyboardInterrupt:
            logger.info("사용자에 의해 중단됨")
        except Exception as e:
            logger.error(f"봇 오류: {e}")
        finally:
            self.running = False
            self._print_summary()
    
    async def _check_btc_trend(self):
        """BTC 추세 확인"""
        try:
            h1_candles = self.api.get_candles_minutes(BTC_MARKET, unit=60, count=2)
            if len(h1_candles) >= 2:
                btc_change = (h1_candles[0]['trade_price'] - h1_candles[1]['trade_price']) / h1_candles[1]['trade_price']
                self.btc_change_rate = btc_change
                if btc_change <= BTC_TREND_THRESHOLD:
                    self.btc_trend = 'bearish'
                    self.market_safe = not BTC_DOWNTREND_BUY_BLOCK
                elif btc_change >= BTC_BULLISH_THRESHOLD:
                    self.btc_trend = 'bullish'
                    self.market_safe = True
                else:
                    self.btc_trend = 'neutral'
                    self.market_safe = True
                
                self.last_btc_check = datetime.now()
                trend_emoji = "🟢" if self.btc_trend == 'bullish' else ("🔴" if self.btc_trend == 'bearish' else "🟡")
                logger.info(f"[{BTC_MARKET}] {trend_emoji} BTC 추세: {self.btc_trend} ({btc_change*100:+.2f}%)")
        except Exception as e:
            logger.error(f"BTC 추세 확인 오류: {e}")
            self.market_safe = True
    
    async def _btc_monitor_loop(self):
        while self.running:
            await asyncio.sleep(BTC_CHECK_INTERVAL)
            await self._check_btc_trend()

    async def _balance_report_loop(self):
        while self.running:
            await asyncio.sleep(BALANCE_REPORT_INTERVAL)
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._check_balance)
            except Exception as e:
                logger.error(f"리포트 루프 오류: {e}")
    
    def _check_balance(self):
        """잔고 확인"""
        try:
            # KRW 잔고 표시
            if 'KRW' in self.assets:
                logger.info(f"💰 KRW 잔고: {self.assets['KRW']['balance']:,.0f}원")
            
            # 보유 자산별 평가
            total_valuation = 0.0
            for currency, asset in self.assets.items():
                if currency == 'KRW': continue
                balance = asset['balance'] + asset['locked']
                if balance <= 0: continue
                
                avg = asset.get('avg_buy_price', 0.0)
                market = f"KRW-{currency}"
                current = self.current_prices.get(market, avg) # 없으면 평단가
                
                val = balance * current
                total_valuation += val
                
                pnl = (current - avg) / avg * 100 if avg > 0 else 0
                logger.info(f"🪙 {currency} | 보유:{balance:.4f} | 평단:{avg:,.0f} | 현재:{current:,.0f} | 수익:{pnl:+.2f}%")
                
            logger.info(f"💵 총 자산 추정: {self.assets.get('KRW', {}).get('balance', 0) + total_valuation:,.0f}원")
        except Exception as e:
            logger.error(f"잔고 확인 실패: {e}")
    
    async def _public_ws_monitor(self):
        """Public WebSocket"""
        while self.running:
            try:
                async with websockets.connect(WS_PUBLIC_URL) as ws:
                    codes = self.markets
                    subscribe = [
                        {"ticket": f"momentum-pub-{uuid.uuid4()}"},
                        {"type": "ticker", "codes": codes, "isOnlyRealtime": True},
                        {"type": "trade", "codes": codes, "isOnlyRealtime": True},
                        {"type": "orderbook", "codes": codes, "isOnlyRealtime": True},
                        {"type": "candle.1s", "codes": codes},
                        {"type": "candle.1m", "codes": codes},
                        {"type": "candle.5m", "codes": codes},
                        {"type": "candle.15m", "codes": codes},
                        {"format": "DEFAULT"}
                    ]
                    await ws.send(json.dumps(subscribe))
                    logger.info("📡 Public WebSocket 연결됨")
                    
                    last_ping = time.time()
                    while self.running:
                        if time.time() - last_ping > 60:
                            await ws.send("PING")
                            last_ping = time.time()
                        
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            if msg == "PONG": continue
                            data = json.loads(msg)
                            
                            type_val = data.get('type')
                            code = data.get('code')
                            
                            if code and code in self.markets:
                                if type_val == 'ticker':
                                    self.current_prices[code] = data.get('trade_price')
                                elif type_val == 'trade':
                                    self.current_prices[code] = data.get('trade_price')
                                    self.analyzers[code].update_trade_from_ws(data)
                                elif type_val == 'orderbook':
                                    self.analyzers[code].update_orderbook_from_ws(data)
                                elif type_val and type_val.startswith('candle.'):
                                    self.analyzers[code].update_candle_from_ws(data, type_val)
                        except asyncio.TimeoutError:
                            await ws.send("PING")
                            last_ping = time.time()
                            
            except Exception as e:
                logger.error(f"Public WebSocket 오류: {e}")
                await asyncio.sleep(5)

    async def _private_ws_monitor(self):
        """Private WebSocket"""
        token = self.api._generate_jwt()
        headers = {'Authorization': f'Bearer {token}'}
        
        while self.running:
            try:
                async with websockets.connect(WS_PRIVATE_URL, additional_headers=headers) as ws:
                    subscribe = [
                        {"ticket": f"momentum-priv-{uuid.uuid4()}"},
                        {"type": "myOrder", "codes": self.markets},
                        {"type": "myAsset"},
                        {"format": "DEFAULT"}
                    ]
                    await ws.send(json.dumps(subscribe))
                    logger.info("🔐 Private WebSocket 연결됨")
                    
                    last_ping = time.time()
                    while self.running:
                        if time.time() - last_ping > 60:
                            await ws.send("PING")
                            last_ping = time.time()
                            
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            if msg == "PONG": continue
                            data = json.loads(msg)
                            
                            type_val = data.get('type')
                            if type_val == 'myAsset':
                                assets = data.get('assets')
                                for asset in assets:
                                    cur = asset.get('currency')
                                    self.assets[cur] = {
                                        'balance': float(asset.get('balance')),
                                        'locked': float(asset.get('locked')),
                                        'avg_buy_price': float(asset.get('avg_buy_price'))
                                    }
                            elif type_val == 'myOrder':
                                uid = data.get('uuid')
                                state = data.get('state')
                                if state in ['wait', 'watch']:
                                    self.active_orders[uid] = data
                                elif state in ['done', 'cancel']:
                                    if uid in self.active_orders:
                                        del self.active_orders[uid]
                        except asyncio.TimeoutError:
                            await ws.send("PING")
                            last_ping = time.time()
                            
            except Exception as e:
                logger.error(f"Private WebSocket 오류: {e}")
                token = self.api._generate_jwt()
                headers = {'Authorization': f'Bearer {token}'}
                await asyncio.sleep(5)
    
    async def _trading_loop(self):
        """메인 트레이딩 루프"""
        await asyncio.sleep(5)
        last_status_log = 0
        while self.running:
            try:
                # BTC 안전 체크
                if not self.market_safe:
                    for market in self.markets:
                         if self.states[market].has_position():
                             await self._manage_position(market)
                    await asyncio.sleep(1)
                    continue

                for market in self.markets:
                    current_price = self.current_prices.get(market, 0)
                    if current_price <= 0: continue
                    
                    state = self.states[market]
                    if state.has_position():
                        await self._manage_position(market)
                    else:
                        await self._find_entry(market)
                
                # 로그 출력
                if time.time() - last_status_log >= 10:
                    last_status_log = time.time()
                    for market in self.markets:
                         if market in self.current_prices:
                             # 간략 로그
                             pass
                
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"트레이딩 루프 오류: {e}")
                await asyncio.sleep(5)

    async def _macro_update_loop(self):
        while self.running:
            await asyncio.sleep(MACRO_UPDATE_INTERVAL)
            for market in self.markets:
                if market in self.analyzers:
                    self.analyzers[market].analyze_macro()
                    # 저장 로직 생략 (Analyzer 내부에서 함)
            await asyncio.sleep(0.01)

    async def _find_entry(self, market: str):
        """진입 기회 탐색"""
        state = self.states[market]
        if not state.can_trade(): return
        
        analyzer = self.analyzers[market]
        current_price = self.current_prices[market]
        
        # 재진입 방지
        if state.last_exit_price > 0 and state.consecutive_losses > 0:
            if current_price > state.last_exit_price * 0.98: return

        if len(analyzer.minute_candles) < MOMENTUM_WINDOW: return

        sentiment = analyzer.analyze_market_sentiment()
        if sentiment['sentiment'] == 'bearish': return
        
        momentum = analyzer.detect_combined_momentum(current_price)
        if not momentum['signal']: return
        
        # 필터링
        if sentiment['rsi'] >= 75: return # 과매수
        if sentiment['fatigue'] >= 40 and momentum['strength'] < 80: return
        
        await self._execute_buy(market)

    async def _execute_buy(self, market: str):
        state = self.states[market]
        if state.processing_order or state.has_position(): return
        state.processing_order = True
        
        try:
            krw_balance = self.assets.get('KRW', {'balance': 0})['balance']
            invest_amount = min(MAX_INVESTMENT, krw_balance * 0.99)
            if invest_amount < MIN_ORDER_AMOUNT: return
            
            if DRY_RUN:
                logger.info(f"[{market}] 🛒 [테스트] 매수: {invest_amount:,.0f}원")
                current = self.current_prices[market]
                state.position = {
                    'side': 'bid', 'price': current, 'amount': invest_amount, 'volume': invest_amount/current
                }
            else:
                self.api.buy_market_order(market, invest_amount)
                # 실제 체결 대기 로직 필요하지만 생략
                await asyncio.sleep(1)
                current = self.current_prices[market]
                state.position = { # 추정
                    'side': 'bid', 'price': current, 'amount': invest_amount, 'volume': invest_amount/current
                }
            
            if state.position:
                state.entry_price = state.position['price']
                state.entry_time = datetime.now()
                state.highest_price = state.entry_price
                state.stop_loss_price = state.entry_price * (1 - INITIAL_STOP_LOSS)
                state.take_profit_price = state.entry_price * (1 + TAKE_PROFIT_TARGET)
                state.record_trade('buy', invest_amount, state.entry_price)
                self._log_trade(market, 'BUY', state.entry_price, invest_amount, reason="진입")

        except Exception as e:
            logger.error(f"매수 실행 오류: {e}")
        finally:
            state.processing_order = False

    async def _manage_position(self, market: str):
        state = self.states[market]
        if not state.has_position(): return
        
        current = self.current_prices[market]
        entry = state.entry_price
        state.highest_price = max(state.highest_price, current)
        profit_rate = (current - entry) / entry
        
        # 트레일링 스탑
        if profit_rate >= TRAILING_STOP_ACTIVATION and not state.trailing_active:
            state.trailing_active = True
            state.stop_loss_price = max(state.stop_loss_price, entry * (1 + TRAILING_MIN_PROFIT))
            logger.info(f"[{market}] 트레일링 활성화")
            
        if state.trailing_active:
            new_stop = state.highest_price * (1 - TRAILING_STOP_DISTANCE)
            state.stop_loss_price = max(state.stop_loss_price, new_stop)
            
        sell_reason = None
        if current <= state.stop_loss_price:
            sell_reason = 'stop_loss'
        elif current >= state.take_profit_price and not state.trailing_active:
             # 익절가 도달 시 트레일링 전환
             state.trailing_active = True
             state.stop_loss_price = max(entry, entry * (1 + TRAILING_MIN_PROFIT))
        
        if sell_reason:
            await self._execute_sell(market, sell_reason)

    async def _execute_sell(self, market: str, reason: str):
        state = self.states[market]
        if not state.has_position(): return
        
        try:
            current = self.current_prices[market]
            volume = state.position['volume']
            
            if DRY_RUN:
                logger.info(f"[{market}] 📉 [테스트] 매도: {reason}")
            else:
                self.api.sell_market_order(market, volume)
                await asyncio.sleep(1)
            
            sell_amount = volume * current
            buy_amount = state.position['amount']
            profit = sell_amount - buy_amount
            
            state.record_trade(reason, sell_amount, current, profit)
            self.cumulative_profit += profit
            self.cumulative_trades += 1
            if profit >= 0: self.cumulative_wins += 1
            else: self.cumulative_losses += 1
            
            self._log_trade(market, 'SELL', current, sell_amount, volume, profit, profit/buy_amount, reason)
            
            state.position = None
            state.trailing_active = False
            logger.info(f"[{market}] 매도 완료 (수익: {profit:,.0f}원)")
            
        except Exception as e:
            logger.error(f"매도 실행 오류: {e}")

    def _sync_state_with_balance(self):
        """기존 보유 종목 상태 복구"""
        logger.info("♻️ 상태 동기화...")
        # (간소화된 로직)
        for market in self.markets:
            currency = market.split('-')[1]
            if currency in self.assets:
                balance = self.assets[currency]['balance']
                if balance * self.assets[currency]['avg_buy_price'] > 5000:
                     if not self.states[market].has_position():
                         avg = self.assets[currency]['avg_buy_price']
                         self.states[market].position = {
                             'side': 'bid', 'price': avg, 'amount': balance*avg, 'volume': balance
                         }
                         self.states[market].entry_price = avg
                         self.states[market].highest_price = avg
                         self.states[market].stop_loss_price = avg * (1 - INITIAL_STOP_LOSS)
                         self.states[market].take_profit_price = avg * (1 + TAKE_PROFIT_TARGET)
                         logger.info(f"[{market}] 상태 복구됨")

    def _print_summary(self):
        logger.info(f"📊 최종 수익: {self.cumulative_profit:,.0f}원")
