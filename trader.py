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
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import deque

from prompt_toolkit import Application
from prompt_toolkit.layout import Layout, HSplit, Window, Dimension
from prompt_toolkit.widgets import TextArea, Frame
from prompt_toolkit.styles import Style

from config import *
from api import UpbitAPI
from state import TradingState
from analyzer import MarketAnalyzer

def strip_ansi_codes(text: str) -> str:
    """ANSI 색상 코드를 제거합니다."""
    # ANSI escape sequence 제거 (ESC[로 시작하는 코드)
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

class TuiLogHandler(logging.Handler):
    """Log handler that writes to a prompt_toolkit TextArea"""
    def __init__(self, text_area):
        super().__init__()
        self.text_area = text_area

    def emit(self, record):
        try:
            msg = self.format(record)
            # ANSI 코드 제거
            msg = strip_ansi_codes(msg)
            
            # read_only 상태를 일시적으로 해제하여 텍스트 추가
            was_read_only = self.text_area.read_only
            if was_read_only:
                self.text_area.read_only = False
            
            try:
                # Append message to buffer
                self.text_area.buffer.text += msg + "\n"
                # Keep buffer size manageable (optional, e.g. last 5000 lines)
                if len(self.text_area.buffer.text) > 100000:
                     self.text_area.buffer.text = self.text_area.buffer.text[-90000:]
                
                # Auto-scroll to bottom
                self.text_area.buffer.cursor_position = len(self.text_area.buffer.text)
            finally:
                # read_only 상태 복원
                if was_read_only:
                    self.text_area.read_only = True
        except Exception as e:
            # 예외 발생 시 기본 핸들러로 처리
            self.handleError(record)

class MomentumTrader:
    """모멘텀 트레이딩 봇"""
    
    def __init__(self):
        self.access_key = ACCESS_KEY
        self.secret_key = SECRET_KEY
        self.api = UpbitAPI(ACCESS_KEY, SECRET_KEY)

        # UI Components
        # log_field는 초기 메시지를 설정한 후 read_only로 변경
        self.log_field = TextArea(style='class:output-field', scrollbar=True, focusable=False, read_only=False)
        self.input_field = TextArea(height=1, prompt='> ', style='class:input-field', multiline=False, wrap_lines=False, read_only=False)
        
        # 초기 메시지 표시 (로딩 중에도 입력이 보이도록)
        initial_msg = "모멘텀 트레이딩 봇 초기화 중...\n"
        self.log_field.buffer.text = initial_msg
        
        # 초기 메시지 설정 후 read_only로 변경
        self.log_field.read_only = True
        
        # Logging Handler 교체 (StreamHandler -> TuiLogHandler)
        root_logger = logging.getLogger()
        for h in root_logger.handlers[:]:
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler):
                root_logger.removeHandler(h)
        
        # TuiHandler 추가
        tui_handler = TuiLogHandler(self.log_field)
        tui_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-7s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
        root_logger.addHandler(tui_handler)
        
        # 백그라운드 태스크 추적
        self.background_tasks = set()
        self._main_logic_started = False
        
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
            logger.info(f"거래 로그 파일 생성: {TRADE_LOG_FILE}")
    
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
                    logger.info(f"수동 마켓 지정 모드: {len(new_markets)}개 종목")
                    logger.info(f"   마켓: {new_markets}")
                    
                    for market in new_markets:
                        # 각 마켓 처리 사이에 이벤트 루프가 다른 작업을 처리할 수 있도록 양보
                        await asyncio.sleep(0)
                        
                        if market not in self.states:
                            self.states[market] = TradingState(market)
                        if market not in self.analyzers:
                            self.analyzers[market] = MarketAnalyzer(self.api, market)
                            
                        try:
                            await asyncio.sleep(0)  # 이벤트 루프 양보
                            self.analyzers[market].initialize_candles_smart(CANDLE_UNIT, 200, self.analyzers[market].minute_candles)
                            
                            await asyncio.sleep(0)  # 이벤트 루프 양보
                            self.analyzers[market].volume_history.clear()
                            for candle in self.analyzers[market].minute_candles:
                                self.analyzers[market].volume_history.append(candle['candle_acc_trade_volume'])

                            await asyncio.sleep(0)  # 이벤트 루프 양보
                            self.analyzers[market].initialize_candles_smart(5, 600, self.analyzers[market].minute5_candles)
                            await asyncio.sleep(0)  # 이벤트 루프 양보
                            self.analyzers[market].initialize_candles_smart(15, 400, self.analyzers[market].minute15_candles)
                            
                            await asyncio.sleep(0)  # 이벤트 루프 양보
                            sec_candles = self.api.get_candles_seconds(market, 120)
                            await asyncio.sleep(0)  # 이벤트 루프 양보
                            self.analyzers[market].update_second_candles(sec_candles)
                            
                            await asyncio.sleep(0)  # 이벤트 루프 양보
                            self.analyzers[market].analyze_macro()
                            self.last_price_updates[market] = None
                            logger.info(f"[{market:<11}] 초기 데이터 로드 완료")
                            
                        except Exception as e:
                            logger.error(f"[{market}] 초기 데이터 로딩 실패: {e}")
                    
                    self.markets = new_markets
                return
            
            # === 자동 마켓 선정 모드 ===
            await asyncio.sleep(0)  # 이벤트 루프 양보
            all_markets = self.api.get_all_markets()
            await asyncio.sleep(0)  # 이벤트 루프 양보
            krw_markets = [m['market'] for m in all_markets if m['market'].startswith('KRW-')]
            
            tickers = []
            chunk_size = 100
            for i in range(0, len(krw_markets), chunk_size):
                await asyncio.sleep(0)  # 이벤트 루프 양보
                chunk = krw_markets[i:i+chunk_size]
                if not chunk: break
                tickers.extend(self.api.get_ticker(','.join(chunk)))
                await asyncio.sleep(0.1)  # API 호출 제한을 위한 대기
            
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
                logger.info(f"마켓 리스트 갱신 (총 {len(new_markets)}개)")
                if added_markets: logger.info(f"   추가: {added_markets}")
                if removed_markets: logger.info(f"   제외: {removed_markets}")
                
                for market in added_markets:
                    # 각 마켓 처리 사이에 이벤트 루프가 다른 작업을 처리할 수 있도록 양보
                    await asyncio.sleep(0)
                    
                    if market not in self.states:
                        self.states[market] = TradingState(market)
                    if market not in self.analyzers:
                        self.analyzers[market] = MarketAnalyzer(self.api, market)
                        
                    try:
                        await asyncio.sleep(0)  # 이벤트 루프 양보
                        self.analyzers[market].initialize_candles_smart(CANDLE_UNIT, 200, self.analyzers[market].minute_candles)
                        
                        await asyncio.sleep(0)  # 이벤트 루프 양보
                        self.analyzers[market].volume_history.clear()
                        for candle in self.analyzers[market].minute_candles:
                            self.analyzers[market].volume_history.append(candle['candle_acc_trade_volume'])

                        await asyncio.sleep(0)  # 이벤트 루프 양보
                        self.analyzers[market].initialize_candles_smart(5, 600, self.analyzers[market].minute5_candles)
                        await asyncio.sleep(0)  # 이벤트 루프 양보
                        self.analyzers[market].initialize_candles_smart(15, 400, self.analyzers[market].minute15_candles)
                        
                        await asyncio.sleep(0)  # 이벤트 루프 양보
                        sec_candles = self.api.get_candles_seconds(market, 120)
                        await asyncio.sleep(0)  # 이벤트 루프 양보
                        self.analyzers[market].update_second_candles(sec_candles)
                        
                        await asyncio.sleep(0)  # 이벤트 루프 양보
                        self.analyzers[market].analyze_macro()
                        self.last_price_updates[market] = None
                        logger.info(f"[{market:<11}] 초기 데이터 로드 완료")
                        
                    except Exception as e:
                        logger.error(f"[{market}] 초기 데이터 로딩 실패: {e}")

                self.markets = new_markets
                
        except Exception as e:
            logger.error(f"마켓 리스트 갱신 실패: {e}")

    async def run_tui(self):
        """TUI 실행 (prompt_toolkit application)"""
        def accept(buff):
            text = buff.text.strip()
            if text:
                self.user_cmd_queue.put(text)
            return False # Clear text

        
        self.input_field.accept_handler = accept
        
        # Layout 구성
        # 상단: 로그 (scrollable)
        # 중간: 구분선
        # 하단: 입력창 (3줄)
        root_container = HSplit([
            self.log_field,
            Window(height=1, char='-', style='class:line'),
            self.input_field
        ])
        
        # 스타일 정의
        style = Style.from_dict({
            'output-field': 'bg:#000000 #ffffff',
            'input-field': 'bg:#000000 #ffffff',
            'line': '#888888',
        })
        
        self.app = Application(
            layout=Layout(root_container),
            full_screen=True,
            style=style,
            mouse_support=False
        )
        
        # 입력 필드에 포커스 설정
        self.app.layout.focus(self.input_field)
        
        # Application이 완전히 시작된 후 초기화 시작
        # run_async() 내부에서 초기화를 시작하도록 함
        # 이렇게 하면 Application이 완전히 렌더링된 후에만 초기화가 시작됨
        
        # 앱 실행 (블로킹)
        # Application이 시작되면 내부적으로 이벤트 루프가 실행됨
        # 이 시점에서 초기화 태스크를 생성하면 Application이 완전히 시작된 후에 실행됨
        async def run_app_with_init():
            # Application 실행을 태스크로 시작
            app_task = asyncio.create_task(self.app.run_async())
            
            # Application이 완전히 시작되고 여러 프레임이 렌더링될 때까지 대기
            # 이렇게 하면 입력 필드가 완전히 렌더링되고 사용자 입력을 받을 준비가 됨
            await asyncio.sleep(1.0)  # 1초 대기
            
            # 메인 로직 시작 (Application이 완전히 시작된 후)
            if not self._main_logic_started:
                self._main_logic_started = True
                main_task = asyncio.create_task(self._main_logic())
                self.background_tasks.add(main_task)
                main_task.add_done_callback(self.background_tasks.discard)
            
            # Application이 종료될 때까지 대기
            await app_task
        
        await run_app_with_init()

    async def process_user_command(self, cmd_line: str):
        """사용자 명령어 처리"""
        try:
            parts = cmd_line.strip().split()
            if not parts: return
            cmd = parts[0].lower()
            
            if cmd in ['/exit', '/quit', 'exit', 'quit']:
                logger.info("사용자 종료 명령 수신")
                self.running = False
                
                # 모든 백그라운드 태스크 취소
                for task in self.background_tasks:
                    if not task.done():
                        task.cancel()
                
                # TUI 종료
                if hasattr(self, 'app') and self.app:
                    self.app.exit()
                
                # 잠시 대기하여 태스크 정리
                await asyncio.sleep(0.1)
                return

            if cmd == '/help':
                logger.info("")
                logger.info("=== 명령어 목록 ===")
                logger.info("/buy <종목> <금액> : 시장가 매수")
                logger.info("/sell <종목>        : 시장가 전량 매도")
                logger.info("/status, /my      : 보유 자산 및 수익 현황")
                logger.info("/price <종목>     : 현재가 조회")
                logger.info("/trend <종목>     : 추세 분석 결과 조회")
                logger.info("/stoploss <종목> <가격> : 손절가 수동 지정")
                logger.info("/tp <종목> <가격>       : 익절가 수동 지정")
                logger.info("/clear            : 화면 지우기")
                logger.info("==================")
                logger.info("")
                return

            if cmd == '/clear':
                # read_only 상태를 일시적으로 해제하여 화면 지우기
                was_read_only = self.log_field.read_only
                if was_read_only:
                    self.log_field.read_only = False
                
                try:
                    # 초기 메시지로 초기화
                    initial_msg = "모멘텀 트레이딩 봇\n"
                    self.log_field.buffer.text = initial_msg
                    self.log_field.buffer.cursor_position = len(initial_msg)
                finally:
                    # read_only 상태 복원
                    if was_read_only:
                        self.log_field.read_only = True
                
                logger.info("화면이 지워졌습니다.")
                return

            if cmd == '/status' or cmd == '/my':
                balance_krw = 0
                total_asset = 0
                if 'KRW' in self.assets:
                    balance_krw = self.assets['KRW']['balance']
                    total_asset += balance_krw
                
                logger.info("보유 종목:")
                # self.markets에 있는 마켓 먼저 표시
                for market in self.markets:
                    if market not in self.states:
                        continue
                    state = self.states[market]
                    if state.has_position():
                        currency = market.split('-')[1]
                        current_price = self.current_prices.get(market, 0)
                        
                        if current_price <= 0:
                            continue
                        
                        # 실제 보유 종목 (assets에 있는 경우)
                        if currency in self.assets:
                            amount = self.assets[currency]['balance'] + self.assets[currency]['locked']
                            if amount <= 0:
                                continue
                            
                            entry_price = self.assets[currency]['avg_buy_price']
                            if entry_price <= 0:
                                entry_price = current_price
                            
                            volume = amount
                            eval_amount = volume * current_price
                            buy_amount = volume * entry_price
                            profit_amount = eval_amount - buy_amount
                            pnl = (current_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
                            
                            total_asset += eval_amount
                            
                            # 익절가 계산
                            target_price = state.take_profit_price if state.take_profit_price > 0 else entry_price * (1 + TAKE_PROFIT_TARGET)
                            take_profit_msg = f" | 익절가: {target_price:,.0f}원"
                            
                            if state.trailing_active:
                                min_profit = entry_price * (1 + TRAILING_MIN_PROFIT)
                                take_profit_msg += f" (트레일링ON/보장:{min_profit:,.0f})"
                            
                            logger.info(f"[{market}] 보유 중 | 수량: {volume:,.4f} | "
                                      f"매수가: {entry_price:,.0f}원 | 현재가: {current_price:,.0f}원 | "
                                      f"평가금액: {eval_amount:,.0f}원")
                            logger.info(f"   수익률: {pnl:+.2f}% | "
                                      f"수익금: {profit_amount:+,.0f}원 | "
                                      f"손절가: {state.stop_loss_price:,.0f}원{take_profit_msg}")
                        # 가상 구매 (DRY_RUN 또는 position만 있는 경우)
                        elif state.position:
                            volume = state.position.get('volume', 0)
                            if volume <= 0:
                                continue
                            
                            entry_price = state.entry_price if state.entry_price > 0 else state.position.get('price', current_price)
                            eval_amount = volume * current_price
                            buy_amount = volume * entry_price
                            profit_amount = eval_amount - buy_amount
                            pnl = (current_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
                            
                            total_asset += eval_amount
                            
                            # 익절가 계산
                            target_price = state.take_profit_price if state.take_profit_price > 0 else entry_price * (1 + TAKE_PROFIT_TARGET)
                            take_profit_msg = f" | 익절가: {target_price:,.0f}원"
                            
                            if state.trailing_active:
                                min_profit = entry_price * (1 + TRAILING_MIN_PROFIT)
                                take_profit_msg += f" (트레일링ON/보장:{min_profit:,.0f})"
                            
                            logger.info(f"[{market}] [가상] 보유 중 | 수량: {volume:,.4f} | "
                                      f"매수가: {entry_price:,.0f}원 | 현재가: {current_price:,.0f}원 | "
                                      f"평가금액: {eval_amount:,.0f}원")
                            logger.info(f"   수익률: {pnl:+.2f}% | "
                                      f"수익금: {profit_amount:+,.0f}원 | "
                                      f"손절가: {state.stop_loss_price:,.0f}원{take_profit_msg}")
                
                # self.markets에 없지만 self.states에 있는 보유 종목도 표시 (수동 매수 등)
                for market, state in self.states.items():
                    if market in self.markets:
                        continue  # 이미 위에서 처리됨
                    if state.has_position():
                        currency = market.split('-')[1]
                        current_price = self.current_prices.get(market, 0)
                        
                        if current_price <= 0:
                            continue
                        
                        # 가상 구매만 표시 (assets에 없으면 가상 구매)
                        if currency not in self.assets and state.position:
                            volume = state.position.get('volume', 0)
                            if volume <= 0:
                                continue
                            
                            entry_price = state.entry_price if state.entry_price > 0 else state.position.get('price', current_price)
                            eval_amount = volume * current_price
                            buy_amount = volume * entry_price
                            profit_amount = eval_amount - buy_amount
                            pnl = (current_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
                            
                            total_asset += eval_amount
                            
                            # 익절가 계산
                            target_price = state.take_profit_price if state.take_profit_price > 0 else entry_price * (1 + TAKE_PROFIT_TARGET)
                            take_profit_msg = f" | 익절가: {target_price:,.0f}원"
                            
                            if state.trailing_active:
                                min_profit = entry_price * (1 + TRAILING_MIN_PROFIT)
                                take_profit_msg += f" (트레일링ON/보장:{min_profit:,.0f})"
                            
                            logger.info(f"[{market}] [가상] 보유 중 | 수량: {volume:,.4f} | "
                                      f"매수가: {entry_price:,.0f}원 | 현재가: {current_price:,.0f}원 | "
                                      f"평가금액: {eval_amount:,.0f}원")
                            logger.info(f"   수익률: {pnl:+.2f}% | "
                                      f"수익금: {profit_amount:+,.0f}원 | "
                                      f"손절가: {state.stop_loss_price:,.0f}원{take_profit_msg}")
                
                logger.info(f"총 자산: {total_asset:,.0f}원 (KRW: {balance_krw:,.0f}원)")
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
                
                logger.info(f"[사용자 매수] {market} {amount_krw:,.0f}원 주문 시도")
                if market not in self.states: 
                    self.states[market] = TradingState(market)
                
                state = self.states[market]
                
                if DRY_RUN:
                    logger.info(f"[Simulation] 매수 체결 가정: {market}")
                    current_price = self.current_prices.get(market, 0)
                    if current_price > 0:
                        state.position = {
                            'side': 'bid',
                            'price': current_price,
                            'amount': amount_krw,
                            'volume': amount_krw / current_price
                        }
                        state.entry_price = current_price
                        state.entry_time = datetime.now()
                        state.highest_price = current_price
                        state.stop_loss_price = current_price * (1 - INITIAL_STOP_LOSS)
                        state.take_profit_price = current_price * (1 + TAKE_PROFIT_TARGET)
                        logger.info(f"[{market}] 가상 매수 완료 | 가격: {current_price:,.0f}원 | 수량: {state.position['volume']:,.8f}")
                else:
                    self.api.buy_market_order(market, amount_krw)
                return

            if cmd == '/sell':
                if len(parts) < 2:
                    logger.warning("사용법: /sell <종목>")
                    return
                coin = parts[1].upper().replace('KRW-', '')
                market = f"KRW-{coin}"
                logger.info(f"[사용자 매도] {market} 전량 매도 시도")
                if DRY_RUN:
                    logger.info(f"[Simulation] 매도 체결 가정")
                    if market in self.states: self.states[market].position = None
                else:
                    await self._execute_sell(market, "사용자 강제 청산")
                return

            if cmd == '/trend':
                if len(parts) >= 2:
                    # 특정 종목 추세 조회
                    coin = parts[1].upper().replace('KRW-', '')
                    market = f"KRW-{coin}"
                    if market in self.analyzers:
                        self.analyzers[market].analyze_macro()
                        res = self.analyzers[market].macro_result or {}
                        trend = self.analyzers[market].macro_trend
                        logger.info(f"{market} 추세: {trend}")
                        logger.info(f"   변화율: 5m({res.get('m5_change',0)*100:+.2f}%) 15m({res.get('m15_change',0)*100:+.2f}%)")
                    else:
                        logger.warning(f"분석 데이터 없음: {market}")
                else:
                    # 전체 종목 추세 조회
                    logger.info("전체 종목 추세 분석")
                    for market in self.markets:
                        if market not in self.analyzers:
                            continue
                        analyzer = self.analyzers[market]
                        analyzer.analyze_macro()
                        res = analyzer.macro_result or {}
                        trend = analyzer.macro_trend
                        m5_change = res.get('m5_change', 0) * 100
                        m15_change = res.get('m15_change', 0) * 100
                        logger.info(f"   [{market:<11}] {trend:<7} | 5m:{m5_change:+6.2f}% 15m:{m15_change:+6.2f}%")
                return
            
            if cmd == '/price':
                if len(parts) >= 2:
                    # 특정 종목 가격 조회
                    coin = parts[1].upper().replace('KRW-', '')
                    market = f"KRW-{coin}"
                    if market in self.current_prices:
                        logger.info(f"{market}: {self.current_prices[market]:,.0f}원")
                    else:
                        logger.warning(f"가격 데이터 없음: {market}")
                else:
                    # 전체 종목 가격 정보 조회
                    logger.info("전체 종목 가격 정보")
                    for market in self.markets:
                        if market not in self.current_prices or market not in self.analyzers:
                            continue
                        
                        price = self.current_prices[market]
                        analyzer = self.analyzers[market]
                        
                        # 1분봉 변화율
                        m1_change_display = 0.0
                        if len(analyzer.minute_candles) >= 2:
                            m1_start = analyzer.minute_candles[-2]['trade_price']
                            m1_curr = analyzer.minute_candles[-1]['trade_price']
                            m1_change_display = (m1_curr - m1_start) / m1_start * 100
                        
                        # 5분봉 변화율
                        m5_change_display = 0.0
                        if len(analyzer.minute5_candles) >= 2:
                            m5_start = analyzer.minute5_candles[-2]['trade_price']
                            m5_curr = analyzer.minute5_candles[-1]['trade_price']
                            m5_change_display = (m5_curr - m5_start) / m5_start * 100
                        
                        # 15분봉 변화율
                        m15_change_display = 0.0
                        if len(analyzer.minute15_candles) >= 2:
                            m15_start = analyzer.minute15_candles[-2]['trade_price']
                            m15_curr = analyzer.minute15_candles[-1]['trade_price']
                            m15_change_display = (m15_curr - m15_start) / m15_start * 100
                        
                        # 시장 심리 분석
                        sentiment = analyzer.analyze_market_sentiment()
                        sentiment_text = sentiment['sentiment']
                        rsi = sentiment.get('rsi', 50)
                        fatigue = sentiment.get('fatigue', 0)
                        
                        # 매수/매도 비율
                        total_vol = analyzer.bid_volume_1m + analyzer.ask_volume_1m
                        buy_ratio = analyzer.bid_volume_1m / total_vol * 100 if total_vol > 0 else 50
                        
                        # 변화율 표시 (상승: +, 하락: -)
                        if market == self.markets[0]:
                            logger.info("------------------------------------")
                        logger.info(f"[{market:<11}] {price:>11,.0f}원 | "
                                  f"1m:{m1_change_display:>6.2f}% "
                                  f"5m:{m5_change_display:>6.2f}% "
                                  f"15m:{m15_change_display:>6.2f}% | "
                                  f"RSI:{rsi:>3.0f} 피로:{fatigue:>3.0f} | "
                                  f"매수:{buy_ratio:>3.0f}% | {sentiment_text:<7}")
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

    async def _main_logic(self):
        """봇 초기화 및 메인 로직 실행"""
        try:
            # 초기 데이터 로딩은 백그라운드에서 비동기로 처리
            logger.info("봇 초기화 및 데이터 로딩 중...")
            
            # 명령 처리 루프는 즉시 시작 (초기화 중에도 명령 입력 가능)
            self.running = True
            cmd_task = asyncio.create_task(self._check_commands())
            self.background_tasks.add(cmd_task)
            cmd_task.add_done_callback(self.background_tasks.discard)
            
            # 초기화 작업을 백그라운드 태스크로 실행 (블로킹하지 않음)
            init_task = asyncio.create_task(self._initialize_background())
            self.background_tasks.add(init_task)
            init_task.add_done_callback(self.background_tasks.discard)
            
            # 초기화가 완료될 때까지 대기하지 않고, 초기화 완료 후 메인 루프 시작
            async def start_main_loops_after_init():
                # 초기화 완료 대기
                await init_task
                
                if not self.running:
                    return
                
                # 초기화 완료 후 메인 루프 시작
                tasks = [
                    asyncio.create_task(self._public_ws_monitor()),
                    asyncio.create_task(self._private_ws_monitor()),
                    asyncio.create_task(self._trading_loop()),
                    asyncio.create_task(self._macro_update_loop()),
                    asyncio.create_task(self._balance_report_loop()),
                    asyncio.create_task(self._market_update_loop()),
                    asyncio.create_task(self._btc_monitor_loop())
                ]
                
                # 태스크 추적
                for task in tasks:
                    self.background_tasks.add(task)
                    task.add_done_callback(self.background_tasks.discard)
                
                # 모든 태스크 완료 대기
                await asyncio.gather(*tasks, return_exceptions=True)
            
            # 메인 루프 시작 태스크
            main_loops_task = asyncio.create_task(start_main_loops_after_init())
            self.background_tasks.add(main_loops_task)
            main_loops_task.add_done_callback(self.background_tasks.discard)
            
            # 명령 처리 루프가 계속 실행되도록 대기 (무한 루프이므로 계속 실행됨)
            # running이 False가 될 때까지 계속 실행
            try:
                await cmd_task
            except asyncio.CancelledError:
                pass
        except Exception as e:
            logger.error(f"메인 로직 오류: {e}")
            self.running = False
    
    async def _initialize_background(self):
        """백그라운드에서 초기화 작업 수행"""
        try:
            # 초기 데이터 로딩 (각 단계 사이에 이벤트 루프가 다른 작업을 처리할 수 있도록 함)
            await asyncio.sleep(0)  # TUI가 렌더링될 수 있도록 양보
            await self._update_top_markets()
            
            if not self.markets:
                logger.error("거래 가능한 마켓이 없습니다. 종료합니다.")
                self.running = False
                return

            await asyncio.sleep(0)  # 이벤트 루프 양보
            logger.info(f"   타겟 마켓: {len(self.markets)}개 종목")
            
            # BTC 추세 확인 및 잔고 동기화
            await asyncio.sleep(0)  # 이벤트 루프 양보
            await self._check_btc_trend()
            await asyncio.sleep(0)  # 이벤트 루프 양보
            self._check_balance()
            await asyncio.sleep(0)  # 이벤트 루프 양보
            self._sync_state_with_balance()
            
            logger.info("초기화 완료")
        except Exception as e:
            logger.error(f"초기화 중 오류: {e}")

    async def start(self):
        """트레이딩 봇 시작"""
        logger.info("=" * 60)
        logger.info("모멘텀 트레이딩 봇 시작 (TUI Version)")
        
        # TUI를 먼저 시작 (TUI 내부에서 초기화가 시작됨)
        try:
            # TUI 실행 (블로킹, TUI 내부에서 초기화 시작)
            await self.run_tui()
        finally:
            # TUI 종료 시 모든 태스크 정리
            self.running = False
            for task in self.background_tasks:
                if not task.done():
                    task.cancel()
            
            # 태스크 정리 대기
            if self.background_tasks:
                await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
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
                logger.info(f"[{BTC_MARKET}] BTC 추세: {self.btc_trend} ({btc_change*100:+.2f}%)")
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
                logger.info(f"KRW 잔고: {self.assets['KRW']['balance']:,.0f}원")
            
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
                logger.info(f"{currency} | 보유:{balance:.4f} | 평단:{avg:,.0f} | 현재:{current:,.0f} | 수익:{pnl:+.2f}%")
                
            logger.info(f"총 자산 추정: {self.assets.get('KRW', {}).get('balance', 0) + total_valuation:,.0f}원")
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
                    logger.info("Public WebSocket 연결됨")
                    
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
                    logger.info("Private WebSocket 연결됨")
                    
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
                
                # 로그 출력 (15초마다)
                if time.time() - last_status_log >= 15:
                    last_status_log = time.time()
                    for market in self.markets:
                        if market not in self.current_prices or market not in self.analyzers:
                            continue
                        
                        price = self.current_prices[market]
                        analyzer = self.analyzers[market]
                        
                        # 1분봉 변화율
                        m1_change_display = 0.0
                        if len(analyzer.minute_candles) >= 2:
                            m1_start = analyzer.minute_candles[-2]['trade_price']
                            m1_curr = analyzer.minute_candles[-1]['trade_price']
                            m1_change_display = (m1_curr - m1_start) / m1_start * 100
                        
                        # 5분봉 변화율
                        m5_change_display = 0.0
                        if len(analyzer.minute5_candles) >= 2:
                            m5_start = analyzer.minute5_candles[-2]['trade_price']
                            m5_curr = analyzer.minute5_candles[-1]['trade_price']
                            m5_change_display = (m5_curr - m5_start) / m5_start * 100
                        
                        # 15분봉 변화율
                        m15_change_display = 0.0
                        if len(analyzer.minute15_candles) >= 2:
                            m15_start = analyzer.minute15_candles[-2]['trade_price']
                            m15_curr = analyzer.minute15_candles[-1]['trade_price']
                            m15_change_display = (m15_curr - m15_start) / m15_start * 100
                        
                        # 시장 심리 분석
                        sentiment = analyzer.analyze_market_sentiment()
                        sentiment_text = sentiment['sentiment']
                        rsi = sentiment.get('rsi', 50)
                        fatigue = sentiment.get('fatigue', 0)
                        
                        # 매수/매도 비율
                        total_vol = analyzer.bid_volume_1m + analyzer.ask_volume_1m
                        buy_ratio = analyzer.bid_volume_1m / total_vol * 100 if total_vol > 0 else 50
                        
                        # 변화율 표시 (상승: +, 하락: -)
                        if market == self.markets[0]:
                            logger.info("------------------------------------")
                        logger.info(f"[{market:<11}] {price:>11,.0f}원 | "
                                  f"1m:{m1_change_display:>6.2f}% "
                                  f"5m:{m5_change_display:>6.2f}% "
                                  f"15m:{m15_change_display:>6.2f}% | "
                                  f"RSI:{rsi:>3.0f} 피로:{fatigue:>3.0f} | "
                                  f"매수:{buy_ratio:>3.0f}% | {sentiment_text:<7}")
                
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
                logger.info(f"[{market}] [테스트] 매수: {invest_amount:,.0f}원")
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
                logger.info(f"[{market}] [테스트] 매도: {reason}")
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
        logger.info(f"최종 수익: {self.cumulative_profit:,.0f}원")
