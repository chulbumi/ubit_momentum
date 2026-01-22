from datetime import datetime, timedelta
from config import (
    INITIAL_STOP_LOSS, 
    MAX_TRADES_PER_HOUR, 
    COOL_DOWN_AFTER_LOSS, 
    CONSECUTIVE_LOSS_COOLDOWN
)

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
        self.processing_order = False     # 주문 처리 중 여부 (중복 주문 방지)
        
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
            
        # [중요] 매도(익절/손절) 후 최소 쿨타임 (5분) - 재진입 방지
        if self.last_trade_time:
            time_diff = (now - self.last_trade_time).total_seconds()
            if time_diff < 300:  # 5분 대기 (300초)
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
