import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# =================================================================================
# 📊 전략 파라미터 (Strategy Parameters)
# =================================================================================

# === 투자 설정 ===
MAX_INVESTMENT = 1000000            # 최대 투자금 (원) - 1천만원으로 상향
MIN_ORDER_AMOUNT = 5_000            # 최소 주문 금액 (업비트 최소금액 5,000원 + 버퍼)

# === BTC 중심 시장 분석 (BTC-Centric Market Analysis) ===
BTC_MARKET = "KRW-BTC"              # 비트코인 마켓 (시장 중심 지표)
BTC_TREND_THRESHOLD = -0.005        # BTC 하락 임계값 (-0.5% 이하면 시장 위험)
BTC_BULLISH_THRESHOLD = 0.003       # BTC 상승 임계값 (+0.3% 이상이면 시장 안정)
BTC_CHECK_INTERVAL = 60             # BTC 추세 체크 주기 (초)
BTC_DOWNTREND_BUY_BLOCK = True      # BTC 하락 시 매수 금지 (True: 적용)

# === 거시적 분석 (Macro Analysis) - 전체 시장 추세 ===
MACRO_LOOKBACK_DAYS = 7             # 일봉 분석 기간 (일)
MACRO_MIN_CHANGE_RATE = -0.015      # 전체 하락장 판단 기준 (-1.5% 이하면 관망) - 강화
MACRO_BULLISH_THRESHOLD = 0.015     # 상승장 판단 기준 (+1.5% 이상) - 강화
MACRO_UPDATE_INTERVAL = 60          # 거시 분석 갱신 주기 (초) - 1분마다

# === 미시적 분석 (Micro Analysis) - 진입 신호 (대폭 강화) ===
MOMENTUM_WINDOW = 20                # 모멘텀 계산 윈도우 (캔들 개수) - 20분으로 확대
MOMENTUM_THRESHOLD = 0.015          # 진입 모멘텀 기준 (1.5% 상승률) - 상향 조정
MIN_SIGNAL_STRENGTH = 75            # 최소 진입 강도 (75점 이상) - 강화
VOLUME_SPIKE_RATIO = 3.0            # 거래량 급등 배율 (평균 대비 3배) - 수급 확인 강화
CONSECUTIVE_UP_CANDLES = 6          # 연속 상승 캔들 개수 - 6개로 강화

# === 초봉 분석 (Second Candle Analysis) - 실시간 변화 감지 ===
SECOND_CANDLE_UNIT = 5              # 초봉 단위 (1, 3, 5, 10, 30, 60 중 선택)
SECOND_MOMENTUM_WINDOW = 15         # 초봉 모멘텀 윈도우 (개수) - 확대
SECOND_MOMENTUM_THRESHOLD = 0.002   # 초봉 모멘텀 기준 (0.2%) - 강화
SECOND_RAPID_RISE_THRESHOLD = 0.006 # 급등 판단 기준 (0.6%/5초) - 노이즈 제거 강화

# === 단타 전문가 기법 (Pro Scalping) 파라미터 ===
SHORT_TREND_WINDOW = 20             # 단기 추세 확인 (20분) - 확대
SHORT_MOMENTUM_THRESHOLD = 0.015    # 단기 급반등 기준 (1.5% 이상) - 상향 조정 (노이즈 제거)
VOL_INTENSITY_THRESHOLD = 2.5       # 수급 집중도 (평균 대비 2.5배 이상)
BREAKOUT_VELOCITY = 0.0015          # 분당 가격 가속도 (0.15%/min) - 강화

# === 다중 타임프레임 분석 (Multi-Timeframe Analysis) - 핵심 개선 ===
MTF_ENABLED = True                  # 다중 타임프레임 분석 활성화
MTF_5M_MIN_CANDLES = 24             # 5분봉 최소 필요 개수 (24개 = 2시간)
MTF_15M_MIN_CANDLES = 12            # 15분봉 최소 필요 개수 (12개 = 3시간)
MTF_5M_TREND_THRESHOLD = 0.002      # 5분봉 상승 추세 기준 (0.2%)
MTF_15M_TREND_THRESHOLD = 0.002     # 15분봉 상승 기준 (0.2% - 상향)
MTF_5M_EARLY_STAGE_MAX = 0.02       # 5분봉 상승 초기 단계 최대치 (1.5% 이하여야 초기) - 2.5%에서 강화
MTF_MAX_1M_CHANGE = 0.03            # 1분봉 급등 제한 (3% 이상 급등 시 진입 차단)
MTF_VOLUME_CONFIRMATION = 1.5       # 5분봉 거래량 확인 배율 (평균 대비)
MTF_STRICT_MODE = True              # 엄격 모드 (15분봉 하락 시 무조건 차단)

# === V3.5 / V3.3 추가 파라미터 (BTC & Macro) ===
H4_BEARISH_THRESHOLD = -0.005       # 4시간봉 하락장 기준 (-0.5%) - 이보다 낮으면 절대 진입 금지

# === 장기 추세 필터 (Long-Term Trend Filter) - v3.2 신규 ===
LONG_TERM_FILTER_ENABLED = True     # 장기 추세 필터 활성화 (핵심!)
DAILY_BEARISH_THRESHOLD = -0.02     # 일봉 하락 임계값 (-2% 이하면 하락장)
DAILY_BEARISH_BLOCK = True          # 일봉 하락 시 무조건 진입 차단
H4_BEARISH_BLOCK = True             # 4시간봉 하락 시 진입 차단
IGNORE_SHORT_SQUEEZE_IN_DOWNTREND = True  # 하락장에서 Short Squeeze 신호 무시

# === 장기하락 예외 처리 (v3.3 신규) ===
STRONG_SHORT_MOMENTUM_5M_THRESHOLD = 0.015    # 단기 급등 예외: 5분봉 임계값 (+1.5%)
STRONG_SHORT_MOMENTUM_H4_MIN = 0.0            # 단기 급등 예외: 4시간봉 최소 (0% 이상)
STRONG_MOMENTUM_BUY_PRESSURE_MIN = 0.55       # 단기 급등 예외: 매수 우위 최소 (55% 이상)
STRONG_MOMENTUM_FATIGUE_MAX = 40              # 단기 급등 예외: 피로도 최대 (40 이하)
STRONG_MOMENTUM_1M_CONSISTENCY_MIN = 3        # 1분봉 일관성: 최근 5개 중 양수 최소 개수

# === V자 반등 및 안정성 체크 (v3.4 신규) ===
V_REVERSAL_ENABLED = True                     # V자 반등 감지 활성화
V_REVERSAL_MIN_DROP = -0.003                  # V자 반등: 최소 하락폭 (-0.3%)
V_REVERSAL_MIN_RISE = 0.002                   # V자 반등: 최소 반등폭 (+0.2%)
VOLATILITY_MAX_STDDEV = 0.008                 # 변동성 최대값: 1분봉 표준편차 0.8% 이하
MARKET_SYNC_MIN_COUNT = 12                    # 동반 상승: 최소 N개 종목 동반 상승
MARKET_SYNC_THRESHOLD = 0.002                 # 동반 상승: 종목별 최소 상승률

# === 분석 주기 ===
ANALYSIS_INTERVAL = 10                        # 분석 주기 (10초)

# === 익절/손절 설정 (핵심 개선) ===
INITIAL_STOP_LOSS = 0.020           # 초기 손절선 (2.0%)
DYNAMIC_STOP_LOSS_ENABLED = True    # 동적 손절선 활성화
DYNAMIC_STOP_LOSS_MIN = 0.015       # 동적 손절 최소 (1.5%)
DYNAMIC_STOP_LOSS_MAX = 0.025       # 동적 손절 최대 (2.5%)
TRAILING_STOP_ACTIVATION = 0.008    # 트레일링 스탑 활성화 기준 (+0.8% 수익 시)
TRAILING_STOP_DISTANCE = 0.004      # 트레일링 스탑 거리 (0.4% - 고점 대비)
TRAILING_MIN_PROFIT = 0.003         # 트레일링 시 최소 수익 보장 (0.3%)
BREAK_EVEN_TRIGGER = 0.006          # 본절 스탑 발동 (+0.6% 도달 시 손절가=매수가)
TAKE_PROFIT_TARGET = 0.025          # 목표 수익률 (2.5%)
MAX_HOLDING_TIME = 21600            # 최대 보유 시간 (초, 6시간)

# === 리스크 관리 (강화) ===
MAX_TRADES_PER_HOUR = 20            # 시간당 최대 거래 횟수
COOL_DOWN_AFTER_LOSS = 600          # 손절 후 대기 시간 (초)
CONSECUTIVE_LOSS_COOLDOWN = 1200    # 연속 손절 시 추가 대기 (20분)
MIN_PRICE_STABILITY = 0.008         # 최소 가격 안정성 (급등락 필터) - 강화

# === 시스템 설정 ===
MARKET = [] 
MARKET_UPDATE_INTERVAL = 600        # 마켓 목록 갱신 주기
TOP_MARKET_COUNT = 20               # 거래대금 상위 20개 선정
CANDLE_UNIT = 1                     # 분봉 단위
LOG_LEVEL = logging.INFO            # 로그 레벨
DRY_RUN = True                      # 테스트 모드
USE_SECOND_CANDLES = True           # 초봉 사용 여부
BALANCE_REPORT_INTERVAL = 60        # 잔고 리포트 주기 (1분)

# === 거래 기록 설정 ===
TRADE_LOG_FILE = "logs/trades.csv"  # 거래 기록 파일 경로

# API 키 설정
ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY")
SECRET_KEY = os.getenv("UPBIT_SECRET_KEY")

# API 엔드포인트
REST_BASE_URL = "https://api.upbit.com/v1"
WS_PUBLIC_URL = "wss://api.upbit.com/websocket/v1"
WS_PRIVATE_URL = "wss://api.upbit.com/websocket/v1/private"

# 기타
TRADING_FEE_RATE = 0.0005 # 0.05%
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

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


def setup_logging():
    # 로깅 설정
    logging.basicConfig(
        level=LOG_LEVEL,
        format='%(asctime)s | %(levelname)-7s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # 로그 파일 핸들러 추가
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = f"{log_dir}/trading_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-7s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    logging.getLogger().addHandler(file_handler)
    
    # 억제할 라이브러리 로그
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.INFO)

    return logging.getLogger(__name__)

logger = setup_logging()
