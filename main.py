import asyncio
import sys
import logging
from trader import MomentumTrader

# 윈도우 환경에서 asyncio 루프 정책 설정 (필요시)
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def main():
    trader = MomentumTrader()
    await trader.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n프로그램 종료")
    except Exception as e:
        print(f"프로그램 오류: {e}")
