"""
Kafka Consumer - 실시간 이벤트 수집

- Kafka 클러스터에서 사용자 이벤트 수신
- MySQL 배치 저장
- 수동 오프셋 커밋 (이벤트 손실 방지)
- 우아한 종료 (Graceful Shutdown)
- 에러 핸들링 및 재시도 로직

실행 예시:
  python src/kafka/consumer.py
  python src/kafka/consumer.py --batch-size 50 --batch-timeout 10 --log-level DEBUG
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError


def _ensure_project_root_on_syspath() -> None:
    """
    `python src/kafka/consumer.py` 형태로 실행해도 프로젝트 루트 import가 되도록 보정합니다.
    """
    # 현재 파일: <root>/src/kafka/consumer.py
    project_root = Path(__file__).resolve().parents[2]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))


_ensure_project_root_on_syspath()

try:
    from config import get_kafka_config
    from src.storage.mysql_client import MySQLClient
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure you run from project root or install required packages")
    sys.exit(1)


logger = logging.getLogger(__name__)


class KafkaEventConsumer:
    """
    Kafka 이벤트 소비자
    
    주요 기능:
    - Kafka 토픽에서 이벤트 실시간 수신
    - JSON 파싱 및 유효성 검증
    - MySQL 배치 저장 (100개 단위)
    - 수동 오프셋 커밋
    - 우아한 종료
    """
    
    def __init__(
        self,
        batch_size: int = 100,
        batch_timeout: int = 5,
    ) -> None:
        """
        생성자
        
        Args:
            batch_size: 배치 저장 단위 (기본 100개)
            batch_timeout: 배치 타임아웃 초 (기본 5초)
        """
        self.config = get_kafka_config()
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        
        # 클라이언트 초기화
        self.consumer: Optional[KafkaConsumer] = None
        self.mysql_client = MySQLClient()
        
        # 배치 버퍼
        self.batch_buffer: List[Dict[str, Any]] = []
        self.last_flush_time = time.time()
        
        # 상태 관리
        self.running = False
        self.graceful_shutdown = False
        
        # 통계
        self.total_consumed = 0
        self.total_saved = 0
        self.total_errors = 0
        self.start_time: Optional[datetime] = None
        
        # Dead Letter Queue 로그
        self.dlq_path = Path("logs") / "dead_letter_queue.log"
        self.dlq_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 시그널 핸들러 등록
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(
            "KafkaEventConsumer initialized: batch_size=%d, batch_timeout=%d",
            batch_size,
            batch_timeout,
        )
    
    def connect(self) -> None:
        """Kafka Consumer 및 외부 서비스 연결"""
        logger.info("Connecting to Kafka cluster...")
        
        try:
            # Kafka Consumer 생성
            self.consumer = KafkaConsumer(
                self.config.topic,
                bootstrap_servers=self.config.get_bootstrap_servers_list(),
                group_id=self.config.consumer_group,
                auto_offset_reset=self.config.auto_offset_reset,
                enable_auto_commit=self.config.enable_auto_commit,
                value_deserializer=lambda m: m.decode("utf-8"),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                # consumer_timeout_ms 제거 - 무한 대기 (파티션 할당 완료 후 메시지 읽기)
                # 배치 플러시는 should_flush()에서 타임아웃 체크
                session_timeout_ms=30000,  # 30초
                heartbeat_interval_ms=10000,  # 10초
                max_poll_records=500,  # 한 번에 최대 500개
            )
            
            logger.info(
                "Kafka Consumer connected: topic=%s, group=%s, bootstrap_servers=%s",
                self.config.topic,
                self.config.consumer_group,
                self.config.bootstrap_servers,
            )
            
            # MySQL 연결
            self.mysql_client.connect()
            logger.info("MySQL connected")
            
        except KafkaError as e:
            logger.error("Kafka connection failed: %s", e)
            raise
        except Exception as e:
            logger.error("Connection failed: %s", e)
            raise
    
    def consume_events(self) -> None:
        """
        이벤트 소비 메인 루프
        
        - Kafka에서 이벤트 폴링
        - 파싱 및 검증
        - 배치 버퍼에 추가
        - 조건 만족 시 MySQL 저장
        - 오프셋 커밋
        """
        if self.consumer is None:
            raise RuntimeError("Consumer not connected. Call connect() first.")
        
        self.running = True
        self.start_time = datetime.now(timezone.utc)
        
        logger.info("Starting event consumption loop...")
        logger.info("Press Ctrl+C to stop gracefully")
        
        try:
            for message in self.consumer:
                # 종료 시그널 확인
                if self.graceful_shutdown:
                    logger.info("Graceful shutdown initiated, stopping consumption...")
                    break
                
                # 메시지 처리
                try:
                    event = self.parse_event(message)
                    if event is None:
                        continue
                    
                    if not self.validate_event(event):
                        continue
                    
                    self.process_event(event)
                    self.total_consumed += 1
                    
                    # 주기적으로 통계 출력 (1000개마다)
                    if self.total_consumed % 1000 == 0:
                        self._log_statistics()
                    
                except Exception as e:
                    self.total_errors += 1
                    logger.error("Error processing message: %s", e, exc_info=True)
                    self._write_to_dlq(message, str(e))
                
                # 배치 플러시 체크
                if self.should_flush():
                    self.flush_batch()
                    self.commit_offset()
            
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received, stopping...")
        except Exception as e:
            logger.error("Fatal error in consumption loop: %s", e, exc_info=True)
            raise
        finally:
            # 최종 배치 플러시
            if self.batch_buffer:
                logger.info("Flushing final batch...")
                self.flush_batch()
                self.commit_offset()
            
            self.running = False
            self._log_final_statistics()
    
    def parse_event(self, message) -> Optional[Dict[str, Any]]:
        """
        Kafka 메시지를 JSON 파싱
        
        Args:
            message: Kafka ConsumerRecord
            
        Returns:
            Optional[Dict[str, Any]]: 파싱된 이벤트 또는 None (실패 시)
        """
        try:
            event = json.loads(message.value)
            return event
            
        except json.JSONDecodeError as e:
            logger.warning("JSON parsing failed: %s, raw=%s", e, message.value[:100])
            self._write_to_dlq(message, f"JSON parsing error: {e}")
            return None
        except Exception as e:
            logger.error("Unexpected error during parsing: %s", e)
            return None
    
    def validate_event(self, event: Dict[str, Any]) -> bool:
        """
        이벤트 유효성 검증
        
        필수 필드:
        - event_id
        - timestamp
        - user_id
        - session_id
        - event_type
        - content_id
        
        Args:
            event: 검증할 이벤트
            
        Returns:
            bool: 유효성 여부
        """
        required_fields = [
            "event_id",
            "timestamp",
            "user_id",
            "session_id",
            "event_type",
            "content_id",
        ]
        
        for field in required_fields:
            if field not in event or not event[field]:
                logger.warning("Event missing required field: %s", field)
                return False
        
        # event_type 검증
        valid_event_types = {"click", "watch", "watchlist", "watch_complete", "rating"}
        if event["event_type"] not in valid_event_types:
            logger.warning("Invalid event_type: %s", event["event_type"])
            return False
        
        return True
    
    def process_event(self, event: Dict[str, Any]) -> None:
        """
        이벤트 처리 - 배치 버퍼 추가
        
        Args:
            event: 처리할 이벤트
        """
        # 배치 버퍼에 추가
        self.add_to_batch(event)
    
    def add_to_batch(self, event: Dict[str, Any]) -> None:
        """
        배치 버퍼에 이벤트 추가
        
        Args:
            event: 추가할 이벤트
        """
        self.batch_buffer.append(event)
        logger.debug("Event added to batch: buffer_size=%d", len(self.batch_buffer))
    
    def should_flush(self) -> bool:
        """
        배치 플러시 필요 여부 판단
        
        조건:
        1. 배치 크기 도달 (batch_size)
        2. 타임아웃 경과 (batch_timeout)
        
        Returns:
            bool: 플러시 필요 여부
        """
        if len(self.batch_buffer) >= self.batch_size:
            logger.debug("Batch size reached: %d", len(self.batch_buffer))
            return True
        
        elapsed = time.time() - self.last_flush_time
        if elapsed >= self.batch_timeout and self.batch_buffer:
            logger.debug("Batch timeout reached: %.2fs", elapsed)
            return True
        
        return False
    
    def flush_batch(self) -> None:
        """
        배치 버퍼를 MySQL에 저장
        
        - 재시도 로직 포함 (최대 3회)
        - 실패 시 DLQ 기록
        """
        if not self.batch_buffer:
            return
        
        batch_size = len(self.batch_buffer)
        logger.info("Flushing batch: size=%d", batch_size)
        
        try:
            # MySQL 배치 저장
            saved_count = self._save_to_mysql_with_retry(self.batch_buffer)
            
            self.total_saved += saved_count
            logger.info("Batch saved successfully: %d events", saved_count)
            
            # 버퍼 초기화
            self.batch_buffer.clear()
            self.last_flush_time = time.time()
            
        except Exception as e:
            logger.error("Batch flush failed after retries: %s", e, exc_info=True)
            
            # DLQ에 기록
            for event in self.batch_buffer:
                self._write_event_to_dlq(event, f"MySQL save failed: {e}")
            
            # 버퍼 초기화 (손실 방지를 위해 오프셋은 커밋하지 않음)
            self.batch_buffer.clear()
            self.last_flush_time = time.time()
    
    def _save_to_mysql_with_retry(
        self,
        events: List[Dict[str, Any]],
        max_retries: int = 3,
    ) -> int:
        """
        MySQL 저장 재시도 로직 (Exponential Backoff)
        
        Args:
            events: 저장할 이벤트 리스트
            max_retries: 최대 재시도 횟수
            
        Returns:
            int: 저장 성공한 이벤트 수
            
        Raises:
            Exception: 최대 재시도 후에도 실패 시
        """
        for attempt in range(max_retries):
            try:
                # user_events 테이블에 저장
                saved_count = self.mysql_client.batch_insert_user_events(events)
                return saved_count
                
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                
                delay = 2 ** attempt  # 1, 2, 4초
                logger.warning(
                    "MySQL save failed (attempt %d/%d), retrying in %ds: %s",
                    attempt + 1,
                    max_retries,
                    delay,
                    e,
                )
                time.sleep(delay)
        
        return 0
    
    def commit_offset(self) -> None:
        """
        Kafka 오프셋 수동 커밋
        
        - 배치 저장 성공 후에만 커밋
        - 실패 시 경고 로그 (자동으로 재시도됨)
        """
        if self.consumer is None:
            return
        
        try:
            self.consumer.commit()
            logger.debug("Offset committed successfully")
            
        except KafkaError as e:
            logger.warning("Offset commit failed: %s", e)
        except Exception as e:
            logger.error("Unexpected error during offset commit: %s", e)
    
    def _signal_handler(self, signum, frame) -> None:
        """
        시그널 핸들러 - 우아한 종료
        
        Args:
            signum: 시그널 번호
            frame: 프레임 객체
        """
        logger.info("Signal received: %d, initiating graceful shutdown...", signum)
        self.graceful_shutdown = True
    
    def _write_to_dlq(self, message, error_msg: str) -> None:
        """
        Dead Letter Queue에 원본 메시지 기록
        
        Args:
            message: Kafka ConsumerRecord
            error_msg: 에러 메시지
        """
        try:
            dlq_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error_type": "MessageProcessingError",
                "error_message": error_msg,
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "key": message.key,
                "raw_value": message.value[:500] if message.value else "",  # 최대 500자
            }
            
            with self.dlq_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(dlq_entry, ensure_ascii=False) + "\n")
                
        except Exception as e:
            logger.error("Failed to write to DLQ: %s", e)
    
    def _write_event_to_dlq(self, event: Dict[str, Any], error_msg: str) -> None:
        """
        Dead Letter Queue에 이벤트 기록
        
        Args:
            event: 이벤트 딕셔너리
            error_msg: 에러 메시지
        """
        try:
            dlq_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error_type": "MySQLSaveError",
                "error_message": error_msg,
                "event": event,
            }
            
            with self.dlq_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(dlq_entry, ensure_ascii=False) + "\n")
                
        except Exception as e:
            logger.error("Failed to write event to DLQ: %s", e)
    
    def _log_statistics(self) -> None:
        """통계 로그 출력"""
        elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds() if self.start_time else 0
        rate = self.total_consumed / elapsed if elapsed > 0 else 0
        
        logger.info(
            "Statistics: consumed=%d, saved=%d, errors=%d, rate=%.2f/s",
            self.total_consumed,
            self.total_saved,
            self.total_errors,
            rate,
        )
    
    def _log_final_statistics(self) -> None:
        """최종 통계 로그 출력"""
        elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds() if self.start_time else 0
        rate = self.total_consumed / elapsed if elapsed > 0 else 0
        
        logger.info("=" * 80)
        logger.info("Consumer stopped - Final Statistics:")
        logger.info("  Total consumed:    %d events", self.total_consumed)
        logger.info("  Total saved:       %d events", self.total_saved)
        logger.info("  Total errors:      %d events", self.total_errors)
        logger.info("  Elapsed time:      %.2f seconds", elapsed)
        logger.info("  Average rate:      %.2f events/s", rate)
        logger.info("=" * 80)
    
    def close(self) -> None:
        """리소스 정리 및 연결 종료"""
        logger.info("Closing consumer and cleaning up resources...")
        
        try:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed")
        except Exception as e:
            logger.error("Error closing Kafka consumer: %s", e)
        
        try:
            self.mysql_client.close()
            logger.info("MySQL client closed")
        except Exception as e:
            logger.error("Error closing MySQL client: %s", e)


def _configure_logging(level: str) -> None:
    """
    로깅 설정
    
    Args:
        level: 로그 레벨 (DEBUG, INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def build_arg_parser() -> argparse.ArgumentParser:
    """CLI 인자 파서 생성"""
    parser = argparse.ArgumentParser(
        description="Kafka Consumer - 실시간 이벤트 수집",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="배치 저장 단위 (개수)",
    )
    parser.add_argument(
        "--batch-timeout",
        type=int,
        default=5,
        help="배치 타임아웃 (초)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="로그 레벨",
    )
    
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """
    메인 실행 함수
    
    Args:
        argv: 명령줄 인자 (테스트용)
        
    Returns:
        int: 종료 코드 (0: 성공, 1: 실패)
    """
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    
    _configure_logging(args.log_level)
    
    logger.info("=" * 80)
    logger.info("Kafka Event Consumer - Starting...")
    logger.info("=" * 80)
    
    consumer = KafkaEventConsumer(
        batch_size=args.batch_size,
        batch_timeout=args.batch_timeout,
    )
    
    try:
        consumer.connect()
        consumer.consume_events()
        return 0
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 0
        
    except Exception as e:
        logger.error("Fatal error: %s", e, exc_info=True)
        return 1
        
    finally:
        consumer.close()


if __name__ == "__main__":
    raise SystemExit(main())

