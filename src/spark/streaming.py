"""
PySpark Streaming - Kafka 이벤트 실시간 처리 및 세션 추적

- Kafka 클러스터에서 사용자 이벤트 스트림 읽기
- JSON 파싱 및 스키마 검증
- 3분 윈도우 기반 세션 추적
- 세션 집계 및 MySQL 저장
- 체크포인트를 통한 장애 복구
- 3초 배치 간격으로 실시간 처리

실행 예시:
  python src/spark/streaming.py
  python src/spark/streaming.py --checkpoint-dir data/checkpoints/streaming/
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    collect_set,
    count,
    from_json,
    max,
    min,
    sum,
    to_json,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)


def _ensure_project_root_on_syspath() -> None:
    """
    `python src/spark/streaming.py` 형태로 실행해도 프로젝트 루트 import가 되도록 보정합니다.
    """
    # 현재 파일: <root>/src/spark/streaming.py
    project_root = Path(__file__).resolve().parents[2]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))


_ensure_project_root_on_syspath()

try:
    from config import get_kafka_config, get_mysql_config, get_spark_config
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure you run from project root or install required packages")
    sys.exit(1)


logger = logging.getLogger(__name__)


def get_user_event_schema() -> StructType:
    """
    UserEvent 스키마 정의 (task_template.md 및 producer.py 기준)
    
    Returns:
        StructType: UserEvent 스키마
    """
    return StructType(
        [
            StructField("event_id", StringType(), nullable=False),
            StructField("timestamp", StringType(), nullable=False),  # ISO 8601 문자열
            StructField("user_id", StringType(), nullable=False),
            StructField("session_id", StringType(), nullable=False),
            StructField("event_type", StringType(), nullable=False),
            StructField("content_id", StringType(), nullable=True),
            StructField("genre", StringType(), nullable=True),
            StructField("duration_minutes", IntegerType(), nullable=True),
            StructField("watched_minutes", IntegerType(), nullable=True),
            StructField(
                "metadata", MapType(StringType(), StringType()), nullable=True
            ),
        ]
    )


def create_spark_session(checkpoint_dir: str) -> SparkSession:
    """
    Spark 세션 생성
    
    Args:
        checkpoint_dir: 체크포인트 디렉토리 경로
        
    Returns:
        SparkSession: 초기화된 Spark 세션
    """
    config = get_spark_config()
    
    # 체크포인트 디렉토리 생성
    checkpoint_path = Path(checkpoint_dir)
    checkpoint_path.mkdir(parents=True, exist_ok=True)
    
    spark = (
        SparkSession.builder.appName(config.app_name)
        .master(config.master)
        .config("spark.executor.memory", config.executor_memory)
        .config("spark.driver.memory", config.driver_memory)
        .config(
            "spark.sql.streaming.checkpointLocation", str(checkpoint_path.absolute())
        )
        # Kafka 커넥터 및 MySQL JDBC 드라이버 패키지 추가 (Spark 3.4.1용)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,mysql:mysql-connector-java:8.0.33"
        )
        # Windows 환경의 Python Worker 연결 불안정 대응
        .config("spark.python.worker.reuse", "false")
        .config("spark.executorEnv.PYTHONHASHSEED", "0")
        .config("spark.python.use.daemon", "false")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "localhost")
        # 메모리 여유 확보 (기존 설정을 덮어써서 4g로 고정)
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        # 가상환경 Python 경로 명시
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel(config.log_level)
    
    logger.info(f"Spark 세션 생성 완료: appName={config.app_name}, master={config.master}")
    logger.info("Python Worker 설정: reuse=false, daemon=false, PYTHONHASHSEED=0")
    logger.info(f"Python 경로: {sys.executable}")
    logger.info(f"체크포인트 위치: {checkpoint_path.absolute()}")
    
    return spark


def create_kafka_stream(
    spark: SparkSession, 
    topic: str, 
    checkpoint_dir: str,
    starting_offsets: Optional[str] = None
) -> object:
    """
    Kafka 스트림 생성
    
    Args:
        spark: Spark 세션
        topic: Kafka 토픽 이름
        checkpoint_dir: 체크포인트 디렉토리 경로
        starting_offsets: 시작 오프셋 ("latest" 또는 "earliest", None이면 자동 결정)
        
    Returns:
        DataFrame: Kafka 스트림 DataFrame
        
    Note:
        - 체크포인트가 없으면 'earliest'로 설정 (처음부터 모든 메시지 읽기)
        - 체크포인트가 있으면 'latest'로 설정 (체크포인트 이후 메시지만 읽기)
        - Spark Structured Streaming은 체크포인트를 통해 오프셋을 자동 관리합니다.
        - kafka.group.id를 명시적으로 설정하지 않아야 합니다.
    """
    kafka_config = get_kafka_config()
    # PySpark는 콤마로 구분된 문자열이 필요함
    bootstrap_servers = kafka_config.bootstrap_servers
    
    # 체크포인트 존재 여부 확인
    checkpoint_path = Path(checkpoint_dir)
    offsets_dir = checkpoint_path / "offsets"
    has_checkpoint = offsets_dir.exists() and any(offsets_dir.iterdir())
    
    # starting_offsets 자동 결정
    if starting_offsets is None:
        if has_checkpoint:
            starting_offsets = "latest"
            logger.info(f"체크포인트 발견: {checkpoint_dir} → startingOffsets='latest' (체크포인트 이후 메시지만 읽기)")
        else:
            starting_offsets = "earliest"
            logger.info(f"체크포인트 없음: {checkpoint_dir} → startingOffsets='earliest' (처음부터 모든 메시지 읽기)")
    else:
        logger.info(f"수동 설정: startingOffsets='{starting_offsets}'")
    
    logger.info(f"Kafka 스트림 생성: topic={topic}, bootstrap_servers={bootstrap_servers}, startingOffsets={starting_offsets}")
    logger.info("체크포인트를 통해 오프셋이 자동 관리됩니다 (Consumer Group 사용 안 함)")
    
    # Spark Structured Streaming은 체크포인트를 통해 오프셋을 관리하므로
    # kafka.group.id를 설정하지 않습니다.
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )
    
    return df


def parse_json_stream(df, schema: StructType) -> DataFrame:
    """
    JSON 파싱 및 스키마 적용
    
    Args:
        df: Kafka 스트림 DataFrame
        schema: UserEvent 스키마
        
    Returns:
        DataFrame: 파싱된 DataFrame
    """
    # value 컬럼을 JSON으로 파싱
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # 디버깅: 파싱 결과 확인 (스트리밍에서는 직접 확인 불가하므로 로그만)
    logger.info("JSON 파싱 완료: Kafka 메시지를 UserEvent 스키마로 변환")
    
    return parsed_df


def convert_timestamp(df: DataFrame) -> DataFrame:
    """
    ISO 8601 타임스탬프 문자열을 TimestampType으로 변환
    
    Args:
        df: 파싱된 DataFrame
        
    Returns:
        DataFrame: 타임스탬프 변환된 DataFrame
    """
    # ISO 8601 형식: "2026-01-06T04:55:51.579395Z" (마이크로초 포함)
    # Spark의 to_timestamp는 마이크로초를 자동으로 처리하므로 패턴에서 제외
    # 또는 "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'" 형식 사용 가능
    df_with_timestamp = df.withColumn(
        "timestamp_ts",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
    )
    
    return df_with_timestamp


def track_sessions(
    df: DataFrame, 
    window_duration: str = "3 minutes",
    watermark_duration: str = "10 minutes"
) -> DataFrame:
    """
    3분 윈도우 기반 세션 추적 및 집계
    
    Args:
        df: 타임스탬프 변환된 DataFrame
        window_duration: 윈도우 크기 (기본: "3 minutes", 테스트용: "30 seconds")
        watermark_duration: 워터마크 지연 시간 (기본: "10 minutes", 테스트용: "1 minute")
        
    Returns:
        DataFrame: 세션 집계된 DataFrame
    """
    # NULL 타임스탬프 필터링 (윈도우 집계에서 제외)
    # 스트리밍 DataFrame에서는 직접 count()를 호출할 수 없으므로 필터만 적용
    df_filtered = df.filter(col("timestamp_ts").isNotNull())
    
    # Watermark 설정 (지연 데이터 처리)
    # 워터마크가 "0 seconds"이면 워터마크를 설정하지 않음 (즉시 처리)
    if watermark_duration and watermark_duration != "0 seconds":
        df_with_watermark = df_filtered.withWatermark("timestamp_ts", watermark_duration)
        logger.info(f"워터마크 설정: {watermark_duration}")
    else:
        # 워터마크 없이 처리 (즉시 윈도우 완료)
        df_with_watermark = df_filtered
        logger.info("워터마크 없음: 즉시 처리 모드 (윈도우 완료 대기 없음)")
    
    # 윈도우로 그룹화 및 세션 집계
    windowed_df = df_with_watermark.groupBy(
        col("session_id"),
        col("user_id"),
        window(col("timestamp_ts"), window_duration).alias("time_window")
    )
    
    # 세션 집계
    aggregated_df = windowed_df.agg(
        count("*").alias("event_count"),
        sum("watched_minutes").alias("total_watched_minutes"),
        collect_set(
            when(col("event_type") == "click", col("content_id"))
        ).alias("browsed_contents"),
        collect_set(
            when(col("event_type") == "watch", col("content_id"))
        ).alias("watched_contents"),
        collect_set(
            when(col("event_type") == "watch_complete", col("content_id"))
        ).alias("completed_contents"),
        min("timestamp_ts").alias("start_time"),
        max("timestamp_ts").alias("end_time")
    )
    
    # NULL 값 제거 및 컬럼 선택
    result_df = aggregated_df.select(
        col("session_id"),
        col("user_id"),
        col("start_time"),
        col("end_time"),
        col("event_count"),
        col("total_watched_minutes"),
        col("browsed_contents"),
        col("watched_contents"),
        col("completed_contents")
    )
    
    return result_df


def save_sessions_to_mysql(batch_df: DataFrame, batch_id: int) -> None:
    """
    배치별 MySQL 저장 (foreachBatch 콜백)
    
    Args:
        batch_df: 배치 DataFrame
        batch_id: 배치 ID
    """
    start_time = time.time()
    
    logger.info(f"배치 {batch_id}: save_sessions_to_mysql 호출됨")
    logger.info(f"배치 {batch_id}: 배치 DataFrame 스키마 = {batch_df.schema}")
    logger.info(f"배치 {batch_id}: 배치 DataFrame 컬럼 = {batch_df.columns}")
    
    # 배치 DataFrame 샘플 데이터 확인 (count()는 시간이 오래 걸리므로 생략)
    logger.info(f"배치 {batch_id}: 배치 DataFrame 샘플 데이터 확인 시작...")
    try:
        sample_data = batch_df.take(1)  # 1개만 확인 (빠름)
        if sample_data:
            row = sample_data[0]
            logger.info(f"배치 {batch_id}: 배치 DataFrame에 데이터가 있습니다!")
            logger.info(f"  샘플: session_id={row['session_id']}, user_id={row['user_id']}, "
                      f"event_count={row['event_count']}, total_watched_minutes={row['total_watched_minutes']}")
        else:
            logger.warning(f"배치 {batch_id}: 배치 DataFrame이 비어있습니다!")
            logger.warning(f"배치 {batch_id}: 윈도우 집계 결과가 없습니다. 이벤트가 읽히지 않았거나 윈도우가 완료되지 않았을 수 있습니다.")
            return  # 데이터가 없으면 저장하지 않음
    except Exception as e:
        logger.warning(f"배치 {batch_id}: 샘플 데이터 조회 실패: {e}", exc_info=True)
        # 샘플 조회 실패해도 저장은 시도
    
    try:
        mysql_config = get_mysql_config()
        
        # JDBC URL 및 Properties
        jdbc_url = f"jdbc:mysql://{mysql_config.host}:{mysql_config.port}/{mysql_config.database}"
        properties = {
            "user": mysql_config.user,
            "password": mysql_config.password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        
        logger.info(f"배치 {batch_id}: JSON 변환 시작...")
        
        # 배열을 JSON 문자열로 변환 (Spark SQL 함수 사용, Python Worker 불필요)
        # to_json()은 Spark SQL 함수이므로 Python Worker 없이 실행 가능
        logger.info(f"배치 {batch_id}: 배열 컬럼을 JSON 문자열로 변환 중...")
        
        # 배열 컬럼을 JSON 문자열로 변환
        # NULL 배열은 NULL 문자열로 유지
        sessions_df = batch_df.select(
            col("session_id"),
            col("user_id"),
            col("start_time"),
            col("end_time"),
            col("event_count"),
            col("total_watched_minutes"),
            # 배열을 JSON 문자열로 변환 (NULL 처리 포함)
            when(col("browsed_contents").isNull(), None)
            .otherwise(to_json(col("browsed_contents"))).alias("browsed_contents"),
            when(col("watched_contents").isNull(), None)
            .otherwise(to_json(col("watched_contents"))).alias("watched_contents"),
            when(col("completed_contents").isNull(), None)
            .otherwise(to_json(col("completed_contents"))).alias("completed_contents")
        )
        
        logger.info(f"배치 {batch_id}: JSON 변환 완료 (Spark SQL 함수 사용)")
        
        # 배치 저장
        # count()는 비용이 많이 들고 시간이 오래 걸릴 수 있으므로, 바로 저장 시도
        logger.info(f"배치 {batch_id}: MySQL 저장 시작 (count() 생략, 바로 저장 시도)...")
        logger.info(f"배치 {batch_id}: JDBC URL = {jdbc_url}")
        logger.info(f"배치 {batch_id}: 테이블 = user_sessions")
        
        # 저장 전 샘플 데이터 확인 제거 (Python worker 연결 문제 방지)
        # sessions_df에 UDF가 없으므로 take() 호출은 안전하지만, 
        # JDBC 저장 전에는 불필요하므로 제거
        logger.info(f"배치 {batch_id}: 저장할 DataFrame 스키마 = {sessions_df.schema}")
        
        # 바로 저장 시도 (count() 없이)
        logger.info(f"배치 {batch_id}: sessions_df.write.jdbc() 호출 시작...")
        logger.info(f"배치 {batch_id}: JDBC URL = {jdbc_url}")
        logger.info(f"배치 {batch_id}: 테이블 = user_sessions")
        logger.info(f"배치 {batch_id}: mode = append")
        
        try:
            # JDBC 저장은 동기적으로 실행됨 (완료될 때까지 대기)
            sessions_df.write.jdbc(
                url=jdbc_url,
                table="user_sessions",
                mode="append",
                properties=properties
            )
            logger.info(f"배치 {batch_id}: sessions_df.write.jdbc() 호출 완료!")
        except Exception as jdbc_error:
            logger.error(f"배치 {batch_id}: sessions_df.write.jdbc() 호출 실패: {jdbc_error}", exc_info=True)
            raise
        
        processing_time = time.time() - start_time
        logger.info(
            f"배치 {batch_id}: ✅ 세션 MySQL 저장 완료! "
            f"(처리 시간: {processing_time:.2f}초)"
        )
            
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(
            f"배치 {batch_id} MySQL 저장 실패 (처리 시간: {processing_time:.2f}초): {str(e)}",
            exc_info=True
        )


def debug_events(batch_df: DataFrame, batch_id: int) -> None:
    """
    디버깅용: 원본 이벤트 확인 (foreachBatch)
    
    Args:
        batch_df: 원본 이벤트 DataFrame (타임스탬프 변환 후)
        batch_id: 배치 ID
    """
    try:
        event_count = batch_df.count()
        logger.info(f"[DEBUG] 배치 {batch_id}: 원본 이벤트 수 = {event_count}")
        
        if event_count > 0:
            sample_events = batch_df.select("event_id", "session_id", "user_id", "timestamp", "timestamp_ts", "event_type").take(3)
            logger.info(f"[DEBUG] 배치 {batch_id}: 원본 이벤트 샘플 (최대 3개):")
            for i, row in enumerate(sample_events):
                # Row 객체는 딕셔너리처럼 접근 가능하지만, getattr도 사용 가능
                try:
                    event_id = row.event_id if hasattr(row, 'event_id') else row['event_id']
                    session_id = row.session_id if hasattr(row, 'session_id') else row['session_id']
                    timestamp = row.timestamp if hasattr(row, 'timestamp') else row['timestamp']
                    timestamp_ts = row.timestamp_ts if hasattr(row, 'timestamp_ts') else row.get('timestamp_ts', 'NULL')
                    event_type = row.event_type if hasattr(row, 'event_type') else row['event_type']
                    logger.info(f"  [{i}] event_id={event_id}, session_id={session_id}, "
                              f"timestamp={timestamp}, timestamp_ts={timestamp_ts}, event_type={event_type}")
                except Exception as e:
                    logger.warning(f"  [{i}] Row 접근 실패: {e}, row={row}")
        else:
            logger.warning(f"[DEBUG] 배치 {batch_id}: 원본 이벤트가 없습니다! (Kafka에서 읽지 못했을 수 있음)")
    except Exception as e:
        logger.error(f"[DEBUG] 배치 {batch_id}: 원본 이벤트 확인 실패: {e}", exc_info=True)


def process_stream(
    parsed_df: DataFrame, 
    checkpoint_dir: str,
    window_duration: str = "3 minutes",
    watermark_duration: str = "10 minutes"
) -> StreamingQuery:
    """
    스트림 처리: 세션 추적 및 MySQL 저장
    
    Args:
        parsed_df: 파싱된 DataFrame
        checkpoint_dir: 체크포인트 디렉토리 경로
        window_duration: 윈도우 크기 (기본: "3 minutes", 테스트용: "30 seconds")
        watermark_duration: 워터마크 지연 시간 (기본: "10 minutes", 테스트용: "1 minute")
        
    Returns:
        StreamingQuery: 스트리밍 쿼리 객체
    """
    # 타임스탬프 변환
    df_with_timestamp = convert_timestamp(parsed_df)
    logger.info("타임스탬프 변환 완료: ISO 8601 문자열 → TimestampType")
    
    # 디버깅: 원본 이벤트 확인을 위한 임시 쿼리 (별도 스레드에서 실행)
    # 이벤트가 실제로 읽히는지 확인
    def start_debug_query():
        try:
            debug_query = (
                df_with_timestamp.writeStream
                .outputMode("append")
                .foreachBatch(debug_events)
                .trigger(processingTime="3 seconds")
                .start()
            )
            logger.info(f"[DEBUG] 디버깅 쿼리 시작: 원본 이벤트 확인 (Query ID: {debug_query.id})")
            debug_query.awaitTermination()
        except Exception as e:
            logger.error(f"[DEBUG] 디버깅 쿼리 오류: {e}")
    
    import threading
    debug_thread = threading.Thread(target=start_debug_query, daemon=True)
    debug_thread.start()
    time.sleep(1)  # 디버깅 쿼리 시작 대기
    
    # 세션 추적 및 집계
    sessions_df = track_sessions(df_with_timestamp, window_duration, watermark_duration)
    logger.info("세션 집계 완료: 윈도우 기반 세션 추적 및 집계")
    logger.info(f"세션 집계 DataFrame 스키마: {sessions_df.schema}")
    logger.info(f"세션 집계 DataFrame 컬럼: {sessions_df.columns}")
    
    # foreachBatch를 사용한 MySQL 저장
    # outputMode("append"): 워터마크가 설정되면 윈도우 완료 시에만 출력
    # outputMode("update"): 윈도우 완료 시에만 출력 (중복 제거)
    # outputMode("complete"): 모든 윈도우 상태 출력 (메모리 사용량 증가)
    # 
    # 워터마크가 설정된 경우:
    # - append: 윈도우가 완료되어야만 출력 (window_end < watermark)
    # - update: 윈도우가 완료되어야만 출력 (중복 제거)
    query = (
        sessions_df.writeStream
        .outputMode("update")  # 윈도우 완료 시에만 출력 (중복 제거)
        .foreachBatch(save_sessions_to_mysql)
        .trigger(processingTime="3 seconds")
        .start()
    )
    
    logger.info("스트리밍 쿼리 시작: 배치 간격=3초, 세션 추적 활성화, MySQL 저장")
    logger.info(f"Streaming Query ID: {query.id}")
    logger.info(f"Streaming Query Name: {query.name}")
    
    return query


class SparkStreamingApp:
    """
    PySpark Streaming 애플리케이션
    
    주요 기능:
    - Kafka 스트림 읽기
    - JSON 파싱 및 스키마 검증
    - 3분 윈도우 기반 세션 추적
    - 세션 집계 및 MySQL 저장
    - 우아한 종료
    """
    
    def __init__(
        self,
        checkpoint_dir: str = "data/checkpoints/streaming/",
        topic: str = "user-events-topic",
        starting_offsets: Optional[str] = None,
        window_duration: str = "3 minutes",
        watermark_duration: str = "10 minutes",
    ) -> None:
        """
        생성자
        
        Args:
            checkpoint_dir: 체크포인트 디렉토리 경로 (오프셋 관리에 사용됨)
            topic: Kafka 토픽 이름
            starting_offsets: 시작 오프셋 ("latest" 또는 "earliest", None이면 자동 결정)
            window_duration: 윈도우 크기 (기본: "3 minutes", 테스트용: "30 seconds")
            watermark_duration: 워터마크 지연 시간 (기본: "10 minutes", 테스트용: "1 minute")
            
        Note:
            Spark Structured Streaming은 체크포인트를 통해 오프셋을 자동 관리합니다.
            Consumer Group을 사용하지 않습니다.
        """
        self.checkpoint_dir = checkpoint_dir
        self.topic = topic
        self.starting_offsets = starting_offsets
        self.window_duration = window_duration
        self.watermark_duration = watermark_duration
        
        self.spark: Optional[SparkSession] = None
        self.query: Optional[StreamingQuery] = None
        self.running = False
        
    def start(self) -> None:
        """스트리밍 애플리케이션 시작"""
        try:
            # Spark 세션 생성
            self.spark = create_spark_session(self.checkpoint_dir)
            
            # Kafka 스트림 생성
            kafka_df = create_kafka_stream(
                self.spark, 
                self.topic, 
                self.checkpoint_dir,
                self.starting_offsets
            )
            
            # 스키마 정의
            schema = get_user_event_schema()
            
            # JSON 파싱
            parsed_df = parse_json_stream(kafka_df, schema)
            
            # 디버깅: 파싱된 데이터 확인을 위한 임시 쿼리 (스트리밍에서는 직접 확인 불가)
            # 실제 데이터는 foreachBatch에서 확인 가능
            
            # 스트림 처리 시작
            self.query = process_stream(
                parsed_df, 
                self.checkpoint_dir,
                self.window_duration,
                self.watermark_duration
            )
            
            self.running = True
            logger.info("스트리밍 애플리케이션 시작 완료")
            logger.info(f"Topic: {self.topic}")
            logger.info(f"Starting Offsets: {self.starting_offsets}")
            logger.info(f"Window Duration: {self.window_duration}")
            logger.info(f"Watermark Duration: {self.watermark_duration}")
            logger.info(f"Checkpoint: {self.checkpoint_dir}")
            logger.info("오프셋은 체크포인트를 통해 자동 관리됩니다 (Consumer Group 미사용)")
            
            # 쿼리 상태 확인
            if self.query:
                logger.info(f"Streaming Query ID: {self.query.id}")
                logger.info(f"Streaming Query Status: {self.query.status}")
            
            # 쿼리 완료 대기
            self.query.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("중단 요청 감지 (CTRL+C). 정상 종료를 시도합니다.")
            self.stop()
        except Exception as e:
            logger.error(f"스트리밍 애플리케이션 오류: {str(e)}", exc_info=True)
            self.stop()
            raise
    
    def stop(self) -> None:
        """스트리밍 애플리케이션 중지"""
        if self.query is not None:
            try:
                self.query.stop()
                logger.info("스트리밍 쿼리 중지 완료")
            except Exception as e:
                logger.error(f"쿼리 중지 오류: {str(e)}")
        
        if self.spark is not None:
            try:
                self.spark.stop()
                logger.info("Spark 세션 종료 완료")
            except Exception as e:
                logger.error(f"Spark 세션 종료 오류: {str(e)}")
        
        self.running = False


def _configure_logging(level: str) -> None:
    """로깅 설정"""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def build_arg_parser() -> argparse.ArgumentParser:
    """CLI 인자 파서 생성"""
    parser = argparse.ArgumentParser(description="PySpark Streaming - Kafka 이벤트 실시간 처리")
    
    parser.add_argument(
        "--checkpoint-dir",
        default="data/checkpoints/streaming/",
        help="체크포인트 디렉토리 경로 (기본: data/checkpoints/streaming/)",
    )
    parser.add_argument(
        "--topic",
        default="user-events-topic",
        help="Kafka 토픽 이름 (기본: user-events-topic)",
    )
    parser.add_argument(
        "--starting-offsets",
        default=None,
        choices=["latest", "earliest"],
        nargs="?",
        help="시작 오프셋 (기본: 자동 결정 - 체크포인트가 없으면 earliest, 있으면 latest)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="로그 레벨 (DEBUG/INFO/WARNING/ERROR)",
    )
    
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    """메인 함수"""
    parser = build_arg_parser()
    args = parser.parse_args(argv if argv is not None else None)
    
    _configure_logging(args.log_level)
    
    # 애플리케이션 생성 및 시작
    app = SparkStreamingApp(
        checkpoint_dir=args.checkpoint_dir,
        topic=args.topic,
        starting_offsets=args.starting_offsets,
    )
    
    # 시그널 핸들러 등록 (우아한 종료)
    def signal_handler(signum, frame):
        logger.info("시그널 수신. 정상 종료를 시도합니다.")
        app.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        app.start()
        return 0
    except Exception as e:
        logger.error(f"애플리케이션 실행 오류: {str(e)}", exc_info=True)
        return 1
    finally:
        app.stop()


if __name__ == "__main__":
    raise SystemExit(main())
