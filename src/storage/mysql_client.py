"""
MySQL 데이터베이스 클라이언트
Connection Pool을 사용한 안전한 데이터베이스 연결 관리
"""

import logging
import time
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
import mysql.connector
from mysql.connector import pooling, Error
from config import get_mysql_config

# 로깅 설정
logger = logging.getLogger(__name__)


class MySQLClient:
    """
    MySQL 데이터베이스 클라이언트
    
    Connection Pool을 사용하여 안정적인 데이터베이스 연결을 제공합니다.
    자동 재연결 및 에러 핸들링을 지원합니다.
    
    Attributes:
        config: MySQL 설정 객체
        pool: Connection Pool 객체
    """
    
    def __init__(self):
        """
        MySQL 클라이언트 초기화
        
        환경 변수에서 설정을 로드하고 Connection Pool을 생성합니다.
        """
        self.config = get_mysql_config()
        self.pool: Optional[pooling.MySQLConnectionPool] = None
        self._initialize_pool()
    
    def _initialize_pool(self, max_retries: int = 3) -> None:
        """
        Connection Pool 초기화
        
        Args:
            max_retries: 최대 재시도 횟수
            
        Raises:
            Exception: Connection Pool 생성 실패 시
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"MySQL Connection Pool 초기화 시도 ({attempt + 1}/{max_retries})...")
                
                self.pool = pooling.MySQLConnectionPool(
                    pool_name=self.config.pool_name,
                    pool_size=self.config.pool_size,
                    pool_reset_session=True,
                    host=self.config.host,
                    port=self.config.port,
                    user=self.config.user,
                    password=self.config.password,
                    database=self.config.database,
                    charset='utf8mb4',
                    collation='utf8mb4_unicode_ci',
                    autocommit=True
                )
                
                logger.info("✅ MySQL Connection Pool 초기화 성공")
                return
                
            except Error as e:
                logger.error(f"❌ MySQL Connection Pool 초기화 실패 (시도 {attempt + 1}/{max_retries}): {e}")
                
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # 지수 백오프
                    logger.info(f"⏳ {wait_time}초 후 재시도...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"MySQL Connection Pool 초기화 실패: {e}")
    
    @contextmanager
    def get_connection(self):
        """
        Connection Pool에서 연결을 가져오는 컨텍스트 매니저
        
        Yields:
            connection: MySQL 연결 객체
            
        Example:
            with client.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM users")
        """
        connection = None
        try:
            connection = self.pool.get_connection()
            yield connection
        except Error as e:
            logger.error(f"❌ MySQL 연결 획득 실패: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()
    
    def execute_query(
        self, 
        query: str, 
        params: Optional[Tuple] = None,
        commit: bool = True
    ) -> int:
        """
        쿼리 실행 (INSERT, UPDATE, DELETE)
        
        Args:
            query: 실행할 SQL 쿼리
            params: 쿼리 파라미터 (Prepared Statement)
            commit: 자동 커밋 여부
            
        Returns:
            영향받은 행의 수
            
        Raises:
            Exception: 쿼리 실행 실패 시
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params or ())
                
                if commit:
                    conn.commit()
                
                affected_rows = cursor.rowcount
                cursor.close()
                
                return affected_rows
                
        except Error as e:
            logger.error(f"❌ 쿼리 실행 실패: {e}")
            logger.error(f"   Query: {query}")
            logger.error(f"   Params: {params}")
            raise
    
    def fetch_one(
        self, 
        query: str, 
        params: Optional[Tuple] = None
    ) -> Optional[Dict[str, Any]]:
        """
        단일 레코드 조회
        
        Args:
            query: 실행할 SELECT 쿼리
            params: 쿼리 파라미터
            
        Returns:
            조회된 레코드 (딕셔너리) 또는 None
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute(query, params or ())
                result = cursor.fetchone()
                cursor.close()
                return result
                
        except Error as e:
            logger.error(f"❌ 단일 레코드 조회 실패: {e}")
            logger.error(f"   Query: {query}")
            raise
    
    def fetch_all(
        self, 
        query: str, 
        params: Optional[Tuple] = None
    ) -> List[Dict[str, Any]]:
        """
        다중 레코드 조회
        
        Args:
            query: 실행할 SELECT 쿼리
            params: 쿼리 파라미터
            
        Returns:
            조회된 레코드 리스트 (각 레코드는 딕셔너리)
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute(query, params or ())
                results = cursor.fetchall()
                cursor.close()
                return results
                
        except Error as e:
            logger.error(f"❌ 다중 레코드 조회 실패: {e}")
            logger.error(f"   Query: {query}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """
        테이블 존재 여부 확인
        
        Args:
            table_name: 확인할 테이블명
            
        Returns:
            테이블 존재 여부 (True/False)
        """
        try:
            query = """
                SELECT COUNT(*) as count
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            """
            result = self.fetch_one(query, (self.config.database, table_name))
            return result['count'] > 0 if result else False
            
        except Error as e:
            logger.error(f"❌ 테이블 존재 확인 실패: {e}")
            return False
    
    def execute_script(self, script: str) -> None:
        """
        여러 SQL 문을 포함한 스크립트 실행
        
        Args:
            script: 실행할 SQL 스크립트
            
        Raises:
            Exception: 스크립트 실행 실패 시
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # 세미콜론으로 분리된 여러 쿼리 실행
                for statement in script.split(';'):
                    statement = statement.strip()
                    if statement:
                        cursor.execute(statement)
                
                conn.commit()
                cursor.close()
                
        except Error as e:
            logger.error(f"❌ 스크립트 실행 실패: {e}")
            raise
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """
        테이블 구조 정보 조회
        
        Args:
            table_name: 조회할 테이블명
            
        Returns:
            컬럼 정보 리스트
        """
        try:
            query = f"DESCRIBE {table_name}"
            return self.fetch_all(query)
        except Error as e:
            logger.error(f"❌ 테이블 정보 조회 실패: {e}")
            return []
    
    def batch_insert_user_events(self, events: List[Dict[str, Any]]) -> int:
        """
        user_events 테이블에 이벤트 배치 삽입
        
        Args:
            events: 삽입할 이벤트 리스트
            
        Returns:
            삽입된 행의 수
            
        Raises:
            Exception: 배치 삽입 실패 시
        """
        if not events:
            return 0
        
        try:
            query = """
                INSERT INTO user_events (
                    user_id,
                    session_id,
                    event_type,
                    content_id,
                    watched_minutes,
                    timestamp,
                    metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            # 이벤트를 튜플 리스트로 변환
            import json
            from datetime import datetime
            
            values = []
            for event in events:
                # timestamp를 datetime 객체로 변환
                timestamp_str = event.get("timestamp", "")
                try:
                    # ISO 8601 형식 파싱 (Z를 +00:00으로 변환)
                    timestamp_str = timestamp_str.replace("Z", "+00:00")
                    timestamp = datetime.fromisoformat(timestamp_str)
                except Exception:
                    # 파싱 실패 시 현재 시각 사용
                    timestamp = datetime.now()
                
                # metadata를 JSON 문자열로 변환
                metadata = event.get("metadata", {})
                metadata_json = json.dumps(metadata, ensure_ascii=False)
                
                values.append((
                    event.get("user_id", ""),
                    event.get("session_id", ""),
                    event.get("event_type", ""),
                    event.get("content_id", ""),
                    event.get("watched_minutes", 0),
                    timestamp,
                    metadata_json,
                ))
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany(query, values)
                conn.commit()
                affected_rows = cursor.rowcount
                cursor.close()
                
                logger.info(f"✅ 배치 삽입 성공: {affected_rows}건")
                return affected_rows
                
        except Error as e:
            logger.error(f"❌ 배치 삽입 실패: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ 배치 삽입 중 예상치 못한 오류: {e}")
            raise
    
    def connect(self) -> None:
        """
        명시적 연결 메서드 (Consumer에서 호출)
        
        실제로는 Connection Pool이 이미 초기화되어 있으므로
        연결 테스트만 수행합니다.
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchall()  # 결과 읽기
                cursor.close()
                logger.info("✅ MySQL 연결 테스트 성공")
        except Exception as e:
            logger.error(f"❌ MySQL 연결 테스트 실패: {e}")
            raise
    
    def close(self) -> None:
        """
        Connection Pool 종료
        
        모든 연결을 정리하고 리소스를 해제합니다.
        """
        try:
            if self.pool:
                # Connection Pool은 자동으로 관리되므로 별도 종료 불필요
                logger.info("✅ MySQL Connection Pool 정리 완료")
        except Exception as e:
            logger.error(f"❌ Connection Pool 종료 중 오류: {e}")


# 싱글톤 인스턴스
_mysql_client: Optional[MySQLClient] = None


def get_mysql_client() -> MySQLClient:
    """
    싱글톤 패턴으로 MySQL 클라이언트 반환
    
    Returns:
        MySQLClient 인스턴스
        
    Example:
        client = get_mysql_client()
        result = client.fetch_all("SELECT * FROM users")
    """
    global _mysql_client
    if _mysql_client is None:
        _mysql_client = MySQLClient()
    return _mysql_client


