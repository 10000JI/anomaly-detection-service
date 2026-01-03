"""
데이터베이스 스키마 마이그레이션
MySQL 테이블 생성 및 스키마 관리
"""

import logging
from typing import Dict, List
from .mysql_client import MySQLClient

# 로깅 설정
logger = logging.getLogger(__name__)


class DatabaseMigration:
    """
    데이터베이스 스키마 마이그레이션 클래스
    
    실시간 사용자 행동 분석 & 개인화 추천 시스템에 필요한
    7개 테이블을 생성하고 관리합니다.
    
    Attributes:
        client: MySQLClient 인스턴스
    """
    
    def __init__(self, mysql_client: MySQLClient):
        """
        DatabaseMigration 초기화
        
        Args:
            mysql_client: MySQL 클라이언트 인스턴스
        """
        self.client = mysql_client
        
    def create_user_profiles_table(self) -> bool:
        """
        user_profiles 테이블 생성
        
        사용자 기본 프로필 및 세그먼트 정보를 저장합니다.
        
        Returns:
            성공 여부
        """
        table_name = "user_profiles"
        
        if self.client.table_exists(table_name):
            logger.info(f"✅ 테이블 '{table_name}'이(가) 이미 존재합니다.")
            return True
        
        try:
            logger.info(f"📝 테이블 '{table_name}' 생성 중...")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id VARCHAR(50) PRIMARY KEY COMMENT '사용자 고유 ID (user-00001 ~ user-00100)',
                user_segment VARCHAR(20) COMMENT '사용자 세그먼트 (VIP/Regular/New)',
                signup_date DATE COMMENT '가입 날짜',
                total_purchases INT DEFAULT 0 COMMENT '총 구매 횟수',
                total_spent DECIMAL(12,2) DEFAULT 0 COMMENT '총 구매 금액',
                favorite_categories JSON COMMENT '선호 장르 목록 (JSON 배열)',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '레코드 생성 시간',
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '레코드 수정 시간',
                INDEX idx_segment (user_segment)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            COMMENT='사용자 프로필 및 세그먼트 정보 - VIP/Regular/New 사용자 관리'
            """
            
            self.client.execute_query(create_table_sql)
            logger.info(f"✅ 테이블 '{table_name}' 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 테이블 '{table_name}' 생성 실패: {e}")
            return False
    
    def create_contents_table(self) -> bool:
        """
        contents 테이블 생성
        
        콘텐츠 메타데이터를 저장합니다 (영화, 드라마, 다큐멘터리).
        
        Returns:
            성공 여부
        """
        table_name = "contents"
        
        if self.client.table_exists(table_name):
            logger.info(f"✅ 테이블 '{table_name}'이(가) 이미 존재합니다.")
            return True
        
        try:
            logger.info(f"📝 테이블 '{table_name}' 생성 중...")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS contents (
                content_id VARCHAR(50) PRIMARY KEY COMMENT '콘텐츠 고유 ID (movie-xxxxx/series-xxxxx/doc-xxxxx)',
                title VARCHAR(255) NOT NULL COMMENT '콘텐츠 제목',
                content_type ENUM('movie', 'series', 'documentary') NOT NULL COMMENT '콘텐츠 유형 (영화/드라마/다큐멘터리)',
                genre VARCHAR(100) COMMENT '주 장르',
                sub_genre VARCHAR(100) COMMENT '서브 장르',
                duration_minutes INT COMMENT '재생 시간 (분)',
                release_year INT COMMENT '출시 연도',
                rating FLOAT COMMENT '평균 평점 (1.0-5.0)',
                review_count INT DEFAULT 0 COMMENT '리뷰 개수',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '레코드 생성 시간',
                INDEX idx_genre (genre),
                INDEX idx_type (content_type),
                INDEX idx_rating (rating)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            COMMENT='콘텐츠 메타데이터 - 영화/드라마/다큐멘터리 정보'
            """
            
            self.client.execute_query(create_table_sql)
            logger.info(f"✅ 테이블 '{table_name}' 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 테이블 '{table_name}' 생성 실패: {e}")
            return False
    
    def create_user_events_table(self) -> bool:
        """
        user_events 테이블 생성
        
        사용자 행동 이벤트 히스토리를 저장합니다.
        
        Returns:
            성공 여부
        """
        table_name = "user_events"
        
        if self.client.table_exists(table_name):
            logger.info(f"✅ 테이블 '{table_name}'이(가) 이미 존재합니다.")
            return True
        
        try:
            logger.info(f"📝 테이블 '{table_name}' 생성 중...")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS user_events (
                id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '이벤트 고유 ID',
                user_id VARCHAR(50) NOT NULL COMMENT '사용자 ID (FK: user_profiles)',
                session_id VARCHAR(100) COMMENT '세션 ID (3분 윈도우)',
                event_type ENUM('click', 'watch', 'watchlist', 'watch_complete', 'rating') NOT NULL COMMENT '이벤트 타입',
                content_id VARCHAR(50) COMMENT '콘텐츠 ID (FK: contents)',
                watched_minutes INT COMMENT '시청 시간 (분)',
                timestamp DATETIME NOT NULL COMMENT '이벤트 발생 시간',
                metadata JSON COMMENT '추가 메타데이터 (디바이스, 화질, referrer 등)',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '레코드 생성 시간',
                INDEX idx_user_timestamp (user_id, timestamp),
                INDEX idx_content (content_id),
                INDEX idx_session (session_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            COMMENT='사용자 행동 이벤트 히스토리 - Kafka로부터 수집된 실시간 이벤트'
            """
            
            self.client.execute_query(create_table_sql)
            logger.info(f"✅ 테이블 '{table_name}' 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 테이블 '{table_name}' 생성 실패: {e}")
            return False
    
    def create_user_sessions_table(self) -> bool:
        """
        user_sessions 테이블 생성
        
        사용자 세션 정보 및 분석 결과를 저장합니다.
        
        Returns:
            성공 여부
        """
        table_name = "user_sessions"
        
        if self.client.table_exists(table_name):
            logger.info(f"✅ 테이블 '{table_name}'이(가) 이미 존재합니다.")
            return True
        
        try:
            logger.info(f"📝 테이블 '{table_name}' 생성 중...")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS user_sessions (
                session_id VARCHAR(100) PRIMARY KEY COMMENT '세션 고유 ID',
                user_id VARCHAR(50) NOT NULL COMMENT '사용자 ID (FK: user_profiles)',
                start_time DATETIME NOT NULL COMMENT '세션 시작 시간',
                end_time DATETIME COMMENT '세션 종료 시간',
                event_count INT DEFAULT 0 COMMENT '세션 내 이벤트 수',
                browsed_contents JSON COMMENT '탐색한 콘텐츠 목록 (JSON 배열)',
                watched_contents JSON COMMENT '시청한 콘텐츠 목록 (JSON 배열)',
                completed_contents JSON COMMENT '완료한 콘텐츠 목록 (JSON 배열)',
                total_watch_minutes INT DEFAULT 0 COMMENT '총 시청 시간 (분)',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '레코드 생성 시간',
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '레코드 수정 시간',
                INDEX idx_user_start (user_id, start_time)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            COMMENT='사용자 세션 분석 결과 - PySpark 3분 윈도우 분석'
            """
            
            self.client.execute_query(create_table_sql)
            logger.info(f"✅ 테이블 '{table_name}' 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 테이블 '{table_name}' 생성 실패: {e}")
            return False
    
    def create_recommendations_table(self) -> bool:
        """
        recommendations 테이블 생성
        
        사용자별 추천 결과를 저장하고 추적합니다.
        
        Returns:
            성공 여부
        """
        table_name = "recommendations"
        
        if self.client.table_exists(table_name):
            logger.info(f"✅ 테이블 '{table_name}'이(가) 이미 존재합니다.")
            return True
        
        try:
            logger.info(f"📝 테이블 '{table_name}' 생성 중...")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS recommendations (
                id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '추천 고유 ID',
                user_id VARCHAR(50) NOT NULL COMMENT '사용자 ID (FK: user_profiles)',
                content_id VARCHAR(50) NOT NULL COMMENT '추천된 콘텐츠 ID (FK: contents)',
                recommendation_score FLOAT COMMENT '추천 점수 (0.0-1.0)',
                algorithm VARCHAR(50) COMMENT '사용 알고리즘 (collaborative_filtering/session_based)',
                ab_test_group VARCHAR(20) COMMENT 'A/B 테스트 그룹 (algorithm_v1/algorithm_v2)',
                is_clicked BOOLEAN DEFAULT FALSE COMMENT '클릭 여부 (CTR 추적)',
                is_watched BOOLEAN DEFAULT FALSE COMMENT '시청 여부 (완료율 추적)',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '추천 생성 시간',
                INDEX idx_user_created (user_id, created_at),
                INDEX idx_algorithm (algorithm)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            COMMENT='개인화 추천 결과 - 협업 필터링 및 세션 기반 추천'
            """
            
            self.client.execute_query(create_table_sql)
            logger.info(f"✅ 테이블 '{table_name}' 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 테이블 '{table_name}' 생성 실패: {e}")
            return False
    
    def create_ab_test_groups_table(self) -> bool:
        """
        ab_test_groups 테이블 생성
        
        A/B 테스트 그룹 정의 및 사용자 할당 정보를 저장합니다.
        
        Returns:
            성공 여부
        """
        table_name = "ab_test_groups"
        
        if self.client.table_exists(table_name):
            logger.info(f"✅ 테이블 '{table_name}'이(가) 이미 존재합니다.")
            return True
        
        try:
            logger.info(f"📝 테이블 '{table_name}' 생성 중...")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS ab_test_groups (
                id INT AUTO_INCREMENT PRIMARY KEY COMMENT '그룹 ID',
                group_name VARCHAR(50) NOT NULL UNIQUE COMMENT '그룹명 (algorithm_v1/algorithm_v2)',
                description TEXT COMMENT '그룹 설명',
                algorithm VARCHAR(50) COMMENT '사용 알고리즘',
                config JSON COMMENT '알고리즘 설정 (하이퍼파라미터 등)',
                is_active BOOLEAN DEFAULT TRUE COMMENT '활성화 여부',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성 시간',
                INDEX idx_active (is_active)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            COMMENT='A/B 테스트 그룹 정의 - 알고리즘 버전 관리'
            """
            
            self.client.execute_query(create_table_sql)
            logger.info(f"✅ 테이블 '{table_name}' 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 테이블 '{table_name}' 생성 실패: {e}")
            return False
    
    def create_ab_test_metrics_table(self) -> bool:
        """
        ab_test_metrics 테이블 생성
        
        A/B 테스트 성능 지표를 저장합니다.
        
        Returns:
            성공 여부
        """
        table_name = "ab_test_metrics"
        
        if self.client.table_exists(table_name):
            logger.info(f"✅ 테이블 '{table_name}'이(가) 이미 존재합니다.")
            return True
        
        try:
            logger.info(f"📝 테이블 '{table_name}' 생성 중...")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS ab_test_metrics (
                id INT AUTO_INCREMENT PRIMARY KEY COMMENT '지표 ID',
                test_group VARCHAR(20) NOT NULL COMMENT '테스트 그룹명',
                metric_name VARCHAR(100) COMMENT '지표명 (click_rate/watch_rate/precision@10)',
                metric_value FLOAT COMMENT '지표 값',
                sample_size INT COMMENT '샘플 크기',
                timestamp DATETIME NOT NULL COMMENT '측정 시간',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '레코드 생성 시간',
                INDEX idx_group_timestamp (test_group, timestamp)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            COMMENT='A/B 테스트 성능 지표 - 그룹별 시계열 성능 데이터'
            """
            
            self.client.execute_query(create_table_sql)
            logger.info(f"✅ 테이블 '{table_name}' 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 테이블 '{table_name}' 생성 실패: {e}")
            return False
    
    def run_migrations(self) -> Dict[str, bool]:
        """
        전체 데이터베이스 마이그레이션 실행
        
        7개의 테이블을 순차적으로 생성합니다.
        
        Returns:
            각 테이블별 생성 결과 딕셔너리
            
        Example:
            {
                'user_profiles': True,
                'contents': True,
                'user_events': True,
                ...
            }
        """
        logger.info("=" * 60)
        logger.info("🚀 데이터베이스 마이그레이션 시작")
        logger.info("=" * 60)
        
        results = {}
        
        # 1. user_profiles 테이블
        results['user_profiles'] = self.create_user_profiles_table()
        
        # 2. contents 테이블
        results['contents'] = self.create_contents_table()
        
        # 3. user_events 테이블
        results['user_events'] = self.create_user_events_table()
        
        # 4. user_sessions 테이블
        results['user_sessions'] = self.create_user_sessions_table()
        
        # 5. recommendations 테이블
        results['recommendations'] = self.create_recommendations_table()
        
        # 6. ab_test_groups 테이블
        results['ab_test_groups'] = self.create_ab_test_groups_table()
        
        # 7. ab_test_metrics 테이블
        results['ab_test_metrics'] = self.create_ab_test_metrics_table()
        
        # 결과 요약
        logger.info("=" * 60)
        logger.info("📊 마이그레이션 결과 요약")
        logger.info("=" * 60)
        
        success_count = sum(1 for success in results.values() if success)
        total_count = len(results)
        
        for table_name, success in results.items():
            status = "✅ 성공" if success else "❌ 실패"
            logger.info(f"  {table_name}: {status}")
        
        logger.info("=" * 60)
        logger.info(f"📌 전체 결과: {success_count}/{total_count} 테이블 생성 완료")
        logger.info("=" * 60)
        
        return results
    
    def verify_schema(self) -> Dict[str, any]:
        """
        데이터베이스 스키마 검증
        
        모든 테이블의 존재 여부와 구조를 확인합니다.
        
        Returns:
            검증 결과 딕셔너리
        """
        logger.info("🔍 데이터베이스 스키마 검증 시작...")
        
        tables = [
            'user_profiles',
            'contents',
            'user_events',
            'user_sessions',
            'recommendations',
            'ab_test_groups',
            'ab_test_metrics'
        ]
        
        verification_results = {
            'all_tables_exist': True,
            'tables': {}
        }
        
        for table_name in tables:
            exists = self.client.table_exists(table_name)
            verification_results['tables'][table_name] = {
                'exists': exists,
                'columns': []
            }
            
            if exists:
                # 테이블 구조 정보 조회
                columns = self.client.get_table_info(table_name)
                verification_results['tables'][table_name]['columns'] = columns
                logger.info(f"  ✅ {table_name}: {len(columns)}개 컬럼")
            else:
                verification_results['all_tables_exist'] = False
                logger.warning(f"  ❌ {table_name}: 존재하지 않음")
        
        if verification_results['all_tables_exist']:
            logger.info("✅ 모든 테이블이 정상적으로 존재합니다.")
        else:
            logger.warning("⚠️  일부 테이블이 누락되었습니다.")
        
        return verification_results
    
    def get_table_list(self) -> List[str]:
        """
        데이터베이스의 모든 테이블 목록 조회
        
        Returns:
            테이블명 리스트
        """
        try:
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s
                ORDER BY table_name
            """
            results = self.client.fetch_all(query, (self.client.config.database,))
            return [row['table_name'] for row in results]
        except Exception as e:
            logger.error(f"❌ 테이블 목록 조회 실패: {e}")
            return []


