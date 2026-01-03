"""
데이터베이스 스키마 생성 테스트

MySQL 연결, 테이블 생성, 스키마 검증을 테스트합니다.
"""

import sys
import os
import logging
from typing import Dict

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.storage.mysql_client import MySQLClient, get_mysql_client
from src.storage.migrations import DatabaseMigration

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestDatabaseSchema:
    """데이터베이스 스키마 테스트 클래스"""
    
    def __init__(self):
        """테스트 초기화"""
        self.client: MySQLClient = None
        self.migration: DatabaseMigration = None
        
    def setup(self):
        """테스트 환경 설정"""
        logger.info("=" * 60)
        logger.info("🧪 테스트 환경 설정 시작")
        logger.info("=" * 60)
        
        try:
            self.client = get_mysql_client()
            self.migration = DatabaseMigration(self.client)
            logger.info("✅ 테스트 환경 설정 완료")
            return True
        except Exception as e:
            logger.error(f"❌ 테스트 환경 설정 실패: {e}")
            return False
    
    def test_mysql_connection(self) -> bool:
        """
        Test 1: MySQL 연결 테스트
        
        Returns:
            테스트 성공 여부
        """
        logger.info("\n" + "=" * 60)
        logger.info("Test 1: MySQL 연결 테스트")
        logger.info("=" * 60)
        
        try:
            with self.client.get_connection() as conn:
                if conn.is_connected():
                    db_info = conn.get_server_info()
                    logger.info(f"✅ MySQL 서버 연결 성공")
                    logger.info(f"   서버 버전: {db_info}")
                    logger.info(f"   데이터베이스: {self.client.config.database}")
                    return True
                else:
                    logger.error("❌ MySQL 서버 연결 실패")
                    return False
        except Exception as e:
            logger.error(f"❌ MySQL 연결 테스트 실패: {e}")
            return False
    
    def test_create_user_profiles_table(self) -> bool:
        """
        Test 2: user_profiles 테이블 생성 테스트
        
        Returns:
            테스트 성공 여부
        """
        logger.info("\n" + "=" * 60)
        logger.info("Test 2: user_profiles 테이블 생성")
        logger.info("=" * 60)
        
        try:
            result = self.migration.create_user_profiles_table()
            if result and self.client.table_exists('user_profiles'):
                logger.info("✅ user_profiles 테이블 생성 성공")
                
                # 테이블 구조 확인
                columns = self.client.get_table_info('user_profiles')
                logger.info(f"   컬럼 수: {len(columns)}")
                for col in columns:
                    logger.info(f"   - {col['Field']}: {col['Type']}")
                return True
            else:
                logger.error("❌ user_profiles 테이블 생성 실패")
                return False
        except Exception as e:
            logger.error(f"❌ user_profiles 테이블 생성 테스트 실패: {e}")
            return False
    
    def test_create_contents_table(self) -> bool:
        """
        Test 3: contents 테이블 생성 테스트
        
        Returns:
            테스트 성공 여부
        """
        logger.info("\n" + "=" * 60)
        logger.info("Test 3: contents 테이블 생성")
        logger.info("=" * 60)
        
        try:
            result = self.migration.create_contents_table()
            if result and self.client.table_exists('contents'):
                logger.info("✅ contents 테이블 생성 성공")
                
                columns = self.client.get_table_info('contents')
                logger.info(f"   컬럼 수: {len(columns)}")
                return True
            else:
                logger.error("❌ contents 테이블 생성 실패")
                return False
        except Exception as e:
            logger.error(f"❌ contents 테이블 생성 테스트 실패: {e}")
            return False
    
    def test_create_user_events_table(self) -> bool:
        """
        Test 4: user_events 테이블 생성 테스트
        
        Returns:
            테스트 성공 여부
        """
        logger.info("\n" + "=" * 60)
        logger.info("Test 4: user_events 테이블 생성")
        logger.info("=" * 60)
        
        try:
            result = self.migration.create_user_events_table()
            if result and self.client.table_exists('user_events'):
                logger.info("✅ user_events 테이블 생성 성공")
                return True
            else:
                logger.error("❌ user_events 테이블 생성 실패")
                return False
        except Exception as e:
            logger.error(f"❌ user_events 테이블 생성 테스트 실패: {e}")
            return False
    
    def test_create_user_sessions_table(self) -> bool:
        """
        Test 5: user_sessions 테이블 생성 테스트
        
        Returns:
            테스트 성공 여부
        """
        logger.info("\n" + "=" * 60)
        logger.info("Test 5: user_sessions 테이블 생성")
        logger.info("=" * 60)
        
        try:
            result = self.migration.create_user_sessions_table()
            if result and self.client.table_exists('user_sessions'):
                logger.info("✅ user_sessions 테이블 생성 성공")
                return True
            else:
                logger.error("❌ user_sessions 테이블 생성 실패")
                return False
        except Exception as e:
            logger.error(f"❌ user_sessions 테이블 생성 테스트 실패: {e}")
            return False
    
    def test_create_recommendations_table(self) -> bool:
        """
        Test 6: recommendations 테이블 생성 테스트
        
        Returns:
            테스트 성공 여부
        """
        logger.info("\n" + "=" * 60)
        logger.info("Test 6: recommendations 테이블 생성")
        logger.info("=" * 60)
        
        try:
            result = self.migration.create_recommendations_table()
            if result and self.client.table_exists('recommendations'):
                logger.info("✅ recommendations 테이블 생성 성공")
                return True
            else:
                logger.error("❌ recommendations 테이블 생성 실패")
                return False
        except Exception as e:
            logger.error(f"❌ recommendations 테이블 생성 테스트 실패: {e}")
            return False
    
    def test_create_ab_test_groups_table(self) -> bool:
        """
        Test 7: ab_test_groups 테이블 생성 테스트
        
        Returns:
            테스트 성공 여부
        """
        logger.info("\n" + "=" * 60)
        logger.info("Test 7: ab_test_groups 테이블 생성")
        logger.info("=" * 60)
        
        try:
            result = self.migration.create_ab_test_groups_table()
            if result and self.client.table_exists('ab_test_groups'):
                logger.info("✅ ab_test_groups 테이블 생성 성공")
                return True
            else:
                logger.error("❌ ab_test_groups 테이블 생성 실패")
                return False
        except Exception as e:
            logger.error(f"❌ ab_test_groups 테이블 생성 테스트 실패: {e}")
            return False
    
    def test_create_ab_test_metrics_table(self) -> bool:
        """
        Test 8: ab_test_metrics 테이블 생성 테스트
        
        Returns:
            테스트 성공 여부
        """
        logger.info("\n" + "=" * 60)
        logger.info("Test 8: ab_test_metrics 테이블 생성")
        logger.info("=" * 60)
        
        try:
            result = self.migration.create_ab_test_metrics_table()
            if result and self.client.table_exists('ab_test_metrics'):
                logger.info("✅ ab_test_metrics 테이블 생성 성공")
                return True
            else:
                logger.error("❌ ab_test_metrics 테이블 생성 실패")
                return False
        except Exception as e:
            logger.error(f"❌ ab_test_metrics 테이블 생성 테스트 실패: {e}")
            return False
    
    def test_table_already_exists(self) -> bool:
        """
        Test 9: 이미 존재하는 테이블 재생성 안전성 테스트
        
        Returns:
            테스트 성공 여부
        """
        logger.info("\n" + "=" * 60)
        logger.info("Test 9: 중복 생성 안전성 테스트")
        logger.info("=" * 60)
        
        try:
            # user_profiles 테이블을 다시 생성 시도
            result = self.migration.create_user_profiles_table()
            if result:
                logger.info("✅ 중복 생성 안전성 테스트 통과 (에러 없음)")
                return True
            else:
                logger.error("❌ 중복 생성 테스트 실패")
                return False
        except Exception as e:
            logger.error(f"❌ 중복 생성 안전성 테스트 실패: {e}")
            return False
    
    def test_full_migration(self) -> bool:
        """
        Test 10: 전체 마이그레이션 테스트
        
        Returns:
            테스트 성공 여부
        """
        logger.info("\n" + "=" * 60)
        logger.info("Test 10: 전체 마이그레이션 실행")
        logger.info("=" * 60)
        
        try:
            results = self.migration.run_migrations()
            
            # 모든 테이블이 성공했는지 확인
            all_success = all(results.values())
            
            if all_success:
                logger.info("✅ 전체 마이그레이션 테스트 통과")
                return True
            else:
                logger.error("❌ 일부 테이블 생성 실패")
                return False
        except Exception as e:
            logger.error(f"❌ 전체 마이그레이션 테스트 실패: {e}")
            return False
    
    def test_schema_verification(self) -> bool:
        """
        Test 11: 스키마 검증 테스트
        
        Returns:
            테스트 성공 여부
        """
        logger.info("\n" + "=" * 60)
        logger.info("Test 11: 스키마 검증")
        logger.info("=" * 60)
        
        try:
            verification = self.migration.verify_schema()
            
            if verification['all_tables_exist']:
                logger.info("✅ 스키마 검증 테스트 통과")
                
                # 테이블 목록 출력
                table_list = self.migration.get_table_list()
                logger.info(f"   데이터베이스 내 테이블 수: {len(table_list)}")
                for table_name in table_list:
                    logger.info(f"   - {table_name}")
                
                return True
            else:
                logger.error("❌ 스키마 검증 실패: 일부 테이블 누락")
                return False
        except Exception as e:
            logger.error(f"❌ 스키마 검증 테스트 실패: {e}")
            return False
    
    def run_all_tests(self) -> Dict[str, bool]:
        """
        모든 테스트 실행
        
        Returns:
            각 테스트별 결과 딕셔너리
        """
        logger.info("\n" + "🎯" * 30)
        logger.info("데이터베이스 스키마 테스트 시작")
        logger.info("🎯" * 30 + "\n")
        
        if not self.setup():
            logger.error("❌ 테스트 환경 설정 실패. 테스트를 중단합니다.")
            return {}
        
        test_results = {}
        
        # 개별 테스트 실행
        test_results['test_mysql_connection'] = self.test_mysql_connection()
        test_results['test_create_user_profiles_table'] = self.test_create_user_profiles_table()
        test_results['test_create_contents_table'] = self.test_create_contents_table()
        test_results['test_create_user_events_table'] = self.test_create_user_events_table()
        test_results['test_create_user_sessions_table'] = self.test_create_user_sessions_table()
        test_results['test_create_recommendations_table'] = self.test_create_recommendations_table()
        test_results['test_create_ab_test_groups_table'] = self.test_create_ab_test_groups_table()
        test_results['test_create_ab_test_metrics_table'] = self.test_create_ab_test_metrics_table()
        test_results['test_table_already_exists'] = self.test_table_already_exists()
        test_results['test_full_migration'] = self.test_full_migration()
        test_results['test_schema_verification'] = self.test_schema_verification()
        
        # 최종 결과 요약
        logger.info("\n" + "=" * 60)
        logger.info("📊 테스트 결과 요약")
        logger.info("=" * 60)
        
        passed = sum(1 for result in test_results.values() if result)
        total = len(test_results)
        
        for test_name, result in test_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            logger.info(f"  {test_name}: {status}")
        
        logger.info("=" * 60)
        logger.info(f"📌 최종 결과: {passed}/{total} 테스트 통과")
        
        if passed == total:
            logger.info("🎉 모든 테스트를 성공적으로 통과했습니다!")
        else:
            logger.warning(f"⚠️  {total - passed}개의 테스트가 실패했습니다.")
        
        logger.info("=" * 60 + "\n")
        
        return test_results


def main():
    """메인 함수"""
    test_suite = TestDatabaseSchema()
    results = test_suite.run_all_tests()
    
    # 테스트 실패 시 exit code 1 반환
    if not all(results.values()):
        sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    main()


