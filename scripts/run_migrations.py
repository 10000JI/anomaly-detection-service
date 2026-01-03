"""
데이터베이스 마이그레이션 실행 스크립트

MySQL 데이터베이스에 필요한 테이블을 생성합니다.
"""

import sys
import os
import logging
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.storage.mysql_client import get_mysql_client
from src.storage.migrations import DatabaseMigration

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            f'migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )
    ]
)
logger = logging.getLogger(__name__)


def main():
    """
    메인 함수
    
    데이터베이스 마이그레이션을 실행하고 결과를 출력합니다.
    """
    logger.info("=" * 70)
    logger.info("🚀 데이터베이스 스키마 생성 스크립트 시작")
    logger.info("=" * 70)
    logger.info(f"실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    
    try:
        # 1. MySQL 클라이언트 생성
        logger.info("Step 1: MySQL 클라이언트 연결 중...")
        client = get_mysql_client()
        logger.info(f"✅ MySQL 연결 성공")
        logger.info(f"   Host: {client.config.host}:{client.config.port}")
        logger.info(f"   Database: {client.config.database}")
        logger.info("")
        
        # 2. 마이그레이션 객체 생성
        logger.info("Step 2: 마이그레이션 객체 생성...")
        migration = DatabaseMigration(client)
        logger.info("✅ 마이그레이션 객체 생성 완료")
        logger.info("")
        
        # 3. 기존 테이블 확인
        logger.info("Step 3: 기존 테이블 확인...")
        existing_tables = migration.get_table_list()
        if existing_tables:
            logger.info(f"📋 현재 {len(existing_tables)}개의 테이블이 존재합니다:")
            for table in existing_tables:
                logger.info(f"   - {table}")
        else:
            logger.info("📋 현재 테이블이 없습니다. (신규 데이터베이스)")
        logger.info("")
        
        # 4. 마이그레이션 실행
        logger.info("Step 4: 마이그레이션 실행...")
        logger.info("-" * 70)
        results = migration.run_migrations()
        logger.info("-" * 70)
        logger.info("")
        
        # 5. 스키마 검증
        logger.info("Step 5: 스키마 검증...")
        verification = migration.verify_schema()
        logger.info("")
        
        # 6. 최종 결과 출력
        logger.info("=" * 70)
        logger.info("📊 최종 결과")
        logger.info("=" * 70)
        
        success_count = sum(1 for success in results.values() if success)
        total_count = len(results)
        
        logger.info(f"생성된 테이블: {success_count}/{total_count}")
        logger.info(f"스키마 검증: {'✅ 통과' if verification['all_tables_exist'] else '❌ 실패'}")
        logger.info("")
        
        if verification['all_tables_exist']:
            logger.info("🎉 데이터베이스 스키마가 성공적으로 생성되었습니다!")
            logger.info("")
            logger.info("생성된 테이블 목록:")
            for table_name in results.keys():
                columns = verification['tables'][table_name]['columns']
                logger.info(f"  ✅ {table_name} ({len(columns)}개 컬럼)")
            
            logger.info("")
            logger.info("다음 단계:")
            logger.info("  1. 테스트 실행: python tests/test_db_schema.py")
            logger.info("  2. 샘플 데이터 생성: python scripts/generate_sample_data.py")
            logger.info("  3. Kafka Producer 구현: Task 003")
            
            return 0
        else:
            logger.error("❌ 일부 테이블 생성에 실패했습니다.")
            logger.error("실패한 테이블:")
            for table_name, success in results.items():
                if not success:
                    logger.error(f"  ❌ {table_name}")
            return 1
        
    except Exception as e:
        logger.error("=" * 70)
        logger.error("❌ 마이그레이션 실행 중 오류 발생")
        logger.error("=" * 70)
        logger.error(f"오류 메시지: {e}")
        logger.exception("상세 오류:")
        logger.error("")
        logger.error("문제 해결 방법:")
        logger.error("  1. .env 파일의 MySQL 연결 정보를 확인하세요")
        logger.error("  2. MySQL 서버가 실행 중인지 확인하세요")
        logger.error("  3. 데이터베이스 접근 권한을 확인하세요")
        logger.error("  4. 네트워크 연결을 확인하세요")
        return 1
    
    finally:
        logger.info("=" * 70)
        logger.info(f"종료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)


