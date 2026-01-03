"""
샘플 데이터 생성 및 MySQL 삽입 스크립트

사용자 프로필 100명과 콘텐츠 1,000개를 생성하여 MySQL에 삽입합니다.
"""

import sys
import os
import argparse
import logging
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_generators import UserGenerator, ContentGenerator
from src.storage import get_mysql_client

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            f'sample_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )
    ]
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """명령줄 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='샘플 데이터 생성 및 MySQL 삽입 스크립트'
    )
    
    parser.add_argument(
        '--users',
        type=int,
        default=100,
        help='생성할 사용자 수 (기본값: 100)'
    )
    
    parser.add_argument(
        '--movies',
        type=int,
        default=600,
        help='생성할 영화 수 (기본값: 600)'
    )
    
    parser.add_argument(
        '--series',
        type=int,
        default=300,
        help='생성할 드라마 수 (기본값: 300)'
    )
    
    parser.add_argument(
        '--docs',
        type=int,
        default=100,
        help='생성할 다큐멘터리 수 (기본값: 100)'
    )
    
    parser.add_argument(
        '--json-only',
        action='store_true',
        help='JSON 파일로만 저장 (MySQL 삽입 안 함)'
    )
    
    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='랜덤 시드 (기본값: 42)'
    )
    
    return parser.parse_args()


def print_banner():
    """배너 출력"""
    logger.info("=" * 70)
    logger.info("🎲 샘플 데이터 생성 스크립트")
    logger.info("=" * 70)
    logger.info(f"실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")


def generate_users(user_count: int, seed: int, json_only: bool, mysql_client) -> bool:
    """
    사용자 프로필 생성
    
    Args:
        user_count: 생성할 사용자 수
        seed: 랜덤 시드
        json_only: JSON 전용 모드
        mysql_client: MySQL 클라이언트
        
    Returns:
        성공 여부
    """
    try:
        logger.info("Step 1: 사용자 프로필 데이터 생성")
        logger.info("-" * 70)
        
        # 사용자 생성기 초기화
        user_gen = UserGenerator(count=user_count, seed=seed)
        
        # 사용자 생성
        users = user_gen.generate()
        
        # JSON 저장
        user_gen.save_to_json('data/users.json')
        
        # 통계 출력
        stats = user_gen.get_statistics()
        logger.info("")
        logger.info("📊 사용자 프로필 통계:")
        logger.info(f"  - 전체 사용자: {stats['total_users']}명")
        for segment, count in stats['segments'].items():
            percentage = (count / stats['total_users']) * 100
            logger.info(f"  - {segment}: {count}명 ({percentage:.1f}%)")
        logger.info(f"  - 평균 구매 횟수: {stats['avg_purchases']}회")
        logger.info(f"  - 평균 구매 금액: {stats['avg_spent']}원")
        logger.info(f"  - 총 구매 금액: {stats['total_spent']}원")
        logger.info("")
        
        # MySQL 삽입
        if not json_only:
            user_gen.insert_to_mysql(mysql_client)
        
        logger.info("-" * 70)
        logger.info("")
        return True
        
    except Exception as e:
        logger.error(f"❌ 사용자 프로필 생성 실패: {e}")
        return False


def generate_contents(
    movie_count: int,
    series_count: int,
    doc_count: int,
    seed: int,
    json_only: bool,
    mysql_client
) -> bool:
    """
    콘텐츠 데이터 생성
    
    Args:
        movie_count: 영화 개수
        series_count: 드라마 개수
        doc_count: 다큐멘터리 개수
        seed: 랜덤 시드
        json_only: JSON 전용 모드
        mysql_client: MySQL 클라이언트
        
    Returns:
        성공 여부
    """
    try:
        logger.info("Step 2: 콘텐츠 데이터 생성")
        logger.info("-" * 70)
        
        # 콘텐츠 생성기 초기화
        content_gen = ContentGenerator(
            movie_count=movie_count,
            series_count=series_count,
            documentary_count=doc_count,
            seed=seed
        )
        
        # 콘텐츠 생성
        contents = content_gen.generate()
        
        # JSON 저장
        content_gen.save_to_json('data/contents.json')
        
        # 통계 출력
        stats = content_gen.get_statistics()
        logger.info("")
        logger.info("📊 콘텐츠 통계:")
        logger.info(f"  - 전체 콘텐츠: {stats['total_contents']}개")
        for content_type, count in stats['types'].items():
            percentage = (count / stats['total_contents']) * 100
            logger.info(f"  - {content_type}: {count}개 ({percentage:.1f}%)")
        logger.info(f"  - 평균 평점: {stats['avg_rating']}/5.0")
        logger.info(f"  - 평균 재생시간: {stats['avg_duration']}분")
        logger.info("")
        logger.info("  상위 5개 장르:")
        for genre, count in stats['top_5_genres'].items():
            logger.info(f"    - {genre}: {count}개")
        logger.info("")
        
        # MySQL 삽입
        if not json_only:
            content_gen.insert_to_mysql(mysql_client)
        
        logger.info("-" * 70)
        logger.info("")
        return True
        
    except Exception as e:
        logger.error(f"❌ 콘텐츠 데이터 생성 실패: {e}")
        return False


def verify_data(mysql_client) -> None:
    """
    MySQL 데이터 검증
    
    Args:
        mysql_client: MySQL 클라이언트
    """
    try:
        logger.info("Step 3: 데이터 검증")
        logger.info("-" * 70)
        
        # 사용자 수 확인
        user_count_query = "SELECT COUNT(*) as count FROM user_profiles"
        user_result = mysql_client.fetch_one(user_count_query)
        user_count = user_result['count'] if user_result else 0
        
        # 세그먼트별 사용자 수
        segment_query = """
            SELECT user_segment, COUNT(*) as count 
            FROM user_profiles 
            GROUP BY user_segment
        """
        segments = mysql_client.fetch_all(segment_query)
        
        # 콘텐츠 수 확인
        content_count_query = "SELECT COUNT(*) as count FROM contents"
        content_result = mysql_client.fetch_one(content_count_query)
        content_count = content_result['count'] if content_result else 0
        
        # 콘텐츠 타입별 수
        type_query = """
            SELECT content_type, COUNT(*) as count 
            FROM contents 
            GROUP BY content_type
        """
        types = mysql_client.fetch_all(type_query)
        
        logger.info("✅ MySQL 데이터 검증 결과:")
        logger.info(f"  - 사용자 프로필: {user_count}명")
        for segment_data in segments:
            logger.info(f"    - {segment_data['user_segment']}: {segment_data['count']}명")
        
        logger.info(f"  - 콘텐츠: {content_count}개")
        for type_data in types:
            logger.info(f"    - {type_data['content_type']}: {type_data['count']}개")
        
        logger.info("-" * 70)
        logger.info("")
        
    except Exception as e:
        logger.error(f"❌ 데이터 검증 실패: {e}")


def main():
    """메인 함수"""
    print_banner()
    
    # 명령줄 인자 파싱
    args = parse_arguments()
    
    logger.info("📋 생성 설정:")
    logger.info(f"  - 사용자: {args.users}명")
    logger.info(f"  - 영화: {args.movies}개")
    logger.info(f"  - 드라마: {args.series}개")
    logger.info(f"  - 다큐멘터리: {args.docs}개")
    logger.info(f"  - 랜덤 시드: {args.seed}")
    logger.info(f"  - JSON 전용 모드: {args.json_only}")
    logger.info("")
    
    mysql_client = None
    
    try:
        # data 폴더 생성
        os.makedirs('data', exist_ok=True)
        
        # MySQL 클라이언트 연결 (JSON 전용 모드가 아닐 때만)
        if not args.json_only:
            logger.info("MySQL 클라이언트 연결 중...")
            mysql_client = get_mysql_client()
            logger.info(f"✅ MySQL 연결 성공 ({mysql_client.config.host}:{mysql_client.config.port})")
            logger.info("")
        
        # 1. 사용자 프로필 생성
        success = generate_users(args.users, args.seed, args.json_only, mysql_client)
        if not success:
            return 1
        
        # 2. 콘텐츠 데이터 생성
        success = generate_contents(
            args.movies,
            args.series,
            args.docs,
            args.seed,
            args.json_only,
            mysql_client
        )
        if not success:
            return 1
        
        # 3. 데이터 검증 (MySQL 모드일 때만)
        if not args.json_only and mysql_client:
            verify_data(mysql_client)
        
        # 최종 결과
        logger.info("=" * 70)
        logger.info("🎉 샘플 데이터 생성 완료!")
        logger.info("=" * 70)
        logger.info("")
        logger.info("생성된 파일:")
        logger.info("  - data/users.json (사용자 프로필)")
        logger.info("  - data/contents.json (콘텐츠 데이터)")
        logger.info("")
        
        if not args.json_only:
            logger.info("MySQL 데이터베이스:")
            logger.info("  - user_profiles 테이블 업데이트 완료")
            logger.info("  - contents 테이블 업데이트 완료")
            logger.info("")
        
        logger.info("다음 단계:")
        logger.info("  1. Kafka Producer 구현: Task 004")
        logger.info("  2. 이벤트 시뮬레이터 시작")
        logger.info("  3. 추천 알고리즘 개발")
        logger.info("")
        
        return 0
        
    except Exception as e:
        logger.error("=" * 70)
        logger.error("❌ 샘플 데이터 생성 중 오류 발생")
        logger.error("=" * 70)
        logger.error(f"오류 메시지: {e}")
        logger.exception("상세 오류:")
        return 1
    
    finally:
        logger.info("=" * 70)
        logger.info(f"종료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)


