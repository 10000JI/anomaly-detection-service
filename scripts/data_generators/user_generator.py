"""
사용자 프로필 데이터 생성기
"""

import json
import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from .data_templates import KOREAN_LAST_NAMES, KOREAN_FIRST_NAMES, ALL_GENRES

logger = logging.getLogger(__name__)


class UserGenerator:
    """
    사용자 프로필 데이터 생성기
    
    100명의 사용자를 VIP(20%), Regular(50%), New(30%) 세그먼트로 생성합니다.
    
    Attributes:
        count: 생성할 사용자 수
        seed: 랜덤 시드 (재현성을 위해)
    """
    
    def __init__(self, count: int = 100, seed: Optional[int] = 42):
        """
        UserGenerator 초기화
        
        Args:
            count: 생성할 사용자 수 (기본값: 100)
            seed: 랜덤 시드 (기본값: 42)
        """
        self.count = count
        self.seed = seed
        self.users: List[Dict] = []
        
        if seed is not None:
            random.seed(seed)
    
    def _generate_date(self, start_year: int, end_year: int) -> str:
        """
        랜덤 날짜 생성
        
        Args:
            start_year: 시작 연도
            end_year: 종료 연도
            
        Returns:
            날짜 문자열 (YYYY-MM-DD)
        """
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)
        
        days_between = (end_date - start_date).days
        random_days = random.randint(0, days_between)
        
        random_date = start_date + timedelta(days=random_days)
        return random_date.strftime('%Y-%m-%d')
    
    def _generate_korean_name(self) -> str:
        """
        한글 이름 생성
        
        Returns:
            한글 이름
        """
        last_name = random.choice(KOREAN_LAST_NAMES)
        first_name = random.choice(KOREAN_FIRST_NAMES)
        return f"{last_name}{first_name}"
    
    def _generate_vip_user(self, user_id: int) -> Dict:
        """
        VIP 세그먼트 사용자 생성
        
        Args:
            user_id: 사용자 ID 번호
            
        Returns:
            사용자 프로필 딕셔너리
        """
        return {
            'user_id': f'user-{user_id:05d}',
            'user_segment': 'VIP',
            'signup_date': self._generate_date(2020, 2023),
            'total_purchases': random.randint(30, 100),
            'total_spent': round(random.uniform(1000, 5000), 2),
            'favorite_categories': json.dumps(
                random.sample(ALL_GENRES, random.randint(3, 5)),
                ensure_ascii=False
            )
        }
    
    def _generate_regular_user(self, user_id: int) -> Dict:
        """
        Regular 세그먼트 사용자 생성
        
        Args:
            user_id: 사용자 ID 번호
            
        Returns:
            사용자 프로필 딕셔너리
        """
        return {
            'user_id': f'user-{user_id:05d}',
            'user_segment': 'Regular',
            'signup_date': self._generate_date(2022, 2024),
            'total_purchases': random.randint(5, 30),
            'total_spent': round(random.uniform(100, 1000), 2),
            'favorite_categories': json.dumps(
                random.sample(ALL_GENRES, random.randint(2, 3)),
                ensure_ascii=False
            )
        }
    
    def _generate_new_user(self, user_id: int) -> Dict:
        """
        New 세그먼트 사용자 생성
        
        Args:
            user_id: 사용자 ID 번호
            
        Returns:
            사용자 프로필 딕셔너리
        """
        return {
            'user_id': f'user-{user_id:05d}',
            'user_segment': 'New',
            'signup_date': self._generate_date(2024, 2025),
            'total_purchases': random.randint(0, 5),
            'total_spent': round(random.uniform(0, 100), 2),
            'favorite_categories': json.dumps(
                random.sample(ALL_GENRES, random.randint(1, 2)),
                ensure_ascii=False
            )
        }
    
    def generate(self) -> List[Dict]:
        """
        사용자 프로필 데이터 생성
        
        Returns:
            사용자 프로필 리스트
        """
        logger.info(f"🧑 사용자 프로필 {self.count}명 생성 시작...")
        
        users = []
        user_id = 1
        
        # VIP 사용자 생성 (20%)
        vip_count = int(self.count * 0.2)
        logger.info(f"  - VIP 세그먼트: {vip_count}명")
        for _ in range(vip_count):
            users.append(self._generate_vip_user(user_id))
            user_id += 1
        
        # Regular 사용자 생성 (50%)
        regular_count = int(self.count * 0.5)
        logger.info(f"  - Regular 세그먼트: {regular_count}명")
        for _ in range(regular_count):
            users.append(self._generate_regular_user(user_id))
            user_id += 1
        
        # New 사용자 생성 (나머지)
        new_count = self.count - vip_count - regular_count
        logger.info(f"  - New 세그먼트: {new_count}명")
        for _ in range(new_count):
            users.append(self._generate_new_user(user_id))
            user_id += 1
        
        self.users = users
        logger.info(f"✅ 사용자 프로필 {len(users)}명 생성 완료")
        
        return users
    
    def save_to_json(self, filepath: str) -> None:
        """
        생성된 데이터를 JSON 파일로 저장
        
        Args:
            filepath: 저장할 파일 경로
        """
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.users, f, ensure_ascii=False, indent=2)
            logger.info(f"✅ 사용자 데이터 JSON 저장 완료: {filepath}")
        except Exception as e:
            logger.error(f"❌ JSON 저장 실패: {e}")
            raise
    
    def insert_to_mysql(self, mysql_client) -> int:
        """
        생성된 데이터를 MySQL에 삽입
        
        Args:
            mysql_client: MySQLClient 인스턴스
            
        Returns:
            삽입된 레코드 수
        """
        if not self.users:
            logger.warning("⚠️  생성된 사용자 데이터가 없습니다. generate()를 먼저 호출하세요.")
            return 0
        
        try:
            logger.info(f"💾 MySQL에 사용자 프로필 {len(self.users)}명 삽입 시작...")
            
            # 배치 삽입을 위한 쿼리
            insert_query = """
                INSERT INTO user_profiles 
                (user_id, user_segment, signup_date, total_purchases, total_spent, favorite_categories)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    user_segment = VALUES(user_segment),
                    signup_date = VALUES(signup_date),
                    total_purchases = VALUES(total_purchases),
                    total_spent = VALUES(total_spent),
                    favorite_categories = VALUES(favorite_categories)
            """
            
            inserted_count = 0
            
            # 배치 크기 (한 번에 10개씩)
            batch_size = 10
            
            for i in range(0, len(self.users), batch_size):
                batch = self.users[i:i + batch_size]
                
                with mysql_client.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    for user in batch:
                        cursor.execute(insert_query, (
                            user['user_id'],
                            user['user_segment'],
                            user['signup_date'],
                            user['total_purchases'],
                            user['total_spent'],
                            user['favorite_categories']
                        ))
                        inserted_count += 1
                    
                    conn.commit()
                    cursor.close()
                
                # 진행 상황 로깅
                if (i + batch_size) % 50 == 0 or (i + batch_size) >= len(self.users):
                    logger.info(f"  진행: {min(i + batch_size, len(self.users))}/{len(self.users)} 삽입 완료")
            
            logger.info(f"✅ MySQL에 사용자 프로필 {inserted_count}명 삽입 완료")
            return inserted_count
            
        except Exception as e:
            logger.error(f"❌ MySQL 삽입 실패: {e}")
            raise
    
    def get_statistics(self) -> Dict:
        """
        생성된 데이터의 통계 정보 반환
        
        Returns:
            통계 딕셔너리
        """
        if not self.users:
            return {}
        
        segments = {}
        total_purchases = 0
        total_spent = 0
        
        for user in self.users:
            segment = user['user_segment']
            segments[segment] = segments.get(segment, 0) + 1
            total_purchases += user['total_purchases']
            total_spent += user['total_spent']
        
        return {
            'total_users': len(self.users),
            'segments': segments,
            'avg_purchases': round(total_purchases / len(self.users), 2),
            'avg_spent': round(total_spent / len(self.users), 2),
            'total_spent': round(total_spent, 2)
        }


