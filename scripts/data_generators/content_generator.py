"""
콘텐츠 데이터 생성기
"""

import json
import random
import logging
from typing import List, Dict, Optional
from .data_templates import (
    MOVIE_TITLES, MOVIE_SUFFIXES, MOVIE_GENRES, MOVIE_SUB_GENRES,
    SERIES_TITLES, SERIES_SUFFIXES, SERIES_GENRES, SERIES_SUB_GENRES,
    DOCUMENTARY_TITLES, DOCUMENTARY_SUFFIXES, DOCUMENTARY_GENRES, DOCUMENTARY_SUB_GENRES
)

logger = logging.getLogger(__name__)


class ContentGenerator:
    """
    콘텐츠 데이터 생성기
    
    영화, 드라마, 다큐멘터리 데이터를 생성합니다.
    
    Attributes:
        movie_count: 영화 개수
        series_count: 드라마 개수
        documentary_count: 다큐멘터리 개수
        seed: 랜덤 시드
    """
    
    def __init__(
        self,
        movie_count: int = 600,
        series_count: int = 300,
        documentary_count: int = 100,
        seed: Optional[int] = 42
    ):
        """
        ContentGenerator 초기화
        
        Args:
            movie_count: 생성할 영화 개수 (기본값: 600)
            series_count: 생성할 드라마 개수 (기본값: 300)
            documentary_count: 생성할 다큐멘터리 개수 (기본값: 100)
            seed: 랜덤 시드 (기본값: 42)
        """
        self.movie_count = movie_count
        self.series_count = series_count
        self.documentary_count = documentary_count
        self.seed = seed
        self.contents: List[Dict] = []
        
        if seed is not None:
            random.seed(seed)
    
    def _select_genre_by_distribution(self, genre_dist: dict) -> str:
        """
        분포에 따라 장르 선택
        
        Args:
            genre_dist: 장르 분포 딕셔너리 {장르: 비율}
            
        Returns:
            선택된 장르
        """
        genres = list(genre_dist.keys())
        weights = list(genre_dist.values())
        return random.choices(genres, weights=weights)[0]
    
    def _generate_movie(self, movie_id: int) -> Dict:
        """
        영화 데이터 생성
        
        Args:
            movie_id: 영화 ID 번호
            
        Returns:
            영화 데이터 딕셔너리
        """
        title_base = random.choice(MOVIE_TITLES)
        
        # 50% 확률로 접미사 추가
        if random.random() < 0.5:
            title = f"{title_base} {random.choice(MOVIE_SUFFIXES)}"
        else:
            title = title_base
        
        genre = self._select_genre_by_distribution(MOVIE_GENRES)
        sub_genre = random.choice(MOVIE_SUB_GENRES)
        
        return {
            'content_id': f'movie-{movie_id:05d}',
            'title': title,
            'content_type': 'movie',
            'genre': genre,
            'sub_genre': sub_genre,
            'duration_minutes': random.randint(80, 200),
            'release_year': random.randint(1990, 2025),
            'rating': round(random.uniform(1.0, 5.0), 1),
            'review_count': random.randint(10, 50000)
        }
    
    def _generate_series(self, series_id: int) -> Dict:
        """
        드라마 데이터 생성
        
        Args:
            series_id: 드라마 ID 번호
            
        Returns:
            드라마 데이터 딕셔너리
        """
        title_base = random.choice(SERIES_TITLES)
        
        # 40% 확률로 접미사 추가
        if random.random() < 0.4:
            title = f"{title_base} {random.choice(SERIES_SUFFIXES)}"
        else:
            title = title_base
        
        genre = self._select_genre_by_distribution(SERIES_GENRES)
        sub_genre = random.choice(SERIES_SUB_GENRES)
        
        return {
            'content_id': f'series-{series_id:05d}',
            'title': title,
            'content_type': 'series',
            'genre': genre,
            'sub_genre': sub_genre,
            'duration_minutes': random.randint(30, 60),  # 에피소드당 시간
            'release_year': random.randint(2010, 2025),
            'rating': round(random.uniform(2.0, 5.0), 1),
            'review_count': random.randint(50, 30000)
        }
    
    def _generate_documentary(self, doc_id: int) -> Dict:
        """
        다큐멘터리 데이터 생성
        
        Args:
            doc_id: 다큐멘터리 ID 번호
            
        Returns:
            다큐멘터리 데이터 딕셔너리
        """
        title_base = random.choice(DOCUMENTARY_TITLES)
        title = f"{title_base} {random.choice(DOCUMENTARY_SUFFIXES)}"
        
        genre = self._select_genre_by_distribution(DOCUMENTARY_GENRES)
        sub_genre = random.choice(DOCUMENTARY_SUB_GENRES)
        
        return {
            'content_id': f'doc-{doc_id:05d}',
            'title': title,
            'content_type': 'documentary',
            'genre': genre,
            'sub_genre': sub_genre,
            'duration_minutes': random.randint(40, 120),
            'release_year': random.randint(2000, 2025),
            'rating': round(random.uniform(2.5, 5.0), 1),
            'review_count': random.randint(10, 10000)
        }
    
    def generate(self) -> List[Dict]:
        """
        전체 콘텐츠 데이터 생성
        
        Returns:
            콘텐츠 리스트
        """
        total_count = self.movie_count + self.series_count + self.documentary_count
        logger.info(f"🎬 콘텐츠 데이터 {total_count}개 생성 시작...")
        
        contents = []
        
        # 영화 생성
        logger.info(f"  - 영화: {self.movie_count}개")
        for i in range(1, self.movie_count + 1):
            contents.append(self._generate_movie(i))
        
        # 드라마 생성
        logger.info(f"  - 드라마: {self.series_count}개")
        for i in range(1, self.series_count + 1):
            contents.append(self._generate_series(i))
        
        # 다큐멘터리 생성
        logger.info(f"  - 다큐멘터리: {self.documentary_count}개")
        for i in range(1, self.documentary_count + 1):
            contents.append(self._generate_documentary(i))
        
        self.contents = contents
        logger.info(f"✅ 콘텐츠 데이터 {len(contents)}개 생성 완료")
        
        return contents
    
    def save_to_json(self, filepath: str) -> None:
        """
        생성된 데이터를 JSON 파일로 저장
        
        Args:
            filepath: 저장할 파일 경로
        """
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.contents, f, ensure_ascii=False, indent=2)
            logger.info(f"✅ 콘텐츠 데이터 JSON 저장 완료: {filepath}")
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
        if not self.contents:
            logger.warning("⚠️  생성된 콘텐츠 데이터가 없습니다. generate()를 먼저 호출하세요.")
            return 0
        
        try:
            logger.info(f"💾 MySQL에 콘텐츠 {len(self.contents)}개 삽입 시작...")
            
            # 배치 삽입을 위한 쿼리
            insert_query = """
                INSERT INTO contents 
                (content_id, title, content_type, genre, sub_genre, 
                 duration_minutes, release_year, rating, review_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    content_type = VALUES(content_type),
                    genre = VALUES(genre),
                    sub_genre = VALUES(sub_genre),
                    duration_minutes = VALUES(duration_minutes),
                    release_year = VALUES(release_year),
                    rating = VALUES(rating),
                    review_count = VALUES(review_count)
            """
            
            inserted_count = 0
            
            # 배치 크기 (한 번에 20개씩)
            batch_size = 20
            
            for i in range(0, len(self.contents), batch_size):
                batch = self.contents[i:i + batch_size]
                
                with mysql_client.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    for content in batch:
                        cursor.execute(insert_query, (
                            content['content_id'],
                            content['title'],
                            content['content_type'],
                            content['genre'],
                            content['sub_genre'],
                            content['duration_minutes'],
                            content['release_year'],
                            content['rating'],
                            content['review_count']
                        ))
                        inserted_count += 1
                    
                    conn.commit()
                    cursor.close()
                
                # 진행 상황 로깅 (100개마다)
                if (i + batch_size) % 100 == 0 or (i + batch_size) >= len(self.contents):
                    logger.info(f"  진행: {min(i + batch_size, len(self.contents))}/{len(self.contents)} 삽입 완료")
            
            logger.info(f"✅ MySQL에 콘텐츠 {inserted_count}개 삽입 완료")
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
        if not self.contents:
            return {}
        
        types = {}
        genres = {}
        total_rating = 0
        total_duration = 0
        
        for content in self.contents:
            # 콘텐츠 타입별 카운트
            content_type = content['content_type']
            types[content_type] = types.get(content_type, 0) + 1
            
            # 장르별 카운트
            genre = content['genre']
            genres[genre] = genres.get(genre, 0) + 1
            
            # 평균 계산용
            total_rating += content['rating']
            total_duration += content['duration_minutes']
        
        return {
            'total_contents': len(self.contents),
            'types': types,
            'top_5_genres': dict(sorted(genres.items(), key=lambda x: x[1], reverse=True)[:5]),
            'avg_rating': round(total_rating / len(self.contents), 2),
            'avg_duration': round(total_duration / len(self.contents), 1)
        }


