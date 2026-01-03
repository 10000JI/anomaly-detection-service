"""
Kafka Producer - 사용자 이벤트 시뮬레이터

- data/users.json, data/contents.json 기반으로 랜덤 사용자 행동 이벤트 생성
- .env의 Kafka 설정(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)을 사용하여 Kafka 토픽으로 전송

실행 예시:
  python src/kafka/producer.py --dry-run --events 10
  python src/kafka/producer.py --events 100 --min-interval-ms 100 --max-interval-ms 800
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from uuid import uuid4

from kafka import KafkaProducer


def _ensure_project_root_on_syspath() -> None:
    """
    `python src/kafka/producer.py` 형태로 실행해도 프로젝트 루트 import가 되도록 보정합니다.
    """

    # 현재 파일: <root>/src/kafka/producer.py
    project_root = Path(__file__).resolve().parents[2]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))


_ensure_project_root_on_syspath()


try:
    from config import get_kafka_config  # type: ignore
except Exception:  # pragma: no cover
    # config 패키지가 없거나 로딩 실패 시에도 드라이런은 가능하도록 처리
    get_kafka_config = None  # type: ignore


logger = logging.getLogger(__name__)


class EventType(str, Enum):
    """이벤트 타입 (ai_docs/task_template.md 기준)"""

    CLICK = "click"
    WATCH = "watch"
    WATCHLIST = "watchlist"
    WATCH_COMPLETE = "watch_complete"
    RATING = "rating"


class ContentType(str, Enum):
    """콘텐츠 타입 (ai_docs/task_template.md 기준)"""

    MOVIE = "movie"
    SERIES = "series"
    DOCUMENTARY = "documentary"


@dataclass(frozen=True)
class User:
    user_id: str
    user_segment: str
    favorite_categories: List[str]


@dataclass(frozen=True)
class Content:
    content_id: str
    title: str
    content_type: str
    genre: str
    sub_genre: str
    duration_minutes: int
    release_year: int
    rating: float
    review_count: int


@dataclass
class SessionState:
    session_id: str
    last_event_ts: datetime
    last_content_id: Optional[str] = None
    last_watch_minutes: int = 0


def _parse_favorite_categories(value: Any) -> List[str]:
    """
    users.json의 favorite_categories가 리스트 또는 JSON 문자열인 경우를 모두 지원합니다.
    예) "[\"SF\", \"역사\"]" 형태의 문자열
    """

    if value is None:
        return []
    if isinstance(value, list):
        return [str(x) for x in value if str(x).strip()]
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []
        # "['a','b']" 가 아닌 JSON 형태("[...]" 또는 "\"[...]\"")가 섞여 있는 케이스를 처리
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [str(x) for x in parsed if str(x).strip()]
            if isinstance(parsed, str):
                parsed2 = json.loads(parsed)
                if isinstance(parsed2, list):
                    return [str(x) for x in parsed2 if str(x).strip()]
        except Exception:
            return []
    return []


def load_users(path: str | Path) -> List[User]:
    """`data/users.json` 로드"""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"users.json 파일을 찾을 수 없습니다: {p}")

    with p.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, list):
        raise ValueError("users.json의 최상위 구조는 list여야 합니다.")

    users: List[User] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        user_id = str(row.get("user_id", "")).strip()
        if not user_id:
            continue
        users.append(
            User(
                user_id=user_id,
                user_segment=str(row.get("user_segment", "Unknown")).strip() or "Unknown",
                favorite_categories=_parse_favorite_categories(row.get("favorite_categories")),
            )
        )

    if not users:
        raise ValueError("users.json에서 유효한 사용자를 로드하지 못했습니다.")
    return users


def load_contents(path: str | Path) -> List[Content]:
    """`data/contents.json` 로드"""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"contents.json 파일을 찾을 수 없습니다: {p}")

    with p.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, list):
        raise ValueError("contents.json의 최상위 구조는 list여야 합니다.")

    contents: List[Content] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        content_id = str(row.get("content_id", "")).strip()
        if not content_id:
            continue
        try:
            contents.append(
                Content(
                    content_id=content_id,
                    title=str(row.get("title", "")),
                    content_type=str(row.get("content_type", "")),
                    genre=str(row.get("genre", "")),
                    sub_genre=str(row.get("sub_genre", "")),
                    duration_minutes=int(row.get("duration_minutes", 0) or 0),
                    release_year=int(row.get("release_year", 0) or 0),
                    rating=float(row.get("rating", 0.0) or 0.0),
                    review_count=int(row.get("review_count", 0) or 0),
                )
            )
        except Exception:
            continue

    if not contents:
        raise ValueError("contents.json에서 유효한 콘텐츠를 로드하지 못했습니다.")
    return contents


def _ab_test_group_for_user(user_id: str) -> str:
    # 안정적인 그룹 할당(간단 버전)
    return "A" if (sum(ord(c) for c in user_id) % 2 == 0) else "B"


def _weighted_random_user(users: List[User]) -> User:
    # VIP 사용자가 좀 더 자주 이벤트를 발생시키도록 가중치 부여(현실성)
    weights = []
    for u in users:
        seg = (u.user_segment or "").lower()
        if "vip" in seg:
            weights.append(3.0)
        elif "regular" in seg:
            weights.append(2.0)
        else:
            weights.append(1.0)
    return random.choices(users, weights=weights, k=1)[0]


def _pick_content_for_user(user: User, contents: List[Content]) -> Content:
    if user.favorite_categories:
        # 선호 장르 기반 선택(확률적으로)
        if random.random() < 0.7:
            preferred = set(user.favorite_categories)
            matches = [c for c in contents if c.genre in preferred]
            if matches:
                return random.choice(matches)
    return random.choice(contents)


def _choose_event_type(prev: Optional[EventType]) -> EventType:
    """
    간단한 상태 기반 분포:
    - 직전 WATCH면 WATCH_COMPLETE 확률을 높임
    - 그 외에는 기본 분포
    """
    if prev == EventType.WATCH and random.random() < 0.25:
        return EventType.WATCH_COMPLETE

    r = random.random()
    if r < 0.40:
        return EventType.CLICK
    if r < 0.75:
        return EventType.WATCH
    if r < 0.85:
        return EventType.WATCHLIST
    if r < 0.95:
        return EventType.WATCH_COMPLETE
    return EventType.RATING


def _iso_utc(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def generate_user_event(
    user: User,
    content: Content,
    event_type: EventType,
    session_id: str,
    ts: datetime,
    session_state: Optional[SessionState] = None,
) -> Dict[str, Any]:
    """
    ai_docs/task_template.md의 UserEvent 스키마와 호환되는 JSON 이벤트 생성.
    """

    duration = max(0, int(content.duration_minutes))

    watched_minutes = 0
    metadata: Dict[str, Any] = {
        "user_segment": user.user_segment,
        "ab_test_group": _ab_test_group_for_user(user.user_id),
        "content_type": content.content_type,
        "title": content.title,
        "sub_genre": content.sub_genre,
        "release_year": content.release_year,
        "source": random.choice(["home", "search", "recommendation"]),
        "device": random.choice(["mobile", "web", "tv"]),
    }

    if event_type == EventType.CLICK:
        watched_minutes = 0
        metadata.update(
            {
                "action": "open_detail",
            }
        )
    elif event_type == EventType.WATCH:
        if duration > 0:
            # 초반 이탈이 많도록 낮은 값 쪽으로 치우친 분포
            mode = min(duration, 10)
            watched_minutes = max(1, int(random.triangular(1, duration, mode)))
        metadata.update(
            {
                "playback_speed": random.choice([1.0, 1.0, 1.0, 1.25]),
                "quality": random.choice(["720p", "1080p", "4k"]),
            }
        )
    elif event_type == EventType.WATCH_COMPLETE:
        watched_minutes = duration
        metadata.update(
            {
                "completion": 1.0,
            }
        )
    elif event_type == EventType.WATCHLIST:
        watched_minutes = 0
        metadata.update(
            {
                "action": random.choice(["add", "remove"]),
            }
        )
    elif event_type == EventType.RATING:
        # 직전 시청 분량이 있으면 반영
        if session_state and session_state.last_content_id == content.content_id:
            watched_minutes = session_state.last_watch_minutes
        else:
            watched_minutes = 0
        metadata.update(
            {
                "rating": random.randint(1, 5),
            }
        )

    return {
        "event_id": str(uuid4()),
        "timestamp": _iso_utc(ts),
        "user_id": user.user_id,
        "session_id": session_id,
        "event_type": event_type.value,
        "content_id": content.content_id,
        "genre": content.genre,
        "duration_minutes": duration,
        "watched_minutes": int(watched_minutes),
        "metadata": metadata,
    }


def _new_session_id(user_id: str) -> str:
    return f"{user_id}-sess-{uuid4().hex[:12]}"


def _sleep_ms(ms: int) -> None:
    time.sleep(max(0, ms) / 1000.0)


def _build_kafka_producer(bootstrap_servers: List[str]) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=3,
        linger_ms=10,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else None,
    )


def run_simulator(
    *,
    users_path: Path,
    contents_path: Path,
    bootstrap_servers: List[str],
    topic: str,
    events: int,
    min_interval_ms: int,
    max_interval_ms: int,
    dry_run: bool,
    seed: Optional[int],
    flush_every: int,
) -> None:
    if seed is not None:
        random.seed(seed)

    users = load_users(users_path)
    contents = load_contents(contents_path)

    logger.info("샘플 데이터 로드 완료: users=%d, contents=%d", len(users), len(contents))
    logger.info("Kafka 설정: bootstrap_servers=%s, topic=%s, dry_run=%s", bootstrap_servers, topic, dry_run)

    producer: Optional[KafkaProducer] = None
    if not dry_run:
        producer = _build_kafka_producer(bootstrap_servers)

    session_by_user: Dict[str, SessionState] = {}
    prev_event_type_by_user: Dict[str, EventType] = {}

    sent = 0
    try:
        while True:
            if events > 0 and sent >= events:
                break

            user = _weighted_random_user(users)
            content = _pick_content_for_user(user, contents)

            now = datetime.now(timezone.utc)
            state = session_by_user.get(user.user_id)
            if state is None:
                state = SessionState(session_id=_new_session_id(user.user_id), last_event_ts=now)
                session_by_user[user.user_id] = state
            else:
                # 일정 시간 갭이 크면 새 세션 시작(3~10분 랜덤)
                gap_seconds = (now - state.last_event_ts).total_seconds()
                session_cut = random.randint(180, 600)
                if gap_seconds >= session_cut:
                    state.session_id = _new_session_id(user.user_id)

            prev = prev_event_type_by_user.get(user.user_id)
            event_type = _choose_event_type(prev)

            event = generate_user_event(
                user=user,
                content=content,
                event_type=event_type,
                session_id=state.session_id,
                ts=now,
                session_state=state,
            )

            # 세션 상태 업데이트
            state.last_event_ts = now
            state.last_content_id = content.content_id
            state.last_watch_minutes = int(event.get("watched_minutes") or 0)
            prev_event_type_by_user[user.user_id] = event_type

            if dry_run:
                logger.info("DRY_RUN event=%s", json.dumps(event, ensure_ascii=False))
            else:
                assert producer is not None
                future = producer.send(topic, key=user.user_id, value=event)
                # 개발/테스트 단계에서는 동기 확인이 디버깅에 유리
                _ = future.get(timeout=10)

            sent += 1
            if (not dry_run) and producer is not None and flush_every > 0 and (sent % flush_every == 0):
                producer.flush(timeout=10)
                logger.info("flush: sent=%d", sent)

            # 현실적인 간격
            if max_interval_ms < min_interval_ms:
                min_interval_ms, max_interval_ms = max_interval_ms, min_interval_ms
            interval = random.randint(min_interval_ms, max_interval_ms)
            _sleep_ms(interval)

    except KeyboardInterrupt:
        logger.info("중단 요청 감지(CTRL+C). 정상 종료를 시도합니다.")
    finally:
        if producer is not None:
            try:
                producer.flush(timeout=10)
            except Exception:
                pass
            try:
                producer.close(timeout=10)
            except Exception:
                pass
        logger.info("시뮬레이터 종료: sent=%d", sent)


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _resolve_kafka_settings(args: argparse.Namespace) -> Tuple[List[str], str]:
    """
    우선순위:
    1) CLI 옵션 (--bootstrap-servers, --topic)
    2) config.get_kafka_config() ('.env' 자동 로드)
    3) 환경 변수 직접 읽기 + 기본값(문서 기준)
    """
    if args.bootstrap_servers and args.topic:
        servers = [s.strip() for s in args.bootstrap_servers.split(",") if s.strip()]
        return servers, args.topic

    if get_kafka_config is not None:
        cfg = get_kafka_config()
        servers = cfg.get_bootstrap_servers_list()
        topic = cfg.topic
        # topic만 override 허용
        if args.topic:
            topic = args.topic
        if args.bootstrap_servers:
            servers = [s.strip() for s in args.bootstrap_servers.split(",") if s.strip()]
        return servers, topic

    # fallback (최소 드라이런은 가능하게)
    default_servers = "192.168.150.115:9092,192.168.150.120:9092,192.168.150.125:9092"
    default_topic = "user-events-topic"
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", default_servers)
    topic = os.getenv("KAFKA_TOPIC", default_topic)
    if args.bootstrap_servers:
        servers = args.bootstrap_servers
    if args.topic:
        topic = args.topic
    return [s.strip() for s in servers.split(",") if s.strip()], topic


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Kafka Producer - 사용자 이벤트 시뮬레이터")

    parser.add_argument("--users-path", default="data/users.json", help="샘플 사용자 JSON 경로")
    parser.add_argument("--contents-path", default="data/contents.json", help="샘플 콘텐츠 JSON 경로")

    parser.add_argument(
        "--bootstrap-servers",
        default="",
        help="Kafka bootstrap servers (comma-separated). 미지정 시 .env 설정 사용",
    )
    parser.add_argument("--topic", default="", help="Kafka topic. 미지정 시 .env 설정 사용")

    parser.add_argument("--events", type=int, default=0, help="전송할 총 이벤트 수 (0이면 무한)")
    parser.add_argument("--min-interval-ms", type=int, default=100, help="이벤트 간 최소 지연(ms)")
    parser.add_argument("--max-interval-ms", type=int, default=800, help="이벤트 간 최대 지연(ms)")
    parser.add_argument("--flush-every", type=int, default=100, help="N건마다 flush (0이면 비활성)")
    parser.add_argument("--dry-run", action="store_true", help="Kafka 전송 없이 이벤트를 로그로만 출력")

    parser.add_argument("--seed", type=int, default=None, help="재현 가능한 시뮬레이션을 위한 시드")
    parser.add_argument("--log-level", default="INFO", help="로그 레벨 (DEBUG/INFO/WARNING/ERROR)")

    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    _configure_logging(args.log_level)

    bootstrap_servers, topic = _resolve_kafka_settings(args)
    if not bootstrap_servers:
        logger.error("Kafka bootstrap servers가 비어 있습니다. (.env 또는 --bootstrap-servers 확인)")
        return 2
    if not topic:
        logger.error("Kafka topic이 비어 있습니다. (.env 또는 --topic 확인)")
        return 2

    run_simulator(
        users_path=Path(args.users_path),
        contents_path=Path(args.contents_path),
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        events=int(args.events),
        min_interval_ms=int(args.min_interval_ms),
        max_interval_ms=int(args.max_interval_ms),
        dry_run=bool(args.dry_run),
        seed=args.seed,
        flush_every=int(args.flush_every),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



