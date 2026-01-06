"""
pytest 설정 파일

- 커스텀 마커 등록
- 공통 fixture 정의
"""

import pytest


def pytest_configure(config):
    """pytest 설정"""
    # 커스텀 마커 등록
    config.addinivalue_line(
        "markers", "integration: 통합 테스트 (실제 외부 서비스 사용)"
    )
    config.addinivalue_line(
        "markers", "spark: PySpark 관련 테스트"
    )
    config.addinivalue_line(
        "markers", "unit: 단위 테스트"
    )


