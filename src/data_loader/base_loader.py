# src/data_loader/base_loader.py

from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseDataLoader(ABC):
    """
    모든 데이터 로더가 상속받아야 하는 추상 기본 클래스입니다.
    모든 로더가 동일한 인터페이스를 갖도록 보장합니다.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.dataset_config = config['dataset']

    @abstractmethod
    def load_data(self) -> List[Dict[str, Any]]:
        """데이터셋을 로드하여 표준화된 포맷의 리스트로 반환합니다."""
        pass

    @abstractmethod
    def get_db_path(self, db_id: str) -> str:
        """db_id에 해당하는 실제 SQLite 파일 경로를 반환합니다."""
        pass
