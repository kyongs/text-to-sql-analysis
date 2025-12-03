# src/evaluator/base_evaluator.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseEvaluator(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @abstractmethod
    def evaluate(self, predictions: List[Dict[str, Any]]) -> Dict[str, Any]:
        pass