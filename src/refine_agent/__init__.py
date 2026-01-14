# src/refine_agent/__init__.py
"""
Refine Agent - SQL 제출 후 자동/조건부로 실행되는 에이전트들

트리거 조건:
- syntax_fixer: SQL 실행 에러 발생 시
- empty_result_handler: SQL 실행 성공 but 결과 0행일 때
"""

from .syntax_fixer import analyze_sql_error, format_syntax_fix_advice
from .empty_result_handler import analyze_empty_result, format_empty_result_advice

__all__ = [
    'analyze_sql_error', 'format_syntax_fix_advice',
    'analyze_empty_result', 'format_empty_result_advice'
]
