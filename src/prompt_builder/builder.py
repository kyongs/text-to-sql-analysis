# src/prompt_builder/builder.py

from typing import Optional
from src.utils import const 

def build_prompt(
    schema: str,
    question: str,
    db_name: Optional[str] = None,
    db_type: str = 'sqlite', 
    hints: Optional[str] = ""

) -> str:
    """
    Generates a detailed SQL prompt for the LLM by formatting the PROMPT_TEMPLATE
    with the provided components and constants.
    """
    sql_dialect = "SQLite"
    if db_type.lower() == 'mysql':
        sql_dialect = "MySQL"
    
    prompt = const.PROMPT_TEMPLATE.format(
        sql_dialect=sql_dialect,
        schema=schema,
        question=question,
        hints=hints if hints else "# No hints provided."
    )
    
    return prompt