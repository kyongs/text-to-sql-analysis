# src/prompt_builder/builder.py

from typing import Optional, List
from src.utils import const

def build_prompt(
    schema: str,
    question: str,
    db_name: Optional[str] = None,
    db_type: str = 'sqlite',
    hints: Optional[str] = "",
    use_tools: bool = False,
    enabled_tools: Optional[List[str]] = None

) -> str:
    """
    Generates a detailed SQL prompt for the LLM by formatting the PROMPT_TEMPLATE
    with the provided components and constants.

    Args:
        schema: Database schema string
        question: Natural language question
        db_name: Database name (optional)
        db_type: Database type ('sqlite' or 'mysql')
        hints: Additional hints for query generation
        use_tools: Whether tool calling is enabled
        enabled_tools: List of enabled tool names (e.g., ['join_inspector', 'join_path_finder'])
    """
    sql_dialect = "SQLite"
    if db_type.lower() == 'mysql':
        sql_dialect = "MySQL"

    # Build base prompt
    base_prompt = const.PROMPT_TEMPLATE

    # If tools are enabled, dynamically add tool guidelines
    if use_tools and enabled_tools:
        tool_guidelines = const.build_tool_guidelines(enabled_tools)

        # Insert tool guidelines after System Instructions
        prompt_parts = base_prompt.split("### Schema ###")
        system_section = prompt_parts[0].rstrip()
        rest = "### Schema ###" + prompt_parts[1]

        base_prompt = f"{system_section}\n\n{tool_guidelines}\n\nDo not add any explanations other than the final SQL query.\n\n{rest}"

    prompt = base_prompt.format(
        sql_dialect=sql_dialect,
        schema=schema,
        question=question,
        hints=hints if hints else "# No hints provided."
    )

    return prompt