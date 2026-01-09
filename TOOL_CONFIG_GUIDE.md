# Tool Calling Configuration Guide

## Overview
OpenAI Model with Tools supports dynamic enabling/disabling of individual tools through CLI arguments.

## CLI Arguments

### Individual Tool Flags
```bash
--join_inspector      # Enable inspect_join_relationship tool
--join_path_finder    # Enable find_join_path tool
```

## Available Tools

### 1. inspect_join_relationship (--join_inspector)
- Analyzes cardinality between two tables (1:1, 1:N, N:1, M:N)
- Returns row counts and sample data
- Helps identify data multiplication risks

### 2. find_join_path (--join_path_finder)
- Finds optimal JOIN paths between tables
- Prevents bridge table skipping
- Shows quality scores and sample data

## Usage Examples

### No Tools (Baseline)
```bash
python main.py --config configs/beaver_dw_openai_with_tools.yaml
```

### Only JOIN Inspector
```bash
python main.py --config configs/beaver_dw_openai_with_tools.yaml --join_inspector
```

### Only JOIN Path Finder
```bash
python main.py --config configs/beaver_dw_openai_with_tools.yaml --join_path_finder
```

### Both Tools (Full System)
```bash
python main.py --config configs/beaver_dw_openai_with_tools.yaml --join_inspector --join_path_finder
```

### With Test Mode (First 10 questions)
```bash
python main.py --config configs/beaver_dw_openai_with_tools.yaml \
    --join_inspector --join_path_finder --test_n 10
```

## Comparison Table

| Command | join_inspector | join_path_finder | Use Case |
|---------|---------------|------------------|----------|
| No flags | ❌ | ❌ | Baseline (no tools) |
| `--join_inspector` | ✅ | ❌ | Test cardinality analysis only |
| `--join_path_finder` | ❌ | ✅ | Test path finding only |
| `--join_inspector --join_path_finder` | ✅ | ✅ | Full system |

## Testing Strategy

1. **Baseline**: Run without any flags
   ```bash
   python main.py --config configs/beaver_dw_openai_with_tools.yaml --test_n 10
   ```

2. **Inspector Only**: Test cardinality analysis impact
   ```bash
   python main.py --config configs/beaver_dw_openai_with_tools.yaml --join_inspector --test_n 10
   ```

3. **Path Finder Only**: Test bridge table detection impact
   ```bash
   python main.py --config configs/beaver_dw_openai_with_tools.yaml --join_path_finder --test_n 10
   ```

4. **Full System**: Test combined effect
   ```bash
   python main.py --config configs/beaver_dw_openai_with_tools.yaml \
       --join_inspector --join_path_finder --test_n 10
   ```

## Notes

- Tools are **disabled by default** - you must explicitly enable them with flags
- Config files no longer control tool usage (use CLI flags instead)
- You can mix and match tools to test individual contributions
- All tool combinations work with the same `openai_with_tools` model
