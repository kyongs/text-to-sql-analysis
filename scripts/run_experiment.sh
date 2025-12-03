# ==========================================================
# scripts/run_experiment.sh
# (This script now ONLY runs the experiment)
# ==========================================================
#!/bin/bash

# Usage: bash scripts/run_experiment.sh [config_file] [options]
# Example: bash scripts/run_experiment.sh configs/beaver_dw_config.yaml --max_workers 10

if [ "$#" -lt 1 ]; then
    echo "🚨 Usage: $0 [config_file_path]"
    exit 1
fi

CONFIG_FILE=$1
shift 1
ADDITIONAL_ARGS="$@"

cd "$(dirname "$0")/.."

echo "====================================================="
echo "Starting Text-to-SQL Experiment"
echo "   - Config: $CONFIG_FILE"
echo "   - Options: $ADDITIONAL_ARGS"
echo "====================================================="

python main.py --config "$CONFIG_FILE" $ADDITIONAL_ARGS

echo "====================================================="


