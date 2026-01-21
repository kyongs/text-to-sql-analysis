# ==========================================================
# scripts/run_evaluation.sh
# ==========================================================

#!/bin/bash

if [ "$#" -lt 2 ]; then
    echo "🚨 Usage: $0 [prediction_file_path] [config_file_path] [options]"
    exit 1
fi

PREDICTION_FILE=$1
CONFIG_FILE=$2
shift 2
ADDITIONAL_ARGS="$@"

cd "$(dirname "$0")/.."

echo "====================================================="
echo "Running Standalone Evaluation"
echo "   - Predictions: $PREDICTION_FILE"
echo "   - Config: $CONFIG_FILE"
echo "====================================================="

python evaluate.py --prediction_path "$PREDICTION_FILE" --config "$CONFIG_FILE" $ADDITIONAL_ARGS

echo "====================================================="
echo "Evaluation finished."
echo "====================================================="
