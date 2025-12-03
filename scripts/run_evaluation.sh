# ==========================================================
# scripts/run_evaluation.sh
# ==========================================================

#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "🚨 Usage: $0 [prediction_file_path] [config_file_path]"
    exit 1
fi

PREDICTION_FILE=$1
CONFIG_FILE=$2

cd "$(dirname "$0")/.."

echo "====================================================="
echo "Running Standalone Evaluation"
echo "   - Predictions: $PREDICTION_FILE"
echo "   - Config: $CONFIG_FILE"
echo "====================================================="

python evaluate.py --prediction_path "$PREDICTION_FILE" --config "$CONFIG_FILE"

echo "====================================================="
echo "Evaluation finished."
echo "====================================================="
