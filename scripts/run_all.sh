#!/bin/bash
# ==========================================================
# scripts/run_full_pipeline.sh
# Runs experiment and then automatically evaluates the results
# ==========================================================

# Usage: bash scripts/run_full_pipeline.sh [config_file] [options]
# Example: bash scripts/run_all.sh configs/beaver_dw_openai_with_tools.yaml --max_workers 8 --test_n 3

if [ "$#" -lt 1 ]; then
    echo "🚨 Usage: $0 [config_file_path] [experiment_options]"
    echo "   Example: $0 configs/beaver_dw_openai_with_tools.yaml --max_workers 8"
    exit 1
fi

CONFIG_FILE=$1
shift 1
EXPERIMENT_ARGS="$@"

# Check if --error_analysis flag is present in experiment args
ERROR_ANALYSIS_FLAG=""
for arg in $EXPERIMENT_ARGS; do
    if [ "$arg" = "--error_analysis" ]; then
        ERROR_ANALYSIS_FLAG="--error_analysis"
        break
    fi
done

cd "$(dirname "$0")/.."

# Extract experiment_name from config file
EXPERIMENT_NAME=$(grep "experiment_name:" "$CONFIG_FILE" | sed 's/.*experiment_name: *"\(.*\)".*/\1/')

if [ -z "$EXPERIMENT_NAME" ]; then
    echo "❌ Could not extract experiment_name from $CONFIG_FILE"
    exit 1
fi

# Generate output directory name: YYYYMMDD_experiment_name
OUTPUT_DIR="$(date +%Y%m%d)_${EXPERIMENT_NAME}"
PREDICTION_FILE="./outputs/${OUTPUT_DIR}/predictions.json"

echo "====================================================="
echo "🚀 Running Full Pipeline"
echo "   - Config: $CONFIG_FILE"
echo "   - Experiment Name: $EXPERIMENT_NAME"
echo "   - Output Directory: $OUTPUT_DIR"
echo "   - Options: $EXPERIMENT_ARGS"
echo "====================================================="

# Step 1: Run experiment
echo ""
echo "📝 STEP 1/2: Running experiment..."
bash scripts/run_experiment.sh "$CONFIG_FILE" $EXPERIMENT_ARGS

if [ $? -ne 0 ]; then
    echo "❌ Experiment failed!"
    exit 1
fi

# Step 2: Run evaluation
echo ""
echo "📊 STEP 2/2: Running evaluation..."
bash scripts/run_evaluation.sh "$PREDICTION_FILE" "$CONFIG_FILE" $ERROR_ANALYSIS_FLAG

if [ $? -ne 0 ]; then
    echo "❌ Evaluation failed!"
    exit 1
fi

echo ""
echo "====================================================="
echo "✅ Full pipeline completed successfully!"
echo "   - Predictions: $PREDICTION_FILE"
echo "   - Results: ./outputs/${OUTPUT_DIR}/evaluation_results.json"
echo "====================================================="
