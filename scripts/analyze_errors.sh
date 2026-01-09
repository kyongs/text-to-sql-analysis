#!/bin/bash

# Error Analysis Script
# 사용법: bash scripts/analyze_errors.sh <output_dir> <config_file> [max_samples]

if [ "$#" -lt 2 ]; then
    echo "Usage: bash scripts/analyze_errors.sh <output_dir> <config_file> [max_samples]"
    echo "Example: bash scripts/analyze_errors.sh outputs/20260103_4o-join_tool_prompt2 configs/beaver_dw_openai_with_tools.yaml 10"
    exit 1
fi

OUTPUT_DIR=$1
CONFIG_FILE=$2
MAX_SAMPLES=${3:-""}  # Optional, empty means analyze all

PREDICTIONS_FILE="${OUTPUT_DIR}/predictions.json"
PREDICT_DW_FILE="${OUTPUT_DIR}/predict_dw.json"
ANALYSIS_OUTPUT="${OUTPUT_DIR}/error_analysis.json"

# Check if required files exist
if [ ! -f "$PREDICTIONS_FILE" ]; then
    echo "Error: predictions.json not found at ${PREDICTIONS_FILE}"
    exit 1
fi

if [ ! -f "$PREDICT_DW_FILE" ]; then
    echo "Error: predict_dw.json not found at ${PREDICT_DW_FILE}"
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file not found at ${CONFIG_FILE}"
    exit 1
fi

echo "================================"
echo "Error Analysis Script"
echo "================================"
echo "Predictions: ${PREDICTIONS_FILE}"
echo "Predict DW: ${PREDICT_DW_FILE}"
echo "Config: ${CONFIG_FILE}"
echo "Output: ${ANALYSIS_OUTPUT}"
if [ -n "$MAX_SAMPLES" ]; then
    echo "Max Samples: ${MAX_SAMPLES}"
else
    echo "Max Samples: ALL"
fi
echo "================================"
echo ""

# Run the analysis
if [ -n "$MAX_SAMPLES" ]; then
    python analyze_errors.py \
        --predictions "${PREDICTIONS_FILE}" \
        --predict_dw "${PREDICT_DW_FILE}" \
        --config "${CONFIG_FILE}" \
        --output "${ANALYSIS_OUTPUT}" \
        --max_samples "${MAX_SAMPLES}"
else
    python analyze_errors.py \
        --predictions "${PREDICTIONS_FILE}" \
        --predict_dw "${PREDICT_DW_FILE}" \
        --config "${CONFIG_FILE}" \
        --output "${ANALYSIS_OUTPUT}"
fi

echo ""
echo "================================"
echo "Analysis complete!"
echo "Results saved to: ${ANALYSIS_OUTPUT}"
echo "================================"
