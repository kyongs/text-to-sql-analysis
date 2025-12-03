#!/bin/bash

# ==============================================================================
# Renamed View 생성 스크립트 (v6 - Config-Driven)
# ==============================================================================
#
# 사용법:
#   ./scripts/create_views.sh beaver
# ==============================================================================

set -e # 에러 발생 시 스크립트 중단

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT=$( cd -- "$SCRIPT_DIR/.." &> /dev/null && pwd )
cd "$PROJECT_ROOT"

# --- 인자 처리 ---
CONFIG_NAME=$1
if [ -z "$CONFIG_NAME" ]; then
    echo "Usage: $0 beaver"
    exit 1
fi

# --- 함수 정의 ---
run_view_generation() {
    local dataset_name=$1
    local config_file_path=""

    # 설정 이름에 따라 실제 yaml 파일 경로를 매핑
    case "$dataset_name" in
        beaver) config_file_path="configs/beaver_dw_config.yaml" ;;
        *)
            echo "Invalid config name: $dataset_name"
            return 1
            ;;
    esac
    
    echo "====================================================="
    echo "  Generating Renamed Views for '$dataset_name'       "
    echo "  - Using Config: $config_file_path"
    echo "====================================================="

    python -m preprocess.generate_views --config_path "$config_file_path"
    
    echo "✅ View generation for '$dataset_name' finished."
}


# --- 메인 로직 ---
case "$CONFIG_NAME" in
    beaver)
        run_view_generation "$CONFIG_NAME"
        ;;
    *)
        echo "Invalid argument: $CONFIG_NAME. Use 'beaver'."
        exit 1
        ;;
esac

echo "====================================================="
echo "  All requested view generation processes finished.  "
echo "====================================================="
