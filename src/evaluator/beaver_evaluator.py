import os
import json
import subprocess
from typing import List, Dict, Any
from .base_evaluator import BaseEvaluator

class BeaverEvaluator(BaseEvaluator):
    def _create_prediction_file(self, predictions: List[Dict[str, Any]], output_path: str):
        """BIRD/Beaver 형식의 예측 JSON 파일을 생성합니다."""
        pred_dict = {}
        for i, pred_item in enumerate(predictions):
            sql = pred_item.get('predicted_sql', '').replace('\n', ' ').strip()
            db_id = pred_item.get('db_id', '')
            pred_dict[str(i)] = f"{sql}\t----- bird -----\t{db_id}"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(pred_dict, f, indent=4)

    def evaluate(self, predictions: List[Dict[str, Any]]) -> Dict[str, Any]:
        if 'prediction_path' not in self.config:
            raise ValueError("Configuration must include 'prediction_path'")

        output_dir = os.path.dirname(self.config['prediction_path'])
        
        try:
            dataset_config = self.config['dataset']
            data_mode = dataset_config.get('split')
            prediction_filename = f"predict_{data_mode}.json"
            pred_json_path = os.path.join(output_dir, prediction_filename)
        except KeyError as e:
            return {"error": f"Missing required key: {e}"}
        
        self._create_prediction_file(predictions, pred_json_path)
        print(f"✅ Created prediction file for Beaver: {pred_json_path}")

        print("\n🚀 Starting Beaver evaluation...")

        try:
            eval_config = self.config['evaluation']
            db_conn_config = self.config['db_connection']
            
            script_path = eval_config['script_path']
            ground_truth_dir = eval_config['ground_truth_dir']
            
        except KeyError as e:
            return {"error": f"🚨 Missing required key in config file: {e}"}

        command = [
            'python', script_path,
            '--predicted_sql_path', output_dir,
            '--ground_truth_path', ground_truth_dir,
            '--data_mode', data_mode,
            '--db_host', str(db_conn_config['host']),
            '--db_port', str(db_conn_config['port']),
            '--db_user', str(db_conn_config['user']),
            '--db_password', str(db_conn_config['password']),
            '--num_cpus', str(eval_config.get('num_cpus', 8)),
            '--meta_time_out', str(eval_config.get('meta_time_out', 30.0))
        ]
        
        print(f"🔍 Executing command: {' '.join(command)}")

        try:
            subprocess.run(command, check=True)
            return {"status": "Evaluation script executed successfully."}
        except (FileNotFoundError, subprocess.CalledProcessError) as e:
            return {"error": f"🚨 Evaluation script failed: {e}"}