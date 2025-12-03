## Initialization

```
python3 -m venv venv

source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Environment variables
- Create a `.env` file or export these before running:
```
OPENAI_API_KEY=sk-...
```

## How to run
- Only the Beaver dataset is supported. Update `configs/beaver_dw_config.yaml` to set `mode` (`baseline`, `gold_schema`, or `view`) and `schema_representation` (`basic`, `basic_plus_type`, `ddl`, `m_schema`).
- Update 'configs/beaver_dw_config.yaml' to set MySQL Server Information.  
- Beaver supports every representation in both baseline and gold_schema modes (gold tables will be filtered automatically).

```
bash scripts/run_experiment.sh configs/beaver_dw_config.yaml
bash scripts/run_evaluation.sh outputs/<run_folder>/predictions.json configs/beaver_dw_config.yaml

# example)
# bash scripts/run_evaluation.sh outputs/20251203_beaver_dw/predictions.json configs/beaver_dw_config.yaml
```


### Beaver m_schema cache 생성
```
# example
bash scripts/run_experiment.sh configs/beaver_dw_config.yaml
```
- If `*_preprocessed_schemas.json` is missing, it will be generated automatically. You can also run it manually:
```
python src/data_loader/preprocess.py \
  --dataset_name beaver \
  --dataset_path ./data/beaver/dw \
  --split dw \
  --db_dir ./data/beaver/dw
```

