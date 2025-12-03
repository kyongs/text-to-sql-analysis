# src/utils/schema_extractor.py

import sqlalchemy
from sqlalchemy import create_engine, MetaData, exc
import warnings

def get_sqlite_db_schema(db_path: str) -> str:
    if not db_path:
        return ""
    try:
        engine = create_engine(f"sqlite:///{db_path}")
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=exc.SAWarning)
            metadata = MetaData()
            metadata.reflect(bind=engine)
        
        schema_statements = []
        for table_name, table in metadata.tables.items():
            statement = str(sqlalchemy.schema.CreateTable(table).compile(engine))
            schema_statements.append(statement)
        return "\n\n".join(schema_statements)
    except Exception as e:
        print(f"Error extracting SQLite schema from {db_path}: {e}")
        return ""

def get_mysql_db_schema(conn_info: dict, db_id: str) -> str:
    try:
        conn_url = (
            f"mysql+mysqlconnector://{conn_info['user']}:{conn_info['password']}"
            f"@{conn_info['host']}:{conn_info['port']}/{db_id}"
        )
        engine = create_engine(conn_url)
        metadata = MetaData()
        metadata.reflect(bind=engine)
        
        schema_statements = []
        for table_name, table in metadata.tables.items():
            statement = str(sqlalchemy.schema.CreateTable(table).compile(engine))
            schema_statements.append(statement)
        return "\n\n".join(schema_statements)
    except Exception as e:
        print(f"Error extracting MySQL schema for db '{db_id}': {e}")
        return ""
