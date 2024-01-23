schema = [
    {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "latitude", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "longitude", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "altitude", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "velocity", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "visibility", "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "footprint", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "units", "type": "STRING", "mode": "REQUIRED", "description": "STRING(25)"},
]
