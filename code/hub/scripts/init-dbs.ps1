# Create the metadata (alelt) and landing (alelt_landing) databases in the local docker Postgres; idempotent.
. "$PSScriptRoot\dev-env.ps1"
python -c @"
import os, psycopg
with psycopg.connect(os.environ['ALELT_PG_ADMIN_URL'] + '/postgres', autocommit=True) as c:
    for db in ('alelt', 'alelt_landing'):
        if not c.execute('SELECT 1 FROM pg_database WHERE datname=%s', (db,)).fetchone():
            c.execute(f'CREATE DATABASE {db}')
from alelt_hub.db import init_db
init_db()
print('databases + metadata schema ready')
"@
