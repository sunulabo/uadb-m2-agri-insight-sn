import happybase
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('HBaseSetup')

def create_agri_tables():
    conn = happybase.Connection('hbase', port=9090, timeout=10000)
    conn.open()
    tables = {
        b'agri:recommendations': {
            b'meta': {'max_versions': 1},
            b'predict': {'max_versions': 5},
            b'conseil': {'max_versions': 1},
        },
        b'agri:predictions': {
            b'input': {'max_versions': 1},
            b'output': {'max_versions': 10},
        },
        b'agri:drift_monitor': {
            b'stats': {'max_versions': 30},
        },
    }
    existing = {t.decode() for t in conn.tables()}
    for name_b, families in tables.items():
        name = name_b.decode()
        if name not in existing:
            conn.create_table(name_b, families)
            logger.info(f'Table {name} créée')
        else:
            logger.info(f'Table {name} existe déjà')
    conn.close()

if __name__ == '__main__':
    create_agri_tables()
