import happybase, logging

logger = logging.getLogger("HBaseSetup")


def create_agri_tables():
    try:
        conn = happybase.Connection("hbase", port=9090, timeout=10000)
        conn.open()
        tables = {
            b"agri:recommandations": {
                b"meta": {"max_versions": 1},
                b"predict": {"max_versions": 5},
                b"conseil": {"max_versions": 1},
            },
            b"agri:predictions": {
                b"input": {"max_versions": 1},
                b"output": {"max_versions": 10},
            },
            b"agri:drift_monitor": {
                b"stats": {"max_versions": 30},
            },
        }
        existantes = [t.decode() for t in conn.tables()]
        for nom_b, fam in tables.items():
            nom = nom_b.decode()
            if nom not in existantes:
                conn.create_table(nom_b, fam)
                logger.info(f"Table {nom} créée ✓")
            else:
                logger.info(f"Table {nom} déjà existante")
        conn.close()
    except Exception as e:
        logger.error(f"Erreur lors de la création des tables HBase : {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_agri_tables()
