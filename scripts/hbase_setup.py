import happybase, logging

logger = logging.getLogger("HBaseSetup")


def create_agri_tables():
    try:
        # Utilisation de localhost car on est sur l'hôte Windows
        conn = happybase.Connection("localhost", port=9090, timeout=10000)
        conn.open()
        
        # 1. Tentative de création du namespace 'agri'
        # On utilise le client thrift directement car happybase n'a pas toujours create_namespace
        try:
            logger.info("Tentative de création du namespace 'agri'...")
            conn.create_namespace('agri')
            logger.info("Namespace 'agri' créé ✓")
        except Exception as e:
            # Si l'erreur contient "AlreadyExists", c'est que c'est bon
            if "AlreadyExists" in str(e) or "NamespaceExistException" in str(e):
                logger.info("Namespace 'agri' déjà présent")
            else:
                logger.warning(f"Note sur le namespace : {e}")

        # 2. Définition des tables
        tables = {
            "agri:recommandations": {
                "meta": {"max_versions": 1},
                "predict": {"max_versions": 5},
                "conseil": {"max_versions": 1},
            },
            "agri:predictions": {
                "input": {"max_versions": 1},
                "output": {"max_versions": 10},
            },
            "agri:drift_monitor": {
                "stats": {"max_versions": 30},
            },
        }

        # 3. Création des Tables
        existantes = [t.decode() for t in conn.tables()]
        for nom, fam in tables.items():
            if nom not in existantes:
                conn.create_table(nom, fam)
                logger.info(f"Table {nom} créée ✓")
            else:
                logger.info(f"Table {nom} déjà existante")
        
        conn.close()
        print("\n[SUCCESS] Initialisation HBase terminée !")

    except Exception as e:
        logger.error(f"Erreur lors de la création des tables HBase : {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_agri_tables()
