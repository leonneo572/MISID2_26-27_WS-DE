"""
ÉTAPE 3 : ZONE PROCESSED EN PARQUET
On convertit les données brutes en Parquet (format en colonnes, compressé et typé),
dans une deuxième zone du lake : lake/processed/.
La zone raw n'est jamais modifiée.
"""
from pathlib import Path
import duckdb
con = duckdb.connect()
PROC = Path("lake/processed")
PROC.mkdir(parents=True, exist_ok=True)
LOGS = """read_csv('lake/raw/coursiers/livraisons/*/livraisons.log',
 columns = {'line': 'VARCHAR'}, delim = '\\t', header = false)"""
# ---------- 1. Commandes : tous les lots, partitionnés par date d'ingestion ----------
con.sql("""
 COPY (SELECT * FROM read_csv('lake/raw/caisse/commandes/*/*.csv',
 hive_partitioning = true, all_varchar = true))
 TO 'lake/processed/commandes' (FORMAT parquet, PARTITION_BY (date_ingestion), 
OVERWRITE_OR_IGNORE)
""")
# ---------- 2. Avis : le JSON imbriqué devient un tableau à plat ----------
con.sql("""
 COPY (SELECT avis_id, commande_id, date::DATE AS date_avis, note, commentaire,
 client.id AS client_id, client.appareil AS appareil, tags
 FROM read_json('lake/raw/app/avis/*/avis.json'))
 TO 'lake/processed/avis.parquet' (FORMAT parquet)
""")
# ---------- 3. Livraisons : le texte devient un tableau typé ----------
con.sql(f"""
 COPY (SELECT TRY_CAST(regexp_extract(line, '^(\\S+ \\S+)', 1) AS TIMESTAMP) AS horodatage,
 TRY_CAST(regexp_extract(line, 'commande=(\\d+)', 1) AS INT) AS commande_id,
 regexp_extract(line, 'livreur=(\\w+)', 1) AS livreur,
 regexp_extract(line, 'statut=(\\w+)', 1) AS statut,
 TRY_CAST(regexp_extract(line, 'duree=(\\d+)min', 1) AS INT) AS duree_min
 FROM {LOGS})
 TO 'lake/processed/livraisons.parquet' (FORMAT parquet)
""")
# ---------- 4. Clients et produits ----------
for source, table in [("crm", "clients"), ("caisse", "produits")]:
 con.sql(f"""COPY (SELECT * FROM read_csv('lake/raw/{source}/{table}/*/*.csv', 
all_varchar = true))
 TO 'lake/processed/{table}.parquet' (FORMAT parquet)""")
print("=== Contenu de la zone processed ===")
for f in sorted(PROC.rglob("*.parquet")):
 print(f" {str(f):70} {f.stat().st_size:>6} octets")
print("\n=== Lecture d'un Parquet : les types sont enregistrés DANS le fichier ===")
con.sql("DESCRIBE SELECT * FROM 'lake/processed/livraisons.parquet'").show()

# Parquet des commandes
print("\n=== Lecture des commandes depuis les fichiers parquet ===")
con.sql("SELECT * FROM 'lake/processed/commandes/*/*.parquet'").show()
con.sql("""
SELECT count (commande_id) AS avec_doublons, count (distinct commande_id) AS sans_doublons 
FROM 'lake/processed/commandes/*/*.parquet'
""").show()

# ---------- 5. Comparaison de taille : petit fichier puis gros volume ----------
csv_small = sum(f.stat().st_size for f in
Path("lake/raw/caisse/commandes").rglob("*.csv"))
pq_small = sum(f.stat().st_size for f in
Path("lake/processed/commandes").rglob("*.parquet"))
print(f"Commandes (18 + 4 lignes) : CSV = {csv_small} octets | Parquet = {pq_small} octets")
# On fabrique 200 000 commandes pour voir l'effet sur un vrai volume
Path("lake/test_volume").mkdir(parents=True, exist_ok=True)
con.sql("""
 CREATE TABLE gros AS
 SELECT i AS commande_id, DATE '2025-01-01' + (i % 365)::INT AS date_commande,
 (i % 6) + 1 AS client_id, (i % 5) + 1 AS produit_id, (i % 4) + 1 AS 
quantite,
 round(((i % 5) + 1) * 3.5, 3) AS montant,
 CASE WHEN i % 3 = 0 THEN 'livraison' ELSE 'sur place' END AS canal
 FROM range(200000) t(i)
""")
con.sql("COPY gros TO 'lake/test_volume/commandes.csv' (HEADER)")
con.sql("COPY gros TO 'lake/test_volume/commandes.parquet' (FORMAT parquet)")
csv_big = Path("lake/test_volume/commandes.csv").stat().st_size
pq_big = Path("lake/test_volume/commandes.parquet").stat().st_size
print(f"Commandes (200 000 lignes) : CSV = {csv_big:,} octets | Parquet = {pq_big:,} octets "
 f"-> Parquet {csv_big / pq_big:.1f} fois plus petit")
con.close()
print("\nÉtape 3 terminée. La zone processed contient des fichiers Parquet typés et compressés.")