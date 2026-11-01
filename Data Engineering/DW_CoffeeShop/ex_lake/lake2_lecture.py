"""
ÉTAPE 2 : SCHEMA-ON-READ
On lit les fichiers du lake directement en SQL, SANS créer de table.
La structure (colonnes, types) est déterminée au moment de la lecture.
"""
import duckdb
con = duckdb.connect() # base en mémoire : rien n'est enregistré sur le disque
# ---------- 1. CSV : toutes les commandes, de tous les lots ----------
print("=== 1. Commandes : les deux lots lus d'un coup grâce au joker * ===")
con.sql("""
 SELECT date_ingestion, count(*) AS nb_lignes
 FROM read_csv('lake/raw/caisse/commandes/*/*.csv', hive_partitioning = true, 
all_varchar = true, union_by_name = true)
 GROUP BY date_ingestion ORDER BY date_ingestion
""").show()
# hive_partitioning = true transforme le nom de dossier "date_ingestion=2025-03-08"
# en une vraie colonne date_ingestion.
print("=== Types devinés par DuckDB pour commandes.csv (sans all_varchar) ===")
con.sql("DESCRIBE SELECT * FROM read_csv('lake/raw/caisse/commandes/*/*.csv')").show()
# ---------- 2. JSON imbriqué : les avis clients ----------
print("=== 2a. Avis : le JSON est lu tel quel, avec ses structures imbriquées ===")
con.sql("SELECT * FROM read_json('lake/raw/app/avis/*/avis.json')").show(max_width=250)
print("=== 2b. On va chercher DANS l'objet client avec un point : client.appareil ===")
con.sql("""
 SELECT avis_id, commande_id, note, client.id AS client_id, client.appareil AS 
appareil
 FROM read_json('lake/raw/app/avis/*/avis.json')
""").show()
print("=== 2c. On « déplie » la liste tags : une ligne par tag (unnest) ===")
con.sql("""
 SELECT tag, count(*) AS nb_avis
 FROM (SELECT unnest(tags) AS tag FROM read_json('lake/raw/app/avis/*/avis.json'))
 GROUP BY tag ORDER BY nb_avis DESC, tag
""").show()
print("=== 2d. Nombre d'avis par appareil ===")
con.sql("""
 SELECT client.appareil AS app, count(*) AS nb_avis
 FROM read_json('lake/raw/app/avis/*/avis.json')
 GROUP BY app ORDER BY nb_avis DESC, app
""").show()
# ---------- 3. Texte non structuré : les logs de livraison ----------
print("=== 3a. Logs lus comme du texte brut : une seule colonne ===")
con.sql("""
 SELECT line FROM read_csv('lake/raw/coursiers/livraisons/*/livraisons.log',
 columns = {'line': 'VARCHAR'}, delim = '\\t', header = 
false)
""").show(max_width=200)
print("=== 3b. Les mêmes logs, structurés à la lecture avec des expressions régulières ===")
con.sql("""
 SELECT 
 TRY_CAST(regexp_extract(line, '^(\\S+ \\S+)', 1) AS TIMESTAMP) AS horodatage,
 regexp_extract(line, '\\| (\\w+) \\|', 1) AS niveau,
 TRY_CAST(regexp_extract(line, 'commande=(\\d+)', 1) AS INT) AS commande_id,
 regexp_extract(line, 'livreur=(\\w+)', 1) AS livreur,
 regexp_extract(line, 'statut=(\\w+)', 1) AS statut,
 TRY_CAST(regexp_extract(line, 'duree=(\\d+)min', 1) AS INT) AS duree_min
 FROM read_csv('lake/raw/coursiers/livraisons/*/livraisons.log',
 columns = {'line': 'VARCHAR'}, delim = '\\t', header = false)
 WHERE line IS NOT NULL 
 AND trim(line) != ''
""").show()
con.close()
print("Étape 2 terminée. Aucune table n'a été créée : la structure n'existe qu'au moment de la lecture.")
