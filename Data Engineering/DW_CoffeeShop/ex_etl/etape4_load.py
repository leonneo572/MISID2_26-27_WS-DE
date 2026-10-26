"""
ÉTAPE 4 : LOAD (chargement en schéma en étoile)
On organise les données propres en 1 table de faits + 3 tables de dimensions, dans le
schéma "dwh".
 dim_date
 |
 dim_client --- fact_commandes --- dim_produit
Granularité de la table de faits : UNE LIGNE = UNE COMMANDE.
"""
import duckdb
con = duckdb.connect("mini_warehouse.duckdb")
con.sql("CREATE SCHEMA IF NOT EXISTS dwh")
# --- Dimension client : QUI a acheté ---
con.sql("CREATE OR REPLACE TABLE dwh.dim_client AS SELECT client_id, nom, ville FROM \
propre.clients")
# --- Dimension produit : QUOI a été acheté ---
con.sql("CREATE OR REPLACE TABLE dwh.dim_produit AS "
 "SELECT produit_id, nom_produit, categorie, prix_unitaire FROM propre.produits")
# --- Dimension date : QUAND (un calendrier, une ligne par jour du mois de mars 2025) ---
con.sql("""
 CREATE OR REPLACE TABLE dwh.dim_date AS
 SELECT d::DATE AS date_id,
 day(d) AS jour,
 month(d) AS mois,
 year(d) AS annee,
 dayname(d) AS jour_semaine
 FROM range(DATE '2025-03-01', DATE '2025-04-01', INTERVAL 1 DAY) t(d)
""")
# --- Table de faits : les commandes, avec les clés vers les dimensions et les mesures ---
con.sql("""
 CREATE OR REPLACE TABLE dwh.fact_commandes AS
 SELECT commande_id,
 date_commande AS date_id , -- clé vers dim_date
 client_id, -- clé vers dim_client
 produit_id, -- clé vers dim_produit
 canal,
 quantite, -- mesure
 montant AS montant_ht -- mesure
 FROM propre.commandes
""")
for t in ["dim_client", "dim_produit", "dim_date", "fact_commandes"]:
 n = con.sql(f"SELECT count(*) FROM dwh.{t}").fetchall()[0][0]
 print(f"dwh.{t} : {n} lignes")
print("\n=== dwh.fact_commandes (centre de l'étoile) ===")
con.sql("SELECT * FROM dwh.fact_commandes ORDER BY commande_id").show(max_rows=50)
print("=== dwh.dim_client ===")
con.sql("SELECT * FROM dwh.dim_client").show()
print("=== dwh.dim_date (7 premiers jours) ===")
con.sql("SELECT * FROM dwh.dim_date LIMIT 7").show()
# --- Vérification : chaque commande doit trouver son client et son produit ---
orphelins = con.sql("""
 SELECT count(*) FROM dwh.fact_commandes f
 LEFT JOIN dwh.dim_client c USING (client_id)
 LEFT JOIN dwh.dim_produit p USING (produit_id)
 WHERE c.client_id IS NULL OR p.produit_id IS NULL
""").fetchall()[0][0]
print("Commandes sans client ou sans produit correspondant :", orphelins)
con.close()
print("Étape 4 terminée. Le data warehouse est prêt à être interrogé.")
