"""
ÉTAPE 3 : TRANSFORM (nettoyage)
On applique les règles de nettoyage UNE PAR UNE, en affichant le nombre de lignes
après chacune.
Le résultat est rangé dans le schéma "propre".
"""
import duckdb
con = duckdb.connect("mini_warehouse.duckdb")
con.sql("CREATE SCHEMA IF NOT EXISTS propre")
def compter(table):
 return con.sql(f"SELECT count(*) FROM {table}").fetchall()[0][0]
print("Départ :", compter("staging.commandes"), "lignes dans staging.commandes\n")
# --- Règle 1 : supprimer les doublons ---
con.sql("CREATE OR REPLACE TEMP TABLE r1 AS SELECT DISTINCT * FROM staging.commandes")
print("Règle 1 - suppression des doublons ->", compter("r1"), "lignes")
# --- Règle 2 : rejeter les commandes sans montant ---
con.sql("CREATE OR REPLACE TEMP TABLE r2 AS SELECT * FROM r1 WHERE montant IS NOT NULL")
print("Règle 2 - rejet des montants vides ->", compter("r2"), "lignes")
# --- Règle 3 : rejeter les quantités négatives ---
con.sql("CREATE OR REPLACE TEMP TABLE r3 AS SELECT * FROM r2 WHERE quantite::INT > 0")
print("Règle 3 - rejet des quantités négatives ->", compter("r3"), "lignes")
# --- Règle 4 : une seule forme de date, et des vrais types ---
# try_strptime essaie un format et renvoie NULL s'il ne correspond pas ;
# coalesce garde le premier résultat non NULL.
con.sql("""
CREATE OR REPLACE TEMP TABLE r4 AS
 SELECT commande_id::INT AS commande_id,
 coalesce(try_strptime(date_commande, '%Y-%m-%d'),
 try_strptime(date_commande, '%d/%m/%Y'))::DATE AS date_commande,
 client_id::INT AS client_id,
 produit_id::INT AS produit_id,
 quantite::INT AS quantite,
 montant::DOUBLE AS montant,
 canal
 FROM r3
""")
print("Règle 4 - dates unifiées + types convertis ->", compter("r4"), "lignes")
# --- Règle 5 : uniformiser le canal (espaces + minuscules) ---
con.sql("CREATE OR REPLACE TEMP TABLE r5 AS "
 "SELECT * REPLACE (lower(trim(canal)) AS canal) FROM r4")
print("Règle 5 - canal en minuscules sans espaces ->", compter("r5"),
"lignes")
# --- Règle 6 : rejeter les canaux autres que livraison et sur place ---
con.sql("""
    CREATE OR REPLACE TABLE propre.commandes AS 
    SELECT * FROM r5 
    WHERE canal IN ('livraison', 'sur place')
""")
print("Règle 6 - rejeter les canaux autres que livraison et sur place ->", compter("propre.commandes"), "lignes")

print("\n=== propre.commandes (résultat final) ===")
con.sql("SELECT * FROM propre.commandes ORDER BY commande_id").show(max_rows=50)
# --- Nettoyage des clients : ville propre, 'Inconnue' si absente ---
# On met tout en minuscules, puis une majuscule au début de chaque mot : 'tunis ' -> 'Tunis'
con.sql("""
 CREATE OR REPLACE TABLE propre.clients AS
 SELECT client_id::INT AS client_id,
 nom,
 coalesce(array_to_string(list_transform(string_split(lower(trim(ville)), ' 
'),
 lambda x: upper(x[1]) || x[2:]), ' '), 'Inconnue') AS ville
 FROM staging.clients
""")
print("=== propre.clients : avant / après ===")
con.sql("""
 SELECT s.client_id::INT, '[' || coalesce(s.ville, 'NULL') || ']' AS ville_avant, 
p.ville AS ville_apres
 FROM staging.clients s JOIN propre.clients p ON p.client_id = s.client_id::INT
 ORDER BY s.client_id::INT
""").show()
# --- Produits : déjà propres, on convertit seulement les types ---
con.sql("""
 CREATE OR REPLACE TABLE propre.produits AS
 SELECT produit_id::INT AS produit_id, nom_produit, categorie, 
prix_unitaire::DOUBLE AS prix_unitaire
 FROM staging.produits
""")
con.close()
print("Étape 3 terminée. Les données sont propres et correctement typées.")
