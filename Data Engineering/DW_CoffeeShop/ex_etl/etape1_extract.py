"""
ÉTAPE 1 : EXTRACT
On copie les fichiers sources TELS QUELS dans la zone de staging du data warehouse.
Aucune correction : on veut une photo fidèle de ce que les systèmes sources nous
envoient.
"""
import duckdb
con = duckdb.connect("mini_warehouse.duckdb") # crée (ou ouvre) le fichier du data warehouse
con.sql("CREATE SCHEMA IF NOT EXISTS staging")
# all_varchar=true : toutes les colonnes sont lues comme du texte.
# On ne fait confiance à aucun type tant que les données n'ont pas été contrôlées.
con.sql("CREATE OR REPLACE TABLE staging.commandes AS "
 "SELECT * FROM read_csv('donnees/commandes.csv', all_varchar=true)")
con.sql("CREATE OR REPLACE TABLE staging.clients AS "
 "SELECT * FROM read_csv('donnees/clients.csv', all_varchar=true)")
con.sql("CREATE OR REPLACE TABLE staging.produits AS "
 "SELECT * FROM read_csv('donnees/produits.csv', all_varchar=true)")
print("=== staging.commandes (copie brute du fichier commandes.csv) ===")
con.sql("SELECT * FROM staging.commandes").show(max_rows=50)
print("=== staging.clients ===")
con.sql("SELECT * FROM staging.clients").show()
print("=== staging.produits ===")
con.sql("SELECT * FROM staging.produits").show()
for t in ["commandes", "clients", "produits"]:
 n = con.sql(f"SELECT count(*) FROM staging.{t}").fetchall()[0][0]
 print(f"staging.{t} : {n} lignes")
con.close()
print("\nÉtape 1 terminée. Observez les tableaux : repérez-vous déjà des erreurs ?")
