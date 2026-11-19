"""
ÉTAPE 1 : COUCHE BRONZE EN TABLES DELTA
On charge le premier lot de données brutes (reçu le 8 mars 2025) dans des tables
Delta.
Comme dans la zone raw du TP2, on ne corrige rien : on garde les données telles
qu'elles arrivent.
La différence : chaque table possède maintenant un journal de transactions
(_delta_log).
"""
import json
import shutil
from pathlib import Path
import duckdb
from deltalake import DeltaTable, write_deltalake
shutil.rmtree("lakehouse", ignore_errors=True) # on repart d'un lakehouse vide à chaque exécution
con = duckdb.connect()
con.sql("SET produce_arrow_string_view = false") # format de texte compatible avec Delta Lake
def arrow(requete):
 """Exécute une requête DuckDB et renvoie le résultat sous forme de table
Arrow."""
 return con.sql(requete).arrow().read_all()
LOT = "2025-03-08"
# Pour chaque table bronze : la requête qui lit le fichier source, sans le transformer
SOURCES = {
 "commandes": "SELECT * FROM read_csv('sources/commandes.csv', all_varchar = true)",
 "clients": "SELECT * FROM read_csv('sources/clients.csv', all_varchar = true)",
 "produits": "SELECT * FROM read_csv('sources/produits.csv', all_varchar = true)",
 "avis": "SELECT * FROM read_json('sources/avis.json')",
 "livraisons": """SELECT line AS ligne FROM read_csv('sources/livraisons.log',
 columns = {'line': 'VARCHAR'}, delim = '\\t', header = 
false)""",
}
for table, requete in SOURCES.items():
 # On ajoute la date du lot : elle permettra de savoir d'où vient chaque ligne
 donnees = arrow(f"SELECT *, DATE '{LOT}' AS date_ingestion FROM ({requete})")
 write_deltalake(f"lakehouse/bronze/{table}", donnees, mode="overwrite")
 print(f"bronze/{table:11} : {donnees.num_rows:>2} lignes chargées")
# ---------- Que contient une table Delta ? ----------
print("\n=== Contenu du dossier lakehouse/bronze/commandes ===")
for f in sorted(Path("lakehouse/bronze/commandes").rglob("*")):
 if f.is_file():
  print(" ", f.relative_to("lakehouse/bronze/commandes"))
print("\n=== Le journal de transactions : une ligne JSON par action ===")
journal = sorted(Path("lakehouse/bronze/commandes/_delta_log").glob("*.json"))[-1]
for ligne in journal.read_text(encoding="utf-8").splitlines():
 action = json.loads(ligne)
 nom = next(iter(action))
 detail = action[nom].get("path") or action[nom].get("operation") or ""
 print(f" {nom:11} {detail}")
dt = DeltaTable("lakehouse/bronze/commandes")
print(f"\nVersion actuelle de bronze/commandes : {dt.version()}")
print("Schéma enregistré dans le journal :")
for champ in dt.schema().fields:
 print(f" {champ.name:15} {champ.type}")
con.close()
print("\nÉtape 1 terminée. Les données brutes sont dans des tables Delta, avec un journal de transactions.")
