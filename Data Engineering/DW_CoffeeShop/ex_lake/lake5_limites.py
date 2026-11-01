"""
ÉTAPE 5 : LES LIMITES DU DATA LAKE
On démontre, une par une, cinq choses qu'un simple data lake ne sait pas faire.
Chaque démonstration travaille sur une COPIE, pour ne pas abîmer le lake des étapes
précédentes.
"""
import shutil
from pathlib import Path
import duckdb
con = duckdb.connect()
TEST = Path("lake/test_limites")
shutil.rmtree(TEST, ignore_errors=True) # on repart d'un dossier de test vide
TEST.mkdir(parents=True)
shutil.copy("lake/processed/avis.parquet", TEST / "avis.parquet")
T = TEST.as_posix() # chemin avec des "/" : fonctionne aussi sous Windows
# ---------- Limite 1 : les erreurs ne sont jamais corrigées ----------
print("=== Limite 1 : doublons et erreurs restent dans le lake ===")
con.sql("""
 SELECT commande_id, count(*) AS nb_fois, list(date_ingestion) AS lots
 FROM 'lake/processed/commandes/*/*.parquet'
 GROUP BY commande_id HAVING count(*) > 1 ORDER BY commande_id::INT
""").show()
print(" La commande 16 a été envoyée dans les deux lots : le lake garde les deux copies.\n")
# ---------- Limite 2 : impossible de modifier une ligne ----------
print("=== Limite 2 : pas de UPDATE ni de DELETE sur un fichier Parquet ===")
try:
 con.sql(f"UPDATE '{T}/avis.parquet' SET note = 4 WHERE avis_id = 6")
except Exception as e:
 print(" Erreur :", str(e).splitlines()[0][:90])
print(" Pour corriger UNE note, il faut relire tout le fichier et le réécrire en entier :")
con.sql(f"""COPY (SELECT * REPLACE (CASE WHEN avis_id = 6 THEN 4 ELSE note END AS 
note)
 FROM '{T}/avis.parquet')
 TO '{T}/avis_corrige.parquet' (FORMAT parquet)""")
print(" -> nouveau fichier avis_corrige.parquet écrit (l'ancien existe toujours à côté).\n")
# ---------- Limite 3 : pas d'historique, un écrasement est définitif ----------
print("=== Limite 3 : pas d'historique ===")
avant = con.sql(f"SELECT count(*) FROM '{T}/avis.parquet'").fetchall()[0][0]
con.sql(f"""COPY (SELECT * FROM '{T}/avis.parquet' WHERE note >= 4)
 TO '{T}/avis.parquet' (FORMAT parquet)""") # erreur de manipulation : on écrase
apres = con.sql(f"SELECT count(*) FROM '{T}/avis.parquet'").fetchall()[0][0]
print(f" Avis avant l'écrasement : {avant} | après : {apres}")
print(" Les avis négatifs ont disparu. Aucune commande ne permet de revenir en arrière.\n")
# ---------- Limite 4 : aucun contrôle du schéma à l'écriture ----------
print("=== Limite 4 : le lake accepte n'importe quel fichier, même incohérent ===")
mauvais = TEST / "commandes_j3.csv"
mauvais.write_text("commande_id,date_commande,client_id,produit_id,quantite,montant,canal\n"
 "20,2025-03-10,3,1,deux,douze dinars,sur place\n", encoding="utf8")
shutil.copy("lake/raw/caisse/commandes/date_ingestion=2025-03-09/commandes_j2.csv",
TEST / "commandes_j2.csv")
print(" Le fichier commandes_j3.csv (quantité 'deux', montant 'douze dinars') a été déposé sans erreur.")
try:
 con.sql(f"SELECT sum(montant) FROM read_csv('{T}/commandes_j*.csv')").show()
except Exception as e:
 print(" Mais la lecture échoue plus tard, chez l'analyste :")
 print(" Erreur :", str(e).splitlines()[0][:95])
 print(" -> la colonne montant contient du texte : DuckDB ne peut plus en faire la somme.")
print()
# ---------- Limite 5 : pas de transaction, lectures incohérentes ----------
print("=== Limite 5 : pas de transaction ===")
print(" Une écriture Parquet n'est pas atomique : si le programme plante au milieu,")
print(" ou si un analyste lit le dossier pendant qu'un autre programme y écrit,")
print(" il peut voir un fichier à moitié écrit, ou un mélange de l'ancien et du nouveau.\n")
# ---------- Bilan : risque de data swamp ----------
print("=== Bilan : à quoi ressemble le lake maintenant ? ===")
for f in sorted(Path("lake").rglob("*.*")):
 print(" ", f)
print("\n Quels fichiers sont fiables ? Lequel est la bonne version des avis ?")
print(" Sans catalogue ni règles, le lake devient un « data swamp » (marécage de données).")
con.close()
print("\nÉtape 5 terminée. Ces limites sont exactement ce que le lakehouse (TP3) va résoudre.")