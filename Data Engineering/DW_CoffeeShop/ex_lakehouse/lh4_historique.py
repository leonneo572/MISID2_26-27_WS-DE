"""
ÉTAPE 4 : HISTORIQUE, TIME TRAVEL, RESTAURATION ET VACUUM
Chaque écriture crée une nouvelle version de la table. On peut relire une version
passée,
annuler une erreur de manipulation, puis nettoyer les anciens fichiers.
"""
from datetime import datetime
from pathlib import Path
import duckdb
from deltalake import DeltaTable, write_deltalake
con = duckdb.connect()
# ================= 1. Historique =================
print("=== 1. Historique de silver/commandes ===")
dt = DeltaTable("lakehouse/silver/commandes")
for h in reversed(dt.history()):
 quand = datetime.fromtimestamp(h["timestamp"] / 1000).strftime("%d/%m/%Y %H:%M:%S")
 print(f" version {h['version']} : {h['operation']:7} le {quand}")
# ================= 2. Time travel =================
print("\n=== 2. Time travel : la table à chaque version ===")
for v in range(dt.version() + 1):
    n = DeltaTable("lakehouse/silver/commandes", version=v).to_pyarrow_table().num_rows
    print(f" version {v} : {n} commandes")
# ================= 3. Erreur de manipulation puis restauration =================
print("\n=== 3. Erreur de manipulation sur silver/avis ===")
avis = DeltaTable("lakehouse/silver/avis")
version_avant = avis.version()
avant = avis.to_pyarrow_table()
print(f" Avant l'erreur (version {version_avant}) : {avant.num_rows} avis")
# Erreur : on voulait CRÉER une table des bons avis, mais on ÉCRASE la table des avis
bons = con.sql("SELECT * FROM avant WHERE note >= 4").arrow().read_all()
write_deltalake("lakehouse/silver/avis", bons, mode="overwrite")
print(f" Après l'écrasement :  {DeltaTable('lakehouse/silver/avis').to_pyarrow_table().num_rows} avis ")
DeltaTable("lakehouse/silver/avis").restore(version_avant)
apres = DeltaTable("lakehouse/silver/avis")
print(f" Après RESTORE vers la version {version_avant} :  {apres.to_pyarrow_table().num_rows} avis ")
print(" Historique :", [h["operation"] for h in reversed(apres.history())])
# ================= 4. VACUUM et droit à l'effacement =================
print("\n=== 4. Les données effacées sont-elles vraiment parties ? ===")
try:
     clients_v0 = DeltaTable("lakehouse/silver/clients", version=0).to_pyarrow_table()
     print(" silver/clients relue en version 0 (avant le DELETE de l'étape 3) :")
     con.sql("SELECT client_id, nom FROM clients_v0 WHERE client_id = 5").show()
     print(" -> La cliente 5 est encore lisible dans l'historique !")
except Exception:
     print(" La version 0 a déjà été nettoyée par un VACUUM précédent (script relancé).")
clients = DeltaTable("lakehouse/silver/clients")
nb_avant = len(list(Path("lakehouse/silver/clients").glob("*.parquet")))
supprimes = clients.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
nb_apres = len(list(Path("lakehouse/silver/clients").glob("*.parquet")))
print(f" VACUUM : {len(supprimes)} ancien(s) fichier(s) supprimé(s) ({nb_avant} -> {nb_apres} fichiers Parquet)")
try:
     DeltaTable("lakehouse/silver/clients", version=0).to_pyarrow_table()
     print(" La version 0 est encore lisible (inattendu !)")
except Exception as e:
     print(" Relire la version 0 est maintenant impossible :", type(e).__name__)
con.close()
print("\nÉtape 4 terminée. Rien n'est perdu par erreur, et ce qui doit être effacé  peut l 'être vraiment.")
