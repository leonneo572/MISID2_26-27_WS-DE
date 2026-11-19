"""
ÉTAPE 3 : CORRIGER, SUPPRIMER, CONTRÔLER LE SCHÉMA
1) UPDATE : corriger une seule ligne, sans réécrire la table à la main.
2) DELETE : appliquer une demande d'effacement d'une cliente.
3) Contrôle du schéma : une donnée au mauvais type est refusée.
4) Évolution du schéma : ajouter une colonne de façon contrôlée.
"""
import duckdb
import pandas as pd
from deltalake import DeltaTable, write_deltalake
con = duckdb.connect()
def lire(chemin):
 return DeltaTable(chemin).to_pyarrow_table()
# ================= 1. UPDATE =================
print("=== 1. UPDATE : la cliente de l'avis 6 a été recontactée, sa note passe de 1 à 4 ===")
avis = lire("lakehouse/silver/avis")
con.sql("SELECT avis_id, note, commentaire FROM avis WHERE avis_id = 6").show()
res = DeltaTable("lakehouse/silver/avis").update(predicate="avis_id = 6",
updates={"note": "4"})
print(f"Lignes modifiées : {res['num_updated_rows']}")
avis = lire("lakehouse/silver/avis")
con.sql("SELECT avis_id, note, commentaire FROM avis WHERE avis_id = 6").show()
# ================= 2. DELETE =================
print("=== 2. DELETE : la cliente 5 (Nour Hammami) demande l'effacement de ses données ===")
for chemin, predicat in [("lakehouse/silver/clients", "client_id = 5"),
 ("lakehouse/silver/avis", "client_id = 5"),
 ("lakehouse/bronze/clients", "client_id = '5'"),
 ("lakehouse/bronze/avis", "client.id = 5")]:
 res = DeltaTable(chemin).delete(predicat)
 print(f" {chemin:28} : {res['num_deleted_rows']} ligne(s) supprimée(s)")
print(" Attention : les données sont aussi effacées du bronze, sinon elles y resteraient.\n")
# ================= 2.1. UPDATE Commande =================
print("=== 2.1. UPDATE commande: rendre les commandes anonymes : id client = 0 ===")
cmd = lire("lakehouse/silver/commandes")
con.sql("SELECT * FROM cmd WHERE client_id = 5").show()
res = DeltaTable("lakehouse/silver/commandes").update(predicate="client_id = 5",
updates={"client_id": "0"})
print(f"Lignes modifiées : {res['num_updated_rows']}")
cmd = lire("lakehouse/silver/commandes")
con.sql("SELECT * FROM cmd WHERE client_id = 0").show()
# ================= 3. Contrôle du schéma =================
print("=== 3. Contrôle du schéma : on tente d'ajouter une commande mal saisie ===")
mauvaise = pd.DataFrame({
 "commande_id": [20], "date_commande": [pd.Timestamp("2025-03-10").date()],
 "client_id": [3], "produit_id": [1],
 "quantite": ["deux"], "montant": ["douze dinars"], "canal": ["sur place"],
 "date_ingestion": [pd.Timestamp("2025-03-10").date()],
})
try:
 write_deltalake("lakehouse/silver/commandes", mauvaise, mode="append")
 print(" Écriture acceptée (inattendu !)")
except Exception as e:
 print(" Écriture REFUSÉE :", str(e).splitlines()[0][:110])
print(f" silver/commandes contient toujours {lire('lakehouse/silver/commandes').num_rows} lignes.\n")
# ================= 4. Évolution du schéma =================
print("=== 4. Évolution du schéma : la caisse envoie maintenant un pourboire ===")
nouvelle = pd.DataFrame({
 "commande_id": [20], "date_commande": [pd.Timestamp("2025-03-10").date()],
 "client_id": [3], "produit_id": [1], "quantite": [2], "montant": [7.0],
 "canal": ["sur place"], "date_ingestion": [pd.Timestamp("2025-03-10").date()],
 "pourboire": [1.5],
}).astype({"commande_id": "int32", "client_id": "int32", "produit_id": "int32",
"quantite": "int32"})
commandes = lire("lakehouse/silver/commandes")
if "pourboire" in commandes.column_names:
 print(" La colonne pourboire existe déjà (script relancé) : rien à ajouter.")
else:
 try:
  write_deltalake("lakehouse/silver/commandes", nouvelle, mode="append")
 except Exception as e:
  print(" Sans autorisation, la nouvelle colonne est refusée :", str(e).splitlines()[0][:80])
 write_deltalake("lakehouse/silver/commandes", nouvelle, mode="append", schema_mode="merge")
 print(" Avec schema_mode='merge', la colonne pourboire est ajoutée au schéma.")
commandes = lire("lakehouse/silver/commandes")
con.sql("""SELECT commande_id, montant, pourboire FROM commandes
 WHERE commande_id IN (1, 2, 20) ORDER BY commande_id""").show()
con.close()
print("Étape 3 terminée. On peut corriger, supprimer, et le schéma est protégé.")
