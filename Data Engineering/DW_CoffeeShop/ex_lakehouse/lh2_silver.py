"""
ÉTAPE 2 : COUCHE SILVER ET MERGE INCRÉMENTAL
1) On nettoie les données bronze une fois pour toutes et on les range dans la couche
silver.
2) Le deuxième lot de commandes arrive : on l'ajoute au bronze, puis on l'intègre
 au silver avec MERGE, sans créer de doublon.
"""
import duckdb
from deltalake import DeltaTable, write_deltalake
con = duckdb.connect()
con.sql("SET produce_arrow_string_view = false")
def arrow(requete):
 return con.sql(requete).arrow().read_all()
def lire(chemin):
 """Charge une table Delta pour l'interroger en SQL avec DuckDB."""
 return DeltaTable(chemin).to_pyarrow_table()
def nettoyer_commandes(source):
 """Les règles de nettoyage du TP1, écrites une seule fois."""
 return f"""
 SELECT commande_id::INT AS commande_id,
 coalesce(try_strptime(date_commande, '%Y-%m-%d'),
 try_strptime(date_commande, '%d/%m/%Y'))::DATE AS 
date_commande,
 client_id::INT AS client_id, produit_id::INT AS produit_id,
 quantite::INT AS quantite, montant::DOUBLE AS montant,
 lower(trim(canal)) AS canal,
 date_ingestion
 FROM {source}
 WHERE montant IS NOT NULL AND quantite::INT > 0
 QUALIFY row_number() OVER (PARTITION BY commande_id::INT ORDER BY 
 date_ingestion DESC) = 1
 """
# ================= 1. Silver initial à partir du lot 1 =================
bronze_commandes = lire("lakehouse/bronze/commandes")
write_deltalake("lakehouse/silver/commandes",
 arrow(nettoyer_commandes("bronze_commandes")), mode="overwrite", schema_mode="merge")
bronze_clients = lire("lakehouse/bronze/clients")
write_deltalake("lakehouse/silver/clients", arrow("""
 SELECT client_id::INT AS client_id, nom,
 coalesce(array_to_string(list_transform(string_split(lower(trim(ville)), ' 
'),
 lambda x: upper(x[1]) || x[2:]), ' '), 'Inconnue') AS ville
 FROM bronze_clients"""), mode="overwrite")
bronze_produits = lire("lakehouse/bronze/produits")
write_deltalake("lakehouse/silver/produits", arrow("""
 SELECT produit_id::INT AS produit_id, nom_produit, categorie, 
prix_unitaire::DOUBLE AS prix_unitaire
 FROM bronze_produits"""), mode="overwrite")
bronze_avis = lire("lakehouse/bronze/avis")
write_deltalake("lakehouse/silver/avis", arrow("""
 SELECT avis_id::INT AS avis_id, commande_id::INT AS commande_id, date AS 
date_avis,
 note::INT AS note, commentaire, client.id::INT AS client_id, 
client.appareil AS appareil, tags
 FROM bronze_avis"""), mode="overwrite")
bronze_livraisons = lire("lakehouse/bronze/livraisons")
write_deltalake("lakehouse/silver/livraisons", arrow("""
 SELECT regexp_extract(ligne, '^(\\S+ \\S+)', 1)::TIMESTAMP AS horodatage,
 regexp_extract(ligne, 'commande=(\\d+)', 1)::INT AS commande_id,
 regexp_extract(ligne, 'livreur=(\\w+)', 1) AS livreur,
 regexp_extract(ligne, 'statut=(\\w+)', 1) AS statut,
 regexp_extract(ligne, 'duree=(\\d+)min', 1)::INT AS duree_min
 FROM bronze_livraisons"""), mode="overwrite")
for t in ["commandes", "clients", "produits", "avis", "livraisons"]:
 print(f"silver/{t:11} : {lire(f'lakehouse/silver/{t}').num_rows:>2} lignes")
# ================= 2. Le lot 2 arrive : bronze en APPEND =================
LOT2 = "2025-03-09"
deja = con.sql(f"SELECT count(*) FROM bronze_commandes WHERE date_ingestion = DATE '{LOT2}'").fetchall()[0][0]
if deja == 0: # idempotence : on n'ajoute pas deux fois le même lot si on relance le script
 lot2 = arrow(f"""SELECT *, DATE '{LOT2}' AS date_ingestion
 FROM read_csv('sources/commandes_j2.csv', all_varchar = 
 true)""")
 write_deltalake("lakehouse/bronze/commandes", lot2, mode="append")
 print(f"\nLot {LOT2} ajouté au bronze : {lot2.num_rows} lignes")
else:
 print(f"\nLot {LOT2} déjà présent dans le bronze : rien à ajouter")
# ================= 3. MERGE du lot 2 dans le silver =================
bronze_commandes = lire("lakehouse/bronze/commandes")
nouvelles = arrow(nettoyer_commandes(f"(SELECT * FROM bronze_commandes WHERE date_ingestion = DATE '{LOT2}')"))
print("\n=== Lot 2 après nettoyage ===")
con.sql("SELECT * FROM nouvelles ORDER BY commande_id").show()
resultat = (
 DeltaTable("lakehouse/silver/commandes")
 .merge(source=nouvelles, predicate="cible.commande_id = lot.commande_id",
 source_alias="lot", target_alias="cible")
 .when_matched_update_all() # la commande existe déjà : on la met à jour
 .when_not_matched_insert_all() # la commande est nouvelle : on l'insère
 .execute()
)
print(f"MERGE : {resultat['num_target_rows_updated']} ligne(s) mise(s) à jour, "
 f"{resultat['num_target_rows_inserted']} ligne(s) insérée(s)")
silver = lire("lakehouse/silver/commandes")
print(f"silver/commandes contient maintenant {silver.num_rows} commandes")
con.sql("""SELECT count(*) - count(DISTINCT commande_id) AS doublons,
 count(DISTINCT canal) AS valeurs_de_canal FROM silver""").show()
print("\n=== Historique de silver/commandes ===")
for h in DeltaTable("lakehouse/silver/commandes").history():
 print(f" version {h['version']} : {h['operation']}")

 # ================= 4. Le lot 3 arrive : bronze en APPEND =================
 LOT3 = "2025-03-10"
 deja = con.sql(f"SELECT count(*) FROM bronze_commandes WHERE date_ingestion = DATE '{LOT3}'").fetchall()[0][0]
 if deja == 0:  # idempotence : on n'ajoute pas deux fois le même lot si on relance le script
  lot3 = arrow(f"""SELECT *, DATE '{LOT3}' AS date_ingestion
  FROM read_csv('sources/commandes_j3.csv', all_varchar = 
  true)""")
  write_deltalake("lakehouse/bronze/commandes", lot3, mode="append", schema_mode="merge")
  print(f"\nLot {LOT3} ajouté au bronze : {lot3.num_rows} lignes")
 else:
  print(f"\nLot {LOT3} déjà présent dans le bronze : rien à ajouter")
 # ================= 5. MERGE du lot 3 dans le silver =================
 bronze_commandes = lire("lakehouse/bronze/commandes")
 nouvelles = arrow(nettoyer_commandes(f"(SELECT * FROM bronze_commandes WHERE date_ingestion = DATE '{LOT3}')"))
 print("\n=== Lot 3 après nettoyage ===")
 con.sql("SELECT * FROM nouvelles ORDER BY commande_id").show()
 resultat = (
  DeltaTable("lakehouse/silver/commandes")
  .merge(source=nouvelles, predicate="cible.commande_id = lot.commande_id",
         source_alias="lot", target_alias="cible")
  .when_matched_update_all()  # la commande existe déjà : on la met à jour
  .when_not_matched_insert_all()  # la commande est nouvelle : on l'insère
  .execute()
 )
 print(f"MERGE : {resultat['num_target_rows_updated']} ligne(s) mise(s) à jour, "
       f"{resultat['num_target_rows_inserted']} ligne(s) insérée(s)")
 silver = lire("lakehouse/silver/commandes")
 print(f"silver/commandes contient maintenant {silver.num_rows} commandes")
 con.sql("""SELECT count(*) - count(DISTINCT commande_id) AS doublons,
  count(DISTINCT canal) AS valeurs_de_canal FROM silver""").show()
 print("\n=== Historique de silver/commandes ===")
 for h in DeltaTable("lakehouse/silver/commandes").history():
  print(f" version {h['version']} : {h['operation']}")

con.close()
print("\nÉtape 2 terminée. Le silver est propre, et un nouveau lot s'intègre sans doublon.")
