"""
ÉTAPE 5 : COUCHE GOLD
On construit, à partir du silver, les tables d'indicateurs que le gérant consulte.
Le nettoyage est déjà fait dans le silver : les requêtes gold sont simples.
"""
import duckdb
from deltalake import DeltaTable, write_deltalake
con = duckdb.connect()
con.sql("SET produce_arrow_string_view = false")
commandes = DeltaTable("lakehouse/silver/commandes").to_pyarrow_table()
produits = DeltaTable("lakehouse/silver/produits").to_pyarrow_table()
avis = DeltaTable("lakehouse/silver/avis").to_pyarrow_table()
livraisons = DeltaTable("lakehouse/silver/livraisons").to_pyarrow_table()
clients = DeltaTable("lakehouse/silver/clients").to_pyarrow_table()
GOLD = {
 "ca_par_jour": """
 SELECT date_commande, count(*) AS nb_commandes,
 round(sum(montant), 3) AS ca, round(sum(coalesce(pourboire, 0)), 3) AS 
pourboires
 FROM commandes GROUP BY date_commande ORDER BY date_commande""",
"ca_par_canal": """
 SELECT canal, count(*) AS nb_commandes,
 round(sum(montant), 3) AS ca, round(ca/nb_commandes, 3) AS panier_moyen
 FROM commandes 
 WHERE canal IN ('sur place', 'livraison') 
 GROUP BY canal ORDER BY ca""",
"ca_par_ville": """
 SELECT ville, canal, count(*) AS nb_commandes,
 round(sum(montant), 3) AS ca, round(ca/nb_commandes, 3) AS panier_moyen
 FROM commandes JOIN clients  USING (client_id)
 GROUP BY ville, canal ORDER BY ca DESC""",
 "satisfaction_produit": """
 SELECT p.nom_produit, count(a.avis_id) AS nb_avis, round(avg(a.note), 2) AS 
note_moyenne
 FROM avis a JOIN commandes c USING (commande_id) JOIN produits p USING 
(produit_id)
 GROUP BY p.nom_produit ORDER BY note_moyenne DESC""",
 "performance_livreurs": """
 SELECT l.livreur, count(*) AS nb_courses,
 count(*) FILTER (WHERE l.statut = 'RETARD') AS nb_retards,
 count(*) FILTER (WHERE l.statut = 'ANNULEE') AS nb_annulees,
 round(avg(l.duree_min) FILTER (WHERE l.statut <> 'ANNULEE'), 1) AS 
duree_moyenne_min,
 round(avg(a.note), 2) AS note_moyenne
 FROM livraisons l LEFT JOIN avis a USING (commande_id)
 GROUP BY l.livreur ORDER BY nb_courses DESC""",
}
for table, requete in GOLD.items():
 resultat = con.sql(requete).arrow().read_all()
 write_deltalake(f"lakehouse/gold/{table}", resultat, mode="overwrite")
 print(f"=== gold/{table} ===")
 con.sql("SELECT * FROM resultat").show()
# ---------- Contrôle de cohérence entre silver et gold ----------
total_silver = con.sql("SELECT round(sum(montant), 3) FROM commandes").fetchall()[0][0]
gold_jour = DeltaTable("lakehouse/gold/ca_par_jour").to_pyarrow_table()
total_gold = con.sql("SELECT round(sum(ca), 3) FROM gold_jour").fetchall()[0][0]
print(f"Contrôle : CA total silver = {total_silver} | somme gold par jour = {total_gold} " f"-> {'OK' if total_silver == total_gold else 'ÉCART !'}")
con.close()
print("\nÉtape 5 terminée. Le lakehouse complet : bronze (brut) -> silver (propre) -> gold (indicateurs).")
