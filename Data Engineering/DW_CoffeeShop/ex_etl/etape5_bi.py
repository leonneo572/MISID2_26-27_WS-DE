"""
ÉTAPE 5 : ANALYSE (BI)
On répond aux questions du gérant en joignant la table de faits aux dimensions.
"""
import duckdb
con = duckdb.connect("mini_warehouse.duckdb")
print("=== Question 1 : quel est le chiffre d'affaires total ? ===")
con.sql("""
 SELECT count(*) AS nb_commandes, round(sum(montant_ht), 3) AS ca_total
 FROM dwh.fact_commandes
""").show()
print("=== Question 2 : CA par jour de la semaine (jointure avec dim_date) ===")
con.sql("""
 SELECT d.jour_semaine, count(*) AS nb_commandes, round(sum(f.montant_ht), 3) AS 
ca
 FROM dwh.fact_commandes f
 JOIN dwh.dim_date d USING (date_id)
 GROUP BY d.jour_semaine
 ORDER BY ca DESC
""").show()
print("=== Question 3 : CA par catégorie de produit (jointure avec dim_produit) ===")
con.sql("""
 SELECT p.categorie, sum(f.quantite) AS quantite_vendue, round(sum(f.montant_ht), 
3) AS ca
 FROM dwh.fact_commandes f
 JOIN dwh.dim_produit p USING (produit_id)
 GROUP BY p.categorie
 ORDER BY ca DESC
""").show()
print("=== Question 4 : CA par ville du client (jointure avec dim_client) ===")
con.sql("""
 SELECT c.ville, count(*) AS nb_commandes, round(sum(f.montant_ht), 3) AS ca
 FROM dwh.fact_commandes f
 JOIN dwh.dim_client c USING (client_id)
 GROUP BY c.ville
 ORDER BY ca DESC
""").show()
print("=== Question 5 : sur place ou en livraison ? ===")
con.sql("""
 SELECT canal, count(*) AS nb_commandes, round(sum(montant_ht), 3) AS ca
 FROM dwh.fact_commandes
 GROUP BY canal
 ORDER BY ca DESC
""").show()
print("=== Question 6 : tout en même temps (les 3 dimensions) ===")
con.sql("""
 SELECT d.jour_semaine, c.ville, p.nom_produit, f.quantite, f.montant_ht
 FROM dwh.fact_commandes f
 JOIN dwh.dim_date d USING (date_id)
 JOIN dwh.dim_client c USING (client_id)
 JOIN dwh.dim_produit p USING (produit_id)
 ORDER BY f.montant_ht DESC
 LIMIT 5
""").show()
con.close()
print("Étape 5 terminée. Le cycle ETL complet est fini : sources -> staging -> propre -> dwh -> BI.")
