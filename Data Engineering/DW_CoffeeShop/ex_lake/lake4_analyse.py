"""
ÉTAPE 4 : ANALYSES CROISÉES DIRECTEMENT SUR LE LAKE
On répond aux questions du gérant en croisant commandes, avis et livraisons.
Attention : les commandes de la zone processed ne sont PAS nettoyées.
Chaque requête doit donc refaire elle-même le nettoyage nécessaire.
"""
import duckdb
con = duckdb.connect()
# Une "vue" n'enregistre aucune donnée : c'est seulement une requête à laquelle on donne un nom.
# Le nettoyage du TP1 est réécrit ici, car le lake ne l'a jamais fait.
con.sql("""
 CREATE VIEW commandes AS
 SELECT DISTINCT
 commande_id::INT AS commande_id,
 coalesce(try_strptime(date_commande, '%Y-%m-%d'),
 try_strptime(date_commande, '%d/%m/%Y'))::DATE AS date_commande,
 client_id::INT AS client_id, produit_id::INT AS produit_id,
 quantite::INT AS quantite, montant::DOUBLE AS montant,
 lower(trim(canal)) AS canal
 FROM 'lake/processed/commandes/*/*.parquet'
 WHERE montant IS NOT NULL AND quantite::INT > 0
""")
con.sql("CREATE VIEW avis AS SELECT * FROM 'lake/processed/avis.parquet'")
con.sql("CREATE VIEW livraisons AS SELECT * FROM 'lake/processed/livraisons.parquet'")
con.sql("CREATE VIEW produits AS SELECT produit_id::INT AS produit_id, nom_produit, categorie "
 "FROM 'lake/processed/produits.parquet'")
print("=== Question 1 : note moyenne des clients par produit (commandes + avis + produits) ===")
con.sql("""
 SELECT p.nom_produit, count(a.avis_id) AS nb_avis, round(avg(a.note), 2) AS 
note_moyenne
 FROM avis a
 JOIN commandes c USING (commande_id)
 JOIN produits p USING (produit_id)
 GROUP BY p.nom_produit ORDER BY note_moyenne DESC
""").show()
print("=== Question 2 : la durée de livraison influence-t-elle la note ? (livraisons + avis) ===")
con.sql("""
 SELECT l.commande_id, l.livreur, l.statut, l.duree_min, a.note, a.commentaire
 FROM livraisons l
 LEFT JOIN avis a USING (commande_id)
 ORDER BY l.duree_min DESC
""").show(max_width=200)
print("=== Question 3 : performance des livreurs ===")
con.sql("""
 SELECT livreur,
 count(*) AS nb_courses,
 count(*) FILTER (WHERE statut = 'RETARD') AS nb_retards,
 count(*) FILTER (WHERE statut = 'ANNULEE') AS nb_annulees,
 round(avg(duree_min) FILTER (WHERE statut <> 'ANNULEE'), 1) AS 
duree_moyenne_min
 FROM livraisons GROUP BY livreur ORDER BY nb_courses DESC
""").show()

print("=== Question 3.1 : performance des livreurs par note ===")
con.sql("""
 SELECT livreur,
 count(*) AS nb_courses,
 round(avg(a.note) FILTER (WHERE a.note NOT NULL), 1) AS note_moyenne
 FROM livraisons 
 JOIN avis a USING (commande_id) 
 GROUP BY livreur 
 ORDER BY note_moyenne DESC
""").show()

print("=== Question 4 : chiffre d'affaires par jour, lots 1 et 2 réunis ===")
con.sql("""
 SELECT date_commande, count(*) AS nb_commandes, round(sum(montant), 3) AS ca
 FROM commandes GROUP BY date_commande ORDER BY date_commande
""").show()
print("=== Question 5 : que disent les clients mécontents ? (texte libre) ===")
con.sql("""
 SELECT note, commentaire, tags FROM avis WHERE note <= 3 ORDER BY note
""").show(max_width=200)
con.close()
print("Étape 4 terminée. On a croisé trois sources de formats différents, sans base de données.")
