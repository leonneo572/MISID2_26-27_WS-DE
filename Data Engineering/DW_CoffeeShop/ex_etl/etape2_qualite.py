"""
ÉTAPE 2 : CONTRÔLE QUALITÉ
Avant de nettoyer, on cherche et on affiche chaque problème, ligne par ligne.
Rien n'est modifié dans cette étape : on fait seulement un diagnostic.
"""
import duckdb
con = duckdb.connect("mini_warehouse.duckdb")
print("=== Problème 1 : commandes en double (même commande_id plusieurs fois) ===")
con.sql("""
 SELECT commande_id, count(*) AS nb_fois
 FROM staging.commandes
 GROUP BY commande_id
 HAVING count(*) > 1
""").show()
print("=== Problème 2 : montants vides ===")
con.sql("SELECT * FROM staging.commandes WHERE montant IS NULL").show()
print("=== Problème 3 : quantités négatives (valeurs aberrantes) ===")
con.sql("SELECT * FROM staging.commandes WHERE quantite::INT < 0").show()
print("=== Problème 4 : dates au format jj/mm/aaaa au lieu de aaaa-mm-jj ===")
con.sql("SELECT commande_id, date_commande FROM staging.commandes WHERE date_commande LIKE '%/%'").show()
print("=== Problème 5 : canal écrit de plusieurs façons (casse, espaces) ===")
con.sql("""
 SELECT '[' || canal || ']' AS canal_tel_quel, count(*) AS nb
 FROM staging.commandes GROUP BY canal ORDER BY canal
""").show()
print(" (les crochets [ ] montrent les espaces cachés)")
print("\n=== Problème 6 : villes mal écrites ou manquantes ===")
con.sql("SELECT client_id, nom, '[' || coalesce(ville, 'NULL') || ']' AS ville_telle_quelle FROM staging.clients").show()
print("=== Résumé du diagnostic ===")
con.sql("""
 SELECT count(*) AS total_lignes,
 count(*) - count(DISTINCT commande_id) AS doublons,
 count(*) FILTER (WHERE montant IS NULL) AS montants_vides,
 count(*) FILTER (WHERE quantite::INT < 0) AS quantites_negatives,
 count(*) FILTER (WHERE date_commande LIKE '%/%') AS dates_format_fr,
 count(DISTINCT canal) AS valeurs_de_canal
 FROM staging.commandes
""").show()
con.close()
print("Étape 2 terminée. On sait maintenant exactement ce qu'il faut corriger.")
