"""
ÉTAPE 1 : INGESTION BRUTE DANS LA ZONE RAW
On copie chaque fichier source TEL QUEL dans le data lake.
Rangement : lake/raw/<source>/<table>/date_ingestion=<date>/<fichier>
Aucune transformation, aucune base de données : seulement des dossiers et des
fichiers.
"""
import shutil
from pathlib import Path
RAW = Path("lake/raw")
SRC = Path("sources")
# (fichier source, système source, table, date du lot d'ingestion)
LOTS = [
 # Lot 1 : reçu le 8 mars 2025
 ("sources/commandes.csv", "caisse", "commandes", "2025-03-08"),
 ("sources/produits.csv", "caisse", "produits", "2025-03-08"),
 ("sources/clients.csv", "crm", "clients", "2025-03-08"),
 ("sources/avis.json", "app", "avis", "2025-03-08"),
 ("sources/livraisons.log", "coursiers", "livraisons", "2025-03-08"),
 # Lot 2 : reçu le 9 mars 2025 (nouvelles commandes seulement)
 ("sources/commandes_j2.csv", "caisse", "commandes", "2025-03-09"),
 # Lot 3 : reçu le 10 mars 2025 (nouvelles commandes seulement)
 ("sources/commandes_j3.csv", "caisse", "commandes", "2025-03-10"),
]
for p in SRC.rglob("*.jpg"):
 # Lot 3 : reçu le 10 mars 2025 (photos des plats)
 print(p.name)
 LOTS.append((f"sources/{p.name}", "marketing", "photos", "2025-03-10"))

for fichier, source, table, date_lot in LOTS:
 dossier = RAW / source / table / f"date_ingestion={date_lot}"
 dossier.mkdir(parents=True, exist_ok=True) # crée les dossiers s'ils n'existent pas
 shutil.copy(fichier, dossier / Path(fichier).name) # copie à l'identique, sans rien modifier
 print(f"Ingéré : {fichier:28} -> {dossier}")
print("\n=== Contenu du data lake (zone raw) ===")
for f in sorted(RAW.rglob("*.*")):
 print(f" {str(f):75} {f.stat().st_size:>6} octets")
nb = len(list(RAW.rglob("*.*")))
print(f"\n{nb} fichiers dans le lake. Formats présents :",
 sorted({f.suffix for f in RAW.rglob('*.*')}))
print("Étape 1 terminée. Les données brutes sont stockées, quel que soit leur format.")
