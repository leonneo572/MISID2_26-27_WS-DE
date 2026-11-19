from datetime import datetime
import duckdb
from deltalake import DeltaTable
con = duckdb.connect()

# ================= 1. Historique =================
print("=== 1. Historique de silver/commandes ===")
dt = DeltaTable("lakehouse/silver/commandes")
for h in reversed(dt.history()):
 quand = datetime.fromtimestamp(h["timestamp"] / 1000).strftime("%d/%m/%Y %H:%M:%S")
 print(f" version {h['version']} : {h['operation']:7} le {quand}")
# ================= 2. Time travel commande_id = 16 =================
commandes_v0 = DeltaTable("lakehouse/silver/commandes", version=0).to_pyarrow_table()
print("\n silver/commandes en version 0 (commande_id = 16) :")
con.sql("SELECT * FROM commandes_v0 WHERE commande_id = 16").show()
commandes_v1 = DeltaTable("lakehouse/silver/commandes", version=1).to_pyarrow_table()
print(" silver/commandes en version 1 (commande_id = 16) :")
con.sql("SELECT * FROM commandes_v1 WHERE commande_id = 16").show()

con.close()