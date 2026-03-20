import sqlite3

conn = sqlite3.connect("lake/gold/sales_mart.db")
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print("Tables:", tables)
for t in [row[0] for row in tables]:
    count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"  {t} : {count} lignes")
conn.close()