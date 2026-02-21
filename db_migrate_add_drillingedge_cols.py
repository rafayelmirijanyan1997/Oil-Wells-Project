
import sqlite3

DB_PATH = "wells.sqlite"

NEW_COLS = [
    ("drillingedge_url", "TEXT"),
    ("well_status", "TEXT"),
    ("well_type", "TEXT"),
    ("closest_city", "TEXT"),
    ("latest_oil_bbl", "REAL"),
    ("latest_gas_mcf", "REAL"),
    ("latest_prod_label", "TEXT")  # e.g. "May 2023" or "Dec 2025"
]

def col_exists(cur, table, col):
    cur.execute("PRAGMA table_info(%s)" % table)
    cols = [r[1] for r in cur.fetchall()]
    return col in cols

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

for col, typ in NEW_COLS:
    if not col_exists(cur, "wells", col):
        cur.execute("ALTER TABLE wells ADD COLUMN %s %s" % (col, typ))
        print("Added:", col)
    else:
        print("Exists:", col)

con.commit()
con.close()
print("Done.")