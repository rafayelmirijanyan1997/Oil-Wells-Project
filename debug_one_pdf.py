import os
import re
import time
import json
import pdfplumber
from pdf2image import convert_from_path
import pytesseract

# =======================
# CONFIG (EDIT THESE)
# =======================
PDF_PATH = r"/Users/rafayelmirijanyan/Desktop/Sprint 2026/DSCI 560/week 6/DSCI560_Lab5/W11745.pdf"   # <-- change to your PDF
OUT_DIR = "ocr_full_dump"                 # output folder
DPI = 200                                 # 150 faster, 200 good, 300 slow but best
PSM = 6                                   # good for blocks/tables
OEM = 1                                   # LSTM engine
BATCH_SIZE = 6                            # pages per poppler call (bigger = faster, but uses more RAM)
MAX_PAGES = None                          # set to e.g. 50 for quick test; None = all pages

# keywords you care about
WELL_KEYWORDS = [
    "well name", "well name and number", "operator", "county", "state", "address",
    "well file", "api"
]
COORD_KEYWORDS = [
    "latitude", "longitude", "datum", "shl", "surface hole location"
]
STIM_KEYWORDS = [
    "well specific stimulation", "well specific stimulations", "stimulation stages",
    "lbs proppant", "maximum treatment pressure", "maximum treatment rate",
    "bbls/min", "acid %", "mesh", "proppant", "date stimulated", "Stimulated Formation","Top (Ft)","Bottom(Ft)","Stimulation Stages",
"Volume","volume units","type treatment","lbs proppant", "maximum treatment pressure (PSI)", "maximum treatment rate (BBLS/Min)"]

# =======================
# HELPERS
# =======================
def ensure_dir(p):
    if not os.path.exists(p):
        os.makedirs(p)

def chunk_ranges(n_pages, batch_size):
    """Yield (start, end) inclusive ranges for 0-index pages."""
    start = 0
    while start < n_pages:
        end = min(n_pages - 1, start + batch_size - 1)
        yield start, end
        start = end + 1

def score_keywords(text, keywords):
    t = (text or "").lower()
    return [k for k in keywords if k in t]

def ocr_images(images):
    cfg = f"--oem {OEM} --psm {PSM}"
    texts = []
    for img in images:
        texts.append(pytesseract.image_to_string(img, config=cfg))
    return texts

# =======================
# MAIN
# =======================
def main():
    ensure_dir(OUT_DIR)
    txt_dir = os.path.join(OUT_DIR, "pages")
    ensure_dir(txt_dir)

    # get page count cheaply
    with pdfplumber.open(PDF_PATH) as pdf:
        n_pages = len(pdf.pages)

    if MAX_PAGES is not None:
        n_pages = min(n_pages, MAX_PAGES)

    print("PDF:", PDF_PATH)
    print("Pages to OCR:", n_pages)
    print("DPI:", DPI, "Batch:", BATCH_SIZE)

    index = []  # list of dicts: page, matches, snippet
    t0 = time.time()

    for start, end in chunk_ranges(n_pages, BATCH_SIZE):
        # pdf2image uses 1-indexed first/last page
        images = convert_from_path(
            PDF_PATH,
            dpi=DPI,
            first_page=start + 1,
            last_page=end + 1
        )

        texts = ocr_images(images)

        for offset, text in enumerate(texts):
            page_i = start + offset
            out_path = os.path.join(txt_dir, f"ocr_page_{page_i:03d}.txt")
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(text)

            well_hits = score_keywords(text, WELL_KEYWORDS)
            coord_hits = score_keywords(text, COORD_KEYWORDS)
            stim_hits = score_keywords(text, STIM_KEYWORDS)

            entry = {
                "page": page_i,
                "well_hits": well_hits,
                "coord_hits": coord_hits,
                "stim_hits": stim_hits,
                "snippet": (re.sub(r"\s+", " ", text).strip()[:240] if text else "")
            }
            index.append(entry)

            # Print progress and interesting hits
            if (page_i % 10) == 0:
                elapsed = time.time() - t0
                print(f"page {page_i:03d}/{n_pages-1:03d}  elapsed={elapsed:.1f}s")

            if len(well_hits) >= 2 or len(coord_hits) >= 2 or len(stim_hits) >= 2:
                print(f"\n=== HIT page {page_i} ===")
                if well_hits:  print("  WELL:", well_hits)
                if coord_hits: print("  COORD:", coord_hits)
                if stim_hits:  print("  STIM:", stim_hits)
                print("  snippet:", entry["snippet"])
                print("  file:", out_path)

    # Write full index JSON
    index_path = os.path.join(OUT_DIR, "index.json")
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2)

    # Write human summary
    summary_lines = []
    stim_pages = [e["page"] for e in index if len(e["stim_hits"]) >= 2]
    coord_pages = [e["page"] for e in index if len(e["coord_hits"]) >= 2]
    well_pages = [e["page"] for e in index if len(e["well_hits"]) >= 2]

    summary_lines.append(f"PDF: {PDF_PATH}")
    summary_lines.append(f"Pages OCR'd: {n_pages}")
    summary_lines.append(f"Likely WELL pages (>=2 hits): {well_pages}")
    summary_lines.append(f"Likely COORD pages (>=2 hits): {coord_pages}")
    summary_lines.append(f"Likely STIM pages (>=2 hits): {stim_pages}")

    summary_path = os.path.join(OUT_DIR, "summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines))

    print("\nDONE.")
    print("All OCR text saved in:", txt_dir)
    print("Index JSON:", index_path)
    print("Summary:", summary_path)
    print("Likely STIM pages:", stim_pages)

if __name__ == "__main__":
    main()