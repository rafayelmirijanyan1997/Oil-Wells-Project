import os
import re
import json
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
from pdf2image import convert_from_path
import pdfplumber
import pytesseract
from PIL import Image
from sqlalchemy import (create_engine, Column, Integer, Float, Text, ForeignKey, UniqueConstraint)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
import warnings
from cryptography.utils import CryptographyDeprecationWarning
warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)


Base = declarative_base()

class Well(Base):
    __tablename__ = "wells"
    id = Column(Integer, primary_key=True)
    api = Column(Text, unique=True, nullable=True)
    well_file_no = Column(Text, unique=True, nullable=True)  # Added for NDIC tracking
    well_name = Column(Text)
    operator = Column(Text)
    county = Column(Text)
    state = Column(Text)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    datum = Column(Text, nullable=True)
    shl = Column(Text, nullable=True)
    source_pdf = Column(Text)
    address = Column(Text, nullable=True)

    stimulations = relationship("Stimulation", back_populates="well")

class Stimulation(Base):
    __tablename__ = "stimulations"
    id = Column(Integer, primary_key=True)
    well_id = Column(Integer, ForeignKey("wells.id"), nullable=False)

    date_stimulated = Column(Text, nullable=True)
    formation = Column(Text, nullable=True)
    top_ft = Column(Integer, nullable=True)
    bottom_ft = Column(Integer, nullable=True)
    stages = Column(Integer, nullable=True)
    volume = Column(Float, nullable=True)
    volume_units = Column(Text, nullable=True)
    treatment_type = Column(Text, nullable=True)
    acid_pct = Column(Float, nullable=True)
    lbs_proppant = Column(Float, nullable=True)
    max_pressure_psi = Column(Float, nullable=True)
    max_rate_bbl_min = Column(Float, nullable=True)
    details_json = Column(Text, nullable=True)
    source_pdf = Column(Text)

    well = relationship("Well", back_populates="stimulations")

#helper functions


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def try_float(x: Optional[str]) -> Optional[float]:
    if not x: 
        return None
    x = x.replace(",", "").strip()
    try: 
        return float(x)
    except: 
        return None

def try_int(x: Optional[str]) -> Optional[int]:
    if not x: 
        return None
    x = x.replace(",", "").strip()
    try: 
        return int(float(x))
    except: 
        return None


def _find(text: str, pattern: str) -> Optional[str]:
    m = re.search(pattern, text, re.I)
    return clean(m.group(1)) if m else None



def pick_scanned_pages(low_text_pages: List[int], first=25, last=25, step=15) -> List[int]:
    if not low_text_pages:
        return []
    low_text_pages = sorted(low_text_pages)
    mid = low_text_pages[first: max(first, len(low_text_pages) - last)]
    sampled_mid = mid[::step]  # every Nth scanned page
    return sorted(set(low_text_pages[:first] + low_text_pages[-last:] + sampled_mid))


def dump_ocr_page(pdf_path, page_index, out_txt_path, dpi=200):
    img = convert_from_path(pdf_path, dpi=dpi,
                            first_page=page_index+1,
                            last_page=page_index+1)[0]
    if img.mode != "RGB":
        img = img.convert("RGB")
    text = pytesseract.image_to_string(img, config="--oem 1 --psm 6")
    with open(out_txt_path, "w", encoding="utf-8") as f:
        f.write(text)
    print("Wrote:", out_txt_path)





#parsing
FORM_LABELS = {
    "well_name": ["Well Name and Number"],
    "operator": ["Operator"],
    "address": ["Address"],
    "city": ["City"],
    "state": ["State"],
    "zip": ["Zip Code", "Zip"],
    "county": ["County"],
    "well_file_no": ["Well File No.", "Well File No", "NDIC File Number", "State File No."],
}

STOP_WORDS = set([
    # common labels that should NOT be treated as values
    "Operator", "Address", "City", "State", "Zip Code", "County", "Telephone Number",
    "Field", "Section", "Township", "Range", "Qtr-Qtr", "Well File No.", "NDIC CTB No.",
    "Name of First Purchaser", "Name of Transporter", "Principal Place of Business",
])

def _norm(s):
    return re.sub(r"\s+", " ", (s or "")).strip()

def value_after_label(lines, label):
    """
    Find the line that equals `label` (or starts with it), then return the next line
    that is not empty and not a label.
    """
    label_l = label.lower()
    for i in range(len(lines)):
        if _norm(lines[i]).lower() == label_l:
            # scan forward for a value
            for j in range(i + 1, min(i + 6, len(lines))):
                v = _norm(lines[j])
                if not v:
                    continue
                # reject if this looks like another label
                if v in STOP_WORDS:
                    return None
                return v
    return None

def parse_form_page_text(txt):
    lines = [_norm(x) for x in (txt or "").splitlines()]
    lines = [x for x in lines if x]  # drop blanks

    out = {}

    # well file no (sometimes "Well File No." then number on next line)
    for lab in FORM_LABELS["well_file_no"]:
        v = value_after_label(lines, lab)
        if v and re.search(r"\b\d{3,6}\b", v):
            out["well_file_no"] = re.search(r"\b(\d{3,6})\b", v).group(1)
            break

    for key, labels in FORM_LABELS.items():
        if key in ("well_file_no",):
            continue
        if out.get(key):
            continue
        for lab in labels:
            v = value_after_label(lines, lab)
            if v:
                out[key] = v
                break

    # Normalize state to uppercase 2 letters when possible
    if out.get("state"):
        st = out["state"].strip().upper()
        if len(st) == 2:
            out["state"] = st

    # County case normalization
    if out.get("county"):
        out["county"] = out["county"].title()

    return out




def ocr_page_pdfplumber(pdf_path: str, page_index: int) -> str:
    """
    OCR a single page using pdf2image
    """
    images = convert_from_path(
        pdf_path,
        dpi=300,
        first_page=page_index + 1,
        last_page=page_index + 1
    )

    if not images:
        return ""

    img = images[0]

    if img.mode != "RGB":
        img = img.convert("RGB")

    return pytesseract.image_to_string(img)





def parse_stimulation_text(txt: str) -> List[Dict[str, Any]]:
    t = txt or ""

    # OCR-tolerant signals
    SIGNAL_PATTERNS = [
        r"\bdate\s*stim",                       # Date Stimulated
        r"\bstimulation\s*stages?\b",
        r"\bvolume\b",
        r"\bvolume\s*units?\b",
        r"\b(proppant|lbs\s*proppant|ilbs\s*proppant)\b",
        r"\bmax(?:imum)?\s*treatment\s*pressure\b",
        r"\bmax(?:imum)?\s*treatment\s*rate\b",
        r"\b(a[cg]id|rcid)\s*%?\b",             # Acid %, Acid, rcid (OCR)
        r"\bmesh\b",
    ]
    signals = sum(1 for p in SIGNAL_PATTERNS if re.search(p, t, re.I))
    if signals < 2:
        return []

    b = {}

    b["date_stimulated"] = _find(t, r"Date\s*Stim(?:ulated)?\s*([0-9]{2}/[0-9]{2}/[0-9]{4})")
    b["formation"]       = _find(t, r"Stim(?:ulated)?\s*Formation\s*([A-Za-z0-9 \-/]+)")
    b["top_ft"]          = try_int(_find(t, r"\bTop\s*\(?(?:Ft|FT)\)?\s*([0-9,]+)"))
    b["bottom_ft"]       = try_int(_find(t, r"\bBottom\s*\(?(?:Ft|FT)\)?\s*([0-9,]+)"))
    b["stages"]          = try_int(_find(t, r"Stimulation\s*Stages?\s*([0-9,]+)"))
    b["volume"]          = try_float(_find(t, r"\bVolume\b\s*([0-9,]+(?:\.[0-9]+)?)"))
    b["volume_units"]    = _find(t, r"Volume\s*Units?\s*([A-Za-z]+)")
    b["treatment_type"]  = _find(t, r"Type\s*Treatment\s*([A-Za-z0-9 \-/]+)")

    # Acid can OCR as rcid% / acid% / ac1d%
    b["acid_pct"]        = try_float(_find(t, r"(?:A[cg]id|rcid)\s*%?\s*([0-9.]+)"))

    # "ILbs Proppant" appears in W22099 :contentReference[oaicite:3]{index=3}
    b["lbs_proppant"]    = try_float(_find(t, r"(?:I?\s*Lbs)\s*Proppant\s*([0-9,]+)"))

    b["max_pressure_psi"]= try_float(_find(t, r"Maximum\s*Treatment\s*Pressure\s*\(PSI\)\s*([0-9,]+)"))
    b["max_rate_bbl_min"]= try_float(_find(t, r"Maximum\s*Treatment\s*Rate\s*\(BBLS/Min\)\s*([0-9.]+)"))

    details = {}
    for dm in re.finditer(r"([0-9]{2,3}(?:/[0-9]{2,3})?\s*Mesh\s*\w*)\s*:\s*([0-9,]+)", t, re.I):
        details[clean(dm.group(1))] = try_int(dm.group(2))
    if details:
        b["details"] = details

    strong = sum(1 for k in ["lbs_proppant", "max_pressure_psi", "stages", "formation", "date_stimulated"] if b.get(k))
    return [b] if strong >= 2 else []

# -------------------------
# Orchestrator
# -------------------------

def ocr_image(img: Image.Image) -> str:
    if img.mode != "RGB":
        img = img.convert("RGB")
    # Faster settings; good for forms/tables
    return pytesseract.image_to_string(img, config="--oem 1 --psm 6")

def ocr_pages_batch(pdf_path: str, page_indices: List[int], dpi: int = 200) -> Dict[int, str]:
    if not page_indices:
        return {}

    page_indices = sorted(set(page_indices))
    out = {}

    # chunk consecutive pages to reduce poppler launches
    chunks = []
    start = prev = page_indices[0]
    for p in page_indices[1:]:
        if p == prev + 1:
            prev = p
        else:
            chunks.append((start, prev))
            start = prev = p
    chunks.append((start, prev))

    for (a, b) in chunks:
        images = convert_from_path(pdf_path, dpi=dpi, first_page=a+1, last_page=b+1)
        for offset, img in enumerate(images):
            out[a + offset] = ocr_image(img)

    return out




WELL_KEYWORDS = ["latitude", "longitude", "datum", "surface hole location", "shl", "api"]
STIM_KEYWORDS = ["Well Specific Stimulations", "Lbs Proppant", "Stimulation Stages",
                 "Maximum Mreatment Pressure", "Maximum Treatment Rate", "Date Stimulated", "Stimulated Formation", "Top (Ft)", "Bottom (Ft)","Volume Units", ]


def extract_from_pdf(pdf_path: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    well: Dict[str, Any] = {}
    stim_blocks: List[Dict[str, Any]] = []

    low_text_pages = []
    likely_well_pages = []
    likely_stim_pages = []

    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            t = text.lower()

            # Parse from embedded text
            w = parse_form_page_text(text)
            for k, v in w.items():
                if v and not well.get(k):
                    well[k] = v

            stim_blocks.extend(parse_stimulation_text(text))

            # Track candidates
            if len(text.strip()) < 40:
                low_text_pages.append(i)

            if any(k in t for k in WELL_KEYWORDS):
                likely_well_pages.append(i)

            if any(k in t for k in STIM_KEYWORDS):
                likely_stim_pages.append(i)

    # If we already have everything without OCR, return
    have_coords = bool(well.get("latitude_raw") and well.get("longitude_raw"))
    have_well_id = bool(well.get("well_file_no") or well.get("api"))
    have_name = bool(well.get("well_name"))
    have_stim = len(stim_blocks) > 0

    if have_coords and have_well_id and have_name and have_stim:
        return well, stim_blocks

    # ---- OCR selection policy (CAPS are key for your scale) ----
    # 1) Always OCR pages that were flagged by keyword (usually small)

    N_SCANNED_CAP = 25
    pages_to_ocr = set(pick_scanned_pages(low_text_pages, first=25, last=25, step=15))
    pages_to_ocr.update(low_text_pages[:N_SCANNED_CAP])
    pages_to_ocr.update(low_text_pages[-8:])


    pages_to_ocr.update(likely_well_pages)
    pages_to_ocr.update(likely_stim_pages)

    # 2) Add only the FIRST N low-text pages (scanned), not all of them
    #    Tune: 15–30 is a good range for 200–300 page PDFs
    
    pages_to_ocr.update(low_text_pages[:N_SCANNED_CAP])

    # 3) (Optional) also OCR last few low-text pages (some packets put stim near end)
    pages_to_ocr.update(low_text_pages[-8:])

    # Batch OCR (FAST)
    ocr_map = ocr_pages_batch(pdf_path, sorted(pages_to_ocr), dpi=200)

    for i in sorted(ocr_map.keys()):
        ocr_txt = ocr_map[i]

        w2 = parse_form_page_text(ocr_txt)
        for k, v in w2.items():
            if v and not well.get(k):
                well[k] = v

        if len(stim_blocks) == 0:
            stim_blocks.extend(parse_stimulation_text(ocr_txt))

        # Early stop conditions (critical speed boost)
        have_coords = bool(well.get("latitude_raw") and well.get("longitude_raw"))
        have_well_id = bool(well.get("well_file_no") or well.get("api"))
        have_name = bool(well.get("well_name"))
        have_stim = len(stim_blocks) > 0

        if have_coords and have_well_id and have_name and have_stim:
            break

    return well, stim_blocks

def upsert_to_db(session, well_data: Dict[str, Any], stim_data: List[Dict[str, Any]], source_pdf: str):
    well_file_no = well_data.get("well_file_no")
    api = well_data.get("api")

    # Match by File No or API
    well_obj = None
    if well_file_no:
        well_obj = session.query(Well).filter(Well.well_file_no == well_file_no).one_or_none()
    elif api:
        well_obj = session.query(Well).filter(Well.api == api).one_or_none()

    if well_obj is None:
        well_obj = Well(
            api=api,
            well_file_no=well_file_no,
            well_name=well_data.get("well_name"),
            address=well_data.get("address"),
            operator=well_data.get("operator"),
            county=well_data.get("county"),
            source_pdf=source_pdf,
        )
        session.add(well_obj)
        session.flush()

    for s in stim_data:
        session.add(Stimulation(
        well_id=well_obj.id,
        date_stimulated=s.get("date_stimulated"),
        formation=s.get("formation"),
        top_ft=s.get("top_ft"),
        bottom_ft=s.get("bottom_ft"),
        stages=s.get("stages"),
        volume=s.get("volume"),
        volume_units=s.get("volume_units"),
        treatment_type=s.get("treatment_type"),
        acid_pct=s.get("acid_pct"),
        lbs_proppant=s.get("lbs_proppant"),
        max_pressure_psi=s.get("max_pressure_psi"),
        max_rate_bbl_min=s.get("max_rate_bbl_min"),
        details_json=json.dumps(s.get("details", {})) if s.get("details") else None,
        source_pdf=source_pdf
        ))



def main(pdf_folder: str, db_path: str = "wells.sqlite"):
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    pdfs = [p for p in os.listdir(pdf_folder) if p.lower().endswith(".pdf")]
    pdfs.sort()

    with Session() as session:
        for filename in pdfs:
            pdf_path = os.path.join(pdf_folder, filename)
            try:
                well, stims = extract_from_pdf(pdf_path)
                upsert_to_db(session, well, stims, source_pdf=filename)
                session.commit()
                print(f"[OK] {filename}: FileNo={well.get('well_file_no')} Stims={len(stims)}")
            except Exception as e:
                session.rollback()
                print(f"[FAIL] {filename}: {e}")

if __name__ == "__main__":
    main("./DSCI560_Lab5")
    