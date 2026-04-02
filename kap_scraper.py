"""
KAP Pay Alım Satım Bildirimi Scraper
- Liste:  POST /tr/api/disclosure/list/main
- Detay:  GET  /tr/api/BildirimPdf/{discIndex}  → PDF (bildirim + ek dosya)
- Parse:  pdfplumber ile tablolardan veri çıkarma

Kurulum:
    pip install requests pandas openpyxl beautifulsoup4 lxml pdfplumber
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from datetime import datetime, date
import re, os, sys, time, json, io

BASE_URL = "https://www.kap.org.tr"
HEADERS  = {
    "User-Agent":    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                     "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept":        "*/*",
    "Accept-Language":"tr",
    "Content-Type":  "application/json",
    "Origin":        BASE_URL,
    "Referer":       f"{BASE_URL}/tr",
}


# ─── LİSTE API ───────────────────────────────────────────

def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        s.get(f"{BASE_URL}/tr", timeout=15)
    except Exception as e:
        print(f"  Session uyarı: {e}")
    return s


def fetch_main_api(session, start_date: date, end_date: date) -> list:
    payload = {
        "fromDate":    start_date.strftime("%d.%m.%Y"),
        "toDate":      end_date.strftime("%d.%m.%Y"),
        "memberTypes": ["IGS", "DDK"],
    }
    try:
        r = session.post(f"{BASE_URL}/tr/api/disclosure/list/main",
                         json=payload, timeout=30)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                print(f"  ✓ API: {len(data)} kayıt")
                return data
        print(f"  API HTTP {r.status_code}")
    except Exception as e:
        print(f"  API hata: {e}")
    return []


def filter_pay_alim_satim(items: list) -> list:
    kw = ["pay alım satım", "pay alim satim"]
    out = []
    for item in items:
        b = item.get("disclosureBasic", item)
        t = " ".join([
            b.get("title","") or "", b.get("summary","") or ""
        ]).lower().replace("ı","i")
        if any(k.replace("ı","i") in t for k in kw):
            out.append(item)
    return out


def normalize_item(item: dict) -> dict:
    b     = item.get("disclosureBasic", item)
    idx   = b.get("disclosureIndex") or ""
    link  = f"{BASE_URL}/tr/Bildirim/{idx}" if idx else ""

    rs_raw = b.get("relatedStocks") or ""
    ilgili = _clean_related(rs_raw)

    stock_code = (b.get("stockCode") or "").strip()
    company = b.get("companyTitle") or ""

    return {
        "no":            str(idx),
        "tarih":         b.get("publishDate") or "",
        "kod":           stock_code,
        "sirket":        company,
        "konu":          b.get("title") or "",
        "ozet":          b.get("summary") or "",
        "link":          link,
        "disc_index":    str(idx),
        "ilgili_sirket": ilgili,
    }


def _clean_related(rs) -> str:
    if not rs:
        return ""
    if isinstance(rs, list):
        parts = []
        for r in rs:
            if isinstance(r, dict):
                c = r.get("stockCode","") or r.get("code","")
            else:
                c = str(r)
            c = c.strip().replace("[","").replace("]","")
            if 2 <= len(c) <= 10:
                parts.append(c)
        return ", ".join(parts)
    clean = str(rs).replace("[","").replace("]","").strip()
    parts = [p.strip() for p in clean.split() if 2 <= len(p.strip()) <= 10]
    return ", ".join(parts)


# ─── BildirimPdf API + PDF PARSE ─────────────────────────

def fetch_bildirim_pdf(session, disc_index: str, log_fn=print) -> dict:
    """
    KAP BildirimPdf API: /tr/api/BildirimPdf/{discIndex}
    Bildirimin tamamını (açıklama + ek dosyalar) tek PDF olarak döndürür.
    """
    result = {
        "islem_tarihi": "",
        "islem_niteligi": "",
        "nominal_tutar": "",
        "fiyat": "",
        "islem_tutari": "",
        "oncesi_nominal": "",
        "oncesi_sermaye": "",
        "sonrasi_nominal": "",
        "sonrasi_sermaye": "",
        "sirket_adi": "",
    }

    if not disc_index:
        return result

    url = f"{BASE_URL}/tr/api/BildirimPdf/{disc_index}"

    try:
        headers_pdf = {
            "User-Agent": HEADERS["User-Agent"],
            "Accept": "application/pdf,*/*",
            "Accept-Language": "tr",
            "Referer": f"{BASE_URL}/tr/Bildirim/{disc_index}",
        }
        r = session.get(url, headers=headers_pdf, timeout=30)
        log_fn(f"    📄 BildirimPdf HTTP {r.status_code} ({len(r.content)} bytes)")

        if r.status_code != 200 or len(r.content) < 500:
            return result

        # PDF mi kontrol
        if b'%PDF' not in r.content[:20]:
            # Belki HTML geldi — tablo var mı bak
            html_result = _parse_html_for_table(r.text, log_fn)
            if html_result:
                return html_result
            log_fn(f"    ⚠ PDF değil ve HTML tabloda veri bulunamadı")
            return result

        return _parse_pdf_bytes(r.content, log_fn)

    except Exception as e:
        log_fn(f"    ✗ BildirimPdf hata: {e}")
        return result


def _parse_html_for_table(html: str, log_fn=print) -> dict:
    """BildirimPdf bazen HTML döndürebilir — tablo varsa parse et."""
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = [td.get_text(strip=True).replace("\n"," ")
                     for td in row.find_all(["td","th"])]
            joined = " ".join(cells)
            if (len(cells) >= 5 and
                re.search(r'\d{2}[/\.]\d{2}[/\.]\d{4}', joined) and
                '%' in joined):
                result = _map_html_row(cells)
                if result.get("islem_tarihi"):
                    # Şirket adı
                    for sel in ["div.comp-name","span.comp-name","a.comp-name"]:
                        el = soup.select_one(sel)
                        if el:
                            name = el.get_text(strip=True)
                            if name and "KAMUYU AYDINLATMA" not in name.upper():
                                result["sirket_adi"] = name
                                break
                    return result
    return {}


def _map_html_row(cells: list) -> dict:
    """HTML tablo satırını (ÖDA tipi — 10+ kolon) eşle."""
    result = {}
    def g(idx):
        return cells[idx].strip() if idx < len(cells) else ""
    result["islem_tarihi"]      = g(0)
    result["oncesi_nominal"]    = g(1)  # alım
    result["sonrasi_nominal"]   = g(2)  # satım
    # Bu formatlar ÖDA tipi bildirimler için — tablo sırası farklı olabilir
    # HTML satır sırası: İşlem Tarihi, Alım Nominal, Satım Nominal, Net, Gün Başı, Gün Sonu, ...
    return result


def _parse_pdf_bytes(pdf_bytes: bytes, log_fn=print) -> dict:
    """PDF bytes'ını pdfplumber ile parse et."""
    result = {
        "islem_tarihi": "",
        "islem_niteligi": "",
        "nominal_tutar": "",
        "fiyat": "",
        "islem_tutari": "",
        "oncesi_nominal": "",
        "oncesi_sermaye": "",
        "sonrasi_nominal": "",
        "sonrasi_sermaye": "",
        "sirket_adi": "",
    }

    try:
        import pdfplumber
    except ImportError:
        log_fn("    ⚠ pdfplumber kurulu değil")
        return result

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            all_text = ""
            all_tables = []
            for page in pdf.pages:
                text = page.extract_text() or ""
                all_text += text + "\n"
                tables = page.extract_tables()
                if tables:
                    all_tables.extend(tables)

            log_fn(f"    📊 PDF: {len(pdf.pages)} sayfa, {len(all_tables)} tablo, "
                   f"{len(all_text)} karakter")

            # ── Şirket adı ──
            # "Saygılarımızla," satırından sonraki şirket adı
            sirket = _extract_company_from_text(all_text)
            if sirket:
                result["sirket_adi"] = sirket

            # ── Tablolardan veri satırı bul ──
            for table in all_tables:
                data = _find_data_in_pdf_table(table, log_fn)
                if data and data.get("islem_tarihi"):
                    result.update(data)
                    log_fn(f"    ✓ Tablo parse: {data.get('islem_tarihi')} | "
                           f"Nitelik:{data.get('islem_niteligi','-')}")
                    return result

            # ── Tablo bulunamadıysa metinden dene ──
            text_data = _parse_text_fallback(all_text)
            if text_data:
                result.update(text_data)
                log_fn(f"    ✓ Metin parse: {text_data.get('islem_tarihi','-')}")

    except Exception as e:
        log_fn(f"    ✗ PDF parse hata: {e}")

    return result


def _extract_company_from_text(text: str) -> str:
    """PDF metninden şirket adını çıkar."""
    # Yöntem 1: "Saygılarımızla" sonrası
    m = re.search(
        r'[Ss]aygılarımızla[,.]?\s*\n\s*\n?\s*([A-ZÇĞİÖŞÜ][A-ZÇĞİÖŞÜa-zçğıöşü\s.&]+(?:A\.Ş\.|A\.S\.))',
        text
    )
    if m:
        name = m.group(1).strip()
        if "KAMUYU" not in name.upper():
            return name

    # Yöntem 2: Metnin başında "XXX A.Ş." formatı
    m2 = re.search(
        r'^.*?([A-ZÇĞİÖŞÜ][A-ZÇĞİÖŞÜ\s.&]{5,}(?:A\.Ş\.|A\.S\.))',
        text[:500]
    )
    if m2:
        name = m2.group(1).strip()
        if "KAMUYU" not in name.upper() and len(name) > 10:
            return name

    return ""


def _find_data_in_pdf_table(table: list, log_fn=print) -> dict:
    """
    PDF tablosunda veri satırını bul.
    Header satırından sonraki ilk tarih + sayısal verili satır = veri satırı.
    """
    if not table or len(table) < 2:
        return {}

    # Header satırını bul — kolon isimlerini eşle
    header_idx = None
    header_map = {}

    for i, row in enumerate(table):
        if not row:
            continue
        joined = " ".join(str(c or "").lower() for c in row)
        # Header satırı: "işlem tarihi", "niteliği", "nominal", "fiyat" gibi kelimeler
        if ("tarih" in joined and
            ("nominal" in joined or "fiyat" in joined or "tutar" in joined)):
            header_idx = i
            header_map = _map_header(row)
            log_fn(f"    📋 Header bulundu (satır {i}): {header_map}")
            break

    if header_idx is None:
        # Header bulunamadı — doğrudan tarih+sayı içeren satır ara
        for row in table:
            if not row:
                continue
            joined = " ".join(str(c or "") for c in row)
            if (re.search(r'\d{2}[./]\d{2}[./]\d{4}', joined) and
                re.search(r'\d{1,3}(?:[.,]\d{3})+', joined)):
                return _map_row_positional(row)
        return {}

    # Header'dan sonraki satırlarda veri ara
    for row in table[header_idx + 1:]:
        if not row:
            continue
        joined = " ".join(str(c or "") for c in row)
        # Tarih içeren satır = veri satırı
        if re.search(r'\d{2}[./]\d{2}[./]\d{4}', joined):
            return _map_row_with_header(row, header_map)

    return {}


def _map_header(header_row: list) -> dict:
    """Header satırındaki kolon isimlerinden indeks haritası oluştur."""
    mapping = {}
    for i, cell in enumerate(header_row):
        if not cell:
            continue
        c = str(cell).lower().strip()
        c = c.replace("\n", " ")

        if "tarih" in c and "işlem" in c:
            mapping["tarih"] = i
        elif "tarih" in c and "tarih" not in [v for k,v in mapping.items()]:
            mapping["tarih"] = i
        elif "niteliğ" in c or "niteligi" in c or ("alım" in c and "satım" in c):
            mapping["nitelik"] = i
        elif "fiyat" in c:
            mapping["fiyat"] = i
        elif "işlem" in c and "tutar" in c:
            mapping["islem_tutari"] = i
        elif ("konu" in c and "nominal" in c) or ("işleme" in c and "nominal" in c):
            mapping["nominal"] = i
        elif "öncesi" in c and "nominal" in c:
            mapping["oncesi_nominal"] = i
        elif "sonrası" in c and "nominal" in c:
            mapping["sonrasi_nominal"] = i
        elif "öncesi" in c and ("sermaye" in c or "pay" in c):
            mapping["oncesi_sermaye"] = i
        elif "sonrası" in c and ("sermaye" in c or "pay" in c):
            mapping["sonrasi_sermaye"] = i
        elif "nominal" in c and "oncesi_nominal" not in mapping:
            mapping["nominal"] = i

    return mapping


def _map_row_with_header(row: list, header_map: dict) -> dict:
    """Header map kullanarak veri satırını eşle."""
    result = {}

    def g(key):
        idx = header_map.get(key)
        if idx is not None and idx < len(row):
            return str(row[idx] or "").strip()
        return ""

    result["islem_tarihi"] = g("tarih")
    result["islem_niteligi"] = g("nitelik")
    result["fiyat"] = g("fiyat")
    result["islem_tutari"] = g("islem_tutari")
    result["nominal_tutar"] = g("nominal")
    result["oncesi_nominal"] = g("oncesi_nominal")
    result["oncesi_sermaye"] = g("oncesi_sermaye")
    result["sonrasi_nominal"] = g("sonrasi_nominal")
    result["sonrasi_sermaye"] = g("sonrasi_sermaye")

    # Nitelik hücresini kontrol et
    nit = result.get("islem_niteligi", "").lower()
    if "alım" in nit or "alış" in nit or "alim" in nit:
        result["islem_niteligi"] = "Alım"
    elif "satım" in nit or "satış" in nit or "satim" in nit:
        result["islem_niteligi"] = "Satım"

    return result if result.get("islem_tarihi") else {}


def _map_row_positional(row: list) -> dict:
    """Header olmadan pozisyonel eşle."""
    cells = [str(c or "").strip() for c in row]
    result = {}

    # İlk tarih
    for i, c in enumerate(cells):
        if re.search(r'\d{2}[./]\d{2}[./]\d{4}', c):
            result["islem_tarihi"] = c
            remaining = cells[i+1:]
            break
    else:
        return {}

    # Nitelik
    nit_idx = None
    for i, c in enumerate(remaining):
        cl = c.lower()
        if any(k in cl for k in ["alım","alim","alış","alis"]):
            result["islem_niteligi"] = "Alım"
            nit_idx = i
            break
        elif any(k in cl for k in ["satım","satim","satış","satis"]):
            result["islem_niteligi"] = "Satım"
            nit_idx = i
            break

    # Kalan sayılar
    start = (nit_idx + 1) if nit_idx is not None else 0
    nums = []
    for c in remaining[start:]:
        clean = c.replace(".","").replace(",",".").replace("%","").replace(" ","").strip()
        if clean and re.match(r'^-?[\d.]+$', clean):
            nums.append(c)

    fields = ["nominal_tutar","fiyat","islem_tutari",
              "oncesi_nominal","oncesi_sermaye","sonrasi_nominal","sonrasi_sermaye"]
    for i, f in enumerate(fields):
        if i < len(nums):
            result[f] = nums[i]

    return result


def _parse_text_fallback(text: str) -> dict:
    """Tablo çıkarılamadıysa metin'den regex ile dene."""
    result = {}
    tm = re.search(r'(\d{2}[./]\d{2}[./]\d{4})', text)
    if tm:
        result["islem_tarihi"] = tm.group(1)

    if re.search(r'(?i)\balım\b|\balış\b', text):
        result["islem_niteligi"] = "Alım"
    elif re.search(r'(?i)\bsatım\b|\bsatış\b', text):
        result["islem_niteligi"] = "Satım"

    # Sermaye oranları
    serm = re.findall(r'%\s*(\d+[.,]\d+)', text)
    if len(serm) >= 2:
        result["oncesi_sermaye"] = f"% {serm[-2]}"
        result["sonrasi_sermaye"] = f"% {serm[-1]}"
    elif len(serm) == 1:
        result["sonrasi_sermaye"] = f"% {serm[0]}"

    return result if result.get("islem_tarihi") else {}


# ─── HTML DETAY (ÖDA tipleri için) ────────────────────────

def fetch_html_detail(session, disc_index: str, log_fn=print) -> dict:
    """ÖDA tipi bildirimler HTML sayfasında tablo içerebilir."""
    result = {"data_rows": [], "fiyat": "", "sirket_adi": ""}

    try:
        url = f"{BASE_URL}/tr/Bildirim/{disc_index}"
        headers_html = {
            "User-Agent": HEADERS["User-Agent"],
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "tr",
            "Referer": f"{BASE_URL}/tr",
        }
        r = session.get(url, headers=headers_html, timeout=20)
        if r.status_code != 200:
            return result
    except Exception:
        return result

    soup = BeautifulSoup(r.text, "lxml")

    # Şirket adı
    for sel in ["div.comp-name","span.comp-name","a.comp-name"]:
        el = soup.select_one(sel)
        if el:
            name = el.get_text(strip=True)
            if name and "KAMUYU AYDINLATMA" not in name.upper():
                result["sirket_adi"] = name
                break

    # Tablo
    for table in soup.find_all("table"):
        for row in reversed(table.find_all("tr")):
            cells = [td.get_text(strip=True).replace("\n"," ")
                     for td in row.find_all(["td","th"])]
            joined = " ".join(cells)
            if (len(cells) >= 5 and
                re.search(r'\d{2}[/\.]\d{2}[/\.]\d{4}', joined) and
                '%' in joined):
                result["data_rows"].append(cells)
        if result["data_rows"]:
            break

    body = soup.get_text(" ", strip=True)
    fm = re.search(r'[Oo]rtalama[\s]+([0-9.,]+)[\s]*fiyat', body)
    if fm:
        result["fiyat"] = fm.group(1)

    return result


# ─── DETAY TOPLAMA ────────────────────────────────────────

def fetch_and_enrich(session, disc: dict, log_fn=print) -> dict:
    """Bir bildirim için tüm detayları çek ve satırı zenginleştir."""
    row = dict(disc)
    disc_index = disc.get("disc_index", "")

    if not disc_index:
        return row

    # ── Yöntem 1: HTML sayfasında tablo (ÖDA tipleri) ──
    html_detail = fetch_html_detail(session, disc_index, log_fn)

    if html_detail.get("sirket_adi"):
        if "KAMUYU AYDINLATMA" not in html_detail["sirket_adi"].upper():
            row["sirket"] = html_detail["sirket_adi"]

    if html_detail.get("data_rows"):
        cells = html_detail["data_rows"][0]
        def g(idx):
            return cells[idx].strip() if idx < len(cells) else ""
        row["islem_tarihi"]           = g(0)
        row["alim_toplam_nominal"]    = g(1)
        row["satim_toplam_nominal"]   = g(2)
        row["net_nominal"]            = g(3)
        row["gun_basi_nominal"]       = g(4)
        row["gun_sonu_nominal"]       = g(5)
        row["sermaye_orani_gun_basi"] = g(6)
        row["oy_haklari_gun_basi"]    = g(7)
        row["sermaye_orani_gun_sonu"] = g(8)
        row["oy_haklari_gun_sonu"]    = g(9)
        if html_detail.get("fiyat"):
            row["fiyat"] = html_detail["fiyat"]
        log_fn(f"    ✓ HTML tablo: {g(0)}")
        return row

    # ── Yöntem 2: BildirimPdf API → PDF parse ──
    pdf_data = fetch_bildirim_pdf(session, disc_index, log_fn)

    if pdf_data.get("sirket_adi"):
        if "KAMUYU AYDINLATMA" not in pdf_data["sirket_adi"].upper():
            row["sirket"] = pdf_data["sirket_adi"]

    if pdf_data.get("islem_tarihi"):
        row["islem_tarihi"] = pdf_data["islem_tarihi"]

        nitelik = pdf_data.get("islem_niteligi", "")
        nominal = pdf_data.get("nominal_tutar", "")
        fiyat   = pdf_data.get("fiyat", "")

        if "Alım" in nitelik:
            row["alim_toplam_nominal"]  = nominal
            row["satim_toplam_nominal"] = "0"
            row["net_nominal"]          = nominal
        elif "Satım" in nitelik:
            row["alim_toplam_nominal"]  = "0"
            row["satim_toplam_nominal"] = nominal
            row["net_nominal"]          = f"-{nominal}" if nominal and not nominal.startswith("-") else nominal
        else:
            row["alim_toplam_nominal"]  = nominal
            row["satim_toplam_nominal"] = "0"
            row["net_nominal"]          = nominal

        if fiyat:
            row["fiyat"] = fiyat
        if pdf_data.get("islem_tutari"):
            row["islem_tutari"] = pdf_data["islem_tutari"]
        if pdf_data.get("oncesi_nominal"):
            row["gun_basi_nominal"] = pdf_data["oncesi_nominal"]
        if pdf_data.get("sonrasi_nominal"):
            row["gun_sonu_nominal"] = pdf_data["sonrasi_nominal"]
        if pdf_data.get("oncesi_sermaye"):
            row["sermaye_orani_gun_basi"] = pdf_data["oncesi_sermaye"]
        if pdf_data.get("sonrasi_sermaye"):
            row["sermaye_orani_gun_sonu"] = pdf_data["sonrasi_sermaye"]
    else:
        log_fn(f"    ⚠ Veri bulunamadı (HTML + PDF)")

    return row


def fetch_details_requests(session, disclosures: list, log_fn=print) -> list:
    """Tüm bildirimlerin detaylarını çek."""
    log_fn(f"  🌐 {len(disclosures)} detay çekiliyor...")

    enriched = []
    for i, disc in enumerate(disclosures, 1):
        ilgili = disc.get("ilgili_sirket", "?")
        sirket = disc.get("sirket", "")[:35]
        log_fn(f"  [{i}/{len(disclosures)}] {ilgili} ← {sirket}")

        for attempt in range(2):
            try:
                row = fetch_and_enrich(session, disc, log_fn)
                if row.get("islem_tarihi"):
                    log_fn(f"    ✓ {row['islem_tarihi']} | "
                           f"Alım:{row.get('alim_toplam_nominal','-')} | "
                           f"Sermaye:{row.get('sermaye_orani_gun_sonu','-')}")
                enriched.append(row)
                break
            except Exception as e:
                if attempt == 0:
                    log_fn(f"    ↻ Tekrar deneniyor...")
                    time.sleep(1)
                else:
                    log_fn(f"    ✗ {e}")
                    enriched.append(dict(disc))

        if i < len(disclosures):
            time.sleep(0.5)

    return enriched


# ─── ANA FONKSİYON ───────────────────────────────────────

def scrape_pay_alim_satim(start_date: date, end_date: date,
                          log_fn=print) -> list:
    log_fn(f"📡 KAP: {start_date.strftime('%d.%m.%Y')} → {end_date.strftime('%d.%m.%Y')}")

    session = make_session()
    log_fn("  ✓ Session kuruldu")

    raw = fetch_main_api(session, start_date, end_date)

    if not raw:
        log_fn("  ⚠ API yanıt vermedi — demo veri")
        return get_demo_data(start_date, end_date)

    log_fn(f"  {len(raw)} kayıt içinde Pay Alım Satım filtreleniyor...")
    filtered    = filter_pay_alim_satim(raw)
    disclosures = [normalize_item(i) for i in filtered]
    log_fn(f"  ✓ {len(disclosures)} bildirim bulundu")

    enriched = fetch_details_requests(session, disclosures, log_fn=log_fn)
    log_fn(f"✅ Tamamlandı: {len(enriched)} bildirim")
    return enriched


def get_demo_data(start_date: date, end_date: date) -> list:
    d1 = start_date.strftime("%d.%m.%Y")
    return [
        {"no":"1583033","tarih":f"{d1} 18:46","kod":"ALNUS, ANC",
         "sirket":"ALNUS YATIRIM MENKUL DEĞERLER A.Ş.",
         "konu":"Pay Alım Satım Bildirimi","ozet":"ISKPL Pay Alım Bildirimi",
         "link":f"{BASE_URL}/tr/Bildirim/1583033","disc_index":"1583033",
         "islem_tarihi":"31.03.2026","alim_toplam_nominal":"93.925.229",
         "satim_toplam_nominal":"10.259","net_nominal":"93.914.970",
         "gun_basi_nominal":"0","gun_sonu_nominal":"93.914.970",
         "sermaye_orani_gun_basi":"% 0","oy_haklari_gun_basi":"% 0",
         "sermaye_orani_gun_sonu":"% 6,26","oy_haklari_gun_sonu":"% 6,26",
         "ilgili_sirket":"ISKPL","fiyat":"12,50"},
    ]


# ─── EXCEL ───────────────────────────────────────────────

COLUMNS_MAP = {
    "no":"No", "tarih":"Yayın Tarihi", "kod":"Hisse Kodu",
    "sirket":"Aracı Kurum", "konu":"Konu", "ozet":"Özet",
    "ilgili_sirket":"İlgili Şirket", "islem_tarihi":"İşlem Tarihi",
    "fiyat":"Ort. Fiyat (TL)", "alim_toplam_nominal":"Alım Nominal (TL)",
    "satim_toplam_nominal":"Satım Nominal (TL)", "net_nominal":"Net Nominal (TL)",
    "gun_basi_nominal":"Gün Başı Nominal (TL)", "gun_sonu_nominal":"Gün Sonu Nominal (TL)",
    "sermaye_orani_gun_basi":"Sermaye Oranı Gün Başı (%)",
    "oy_haklari_gun_basi":"Oy Hakları Gün Başı (%)",
    "sermaye_orani_gun_sonu":"Sermaye Oranı Gün Sonu (%)",
    "oy_haklari_gun_sonu":"Oy Hakları Gün Sonu (%)",
    "link":"KAP Linki",
}

COL_WIDTHS = {
    "No":10,"Yayın Tarihi":18,"Hisse Kodu":12,"Aracı Kurum":32,
    "Konu":36,"Özet":28,"İlgili Şirket":14,"İşlem Tarihi":14,
    "Ort. Fiyat (TL)":14,"Alım Nominal (TL)":20,"Satım Nominal (TL)":20,
    "Net Nominal (TL)":20,"Gün Başı Nominal (TL)":22,"Gün Sonu Nominal (TL)":22,
    "Sermaye Oranı Gün Başı (%)":24,"Oy Hakları Gün Başı (%)":22,
    "Sermaye Oranı Gün Sonu (%)":24,"Oy Hakları Gün Sonu (%)":22,"KAP Linki":20,
}


def save_to_excel(enriched: list, start_date: date, end_date: date,
                  output_dir: str = ".") -> tuple:
    os.makedirs(output_dir, exist_ok=True)
    fname    = f"KAP_PayAlimSatim_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"
    filepath = os.path.join(output_dir, fname)

    rows = [{col: item.get(key,"") for key,col in COLUMNS_MAP.items()} for item in enriched]
    df   = pd.DataFrame(rows)
    df.to_excel(filepath, index=False, sheet_name="Pay Alım Satım")

    wb  = load_workbook(filepath)
    ws  = wb.active
    hf  = PatternFill("solid", start_color="1F4E79")
    af  = PatternFill("solid", start_color="D6E4F0")
    wf  = PatternFill("solid", start_color="FFFFFF")
    t   = Side(style="thin", color="BDD7EE")
    brd = Border(left=t, right=t, top=t, bottom=t)
    hfont = Font(bold=True, color="FFFFFF", name="Arial", size=10)
    bfont = Font(name="Arial", size=9)
    lfont = Font(name="Arial", size=9, color="0563C1", underline="single")

    for cell in ws[1]:
        cell.fill = hf; cell.font = hfont; cell.border = brd
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    ws.row_dimensions[1].height = 38

    lc = next((i for i,c in enumerate(ws[1],1) if c.value=="KAP Linki"), None)
    for i, row in enumerate(ws.iter_rows(min_row=2), start=2):
        fill = af if i%2==0 else wf
        for cell in row:
            cell.fill = fill; cell.border = brd
            cell.alignment = Alignment(vertical="center", wrap_text=True)
            if lc and cell.column==lc and cell.value:
                cell.font = lfont; cell.hyperlink = str(cell.value)
                cell.value = "Bildirimi Görüntüle"
            else:
                cell.font = bfont

    for idx, col_name in enumerate(df.columns, 1):
        ws.column_dimensions[get_column_letter(idx)].width = COL_WIDTHS.get(col_name, 15)
    ws.freeze_panes = "A2"

    ws2 = wb.create_sheet("Özet")
    ws2["A1"] = "KAP Pay Alım Satım Bildirimleri"
    ws2["A1"].font = Font(bold=True, size=14, name="Arial", color="1F4E79")
    for r,(lbl,val) in enumerate([
        ("Başlangıç", start_date.strftime("%d.%m.%Y")),
        ("Bitiş",     end_date.strftime("%d.%m.%Y")),
        ("Kayıt",     str(len(enriched))),
        ("Rapor",     datetime.now().strftime("%d.%m.%Y %H:%M")),
    ], start=3):
        ws2.cell(r,1,lbl).font = Font(bold=True, name="Arial", size=10)
        ws2.cell(r,2,val).font = Font(name="Arial", size=10)
    ws2.column_dimensions["A"].width = 22
    ws2.column_dimensions["B"].width = 22
    wb.save(filepath)
    print(f"✓ Excel: {filepath}")
    return filepath, df


if __name__ == "__main__":
    today = date.today()
    if len(sys.argv) >= 3:
        try:
            start = datetime.strptime(sys.argv[1], "%d.%m.%Y").date()
            end   = datetime.strptime(sys.argv[2], "%d.%m.%Y").date()
        except ValueError:
            print("Kullanım: python kap_scraper.py GG.AA.YYYY GG.AA.YYYY [klasör]")
            sys.exit(1)
    else:
        start = today; end = today

    out = sys.argv[3] if len(sys.argv) >= 4 else "."
    data = scrape_pay_alim_satim(start, end)
    fp, df = save_to_excel(data, start, end, out)
    print(f"✓ {len(df)} kayıt → {fp}")
