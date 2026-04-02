"""
KAP Pay Alım Satım Bildirimi Scraper
- Liste: POST /tr/api/disclosure/list/main
- Detay: HTML sayfasından tablo + PDF eki parse
- PDF ekleri: pdfplumber ile tablo çıkarma

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
    "Sec-Fetch-Dest":"empty",
    "Sec-Fetch-Mode":"cors",
    "Sec-Fetch-Site":"same-origin",
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
    disc_id = b.get("disclosureId") or ""

    return {
        "no":            str(idx),
        "tarih":         b.get("publishDate") or "",
        "kod":           stock_code,
        "sirket":        company,
        "konu":          b.get("title") or "",
        "ozet":          b.get("summary") or "",
        "link":          link,
        "disc_index":    str(idx),
        "disc_id":       disc_id,
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


# ─── PDF PARSE ────────────────────────────────────────────

def _parse_pdf_table(pdf_bytes: bytes, log_fn=print) -> dict:
    """PDF içindeki Pay Alım Satım tablosunu parse et."""
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
        "raw_text": "",
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

            result["raw_text"] = all_text[:1000]

            # Şirket adı
            sirket_match = re.search(
                r'(?:Saygılarımızla|saygılarımızla)[,.]?\s*\n?\s*([A-ZÇĞİÖŞÜa-zçğıöşü\s]+(?:A\.Ş\.|A\.S\.))',
                all_text
            )
            if sirket_match:
                result["sirket_adi"] = sirket_match.group(1).strip()

            # Tablolardan veri satırı bul
            for table in all_tables:
                data_row = _find_data_row_in_table(table)
                if data_row:
                    result.update(data_row)
                    break

            # Tablo bulunamadıysa metin parse dene
            if not result["islem_tarihi"]:
                text_parsed = _parse_pdf_text(all_text)
                if text_parsed:
                    result.update(text_parsed)

    except Exception as e:
        log_fn(f"    ✗ PDF parse hata: {e}")

    return result


def _find_data_row_in_table(table: list) -> dict:
    if not table:
        return {}
    for row in table:
        if not row:
            continue
        joined = " ".join(str(c or "") for c in row)
        if re.search(r'\d{2}[./]\d{2}[./]\d{4}', joined):
            nums = [c for c in row if c and re.search(r'\d', str(c))]
            if len(nums) >= 3:
                return _map_table_row(row)
    return {}


def _map_table_row(row: list) -> dict:
    """
    Tablo satırını field'lara eşle.
    PDF tablo sırası (resimden):
    İşlem Tarihi | İşlemin Niteliği | Nominal Tutar | Fiyat | İşlem Tutarı |
    Öncesi Nominal | Öncesi Sermaye % | Sonrası Nominal | Sonrası Sermaye %
    """
    cells = [str(c or "").strip() for c in row]
    result = {}

    # Tarih bul
    tarih_idx = None
    for i, c in enumerate(cells):
        if re.search(r'\d{2}[./]\d{2}[./]\d{4}', c):
            tarih_idx = i
            result["islem_tarihi"] = c
            break

    if tarih_idx is None:
        return {}

    remaining = cells[tarih_idx + 1:]

    # İşlem niteliği
    nitelik_idx = None
    for i, c in enumerate(remaining):
        cl = c.lower()
        if any(k in cl for k in ["alım", "alim", "alış", "alis"]):
            result["islem_niteligi"] = "Alım"
            nitelik_idx = i
            break
        elif any(k in cl for k in ["satım", "satim", "satış", "satis"]):
            result["islem_niteligi"] = "Satım"
            nitelik_idx = i
            break

    # Sayısal değerler
    nums = []
    start = (nitelik_idx + 1) if nitelik_idx is not None else 0
    for c in remaining[start:]:
        clean = c.replace(".", "").replace(",", ".").replace("%", "").replace(" ", "").strip()
        if clean and re.match(r'^-?[\d.]+$', clean):
            nums.append(c)

    field_order = [
        "nominal_tutar", "fiyat", "islem_tutari",
        "oncesi_nominal", "oncesi_sermaye",
        "sonrasi_nominal", "sonrasi_sermaye"
    ]
    for i, field in enumerate(field_order):
        if i < len(nums):
            result[field] = nums[i]

    return result


def _parse_pdf_text(text: str) -> dict:
    """Tablo çıkarılamadıysa metin'den regex ile dene."""
    result = {}
    tm = re.search(r'(\d{2}[./]\d{2}[./]\d{4})', text)
    if tm:
        result["islem_tarihi"] = tm.group(1)
    if re.search(r'(?i)\balım\b|\balış\b', text):
        result["islem_niteligi"] = "Alım"
    elif re.search(r'(?i)\bsatım\b|\bsatış\b', text):
        result["islem_niteligi"] = "Satım"
    serm = re.findall(r'%\s*(\d+[.,]\d+)', text)
    if len(serm) >= 2:
        result["oncesi_sermaye"] = f"% {serm[-2]}"
        result["sonrasi_sermaye"] = f"% {serm[-1]}"
    elif len(serm) == 1:
        result["sonrasi_sermaye"] = f"% {serm[0]}"
    return result if result.get("islem_tarihi") else {}


# ─── DETAY FETCH (HTML + PDF) ────────────────────────────

def fetch_detail_page(session, disc_index: str, log_fn=print) -> dict:
    """
    Bildirim detay sayfasını çek:
    1) HTML'den tablo verisi dene
    2) PDF ek dosya linki bul, indir, parse et
    """
    result = {
        "data_rows": [],
        "fiyat": "",
        "sirket_adi": "",
        "ilgili": "",
        "pdf_data": {},
    }

    if not disc_index:
        return result

    # ── HTML sayfası çek ──
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
            log_fn(f"    ⚠ HTTP {r.status_code}")
            return result
    except Exception as e:
        log_fn(f"    ✗ Sayfa çekilemedi: {e}")
        return result

    html = r.text
    soup = BeautifulSoup(html, "lxml")

    # ── Şirket adı ──
    for sel in ["div.comp-name", "span.comp-name", "a.comp-name",
                "div.modal-dialog h2", ".w-clearfix span"]:
        el = soup.select_one(sel)
        if el:
            name = el.get_text(strip=True)
            if name and "KAMUYU AYDINLATMA" not in name.upper():
                result["sirket_adi"] = name
                break

    # ── HTML'den tablo verisi dene ──
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in reversed(rows):
            cells = [td.get_text(strip=True).replace("\n", " ")
                     for td in row.find_all(["td", "th"])]
            joined = " ".join(cells)
            if (len(cells) >= 5 and
                re.search(r'\d{2}[/\.]\d{2}[/\.]\d{4}', joined) and
                '%' in joined):
                result["data_rows"].append(cells)
        if result["data_rows"]:
            break

    # Fiyat
    body_text = soup.get_text(" ", strip=True)
    fm = re.search(r'[Oo]rtalama[\s]+([0-9.,]+)[\s]*fiyat', body_text)
    if fm:
        result["fiyat"] = fm.group(1)

    # İlgili şirket
    im = re.search(r'\[([A-Z][A-Z0-9]{1,7})\]', body_text)
    if im:
        result["ilgili"] = im.group(1)

    # ── HTML tabloda veri bulunamadıysa → PDF eki ara ──
    if not result["data_rows"]:
        pdf_data = _try_fetch_pdf_attachment(session, soup, html, disc_index, log_fn)
        if pdf_data:
            result["pdf_data"] = pdf_data
            if pdf_data.get("sirket_adi") and not result["sirket_adi"]:
                result["sirket_adi"] = pdf_data["sirket_adi"]

    return result


def _try_fetch_pdf_attachment(session, soup, html: str, disc_index: str, log_fn=print) -> dict:
    """Detay sayfasından PDF ek dosyasını bul, indir ve parse et."""

    pdf_urls = set()

    # Yöntem 1: <a> linklerinden PDF/dosya bul
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/api/file/download/" in href or href.endswith(".pdf"):
            if href.startswith("/"):
                href = BASE_URL + href
            pdf_urls.add(href)

    # Yöntem 2: HTML kaynağından dosya ID'leri
    file_ids = re.findall(r'/api/file/download/([a-f0-9]+)', html)
    for fid in file_ids:
        pdf_urls.add(f"{BASE_URL}/tr/api/file/download/{fid}")

    # Yöntem 3: data-file-id attribute
    for el in soup.find_all(attrs={"data-file-id": True}):
        fid = el["data-file-id"]
        pdf_urls.add(f"{BASE_URL}/tr/api/file/download/{fid}")

    # Yöntem 4: Detay JSON API'sinden dosya listesi
    if not pdf_urls:
        try:
            detail_url = f"{BASE_URL}/tr/api/disclosure/detail/{disc_index}"
            r = session.get(detail_url, timeout=15)
            if r.status_code == 200:
                try:
                    detail = r.json()
                    for key in ["attachments", "files", "disclosureFiles"]:
                        attachments = detail.get(key) or []
                        if isinstance(attachments, list):
                            for att in attachments:
                                if isinstance(att, dict):
                                    fid = (att.get("fileId") or att.get("id") or
                                           att.get("fileUuid") or "")
                                    if fid:
                                        pdf_urls.add(f"{BASE_URL}/tr/api/file/download/{fid}")
                                elif isinstance(att, str):
                                    pdf_urls.add(f"{BASE_URL}/tr/api/file/download/{att}")

                    content = detail.get("content") or detail.get("disclosureHtml") or ""
                    if content:
                        for fid in re.findall(r'/api/file/download/([a-f0-9]+)', content):
                            pdf_urls.add(f"{BASE_URL}/tr/api/file/download/{fid}")
                except (json.JSONDecodeError, ValueError):
                    for fid in re.findall(r'/api/file/download/([a-f0-9]+)', r.text):
                        pdf_urls.add(f"{BASE_URL}/tr/api/file/download/{fid}")
        except Exception:
            pass

    if not pdf_urls:
        log_fn(f"    ⚠ PDF eki bulunamadı")
        return {}

    # PDF'leri dene
    for pdf_url in pdf_urls:
        try:
            log_fn(f"    📄 PDF indiriliyor...")
            headers_dl = {
                "User-Agent": HEADERS["User-Agent"],
                "Accept": "application/pdf,*/*",
                "Referer": f"{BASE_URL}/tr/Bildirim/{disc_index}",
            }
            r = session.get(pdf_url, headers=headers_dl, timeout=30)
            if r.status_code == 200 and len(r.content) > 500:
                if r.content[:4] == b'%PDF' or b'%PDF' in r.content[:20]:
                    parsed = _parse_pdf_table(r.content, log_fn=log_fn)
                    if parsed.get("islem_tarihi"):
                        log_fn(f"    ✓ PDF parse başarılı: {parsed['islem_tarihi']}")
                        return parsed
                    else:
                        log_fn(f"    ⚠ PDF'de tablo bulunamadı")
        except Exception as e:
            log_fn(f"    ✗ PDF hata: {e}")

    return {}


# ─── DETAY → SATIR EŞLEŞTİRME ────────────────────────────

def _apply_detail_to_row(row: dict, detail: dict) -> dict:
    """Detay sayfasından/PDF'den alınan verileri satıra uygula."""

    # Şirket adı
    if detail.get("sirket_adi"):
        sirket = detail["sirket_adi"]
        if "KAMUYU AYDINLATMA" not in sirket.upper():
            row["sirket"] = sirket

    # İlgili şirket
    if detail.get("ilgili") and not row.get("ilgili_sirket"):
        row["ilgili_sirket"] = detail["ilgili"]

    # HTML tablodan veri var mı?
    if detail.get("data_rows"):
        cells = detail["data_rows"][0]
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
        return row

    # PDF verisinden doldur
    pdf = detail.get("pdf_data", {})
    if pdf.get("islem_tarihi"):
        row["islem_tarihi"] = pdf["islem_tarihi"]

        nitelik = pdf.get("islem_niteligi", "")
        nominal = pdf.get("nominal_tutar", "")
        fiyat   = pdf.get("fiyat", "")

        if "Alım" in nitelik or "Alış" in nitelik:
            row["alim_toplam_nominal"]  = nominal
            row["satim_toplam_nominal"] = "0"
            row["net_nominal"]          = nominal
        elif "Satım" in nitelik or "Satış" in nitelik:
            row["alim_toplam_nominal"]  = "0"
            row["satim_toplam_nominal"] = nominal
            row["net_nominal"]          = f"-{nominal}" if nominal and not nominal.startswith("-") else nominal
        else:
            row["alim_toplam_nominal"]  = nominal
            row["satim_toplam_nominal"] = "0"
            row["net_nominal"]          = nominal

        if fiyat:
            row["fiyat"] = fiyat
        if pdf.get("islem_tutari"):
            row["islem_tutari"] = pdf["islem_tutari"]
        if pdf.get("oncesi_nominal"):
            row["gun_basi_nominal"] = pdf["oncesi_nominal"]
        if pdf.get("sonrasi_nominal"):
            row["gun_sonu_nominal"] = pdf["sonrasi_nominal"]
        if pdf.get("oncesi_sermaye"):
            row["sermaye_orani_gun_basi"] = pdf["oncesi_sermaye"]
        if pdf.get("sonrasi_sermaye"):
            row["sermaye_orani_gun_sonu"] = pdf["sonrasi_sermaye"]

        if pdf.get("sirket_adi") and "KAMUYU AYDINLATMA" not in pdf["sirket_adi"].upper():
            row["sirket"] = pdf["sirket_adi"]

    # Fiyat (HTML'den)
    if detail.get("fiyat") and not row.get("fiyat"):
        row["fiyat"] = detail["fiyat"]

    return row


def fetch_details_requests(session, disclosures: list, log_fn=print) -> list:
    """Detay sayfalarını requests + BS4 + pdfplumber ile çek."""
    log_fn(f"  🌐 {len(disclosures)} detay sayfası çekiliyor...")

    enriched = []
    for i, disc in enumerate(disclosures, 1):
        row = dict(disc)
        disc_index = disc.get("disc_index", "")
        ilgili = disc.get("ilgili_sirket", "?")
        sirket = disc.get("sirket", "")[:35]
        log_fn(f"  [{i}/{len(disclosures)}] {ilgili} ← {sirket}")

        if not disc_index:
            enriched.append(row)
            continue

        for attempt in range(2):
            try:
                detail = fetch_detail_page(session, disc_index, log_fn=log_fn)
                row = _apply_detail_to_row(row, detail)

                if row.get("islem_tarihi"):
                    log_fn(f"    ✓ {row['islem_tarihi']} | "
                           f"Alım:{row.get('alim_toplam_nominal','-')} | "
                           f"Sermaye:{row.get('sermaye_orani_gun_sonu','-')}")
                break

            except Exception as e:
                if attempt == 0:
                    log_fn(f"    ↻ Tekrar deneniyor...")
                    time.sleep(1)
                else:
                    log_fn(f"    ✗ {e}")

        enriched.append(row)
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
        {"no":"1582954","tarih":f"{d1} 17:37","kod":"",
         "sirket":"DENİZ PORTFÖY YÖNETİMİ A.Ş.",
         "konu":"Pay Alım Satım Bildirimi","ozet":"KRONT Pay Satım",
         "link":f"{BASE_URL}/tr/Bildirim/1582954","disc_index":"1582954",
         "islem_tarihi":d1,"alim_toplam_nominal":"0",
         "satim_toplam_nominal":"1.500.000","net_nominal":"-1.500.000",
         "gun_basi_nominal":"8.000.000","gun_sonu_nominal":"6.500.000",
         "sermaye_orani_gun_basi":"% 2,10","oy_haklari_gun_basi":"% 2,10",
         "sermaye_orani_gun_sonu":"% 1,71","oy_haklari_gun_sonu":"% 1,71",
         "ilgili_sirket":"KRONT","fiyat":"32,40"},
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
