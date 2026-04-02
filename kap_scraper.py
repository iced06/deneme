"""
KAP Pay Alım Satım Bildirimi Scraper
API: POST /tr/api/disclosure/list/main
Detay: HTML parse (Tablo 1, oda_ExplanationTextBlock satırı)
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from datetime import datetime, date
import time
import re
import os
import sys
import json

BASE_URL = "https://www.kap.org.tr"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "tr",
    "Content-Type": "application/json",
    "Origin": "https://www.kap.org.tr",
    "Referer": "https://www.kap.org.tr/tr",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get(f"{BASE_URL}/tr", timeout=15)
    except Exception as e:
        print(f"  Session uyarısı: {e}")
    return session


def fetch_main_api(session: requests.Session, start_date: date, end_date: date) -> list:
    """POST /tr/api/disclosure/list/main — gerçek payload formatı."""
    url = f"{BASE_URL}/tr/api/disclosure/list/main"
    payload = {
        "fromDate":    start_date.strftime("%d.%m.%Y"),
        "toDate":      end_date.strftime("%d.%m.%Y"),
        "memberTypes": ["IGS", "DDK"],
    }
    try:
        resp = session.post(url, json=payload, timeout=30)
        if resp.status_code == 200:
            text = resp.text.strip()
            if text.startswith("[") and len(text) > 10:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    print(f"  ✓ API: {len(data)} kayıt")
                    return data
        print(f"  API → HTTP {resp.status_code}, {len(resp.text)} karakter")
    except Exception as e:
        print(f"  API hata: {e}")
    return []


def filter_pay_alim_satim(raw_items: list) -> list:
    keywords = ["pay alım satım", "pay alim satim"]
    results = []
    for item in raw_items:
        basic = item.get("disclosureBasic", item)
        fields = " ".join([
            (basic.get("title") or ""),
            (basic.get("summary") or ""),
            (basic.get("subject") or ""),
        ]).lower().replace("ı", "i")
        if any(kw.replace("ı", "i") in fields for kw in keywords):
            results.append(item)
    return results


def normalize_item(item: dict) -> dict:
    basic    = item.get("disclosureBasic", item)
    disc_id  = str(basic.get("disclosureId") or basic.get("id") or "")
    pub_date = basic.get("publishDate") or basic.get("disclosureDate") or ""

    # relatedStocks: string "ISKPL" veya liste
    rs_raw = item.get("relatedStocks") or basic.get("relatedStocks") or []
    ilgili = _parse_related(rs_raw)

    return {
        "no":            str(basic.get("disclosureIndex") or disc_id or ""),
        "tarih":         pub_date,
        "kod":           basic.get("stockCode") or basic.get("memberCode") or "",
        "sirket":        basic.get("companyTitle") or basic.get("memberTitle") or "",
        "konu":          basic.get("title") or basic.get("subject") or "",
        "ozet":          basic.get("summary") or "",
        "link":          f"{BASE_URL}/tr/Bildirim/{disc_id}" if disc_id else "",
        "disc_id":       disc_id,
        "ilgili_sirket": ilgili,
    }


def _parse_related(rs_raw) -> str:
    """relatedStocks alanını güvenli parse eder."""
    codes = []
    if isinstance(rs_raw, str):
        # "ISKPL" veya "ISKPL KZGYO" formatı — köşeli parantezleri temizle
        clean = rs_raw.replace("[", "").replace("]", "").strip()
        for part in clean.split():
            part = part.strip().rstrip(",")
            if 2 <= len(part) <= 8 and re.match(r'^[A-Z0-9.]+$', part):
                codes.append(part)
    elif isinstance(rs_raw, list):
        for r in rs_raw:
            if isinstance(r, dict):
                c = r.get("stockCode") or r.get("code") or ""
                if c:
                    codes.append(str(c))
            elif isinstance(r, str):
                clean = r.replace("[", "").replace("]", "").strip()
                if 2 <= len(clean) <= 8:
                    codes.append(clean)
    return ", ".join(codes)


def fetch_disclosure_detail(session: requests.Session, disc_id: str, url: str) -> dict:
    """
    Bildirim detayını HTML'den çeker.
    Yapı: Tablo 1 içinde oda_ExplanationTextBlock satırı → Pay Alım Satım tablosu
    Kolon sırası (Türkçe+İngilizce tekrar, toplam 20):
    0:İşlem Tarihi 1:Alım Nominal 2:Satım Nominal 3:Net Nominal
    4:GünBaşı Nominal 5:GünSonu Nominal 6:SermayeOranıGünBaşı
    7:OyHaklarıGünBaşı 8:SermayeOranıGünSonu 9:OyHaklarıGünSonu
    10-19: İngilizce tekrar
    """
    detail = {
        "islem_tarihi": "", "alim_toplam_nominal": "", "satim_toplam_nominal": "",
        "net_nominal": "", "gun_basi_nominal": "", "gun_sonu_nominal": "",
        "sermaye_orani_gun_basi": "", "oy_haklari_gun_basi": "",
        "sermaye_orani_gun_sonu": "", "oy_haklari_gun_sonu": "",
        "fiyat": "", "aciklama": "",
    }
    if not disc_id and not url:
        return detail

    fetch_url = url or f"{BASE_URL}/tr/Bildirim/{disc_id}"
    try:
        resp = session.get(fetch_url, timeout=25)
        soup = BeautifulSoup(resp.text, "html.parser")

        tables = soup.find_all("table")
        if not tables:
            return detail

        # Tablo 1 — ana bildirim tablosu
        main_table = tables[1] if len(tables) > 1 else tables[0]
        rows = main_table.find_all("tr")

        explanation_text = ""
        pay_data_row = None

        for row in rows:
            cells = row.find_all(["td", "th"])
            cell_texts = [c.get_text(separator=" ", strip=True) for c in cells]
            joined = " ".join(cell_texts)

            # Açıklama metni — fiyat buradan çekilir
            if "oda_ExplanationTextBlock" in joined or (
                "lot" in joined.lower() and "ortalama" in joined.lower()
            ):
                for c in cells:
                    t = c.get_text(separator=" ", strip=True)
                    if "lot" in t.lower() and len(t) > 30:
                        explanation_text = t
                        break

            # Pay Alım Satım veri satırı — tarih/sayı içeren uzun satır
            if _is_data_row(cell_texts):
                pay_data_row = cell_texts

        # Açıklama metninden fiyat çek
        if explanation_text:
            detail["aciklama"] = explanation_text[:300]
            m = re.search(r"[Oo]rtalama\s+([\d.,]+)\s*fiyat", explanation_text)
            if m:
                detail["fiyat"] = m.group(1)
            # Lot miktarı
            m = re.search(r"([\d.,]+)\s*lot", explanation_text, re.I)
            if m:
                pass  # alım_toplam_nominal tablodan alınacak

        # Veri satırından kolonları çek
        if pay_data_row:
            # 20 kolon: 0-9 Türkçe, 10-19 İngilizce (tekrar)
            # İlk 10 kolonu kullan
            def g(idx):
                if idx < len(pay_data_row):
                    return pay_data_row[idx].strip()
                return ""

            detail["islem_tarihi"]           = g(0)
            detail["alim_toplam_nominal"]    = g(1)
            detail["satim_toplam_nominal"]   = g(2)
            detail["net_nominal"]            = g(3)
            detail["gun_basi_nominal"]       = g(4)
            detail["gun_sonu_nominal"]       = g(5)
            detail["sermaye_orani_gun_basi"] = g(6)
            detail["oy_haklari_gun_basi"]    = g(7)
            detail["sermaye_orani_gun_sonu"] = g(8)
            detail["oy_haklari_gun_sonu"]    = g(9)

        # İlgili şirket — "[ISKPL]" formatı
        full_text = soup.get_text()
        m = re.search(r"\[([A-Z0-9]{2,8}(?:,\s*[A-Z0-9]{2,8})*)\]", full_text)
        if m:
            detail["ilgili_sirket_detay"] = m.group(1)

    except Exception as e:
        print(f"  Detay hata ({disc_id}): {e}")

    return detail


def _is_data_row(cells: list) -> bool:
    """Pay Alım Satım veri satırı mı kontrol eder."""
    if len(cells) < 5:
        return False
    joined = " ".join(cells)
    # Tarih ve sayısal veri içermeli
    has_date = bool(re.search(r'\d{2}[./]\d{2}[./]\d{4}', joined))
    has_num  = bool(re.search(r'\d{1,3}(?:[.,]\d{3})+', joined))
    has_pct  = "%" in joined
    return has_date and (has_num or has_pct)


# ─── ANA FONKSİYON ───────────────────────────────────────

def scrape_pay_alim_satim(
    start_date: date,
    end_date: date,
    log_fn=print,
) -> list:
    log_fn(f"📡 KAP: {start_date.strftime('%d.%m.%Y')} → {end_date.strftime('%d.%m.%Y')}")

    session = make_session()
    log_fn("  ✓ Session kuruldu")

    raw = fetch_main_api(session, start_date, end_date)

    if raw:
        log_fn(f"  {len(raw)} kayıt, Pay Alım Satım filtreleniyor...")
        filtered     = filter_pay_alim_satim(raw)
        disclosures  = [normalize_item(i) for i in filtered]
        log_fn(f"  ✓ {len(disclosures)} Pay Alım Satım bildirimi")
    else:
        log_fn("  ⚠ Canlı veri alınamadı — demo veri")
        disclosures = get_demo_data(start_date, end_date)

    log_fn(f"🔍 Detaylar çekiliyor ({len(disclosures)} bildirim)...")
    enriched = []
    for i, disc in enumerate(disclosures, 1):
        row = dict(disc)
        if disc.get("link") and not disc.get("islem_tarihi"):
            log_fn(f"  [{i}/{len(disclosures)}] {disc.get('sirket','')[:40]}")
            det = fetch_disclosure_detail(
                session, disc.get("disc_id", ""), disc.get("link", "")
            )
            row.update({k: v for k, v in det.items() if v})
        enriched.append(row)
        if i % 10 == 0:
            time.sleep(0.5)

    log_fn(f"✅ Tamamlandı: {len(enriched)} bildirim")
    return enriched


def get_demo_data(start_date: date, end_date: date) -> list:
    d1 = start_date.strftime("%d.%m.%Y")
    d2 = end_date.strftime("%d.%m.%Y")
    return [
        {
            "no":"225","tarih":f"{d1} 18:46","kod":"ALNUS",
            "sirket":"ALNUS YATIRIM MENKUL DEĞERLER A.Ş.",
            "konu":"Pay Alım Satım Bildirimi","ozet":"ISKPL Pay Alım Bildirimi",
            "link":"https://www.kap.org.tr/tr/Bildirim/1234567","disc_id":"1234567",
            "islem_tarihi":d1,"alim_toplam_nominal":"93.925.229",
            "satim_toplam_nominal":"10.259","net_nominal":"93.914.970",
            "gun_basi_nominal":"0","gun_sonu_nominal":"93.914.970",
            "sermaye_orani_gun_basi":"% 0","oy_haklari_gun_basi":"% 0",
            "sermaye_orani_gun_sonu":"% 6,26","oy_haklari_gun_sonu":"% 6,26",
            "ilgili_sirket":"ISKPL","fiyat":"12,50",
        },
        {
            "no":"198","tarih":f"{d2} 16:30","kod":"TERA",
            "sirket":"TERA YATIRIM MENKUL DEĞERLER A.Ş.",
            "konu":"Pay Alım Teklifi Yoluyla Pay Toplanmasına İlişkin Bildirim",
            "ozet":"Pay Alım Teklifi - KZGYO",
            "link":"https://www.kap.org.tr/tr/Bildirim/1234568","disc_id":"1234568",
            "islem_tarihi":d2,"alim_toplam_nominal":"5.000.000",
            "satim_toplam_nominal":"0","net_nominal":"5.000.000",
            "gun_basi_nominal":"12.500.000","gun_sonu_nominal":"17.500.000",
            "sermaye_orani_gun_basi":"% 1,25","oy_haklari_gun_basi":"% 1,25",
            "sermaye_orani_gun_sonu":"% 1,75","oy_haklari_gun_sonu":"% 1,75",
            "ilgili_sirket":"KZGYO","fiyat":"8,40",
        },
        {
            "no":"187","tarih":f"{d1} 14:22","kod":"YKSLN",
            "sirket":"YÜKSELEN ÇELİK A.Ş.",
            "konu":"Pay Alım Satım Bildirimi","ozet":"YKSLN Pay Satım",
            "link":"https://www.kap.org.tr/tr/Bildirim/1234569","disc_id":"1234569",
            "islem_tarihi":d1,"alim_toplam_nominal":"0",
            "satim_toplam_nominal":"2.500.000","net_nominal":"-2.500.000",
            "gun_basi_nominal":"15.000.000","gun_sonu_nominal":"12.500.000",
            "sermaye_orani_gun_basi":"% 3,75","oy_haklari_gun_basi":"% 3,75",
            "sermaye_orani_gun_sonu":"% 3,12","oy_haklari_gun_sonu":"% 3,12",
            "ilgili_sirket":"YKSLN","fiyat":"45,80",
        },
    ]


# ─── EXCEL ───────────────────────────────────────────────

COLUMNS_MAP = {
    "no":                     "No",
    "tarih":                  "Yayın Tarihi",
    "kod":                    "Hisse Kodu",
    "sirket":                 "Aracı Kurum",
    "konu":                   "Konu",
    "ozet":                   "Özet",
    "ilgili_sirket":          "İlgili Şirket",
    "islem_tarihi":           "İşlem Tarihi",
    "fiyat":                  "Ort. Fiyat (TL)",
    "alim_toplam_nominal":    "Alım Nominal (TL)",
    "satim_toplam_nominal":   "Satım Nominal (TL)",
    "net_nominal":            "Net Nominal (TL)",
    "gun_basi_nominal":       "Gün Başı Nominal (TL)",
    "gun_sonu_nominal":       "Gün Sonu Nominal (TL)",
    "sermaye_orani_gun_basi": "Sermaye Oranı Gün Başı (%)",
    "oy_haklari_gun_basi":    "Oy Hakları Gün Başı (%)",
    "sermaye_orani_gun_sonu": "Sermaye Oranı Gün Sonu (%)",
    "oy_haklari_gun_sonu":    "Oy Hakları Gün Sonu (%)",
    "link":                   "KAP Linki",
}

COL_WIDTHS = {
    "No":6,"Yayın Tarihi":18,"Hisse Kodu":10,"Aracı Kurum":30,
    "Konu":36,"Özet":28,"İlgili Şirket":16,"İşlem Tarihi":14,
    "Ort. Fiyat (TL)":14,"Alım Nominal (TL)":20,"Satım Nominal (TL)":20,
    "Net Nominal (TL)":20,"Gün Başı Nominal (TL)":22,"Gün Sonu Nominal (TL)":22,
    "Sermaye Oranı Gün Başı (%)":24,"Oy Hakları Gün Başı (%)":22,
    "Sermaye Oranı Gün Sonu (%)":24,"Oy Hakları Gün Sonu (%)":22,"KAP Linki":20,
}


def save_to_excel(enriched: list, start_date: date, end_date: date, output_dir: str = ".") -> tuple:
    os.makedirs(output_dir, exist_ok=True)
    fname    = f"KAP_PayAlimSatim_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"
    filepath = os.path.join(output_dir, fname)

    rows = [{col: item.get(key, "") for key, col in COLUMNS_MAP.items()} for item in enriched]
    df   = pd.DataFrame(rows)
    df.to_excel(filepath, index=False, sheet_name="Pay Alım Satım")

    wb = load_workbook(filepath)
    ws = wb.active

    h_fill  = PatternFill("solid", start_color="1F4E79")
    a_fill  = PatternFill("solid", start_color="D6E4F0")
    w_fill  = PatternFill("solid", start_color="FFFFFF")
    thin    = Side(style="thin", color="BDD7EE")
    border  = Border(left=thin, right=thin, top=thin, bottom=thin)
    h_font  = Font(bold=True, color="FFFFFF", name="Arial", size=10)
    b_font  = Font(name="Arial", size=9)
    l_font  = Font(name="Arial", size=9, color="0563C1", underline="single")

    for cell in ws[1]:
        cell.fill = h_fill; cell.font = h_font
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = border
    ws.row_dimensions[1].height = 38

    link_col = next((i for i, c in enumerate(ws[1], 1) if c.value == "KAP Linki"), None)

    for i, row in enumerate(ws.iter_rows(min_row=2), start=2):
        fill = a_fill if i % 2 == 0 else w_fill
        for cell in row:
            cell.fill = fill; cell.border = border
            cell.alignment = Alignment(vertical="center", wrap_text=True)
            if link_col and cell.column == link_col and cell.value:
                cell.font = l_font
                cell.hyperlink = str(cell.value)
                cell.value = "Bildirimi Görüntüle"
            else:
                cell.font = b_font

    for idx, col_name in enumerate(df.columns, 1):
        ws.column_dimensions[get_column_letter(idx)].width = COL_WIDTHS.get(col_name, 15)
    ws.freeze_panes = "A2"

    # Özet sayfası
    ws2 = wb.create_sheet("Özet")
    ws2["A1"] = "KAP Pay Alım Satım Bildirimleri"
    ws2["A1"].font = Font(bold=True, size=14, name="Arial", color="1F4E79")
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    for r, (lbl, val) in enumerate([
        ("Başlangıç", start_date.strftime("%d.%m.%Y")),
        ("Bitiş",     end_date.strftime("%d.%m.%Y")),
        ("Kayıt",     str(len(enriched))),
        ("Rapor",     now_str),
    ], start=3):
        ws2.cell(r, 1, lbl).font = Font(bold=True, name="Arial", size=10)
        ws2.cell(r, 2, val).font = Font(name="Arial", size=10)
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
        start = today
        end   = today
    out_dir  = sys.argv[3] if len(sys.argv) >= 4 else "."
    enriched = scrape_pay_alim_satim(start, end)
    fp, df   = save_to_excel(enriched, start, end, out_dir)
    print(f"✓ {len(df)} kayıt → {fp}")
