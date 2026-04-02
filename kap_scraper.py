"""
KAP Pay Alım Satım Bildirimi Scraper
Gerçek API: POST /tr/api/disclosure/list/main
Cookie session ile çalışır.
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
    "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}


def make_session() -> requests.Session:
    """
    Ana sayfayı ziyaret ederek gerçek tarayıcı cookie'lerini alır.
    KAP, NSC_* ve KAP= cookie'lerini session başında veriyor.
    """
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        # Ana sayfayı ziyaret et → cookie'leri al
        session.get(f"{BASE_URL}/tr", timeout=15)
        # Bildirim sayfasını da ziyaret et (Referer için)
        session.get(f"{BASE_URL}/tr/", timeout=10)
    except Exception as e:
        print(f"  Session init uyarısı: {e}")
    return session


def fetch_main_api(session: requests.Session, start_date: date, end_date: date) -> list:
    """
    POST /tr/api/disclosure/list/main
    Gerçek payload formatı: {"fromDate":"DD.MM.YYYY","toDate":"DD.MM.YYYY","memberTypes":["IGS","DDK"]}
    """
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
                    print(f"  ✓ /list/main API çalıştı — {len(data)} kayıt")
                    return data
        print(f"  /list/main → HTTP {resp.status_code}, yanıt boyutu: {len(resp.text)} karakter")
    except Exception as e:
        print(f"  /list/main hata: {e}")

    return []


def fetch_detayli_sorgu(session: requests.Session, start_date: date, end_date: date) -> list:
    """
    POST /tr/api/disclosure/query veya benzeri detaylı sorgulama endpoint'i.
    """
    start_str = start_date.strftime("%d-%m-%Y")
    end_str   = end_date.strftime("%d-%m-%Y")

    candidates = [
        (f"{BASE_URL}/tr/api/disclosure/query", {
            "startDate": start_str, "endDate": end_str,
            "memberType": "IGS", "subject": "Pay Alım Satım Bildirimi"
        }),
        (f"{BASE_URL}/tr/api/disclosure/list/query", {
            "fromDate": start_str, "toDate": end_str,
            "disclosureClass": "ODA",
        }),
        (f"{BASE_URL}/tr/api/memberDisclosure/list", {
            "startDate": start_str, "endDate": end_str,
            "memberType": "IGS",
        }),
        # GET endpoint'leri
    ]

    get_endpoints = [
        f"{BASE_URL}/tr/api/general/member-disclosure-query/IGS/{start_str}/{end_str}/null/null/null",
        f"{BASE_URL}/tr/api/memberDisclosure/query/IGS/{start_str}/{end_str}/null/null/null/null/null/null",
    ]

    for url, payload in candidates:
        try:
            resp = session.post(url, json=payload, timeout=20)
            if resp.status_code == 200:
                text = resp.text.strip()
                if (text.startswith("[") or text.startswith("{")) and len(text) > 20:
                    data = resp.json()
                    items = data if isinstance(data, list) else data.get("data", data.get("items", []))
                    if items:
                        print(f"  ✓ {url} çalıştı ({len(items)} kayıt)")
                        return items
        except Exception:
            continue

    for url in get_endpoints:
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 200:
                text = resp.text.strip()
                if text.startswith("[") and len(text) > 10:
                    data = resp.json()
                    if isinstance(data, list) and len(data) > 0:
                        print(f"  ✓ GET {url} çalıştı ({len(data)} kayıt)")
                        return data
        except Exception:
            continue

    return []


def filter_pay_alim_satim(raw_items: list) -> list:
    """
    Ham API verisinden Pay Alım Satım bildirimlerini filtreler.
    /list/main: disclosureBasic > title ve summary alanlarına bakar.
    """
    keywords = ["pay alım satım", "pay alim satim"]
    results = []
    for item in raw_items:
        basic = item.get("disclosureBasic", item)
        fields = [
            (basic.get("title") or "").lower(),
            (basic.get("summary") or "").lower(),
            (basic.get("subject") or "").lower(),
        ]
        text = " ".join(fields).replace("\u0131","i").replace("ı","i")
        if any(kw.replace("ı","i") in text for kw in keywords):
            results.append(item)
    return results


def normalize_item(item: dict) -> dict:
    """
    /list/main API response satırını standart iç formata çevirir.
    Gerçek alanlar: disclosureBasic > disclosureId, stockCode,
    companyTitle, publishDate, title, summary, relatedStocks
    """
    basic   = item.get("disclosureBasic", item)
    disc_id = str(basic.get("disclosureId") or basic.get("id") or "")
    link    = f"{BASE_URL}/tr/Bildirim/{disc_id}" if disc_id else ""
    pub_date = basic.get("publishDate") or basic.get("disclosureDate") or ""

    # İlgili şirketler
    related = item.get("relatedStocks") or basic.get("relatedStocks") or []
    # relatedStocks bazen string liste ["ISKPL"], bazen dict liste [{"stockCode":"ISKPL"}] gelir
    ilgili_list = []
    for r in related:
        if isinstance(r, dict):
            code = r.get("stockCode") or r.get("code") or r.get("memberCode") or ""
            if code:
                ilgili_list.append(code)
        elif isinstance(r, str) and r.strip():
            ilgili_list.append(r.strip())
    ilgili = ", ".join(ilgili_list)

    return {
        "no":            str(basic.get("disclosureIndex") or disc_id or ""),
        "tarih":         pub_date,
        "kod":           basic.get("stockCode") or basic.get("memberCode") or "",
        "sirket":        basic.get("companyTitle") or basic.get("memberTitle") or "",
        "konu":          basic.get("title") or basic.get("subject") or "",
        "ozet":          basic.get("summary") or "",
        "link":          link,
        "disc_id":       disc_id,
        "ilgili_sirket": ilgili,
    }


def fetch_disclosure_detail(session: requests.Session, disc_id: str, url: str) -> dict:
    """Bildirim detay sayfasını veya API'yi çeker."""
    detail = {
        "islem_tarihi": "", "alim_toplam_nominal": "", "satim_toplam_nominal": "",
        "net_nominal": "", "gun_basi_nominal": "", "gun_sonu_nominal": "",
        "sermaye_orani_gun_basi": "", "sermaye_orani_gun_sonu": "",
        "oy_haklari_gun_sonu": "", "fiyat": "", "ilgili_sirket": "",
    }

    # Önce detay API'yi dene
    if disc_id:
        try:
            api_url = f"{BASE_URL}/tr/api/disclosure/{disc_id}"
            resp = session.get(api_url, timeout=15)
            if resp.status_code == 200 and resp.text.strip().startswith("{"):
                data = resp.json()
                basic = data.get("disclosureBasic", {})
                # İlgili şirketleri çek
                related = data.get("relatedStocks", []) or []
                if related:
                    codes = []
                    for r in related:
                        if isinstance(r, dict):
                            c = r.get("stockCode") or r.get("code") or ""
                            if c: codes.append(c)
                        elif isinstance(r, str) and r.strip():
                            codes.append(r.strip())
                    if codes:
                        detail["ilgili_sirket"] = ", ".join(codes)
        except Exception:
            pass

    # HTML parse
    fetch_url = url or (f"{BASE_URL}/tr/Bildirim/{disc_id}" if disc_id else "")
    if not fetch_url:
        return detail

    try:
        resp = session.get(fetch_url, timeout=20)
        soup = BeautifulSoup(resp.text, "html.parser")
        full_text = soup.get_text()

        # Tarih
        m = re.search(r"(\d{2}[./]\d{2}[./]\d{4})\s*[İi]şlem\s*[Tt]arih", full_text)
        if m:
            detail["islem_tarihi"] = m.group(1)

        # Lot
        m = re.search(r"(\d[\d.,]+)\s*lot", full_text, re.I)
        if m:
            detail["alim_toplam_nominal"] = m.group(1)

        # Fiyat
        m = re.search(r"[Oo]rtalama\s+([\d.,]+)\s*fiyat", full_text)
        if m:
            detail["fiyat"] = m.group(1)

        # İlgili şirket
        if not detail["ilgili_sirket"]:
            m = re.search(r"\[([A-Z]{3,6})\]", full_text)
            if m:
                detail["ilgili_sirket"] = m.group(1)

        # Tablo parse
        for table in soup.find_all("table"):
            header_row = table.find("tr")
            if not header_row:
                continue
            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th","td"])]
            if not any(k in " ".join(headers) for k in ["nominal","sermaye","satım","alım"]):
                continue
            for tr in table.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in tr.find_all("td")]
                if len(cells) >= 7:
                    detail["islem_tarihi"]           = cells[0] or detail["islem_tarihi"]
                    detail["alim_toplam_nominal"]    = cells[1]
                    detail["satim_toplam_nominal"]   = cells[2]
                    detail["net_nominal"]            = cells[3]
                    detail["gun_basi_nominal"]       = cells[4]
                    detail["gun_sonu_nominal"]       = cells[5]
                    detail["sermaye_orani_gun_basi"] = cells[6] if len(cells) > 6 else ""
                    detail["sermaye_orani_gun_sonu"] = cells[7] if len(cells) > 7 else ""
                    detail["oy_haklari_gun_sonu"]    = cells[8] if len(cells) > 8 else ""
    except Exception as e:
        print(f"  HTML detay hata: {e}")

    return detail


# ─── ANA FONKSİYON ───────────────────────────────────────

def scrape_pay_alim_satim(
    start_date: date,
    end_date: date,
    log_fn=print,
) -> list:
    """
    Verilen tarih aralığında KAP'tan Pay Alım Satım bildirimlerini çeker.
    """
    log_fn(f"📡 KAP bağlantısı kuruluyor: {start_date.strftime('%d.%m.%Y')} → {end_date.strftime('%d.%m.%Y')}")

    session = make_session()
    log_fn("  ✓ Session ve cookie'ler alındı")

    # 1) /list/main endpoint
    raw = fetch_main_api(session, start_date, end_date)

    # 2) Alternatif endpoint'ler
    if not raw:
        log_fn("  /list/main boş döndü, alternatif endpoint'ler deneniyor...")
        raw = fetch_detayli_sorgu(session, start_date, end_date)

    if raw:
        log_fn(f"  Toplam {len(raw)} kayıt alındı, Pay Alım Satım filtreleniyor...")
        filtered = filter_pay_alim_satim(raw)
        log_fn(f"  ✓ {len(filtered)} Pay Alım Satım bildirimi bulundu.")
        disclosures = [normalize_item(i) for i in filtered]
    else:
        log_fn("  ⚠ Canlı veri alınamadı — demo veri kullanılıyor.")
        disclosures = get_demo_data(start_date, end_date)

    # 3) Detayları çek
    log_fn(f"🔍 {len(disclosures)} bildirimin detayları çekiliyor...")
    enriched = []
    for i, disc in enumerate(disclosures, 1):
        row = dict(disc)
        if not disc.get("islem_tarihi"):
            log_fn(f"  [{i}/{len(disclosures)}] {disc.get('sirket','')[:45]}")
            detail = fetch_disclosure_detail(session, disc.get("disc_id",""), disc.get("link",""))
            row.update({k: v for k, v in detail.items() if v})
            time.sleep(0.3)
        enriched.append(row)

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
            "sermaye_orani_gun_basi":"% 0","sermaye_orani_gun_sonu":"% 6,26",
            "oy_haklari_gun_sonu":"% 6,26","ilgili_sirket":"ISKPL","fiyat":"12,50",
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
            "sermaye_orani_gun_basi":"% 1,25","sermaye_orani_gun_sonu":"% 1,75",
            "oy_haklari_gun_sonu":"% 1,75","ilgili_sirket":"KZGYO","fiyat":"8,40",
        },
        {
            "no":"187","tarih":f"{d1} 14:22","kod":"YKSLN",
            "sirket":"YÜKSELEN ÇELİK A.Ş.",
            "konu":"Pay Alım Satım Bildirimi","ozet":"YKSLN Pay Satım",
            "link":"https://www.kap.org.tr/tr/Bildirim/1234569","disc_id":"1234569",
            "islem_tarihi":d1,"alim_toplam_nominal":"0",
            "satim_toplam_nominal":"2.500.000","net_nominal":"-2.500.000",
            "gun_basi_nominal":"15.000.000","gun_sonu_nominal":"12.500.000",
            "sermaye_orani_gun_basi":"% 3,75","sermaye_orani_gun_sonu":"% 3,12",
            "oy_haklari_gun_sonu":"% 3,12","ilgili_sirket":"YKSLN","fiyat":"45,80",
        },
    ]


# ─── EXCEL ───────────────────────────────────────────────

COLUMNS_MAP = {
    "no":"No","tarih":"Yayın Tarihi","kod":"Hisse Kodu","sirket":"Aracı Kurum",
    "konu":"Konu","ozet":"Özet","ilgili_sirket":"İlgili Şirket",
    "islem_tarihi":"İşlem Tarihi","fiyat":"Ort. Fiyat (TL)",
    "alim_toplam_nominal":"Alım Nominal (TL)","satim_toplam_nominal":"Satım Nominal (TL)",
    "net_nominal":"Net Nominal (TL)","gun_basi_nominal":"Gün Başı Nominal (TL)",
    "gun_sonu_nominal":"Gün Sonu Nominal (TL)",
    "sermaye_orani_gun_basi":"Sermaye Oranı Gün Başı (%)",
    "sermaye_orani_gun_sonu":"Sermaye Oranı Gün Sonu (%)",
    "oy_haklari_gun_sonu":"Oy Hakları Gün Sonu (%)","link":"KAP Linki",
}

COL_WIDTHS = {
    "No":6,"Yayın Tarihi":16,"Hisse Kodu":10,"Aracı Kurum":30,
    "Konu":36,"Özet":28,"İlgili Şirket":14,"İşlem Tarihi":14,
    "Ort. Fiyat (TL)":16,"Alım Nominal (TL)":20,"Satım Nominal (TL)":20,
    "Net Nominal (TL)":20,"Gün Başı Nominal (TL)":22,"Gün Sonu Nominal (TL)":22,
    "Sermaye Oranı Gün Başı (%)":24,"Sermaye Oranı Gün Sonu (%)":24,
    "Oy Hakları Gün Sonu (%)":22,"KAP Linki":20,
}


def save_to_excel(enriched: list, start_date: date, end_date: date, output_dir: str = ".") -> tuple:
    os.makedirs(output_dir, exist_ok=True)
    fname    = f"KAP_PayAlimSatim_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"
    filepath = os.path.join(output_dir, fname)

    rows = [{col: item.get(key,"") for key,col in COLUMNS_MAP.items()} for item in enriched]
    df   = pd.DataFrame(rows)
    df.to_excel(filepath, index=False, sheet_name="Pay Alım Satım")

    wb = load_workbook(filepath)
    ws = wb.active

    h_fill   = PatternFill("solid", start_color="1F4E79")
    alt_fill = PatternFill("solid", start_color="D6E4F0")
    wh_fill  = PatternFill("solid", start_color="FFFFFF")
    thin     = Side(style="thin", color="BDD7EE")
    border   = Border(left=thin, right=thin, top=thin, bottom=thin)
    h_font   = Font(bold=True, color="FFFFFF", name="Arial", size=10)
    b_font   = Font(name="Arial", size=9)
    l_font   = Font(name="Arial", size=9, color="0563C1", underline="single")

    for cell in ws[1]:
        cell.fill      = h_fill
        cell.font      = h_font
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = border
    ws.row_dimensions[1].height = 36

    link_col = next((i for i,c in enumerate(ws[1],1) if c.value=="KAP Linki"), None)

    for i, row in enumerate(ws.iter_rows(min_row=2), start=2):
        fill = alt_fill if i%2==0 else wh_fill
        for cell in row:
            cell.fill      = fill
            cell.border    = border
            cell.alignment = Alignment(vertical="center", wrap_text=True)
            if link_col and cell.column==link_col and cell.value:
                cell.font      = l_font
                cell.hyperlink = str(cell.value)
                cell.value     = "Bildirimi Görüntüle"
            else:
                cell.font = b_font

    for idx, col_name in enumerate(df.columns, 1):
        ws.column_dimensions[get_column_letter(idx)].width = COL_WIDTHS.get(col_name, 15)
    ws.freeze_panes = "A2"

    ws2 = wb.create_sheet("Özet")
    ws2["A1"] = "KAP Pay Alım Satım Bildirimleri"
    ws2["A1"].font = Font(bold=True, size=14, name="Arial", color="1F4E79")
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    for r,(lbl,val) in enumerate([
        ("Başlangıç",   start_date.strftime("%d.%m.%Y")),
        ("Bitiş",       end_date.strftime("%d.%m.%Y")),
        ("Kayıt Sayısı",str(len(enriched))),
        ("Rapor Tarihi",now_str),
    ], start=3):
        ws2.cell(r,1,lbl).font = Font(bold=True, name="Arial", size=10)
        ws2.cell(r,2,val).font = Font(name="Arial", size=10)
    ws2.column_dimensions["A"].width = 22
    ws2.column_dimensions["B"].width = 22

    wb.save(filepath)
    print(f"✓ Excel kaydedildi: {filepath}")
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
    print(f"✓ Tamamlandı: {len(df)} kayıt → {fp}")
