"""
KAP Pay Alım Satım Bildirimi Scraper
- Liste: POST /tr/api/disclosure/list/main (requests)
- Detay: Playwright headless browser (JS render gerekli)

Kurulum:
    pip install requests beautifulsoup4 pandas openpyxl playwright
    playwright install chromium
"""

import requests
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
    "Origin": BASE_URL,
    "Referer": f"{BASE_URL}/tr",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


# ─── LİSTE API (requests) ────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        s.get(f"{BASE_URL}/tr", timeout=15)
    except Exception as e:
        print(f"  Session uyarı: {e}")
    return s


def fetch_main_api(session, start_date: date, end_date: date) -> list:
    url     = f"{BASE_URL}/tr/api/disclosure/list/main"
    payload = {
        "fromDate":    start_date.strftime("%d.%m.%Y"),
        "toDate":      end_date.strftime("%d.%m.%Y"),
        "memberTypes": ["IGS", "DDK"],
    }
    try:
        r = session.post(url, json=payload, timeout=30)
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
            (b.get("title") or ""), (b.get("summary") or ""), (b.get("subject") or "")
        ]).lower().replace("ı", "i")
        if any(k.replace("ı","i") in t for k in kw):
            out.append(item)
    return out


def _parse_related(rs) -> str:
    codes = []
    if isinstance(rs, str):
        for p in rs.replace("[","").replace("]","").split():
            p = p.strip().rstrip(",")
            if 2 <= len(p) <= 8 and re.match(r'^[A-Z0-9.]+$', p):
                codes.append(p)
    elif isinstance(rs, list):
        for r in rs:
            if isinstance(r, dict):
                c = r.get("stockCode") or r.get("code") or ""
                if c: codes.append(str(c))
            elif isinstance(r, str):
                c = r.strip().replace("[","").replace("]","")
                if 2 <= len(c) <= 8: codes.append(c)
    return ", ".join(codes)


def normalize_item(item: dict) -> dict:
    b       = item.get("disclosureBasic", item)
    disc_id = str(b.get("disclosureId") or b.get("id") or "")
    rs      = item.get("relatedStocks") or b.get("relatedStocks") or []
    return {
        "no":            str(b.get("disclosureIndex") or disc_id or ""),
        "tarih":         b.get("publishDate") or b.get("disclosureDate") or "",
        "kod":           b.get("stockCode") or b.get("memberCode") or "",
        "sirket":        b.get("companyTitle") or b.get("memberTitle") or "",
        "konu":          b.get("title") or b.get("subject") or "",
        "ozet":          b.get("summary") or "",
        "link":          f"{BASE_URL}/tr/Bildirim/{disc_id}" if disc_id else "",
        "disc_id":       disc_id,
        "ilgili_sirket": _parse_related(rs),
    }


# ─── DETAY (Playwright) ──────────────────────────────────

# JS: sayfadan tablo verisini çeker
_EXTRACT_JS = """
() => {
    const tables = document.querySelectorAll('table');
    const result = {
        data_row: null,
        aciklama: '',
        ilgili: '',
        fiyat: ''
    };

    // İlgili şirket — [ISKPL] formatı
    const bodyText = document.body.innerText;
    const ilgiliMatch = bodyText.match(/\\[([A-Z0-9]{2,8}(?:,\\s*[A-Z0-9]{2,8})*)\\]/);
    if (ilgiliMatch) result.ilgili = ilgiliMatch[1];

    // Açıklama ve fiyat
    const fiyatMatch = bodyText.match(/[Oo]rtalama\\s+([\\d.,]+)\\s*fiyat/);
    if (fiyatMatch) result.fiyat = fiyatMatch[1];

    const aciklamaMatch = bodyText.match(/\\d+\\.\\d+\\.\\d{4}[^\\n]*lot[^\\n]*/i);
    if (aciklamaMatch) result.aciklama = aciklamaMatch[0].substring(0, 300);

    // Tablo 1 — son satır veri satırı
    if (tables.length > 1) {
        const tbl = tables[1];
        const rows = tbl.querySelectorAll('tr');
        // Son satırdan geriye doğru tara — veri satırını bul
        for (let i = rows.length - 1; i >= 0; i--) {
            const cells = Array.from(rows[i].querySelectorAll('td'))
                              .map(c => c.innerText.trim());
            const joined = cells.join(' ');
            // Tarih + sayı içeren satır
            if (/\\d{2}[\\/.]\\d{2}[\\/.]\\d{4}/.test(joined) && cells.length >= 5) {
                result.data_row = cells;
                break;
            }
        }
    }
    return result;
}
"""


def fetch_details_playwright(disclosures: list, log_fn=print) -> list:
    """
    Playwright ile tüm bildirim detaylarını çeker.
    Tek browser instance açar, sayfaları sırayla ziyaret eder.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log_fn("  ⚠ Playwright kurulu değil!")
        log_fn("  Kurmak için: pip install playwright && playwright install chromium")
        return disclosures

    log_fn(f"  🌐 Playwright başlatılıyor ({len(disclosures)} sayfa)...")

    enriched = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx     = browser.new_context(
            locale="tr-TR",
            user_agent=HEADERS["User-Agent"],
        )
        page = ctx.new_page()

        # Gereksiz kaynakları engelle — hızlanır
        page.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,css}", lambda r: r.abort())
        page.route("**/google-analytics**", lambda r: r.abort())
        page.route("**/market-data-tracker**", lambda r: r.abort())

        for i, disc in enumerate(disclosures, 1):
            row  = dict(disc)
            link = disc.get("link", "")
            if not link:
                enriched.append(row)
                continue

            log_fn(f"  [{i}/{len(disclosures)}] {disc.get('ilgili_sirket','?')} — {disc.get('sirket','')[:30]}")
            try:
                page.goto(link, timeout=30000, wait_until="domcontentloaded")
                # Tablo yüklenene kadar bekle
                page.wait_for_selector("table", timeout=15000)
                # Biraz daha bekle (JS render)
                page.wait_for_timeout(1200)

                res = page.evaluate(_EXTRACT_JS)

                if res.get("data_row"):
                    cells = res["data_row"]
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
                else:
                    log_fn(f"    ⚠ Veri satırı bulunamadı")

                if res.get("fiyat"):
                    row["fiyat"] = res["fiyat"]
                if res.get("aciklama"):
                    row["aciklama"] = res["aciklama"]
                if res.get("ilgili") and not row.get("ilgili_sirket"):
                    row["ilgili_sirket"] = res["ilgili"]

            except Exception as e:
                log_fn(f"    ✗ Hata: {e}")

            enriched.append(row)

        browser.close()

    return enriched


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
        log_fn(f"  {len(raw)} kayıt filtreleniyor...")
        filtered    = filter_pay_alim_satim(raw)
        disclosures = [normalize_item(i) for i in filtered]
        log_fn(f"  ✓ {len(disclosures)} Pay Alım Satım bildirimi")
    else:
        log_fn("  ⚠ Canlı veri alınamadı — demo veri kullanılıyor")
        return get_demo_data(start_date, end_date)

    # Detayları Playwright ile çek
    enriched = fetch_details_playwright(disclosures, log_fn=log_fn)
    log_fn(f"✅ Tamamlandı: {len(enriched)} bildirim")
    return enriched


def get_demo_data(start_date: date, end_date: date) -> list:
    d1, d2 = start_date.strftime("%d.%m.%Y"), end_date.strftime("%d.%m.%Y")
    return [
        {"no":"225","tarih":f"{d1} 18:46","kod":"ALNUS",
         "sirket":"ALNUS YATIRIM MENKUL DEĞERLER A.Ş.",
         "konu":"Pay Alım Satım Bildirimi","ozet":"ISKPL Pay Alım Bildirimi",
         "link":"https://www.kap.org.tr/tr/Bildirim/1234567","disc_id":"1234567",
         "islem_tarihi":d1,"alim_toplam_nominal":"93.925.229",
         "satim_toplam_nominal":"10.259","net_nominal":"93.914.970",
         "gun_basi_nominal":"0","gun_sonu_nominal":"93.914.970",
         "sermaye_orani_gun_basi":"% 0","oy_haklari_gun_basi":"% 0",
         "sermaye_orani_gun_sonu":"% 6,26","oy_haklari_gun_sonu":"% 6,26",
         "ilgili_sirket":"ISKPL","fiyat":"12,50"},
        {"no":"198","tarih":f"{d2} 16:30","kod":"TERA",
         "sirket":"TERA YATIRIM MENKUL DEĞERLER A.Ş.",
         "konu":"Pay Alım Teklifi Yoluyla Pay Toplanmasına İlişkin Bildirim",
         "ozet":"Pay Alım Teklifi - KZGYO",
         "link":"https://www.kap.org.tr/tr/Bildirim/1234568","disc_id":"1234568",
         "islem_tarihi":d2,"alim_toplam_nominal":"5.000.000",
         "satim_toplam_nominal":"0","net_nominal":"5.000.000",
         "gun_basi_nominal":"12.500.000","gun_sonu_nominal":"17.500.000",
         "sermaye_orani_gun_basi":"% 1,25","oy_haklari_gun_basi":"% 1,25",
         "sermaye_orani_gun_sonu":"% 1,75","oy_haklari_gun_sonu":"% 1,75",
         "ilgili_sirket":"KZGYO","fiyat":"8,40"},
        {"no":"187","tarih":f"{d1} 14:22","kod":"YKSLN",
         "sirket":"YÜKSELEN ÇELİK A.Ş.",
         "konu":"Pay Alım Satım Bildirimi","ozet":"YKSLN Pay Satım",
         "link":"https://www.kap.org.tr/tr/Bildirim/1234569","disc_id":"1234569",
         "islem_tarihi":d1,"alim_toplam_nominal":"0",
         "satim_toplam_nominal":"2.500.000","net_nominal":"-2.500.000",
         "gun_basi_nominal":"15.000.000","gun_sonu_nominal":"12.500.000",
         "sermaye_orani_gun_basi":"% 3,75","oy_haklari_gun_basi":"% 3,75",
         "sermaye_orani_gun_sonu":"% 3,12","oy_haklari_gun_sonu":"% 3,12",
         "ilgili_sirket":"YKSLN","fiyat":"45,80"},
    ]


# ─── EXCEL ───────────────────────────────────────────────

COLUMNS_MAP = {
    "no":"No","tarih":"Yayın Tarihi","kod":"Hisse Kodu","sirket":"Aracı Kurum",
    "konu":"Konu","ozet":"Özet","ilgili_sirket":"İlgili Şirket",
    "islem_tarihi":"İşlem Tarihi","fiyat":"Ort. Fiyat (TL)",
    "alim_toplam_nominal":"Alım Nominal (TL)","satim_toplam_nominal":"Satım Nominal (TL)",
    "net_nominal":"Net Nominal (TL)","gun_basi_nominal":"Gün Başı Nominal (TL)",
    "gun_sonu_nominal":"Gün Sonu Nominal (TL)",
    "sermaye_orani_gun_basi":"Sermaye Oranı Gün Başı (%)","oy_haklari_gun_basi":"Oy Hakları Gün Başı (%)",
    "sermaye_orani_gun_sonu":"Sermaye Oranı Gün Sonu (%)","oy_haklari_gun_sonu":"Oy Hakları Gün Sonu (%)",
    "link":"KAP Linki",
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

    rows = [{col: item.get(key,"") for key,col in COLUMNS_MAP.items()} for item in enriched]
    df   = pd.DataFrame(rows)
    df.to_excel(filepath, index=False, sheet_name="Pay Alım Satım")

    wb = load_workbook(filepath)
    ws = wb.active
    h_fill = PatternFill("solid", start_color="1F4E79")
    a_fill = PatternFill("solid", start_color="D6E4F0")
    w_fill = PatternFill("solid", start_color="FFFFFF")
    thin   = Side(style="thin", color="BDD7EE")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    h_font = Font(bold=True, color="FFFFFF", name="Arial", size=10)
    b_font = Font(name="Arial", size=9)
    l_font = Font(name="Arial", size=9, color="0563C1", underline="single")

    for cell in ws[1]:
        cell.fill = h_fill; cell.font = h_font
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = border
    ws.row_dimensions[1].height = 38

    link_col = next((i for i,c in enumerate(ws[1],1) if c.value=="KAP Linki"), None)
    for i, row in enumerate(ws.iter_rows(min_row=2), start=2):
        fill = a_fill if i%2==0 else w_fill
        for cell in row:
            cell.fill = fill; cell.border = border
            cell.alignment = Alignment(vertical="center", wrap_text=True)
            if link_col and cell.column==link_col and cell.value:
                cell.font = l_font
                cell.hyperlink = str(cell.value)
                cell.value = "Bildirimi Görüntüle"
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
        ("Başlangıç", start_date.strftime("%d.%m.%Y")),
        ("Bitiş",     end_date.strftime("%d.%m.%Y")),
        ("Kayıt",     str(len(enriched))),
        ("Rapor",     now_str),
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
    out_dir  = sys.argv[3] if len(sys.argv) >= 4 else "."
    enriched = scrape_pay_alim_satim(start, end)
    fp, df   = save_to_excel(enriched, start, end, out_dir)
    print(f"✓ {len(df)} kayıt → {fp}")
