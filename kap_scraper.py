"""
KAP Pay Alım Satım Bildirimi Scraper
- Liste: POST /tr/api/disclosure/list/main
- Detay: Playwright headless browser (JS render gerekli)

Kurulum (bir kez):
    pip install requests pandas openpyxl playwright
    playwright install chromium
"""

import requests
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from datetime import datetime, date
import re, os, sys, time

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
    """
    API alanları:
    - companyTitle  → Aracı Kurum (bildirimi yapan)
    - stockCode     → "ALNUS, ANC" — aracı kurum hisse kodu(ları)
    - relatedStocks → "ISKPL" — işlem yapılan hisse (string)
    - disclosureIndex → sayısal ID → link için kullan
    - disclosureId  → UUID
    """
    b     = item.get("disclosureBasic", item)
    idx   = b.get("disclosureIndex") or ""          # sayısal: 1583033
    link  = f"{BASE_URL}/tr/Bildirim/{idx}" if idx else ""

    # relatedStocks string geliyor: "ISKPL" veya "ISKPL KZGYO"
    rs_raw = b.get("relatedStocks") or ""
    ilgili = _clean_related(rs_raw)

    # stockCode virgüllü olabilir: "ALNUS, ANC"
    stock_code = (b.get("stockCode") or "").strip()

    return {
        "no":            str(idx),
        "tarih":         b.get("publishDate") or "",
        "kod":           stock_code,
        "sirket":        b.get("companyTitle") or "",  # Aracı Kurum
        "konu":          b.get("title") or "",
        "ozet":          b.get("summary") or "",
        "link":          link,
        "disc_index":    str(idx),
        "ilgili_sirket": ilgili,
    }


def _clean_related(rs) -> str:
    """'ISKPL' veya 'ISKPL KZGYO' → 'ISKPL' veya 'ISKPL, KZGYO'"""
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
    # String
    clean = str(rs).replace("[","").replace("]","").strip()
    parts = [p.strip() for p in clean.split() if 2 <= len(p.strip()) <= 10]
    return ", ".join(parts)


# ─── DETAY: Playwright ───────────────────────────────────

_EXTRACT_JS = """
() => {
    const result = {
        data_rows: [],
        fiyat: '',
        aciklama: '',
        ilgili: ''
    };

    const bodyText = document.body.innerText || '';

    // Fiyat
    const fm = bodyText.match(/[Oo]rtalama[\\s]+([\\d.,]+)[\\s]*fiyat/);
    if (fm) result.fiyat = fm[1];

    // Açıklama
    const am = bodyText.match(/\\d{2}[.,]\\d{2}[.,]\\d{4}[^\\n]{10,200}(?:lot|LOT)[^\\n]*/);
    if (am) result.aciklama = am[0].substring(0, 300);

    // İlgili şirket köşeli parantez içinde
    const im = bodyText.match(/\\[([A-Z][A-Z0-9]{1,7})\\]/);
    if (im) result.ilgili = im[1];

    // Tüm tablolarda veri satırı ara
    const tables = document.querySelectorAll('table');
    for (let t = 0; t < tables.length; t++) {
        const rows = tables[t].querySelectorAll('tr');
        for (let i = rows.length - 1; i >= 0; i--) {
            const cells = Array.from(rows[i].querySelectorAll('td,th'))
                              .map(c => c.innerText.replace(/\\n/g,' ').trim());
            const joined = cells.join(' ');
            if (cells.length >= 5 &&
                /\\d{2}[\\/.]\\d{2}[\\/.]\\d{4}/.test(joined) &&
                /%/.test(joined)) {
                result.data_rows.push(cells);
            }
        }
        if (result.data_rows.length > 0) break;
    }
    return result;
}
"""


def fetch_details_playwright(disclosures: list, log_fn=print) -> list:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log_fn("  ⚠ Playwright kurulu değil — detaylar çekilemiyor")
        return disclosures

    # Streamlit Cloud / CI ortamında chromium binary eksik olabilir — otomatik kur
    import subprocess
    try:
        result = subprocess.run(
            ["playwright", "install", "chromium", "--with-deps"],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode != 0:
            # Sessizce devam et, zaten kurulu olabilir
            pass
    except Exception:
        pass

    log_fn(f"  🌐 Playwright ile {len(disclosures)} detay sayfası çekiliyor...")

    enriched = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            locale="tr-TR",
            user_agent=HEADERS["User-Agent"],
        )
        page = ctx.new_page()

        # Gereksiz kaynakları engelle (hızlanır)
        def block_resource(route):
            if any(route.request.url.endswith(ext)
                   for ext in [".woff",".woff2",".ttf",".gif",".jpg",".jpeg",".png"]):
                route.abort()
            elif any(d in route.request.url for d in ["google-analytics","gtag","market-data"]):
                route.abort()
            else:
                route.continue_()
        page.route("**/*", block_resource)

        for i, disc in enumerate(disclosures, 1):
            row  = dict(disc)
            link = disc.get("link","")
            log_fn(f"  [{i}/{len(disclosures)}] {disc.get('ilgili_sirket','?')} "
                   f"← {disc.get('sirket','')[:35]}")

            if not link:
                enriched.append(row)
                continue

            for attempt in range(2):
                try:
                    page.goto(link, timeout=30000, wait_until="domcontentloaded")
                    page.wait_for_selector("table", timeout=15000)
                    page.wait_for_timeout(1500)

                    res = page.evaluate(_EXTRACT_JS)

                    # Veri satırları
                    if res.get("data_rows"):
                        cells = res["data_rows"][0]  # ilk eşleşen satır
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
                        log_fn(f"    ✓ {g(0)} | Alım:{g(1)} | Sermaye:{g(8)}")
                    else:
                        log_fn(f"    ⚠ Veri satırı bulunamadı")

                    if res.get("fiyat"):
                        row["fiyat"] = res["fiyat"]
                    if res.get("ilgili") and not row.get("ilgili_sirket"):
                        row["ilgili_sirket"] = res["ilgili"]
                    if res.get("aciklama"):
                        row["aciklama"] = res["aciklama"]
                    break

                except Exception as e:
                    if attempt == 0:
                        log_fn(f"    ↻ Tekrar deneniyor...")
                        time.sleep(2)
                    else:
                        log_fn(f"    ✗ {e}")

            enriched.append(row)

        browser.close()

    return enriched


# ─── ANA FONKSİYON ───────────────────────────────────────

def scrape_pay_alim_satim(start_date: date, end_date: date, log_fn=print) -> list:
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

    enriched = fetch_details_playwright(disclosures, log_fn=log_fn)
    log_fn(f"✅ Tamamlandı: {len(enriched)} bildirim")
    return enriched


def get_demo_data(start_date: date, end_date: date) -> list:
    d1, d2 = start_date.strftime("%d.%m.%Y"), end_date.strftime("%d.%m.%Y")
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
         "sirket":"KAMUYU AYDINLATMA PLATFORMU",
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
    out  = sys.argv[3] if len(sys.argv) >= 4 else "."
    data = scrape_pay_alim_satim(start, end)
    fp, df = save_to_excel(data, start, end, out)
    print(f"✓ {len(df)} kayıt → {fp}")
