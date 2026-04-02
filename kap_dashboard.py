"""
KAP Pay Alım Satım Bildirimleri — Streamlit Dashboard
Çalıştır: streamlit run kap_dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, date, timedelta
import os
import io

from kap_scraper import scrape_pay_alim_satim, save_to_excel, COLUMNS_MAP

# ─── SAYFA AYARLARI ──────────────────────────────────────
st.set_page_config(
    page_title="KAP Pay Alım Satım Takip",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
div[data-testid="stMetricValue"] { font-size: 1.9rem; font-weight: 700; color: #1F4E79; }
div[data-testid="stMetricLabel"] { font-size: 0.8rem; color: #666; }
.stDataFrame { font-size: 13px; }
.block-container { padding-top: 1.5rem; }
.log-box {
    background: #0e1117; color: #00ff88; font-family: monospace;
    font-size: 12px; padding: 12px 16px; border-radius: 8px;
    max-height: 220px; overflow-y: auto; line-height: 1.6;
}
</style>
""", unsafe_allow_html=True)

# ─── SESSION STATE ────────────────────────────────────────
if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame()
if "last_excel" not in st.session_state:
    st.session_state.last_excel = None
if "last_excel_bytes" not in st.session_state:
    st.session_state.last_excel_bytes = None
if "log_lines" not in st.session_state:
    st.session_state.log_lines = []
if "last_start" not in st.session_state:
    st.session_state.last_start = None
if "last_end" not in st.session_state:
    st.session_state.last_end = None


def clean_numeric(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    v = str(val).replace("TL", "").replace("%", "").replace(" ", "")
    v = v.replace(".", "").replace(",", ".")
    try:
        return float(v)
    except Exception:
        return None


def load_existing_excels(folder="."):
    """Klasördeki KAP Excel dosyalarını listeler."""
    files = []
    try:
        for f in os.listdir(folder):
            if f.startswith("KAP_PayAlimSatim_") and f.endswith(".xlsx"):
                fp = os.path.join(folder, f)
                mtime = datetime.fromtimestamp(os.path.getmtime(fp)).strftime("%d.%m.%Y %H:%M")
                size_kb = os.path.getsize(fp) // 1024
                files.append({"Dosya": f, "Son Değişiklik": mtime, "Boyut": f"{size_kb} KB", "yol": fp})
    except Exception:
        pass
    return files


# ─── SIDEBAR ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📊 KAP Takip Paneli")
    st.caption("Pay Alım Satım Bildirimleri")
    st.divider()

    # ── Tarih seçimi ──
    st.markdown("### 📅 Tarih Aralığı")

    preset = st.radio(
        "Hızlı seçim",
        ["Bugün", "Son 7 Gün", "Son 30 Gün", "Bu Ay", "Özel"],
        horizontal=False,
        label_visibility="collapsed",
    )

    today = date.today()
    if preset == "Bugün":
        default_start, default_end = today, today
    elif preset == "Son 7 Gün":
        default_start, default_end = today - timedelta(days=6), today
    elif preset == "Son 30 Gün":
        default_start, default_end = today - timedelta(days=29), today
    elif preset == "Bu Ay":
        default_start = today.replace(day=1)
        default_end   = today
    else:
        default_start, default_end = today, today

    if preset == "Özel":
        col_s, col_e = st.columns(2)
        with col_s:
            start_date = st.date_input("Başlangıç", value=default_start, max_value=today, key="d_start")
        with col_e:
            end_date = st.date_input("Bitiş", value=default_end, max_value=today, key="d_end")
    else:
        start_date = default_start
        end_date   = default_end
        st.caption(f"📆 {start_date.strftime('%d.%m.%Y')} – {end_date.strftime('%d.%m.%Y')}")

    if start_date > end_date:
        st.error("Başlangıç tarihi bitiş tarihinden büyük olamaz.")

    # ── Çıktı klasörü ──
    st.divider()
    st.markdown("### 📁 Kayıt Ayarları")
    output_dir = st.text_input("Excel Kayıt Klasörü", value=".", help="Dosyanın kaydedileceği klasör yolu")

    # ── ANA BUTON: Scraper'ı çalıştır ──
    st.divider()
    run_btn = st.button(
        "🚀 Verileri Çek & Excel Oluştur",
        use_container_width=True,
        type="primary",
        disabled=(start_date > end_date),
    )

    st.divider()
    st.markdown("### 🔍 Tablo Filtreleri")


# ─── SCRAPER ÇALIŞTIRMA ──────────────────────────────────
if run_btn:
    st.session_state.log_lines = []
    log_placeholder = st.empty()

    def log_fn(msg: str):
        st.session_state.log_lines.append(msg)
        log_html = "<br>".join(st.session_state.log_lines[-30:])
        log_placeholder.markdown(f'<div class="log-box">{log_html}</div>', unsafe_allow_html=True)

    progress = st.progress(0, text="Başlatılıyor...")

    try:
        progress.progress(10, "KAP sorgulanıyor...")
        enriched = scrape_pay_alim_satim(start_date, end_date, log_fn=log_fn)

        progress.progress(70, "Excel oluşturuluyor...")
        filepath, df = save_to_excel(enriched, start_date, end_date, output_dir)

        progress.progress(90, "Dashboard yükleniyor...")
        st.session_state.df         = df
        st.session_state.last_excel = filepath
        st.session_state.last_start = start_date
        st.session_state.last_end   = end_date

        with open(filepath, "rb") as f:
            st.session_state.last_excel_bytes = f.read()

        progress.progress(100, "✅ Tamamlandı!")
        log_fn(f"✅ Excel kaydedildi → {filepath}")

        st.success(f"✅ **{len(df)} bildirim** bulundu ve Excel'e kaydedildi.")

    except Exception as e:
        progress.empty()
        st.error(f"Hata: {e}")


# ─── MEVCUT EXCEL YÜKLE ──────────────────────────────────
with st.sidebar:
    existing = load_existing_excels(output_dir if output_dir else ".")
    if existing:
        st.markdown("### 📂 Mevcut Dosyalar")
        sel_file = st.selectbox(
            "Yükle",
            options=[e["yol"] for e in existing],
            format_func=lambda p: os.path.basename(p),
            key="file_sel",
        )
        if st.button("📂 Seçili Dosyayı Yükle", use_container_width=True):
            try:
                df_load = pd.read_excel(sel_file, sheet_name="Pay Alım Satım")
                st.session_state.df         = df_load
                st.session_state.last_excel = sel_file
                with open(sel_file, "rb") as f:
                    st.session_state.last_excel_bytes = f.read()
                st.success("Yüklendi!")
            except Exception as e:
                st.error(f"Yükleme hatası: {e}")


# ─── ANA İÇERİK ──────────────────────────────────────────
st.title("📊 KAP Pay Alım Satım Bildirimleri")

df = st.session_state.df

if df.empty:
    st.info("👈 Sol panelden tarih aralığı seçip **Verileri Çek & Excel Oluştur** butonuna basın.")
    
    with st.expander("ℹ️ Nasıl Çalışır?", expanded=True):
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("""
            **Adımlar:**
            1. Sol panelden tarih aralığını seçin
            2. Excel kayıt klasörünü belirtin (varsayılan: mevcut klasör)
            3. **Verileri Çek & Excel Oluştur** butonuna basın
            4. Dashboard otomatik güncellenir
            
            **CLI kullanımı:**
            ```bash
            python kap_scraper.py 01.03.2026 02.04.2026
            python kap_scraper.py 01.03.2026 02.04.2026 ./cikti
            ```
            """)
        with c2:
            st.markdown("""
            **Oluşturulan Excel:**
            - Dosya adı: `KAP_PayAlimSatim_YYYYMMDD_YYYYMMDD.xlsx`
            - 2 sayfa: Detay tablosu + Özet
            - Renk kodlu, dondurulmuş başlık
            - KAP linkleri ile
            
            **Çekilen Veriler:**
            - Aracı kurum & hisse bilgileri
            - İşlem tarihi & fiyat
            - Alım/satım nominal tutarlar
            - Sermaye & oy hakları oranları
            """)
    st.stop()

df.columns = df.columns.str.strip()

# ─── Sidebar Filtreleri ───────────────────────────────────
with st.sidebar:
    all_kodlar = sorted(df["Hisse Kodu"].dropna().unique().tolist()) if "Hisse Kodu" in df.columns else []
    sel_kodlar = st.multiselect("Hisse Kodu", all_kodlar, default=all_kodlar)

    all_konular = sorted(df["Konu"].dropna().unique().tolist()) if "Konu" in df.columns else []
    sel_konular = st.multiselect("Konu", all_konular, default=all_konular)

    if "İlgili Şirket" in df.columns:
        all_ilgili = sorted(df["İlgili Şirket"].dropna().replace("", pd.NA).dropna().unique().tolist())
        if all_ilgili:
            sel_ilgili = st.multiselect("İlgili Şirket", all_ilgili, default=all_ilgili)
        else:
            sel_ilgili = []
    else:
        sel_ilgili = []

flt = df.copy()
if sel_kodlar and "Hisse Kodu" in flt.columns:
    flt = flt[flt["Hisse Kodu"].isin(sel_kodlar)]
if sel_konular and "Konu" in flt.columns:
    flt = flt[flt["Konu"].isin(sel_konular)]
if sel_ilgili and "İlgili Şirket" in flt.columns:
    flt = flt[flt["İlgili Şirket"].isin(sel_ilgili)]

# ─── Başlık Bilgisi ──────────────────────────────────────
if st.session_state.last_start and st.session_state.last_end:
    st.caption(
        f"📆 {st.session_state.last_start.strftime('%d.%m.%Y')} – "
        f"{st.session_state.last_end.strftime('%d.%m.%Y')}  |  "
        f"📁 {st.session_state.last_excel or '—'}"
    )

# ─── METRİKLER ───────────────────────────────────────────
flt["_sonu_n"] = flt["Gün Sonu Nominal (TL)"].apply(clean_numeric) if "Gün Sonu Nominal (TL)" in flt.columns else 0
flt["_serm_n"] = flt["Sermaye Oranı Gün Sonu (%)"].apply(clean_numeric) if "Sermaye Oranı Gün Sonu (%)" in flt.columns else 0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Toplam Bildirim",   len(flt))
c2.metric("Benzersiz Hisse",   flt["Hisse Kodu"].nunique() if "Hisse Kodu" in flt.columns else "—")
c3.metric("İlgili Şirket",     flt["İlgili Şirket"].nunique() if "İlgili Şirket" in flt.columns else "—")
total_sonu = flt["_sonu_n"].sum()
c4.metric("Toplam Gün Sonu",   f"{total_sonu:,.0f} TL" if total_sonu else "—")
avg_serm   = flt["_serm_n"].mean()
c5.metric("Ort. Sermaye Oranı", f"%{avg_serm:.2f}" if avg_serm else "—")

st.divider()

# ─── TABS ────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📋 Bildirimler", "📊 Grafikler", "🔍 Detay", "📁 Dosya Yönetimi"
])

# ── TAB 1: Tablo ─────────────────────────────────────────
with tab1:
    st.markdown(f"**{len(flt)} bildirim** gösteriliyor")

    display_cols = [c for c in [
        "No", "Yayın Tarihi", "Hisse Kodu", "Aracı Kurum", "Konu",
        "İlgili Şirket", "İşlem Tarihi", "Ort. Fiyat (TL)",
        "Alım Nominal (TL)", "Satım Nominal (TL)", "Net Nominal (TL)",
        "Sermaye Oranı Gün Sonu (%)", "Oy Hakları Gün Sonu (%)", "KAP Linki",
    ] if c in flt.columns]

    show_df = flt[display_cols].copy() if display_cols else flt.copy()

    col_cfg = {}
    if "KAP Linki" in show_df.columns:
        col_cfg["KAP Linki"] = st.column_config.LinkColumn("KAP Linki", display_text="🔗 Görüntüle")
    if "No" in show_df.columns:
        col_cfg["No"] = st.column_config.NumberColumn("No", width="small")
    if "Hisse Kodu" in show_df.columns:
        col_cfg["Hisse Kodu"] = st.column_config.TextColumn("Hisse", width="small")

    st.dataframe(show_df, use_container_width=True, hide_index=True, column_config=col_cfg, height=480)

    # ── Excel İndir ──
    st.markdown("#### ⬇️ Excel Dosyası")
    col_dl1, col_dl2 = st.columns([1, 1])

    with col_dl1:
        # Mevcut tam Excel dosyasını indir
        if st.session_state.last_excel_bytes:
            fname = os.path.basename(st.session_state.last_excel) if st.session_state.last_excel else "kap_export.xlsx"
            st.download_button(
                "⬇️ Son Oluşturulan Excel'i İndir",
                data=st.session_state.last_excel_bytes,
                file_name=fname,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

    with col_dl2:
        # Filtrelenmiş veriyi yeni Excel olarak indir
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            flt.drop(columns=[c for c in ["_sonu_n", "_serm_n"] if c in flt.columns]).to_excel(
                writer, index=False, sheet_name="Filtrelenmiş"
            )
        st.download_button(
            "⬇️ Filtrelenmiş Veriyi İndir",
            data=buf.getvalue(),
            file_name=f"KAP_Filtreli_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )


# ── TAB 2: Grafikler ─────────────────────────────────────
with tab2:
    if flt.empty:
        st.info("Grafik için veri yok.")
    else:
        r1c1, r1c2 = st.columns(2)

        with r1c1:
            if "Hisse Kodu" in flt.columns:
                vc = flt["Hisse Kodu"].value_counts().reset_index()
                vc.columns = ["Hisse Kodu", "Sayı"]
                fig = px.bar(vc.head(15), x="Hisse Kodu", y="Sayı",
                             title="Hisse Koduna Göre Bildirim Sayısı",
                             color="Sayı", color_continuous_scale="Blues")
                fig.update_layout(showlegend=False, height=340, margin=dict(t=40))
                st.plotly_chart(fig, use_container_width=True)

        with r1c2:
            if "Konu" in flt.columns:
                vc2 = flt["Konu"].value_counts().reset_index()
                vc2.columns = ["Konu", "Sayı"]
                fig2 = px.pie(vc2, values="Sayı", names="Konu",
                              title="Konu Dağılımı",
                              color_discrete_sequence=px.colors.sequential.Blues_r)
                fig2.update_layout(height=340, margin=dict(t=40))
                st.plotly_chart(fig2, use_container_width=True)

        # Sermaye oranı
        if "Sermaye Oranı Gün Sonu (%)" in flt.columns:
            tmp = flt[["İlgili Şirket", "Hisse Kodu", "_serm_n"]].dropna(subset=["_serm_n"])
            tmp = tmp[tmp["_serm_n"] > 0]
            if not tmp.empty:
                lbl = "İlgili Şirket" if "İlgili Şirket" in tmp.columns else "Hisse Kodu"
                fig3 = px.bar(
                    tmp.sort_values("_serm_n", ascending=False),
                    x=lbl, y="_serm_n",
                    title="Gün Sonu Sermaye Oranı (%)",
                    color="_serm_n", color_continuous_scale="RdYlGn",
                    labels={"_serm_n": "Sermaye Oranı (%)"},
                )
                fig3.update_layout(height=340, margin=dict(t=40))
                st.plotly_chart(fig3, use_container_width=True)

        # Alım vs Satım scatter
        if all(c in flt.columns for c in ["Alım Nominal (TL)", "Satım Nominal (TL)"]):
            tmp2 = flt.copy()
            tmp2["_alim"] = tmp2["Alım Nominal (TL)"].apply(clean_numeric)
            tmp2["_satim"] = tmp2["Satım Nominal (TL)"].apply(clean_numeric)
            tmp2 = tmp2.dropna(subset=["_alim", "_satim"])
            if not tmp2.empty:
                lbl2 = "İlgili Şirket" if "İlgili Şirket" in tmp2.columns else "Hisse Kodu"
                fig4 = px.scatter(
                    tmp2, x="_alim", y="_satim", text=lbl2,
                    title="Alım vs Satım Nominal Karşılaştırması",
                    labels={"_alim": "Alım Nominal", "_satim": "Satım Nominal"},
                    color="_serm_n", color_continuous_scale="Blues",
                )
                fig4.update_traces(textposition="top center")
                fig4.update_layout(height=380, margin=dict(t=40))
                st.plotly_chart(fig4, use_container_width=True)


# ── TAB 3: Detay ─────────────────────────────────────────
with tab3:
    if flt.empty:
        st.info("Veri yok.")
    else:
        lbl_col = "İlgili Şirket" if "İlgili Şirket" in flt.columns else "Hisse Kodu"
        options = [
            f"#{r.get('No','?')} | {r.get('Hisse Kodu','')} | {r.get(lbl_col,'')} | {r.get('Konu','')[:50]}"
            for _, r in flt.iterrows()
        ]
        idx = st.selectbox("Bildirim Seçin", range(len(options)), format_func=lambda i: options[i])

        row = flt.iloc[idx]

        d1, d2 = st.columns(2)
        with d1:
            st.markdown("##### 🏢 Kurum Bilgileri")
            for f in ["Aracı Kurum", "Hisse Kodu", "İlgili Şirket", "Yayın Tarihi", "Konu", "Özet"]:
                if f in row.index and pd.notna(row[f]) and str(row[f]).strip():
                    st.markdown(f"**{f}:** {row[f]}")

        with d2:
            st.markdown("##### 📊 İşlem Detayları")
            for f in [
                "İşlem Tarihi", "Ort. Fiyat (TL)",
                "Alım Nominal (TL)", "Satım Nominal (TL)", "Net Nominal (TL)",
                "Gün Başı Nominal (TL)", "Gün Sonu Nominal (TL)",
                "Sermaye Oranı Gün Başı (%)", "Sermaye Oranı Gün Sonu (%)",
                "Oy Hakları Gün Sonu (%)",
            ]:
                if f in row.index and pd.notna(row[f]) and str(row[f]).strip():
                    st.markdown(f"**{f}:** {row[f]}")

        link = row.get("KAP Linki", "")
        if pd.notna(link) and str(link).startswith("http"):
            st.link_button("🔗 KAP'ta Görüntüle", str(link))


# ── TAB 4: Dosya Yönetimi ────────────────────────────────
with tab4:
    st.markdown("### 📂 Oluşturulmuş Excel Dosyaları")

    scan_dir = output_dir if output_dir and os.path.isdir(output_dir) else "."
    files = load_existing_excels(scan_dir)

    if not files:
        st.info(f"'{scan_dir}' klasöründe henüz KAP dosyası yok.")
    else:
        files_df = pd.DataFrame([{k: v for k, v in f.items() if k != "yol"} for f in files])
        st.dataframe(files_df, use_container_width=True, hide_index=True)

        st.markdown("#### Dosya İndir")
        sel = st.selectbox("Dosya seçin", [f["yol"] for f in files],
                           format_func=lambda p: os.path.basename(p), key="dl_sel")
        if sel and os.path.exists(sel):
            with open(sel, "rb") as f:
                st.download_button(
                    f"⬇️ {os.path.basename(sel)}",
                    data=f.read(),
                    file_name=os.path.basename(sel),
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )

    st.divider()
    st.markdown("### 🗑️ Oluştur / Temizle")
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("**Yeni Excel Oluştur (mevcut filtreyle)**")
        custom_name = st.text_input("Dosya Adı (isteğe bağlı)", placeholder="bos bırakırsanız otomatik adlandırılır")
        if st.button("💾 Dosya Oluştur", use_container_width=True):
            if st.session_state.df.empty:
                st.warning("Önce veri çekin.")
            else:
                start_f = st.session_state.last_start or date.today()
                end_f   = st.session_state.last_end or date.today()
                clean_flt = flt.drop(columns=[c for c in ["_sonu_n","_serm_n"] if c in flt.columns])
                
                from openpyxl import load_workbook as lw
                from openpyxl.styles import Font as OFont, PatternFill as OFill, Alignment as OAlign
                
                auto_name = custom_name.strip() or f"KAP_PayAlimSatim_{start_f.strftime('%Y%m%d')}_{end_f.strftime('%Y%m%d')}_filtreli.xlsx"
                if not auto_name.endswith(".xlsx"):
                    auto_name += ".xlsx"
                out_path = os.path.join(scan_dir, auto_name)
                
                clean_flt.to_excel(out_path, index=False, sheet_name="Pay Alım Satım")
                st.success(f"✅ Oluşturuldu: {out_path}")
                st.rerun()

    with col_b:
        st.markdown("**Eski Dosyaları Temizle**")
        if files:
            del_file = st.selectbox("Silinecek dosya", [f["yol"] for f in files],
                                    format_func=lambda p: os.path.basename(p), key="del_sel")
            if st.button("🗑️ Seçili Dosyayı Sil", use_container_width=True, type="secondary"):
                try:
                    os.remove(del_file)
                    st.success(f"Silindi: {os.path.basename(del_file)}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Silinemedi: {e}")


# ─── FOOTER ──────────────────────────────────────────────
st.divider()
st.caption(
    f"Kaynak: [KAP - Kamuyu Aydınlatma Platformu](https://www.kap.org.tr) | "
    f"Yenileme: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
)
