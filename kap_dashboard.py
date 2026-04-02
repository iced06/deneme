"""
KAP Pay Alım Satım Bildirimleri — Streamlit Dashboard
streamlit run kap_dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date, timedelta
import os
import io

from kap_scraper import scrape_pay_alim_satim, save_to_excel

st.set_page_config(
    page_title="KAP Pay Alım Satım",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
div[data-testid="stMetricValue"] { font-size:1.9rem; font-weight:700; color:#1F4E79; }
div[data-testid="stMetricLabel"] { font-size:0.78rem; color:#666; }
.block-container { padding-top:1.2rem; }
.log-box {
    background:#0e1117; color:#00e676; font-family:monospace;
    font-size:12px; padding:12px 16px; border-radius:8px;
    max-height:200px; overflow-y:auto; line-height:1.7;
    border: 1px solid #1F4E79;
}
</style>
""", unsafe_allow_html=True)

# ── Session state ──────────────────────────────────────────
for k, v in [("df", pd.DataFrame()), ("last_excel", None),
             ("last_bytes", None), ("log", []),
             ("last_start", None), ("last_end", None)]:
    if k not in st.session_state:
        st.session_state[k] = v


def to_num(val):
    """TL/% string → float"""
    if pd.isna(val) or str(val).strip() in ("", "-", "—"):
        return None
    v = str(val).replace("TL","").replace("%","").replace(" ","")
    v = v.replace(".","").replace(",",".")
    try:
        return float(v)
    except Exception:
        return None


def load_excel(path):
    try:
        df = pd.read_excel(path, sheet_name="Pay Alım Satım")
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        st.error(f"Excel okunamadı: {e}")
        return pd.DataFrame()


def list_excels(folder="."):
    files = []
    try:
        for f in sorted(os.listdir(folder), reverse=True):
            if f.startswith("KAP_PayAlimSatim_") and f.endswith(".xlsx"):
                fp   = os.path.join(folder, f)
                mtime = datetime.fromtimestamp(os.path.getmtime(fp)).strftime("%d.%m.%Y %H:%M")
                files.append({"dosya": f, "tarih": mtime, "yol": fp})
    except Exception:
        pass
    return files


# ═══════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## 📊 KAP Takip")
    st.caption("Pay Alım Satım Bildirimleri")
    st.divider()

    # ── Tarih ──
    st.markdown("### 📅 Tarih Aralığı")
    preset = st.radio("", ["Bugün","Son 7 Gün","Son 30 Gün","Bu Ay","Özel"],
                      label_visibility="collapsed")
    today = date.today()
    presets = {
        "Bugün":     (today, today),
        "Son 7 Gün": (today-timedelta(6), today),
        "Son 30 Gün":(today-timedelta(29), today),
        "Bu Ay":     (today.replace(day=1), today),
    }
    if preset == "Özel":
        c1, c2 = st.columns(2)
        start_date = c1.date_input("Başlangıç", today, max_value=today)
        end_date   = c2.date_input("Bitiş",     today, max_value=today)
    else:
        start_date, end_date = presets[preset]
        st.caption(f"📆 {start_date.strftime('%d.%m.%Y')} – {end_date.strftime('%d.%m.%Y')}")

    if start_date > end_date:
        st.error("Başlangıç > Bitiş!")

    # ── Klasör ──
    st.divider()
    st.markdown("### 📁 Kayıt")
    output_dir = st.text_input("Klasör", value=".")

    # ── ANA BUTON ──
    st.divider()
    run_btn = st.button("🚀 Verileri Çek & Excel Oluştur",
                        use_container_width=True, type="primary",
                        disabled=(start_date > end_date))

    # ── Mevcut dosyalar ──
    excels = list_excels(output_dir)
    if excels:
        st.divider()
        st.markdown("### 📂 Mevcut Dosyalar")
        sel = st.selectbox("Yükle", [e["yol"] for e in excels],
                           format_func=lambda p: os.path.basename(p))
        if st.button("📂 Yükle", use_container_width=True):
            df_load = load_excel(sel)
            if not df_load.empty:
                st.session_state.df         = df_load
                st.session_state.last_excel = sel
                with open(sel, "rb") as f:
                    st.session_state.last_bytes = f.read()
                st.success("Yüklendi!")

    # ── Filtreler (veri varsa) ──
    st.divider()
    st.markdown("### 🔍 Filtreler")
    df_raw = st.session_state.df

    if not df_raw.empty:
        # Hisse Kodu filtresi
        kodlar = sorted(df_raw["Hisse Kodu"].dropna().unique().tolist()) \
                 if "Hisse Kodu" in df_raw.columns else []
        sel_kod = st.multiselect("Hisse Kodu", kodlar, default=kodlar,
                                 placeholder="Tümü")

        # İlgili Şirket filtresi
        if "İlgili Şirket" in df_raw.columns:
            ilgili_vals = sorted(
                df_raw["İlgili Şirket"].dropna()
                .apply(lambda x: [s.strip() for s in str(x).split(",")])
                .explode().replace("", pd.NA).dropna().unique().tolist()
            )
            sel_ilgili = st.multiselect("İlgili Şirket", ilgili_vals,
                                        default=ilgili_vals, placeholder="Tümü")
        else:
            sel_ilgili = []

        # Aracı Kurum filtresi
        kurumlar = sorted(df_raw["Aracı Kurum"].dropna().unique().tolist()) \
                   if "Aracı Kurum" in df_raw.columns else []
        sel_kurum = st.multiselect("Aracı Kurum", kurumlar, default=kurumlar,
                                   placeholder="Tümü")
    else:
        sel_kod = sel_ilgili = sel_kurum = []


# ═══════════════════════════════════════════════════════════
# SCRAPER ÇALIŞTIR
# ═══════════════════════════════════════════════════════════
if run_btn:
    st.session_state.log = []
    log_ph = st.empty()
    prog   = st.progress(0, "Başlatılıyor...")

    def log(msg):
        st.session_state.log.append(msg)
        html = "<br>".join(st.session_state.log[-25:])
        log_ph.markdown(f'<div class="log-box">{html}</div>', unsafe_allow_html=True)

    try:
        prog.progress(10, "KAP sorgulanıyor...")
        enriched = scrape_pay_alim_satim(start_date, end_date, log_fn=log)

        prog.progress(75, "Excel oluşturuluyor...")
        fp, df = save_to_excel(enriched, start_date, end_date, output_dir)

        prog.progress(100, "✅ Tamamlandı!")
        st.session_state.df         = df
        st.session_state.last_excel = fp
        st.session_state.last_start = start_date
        st.session_state.last_end   = end_date
        with open(fp, "rb") as f:
            st.session_state.last_bytes = f.read()

        log(f"✅ Excel → {fp}")
        st.success(f"✅ **{len(df)} bildirim** bulundu ve kaydedildi.")
        st.rerun()

    except Exception as e:
        prog.empty()
        st.error(f"Hata: {e}")


# ═══════════════════════════════════════════════════════════
# ANA İÇERİK
# ═══════════════════════════════════════════════════════════
st.title("📊 KAP Pay Alım Satım Bildirimleri")

df = st.session_state.df

if df.empty:
    st.info("👈 Sol panelden tarih seçip **Verileri Çek & Excel Oluştur** butonuna basın.")
    st.stop()

# Başlık bilgisi
if st.session_state.last_start:
    st.caption(
        f"📆 {st.session_state.last_start.strftime('%d.%m.%Y')} – "
        f"{st.session_state.last_end.strftime('%d.%m.%Y')}  |  "
        f"📁 {st.session_state.last_excel or '—'}"
    )

# ── Filtre uygula ──
flt = df.copy()
if sel_kod and "Hisse Kodu" in flt.columns:
    flt = flt[flt["Hisse Kodu"].isin(sel_kod)]
if sel_kurum and "Aracı Kurum" in flt.columns:
    flt = flt[flt["Aracı Kurum"].isin(sel_kurum)]
if sel_ilgili and "İlgili Şirket" in flt.columns:
    mask = flt["İlgili Şirket"].apply(
        lambda x: any(s.strip() in sel_ilgili for s in str(x).split(","))
    )
    flt = flt[mask]

# Sayısal kolonlar
NUM_COLS = {
    "Alım Nominal (TL)":          "_alim",
    "Satım Nominal (TL)":         "_satim",
    "Net Nominal (TL)":           "_net",
    "Gün Sonu Nominal (TL)":      "_gun_sonu",
    "Sermaye Oranı Gün Sonu (%)": "_sermaye",
    "Ort. Fiyat (TL)":            "_fiyat",
}
for col, alias in NUM_COLS.items():
    if col in flt.columns:
        flt[alias] = flt[col].apply(to_num)

# İşlem tarihi parse
if "İşlem Tarihi" in flt.columns:
    flt["_islem_dt"] = pd.to_datetime(
        flt["İşlem Tarihi"].astype(str).str.replace("/", "."),
        format="%d.%m.%Y", errors="coerce"
    )

# ── Metrikler ──
c1,c2,c3,c4,c5 = st.columns(5)
c1.metric("Toplam Bildirim",  len(flt))
c2.metric("Benzersiz Hisse",  flt["Hisse Kodu"].nunique() if "Hisse Kodu" in flt.columns else "—")
c3.metric("İlgili Şirket",
          flt["İlgili Şirket"].dropna().apply(lambda x: [s.strip() for s in str(x).split(",")]).explode().nunique()
          if "İlgili Şirket" in flt.columns else "—")
c4.metric("Toplam Alım",
          f"{flt['_alim'].sum():,.0f} TL" if "_alim" in flt.columns and flt["_alim"].sum() else "—")
c5.metric("Ort. Sermaye Oranı",
          f"%{flt['_sermaye'].mean():.2f}" if "_sermaye" in flt.columns and flt["_sermaye"].notna().any() else "—")

st.divider()

# ═══════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4 = st.tabs([
    "📋 Bildirimler", "📈 Zaman Serisi", "📊 Dağılım", "🔍 Detay"
])


# ── TAB 1: Tablo ──────────────────────────────────────────
with tab1:
    show_cols = [c for c in [
        "No","Yayın Tarihi","Hisse Kodu","Aracı Kurum","Konu","İlgili Şirket",
        "İşlem Tarihi","Ort. Fiyat (TL)","Alım Nominal (TL)","Satım Nominal (TL)",
        "Net Nominal (TL)","Gün Sonu Nominal (TL)",
        "Sermaye Oranı Gün Başı (%)","Sermaye Oranı Gün Sonu (%)",
        "Oy Hakları Gün Sonu (%)","KAP Linki",
    ] if c in flt.columns]

    show_df = flt[show_cols].copy()
    col_cfg = {}
    if "KAP Linki" in show_df.columns:
        col_cfg["KAP Linki"] = st.column_config.LinkColumn("KAP", display_text="🔗")
    if "No" in show_df.columns:
        col_cfg["No"] = st.column_config.NumberColumn("No", width="small")

    st.markdown(f"**{len(flt)} bildirim**")
    st.dataframe(show_df, use_container_width=True, hide_index=True,
                 column_config=col_cfg, height=480)

    # İndir butonları
    dl1, dl2 = st.columns(2)
    if st.session_state.last_bytes:
        dl1.download_button(
            "⬇️ Tam Excel",
            data=st.session_state.last_bytes,
            file_name=os.path.basename(st.session_state.last_excel or "kap.xlsx"),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    buf = io.BytesIO()
    clean = flt.drop(columns=[c for c in flt.columns if c.startswith("_")], errors="ignore")
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        clean.to_excel(w, index=False, sheet_name="Filtreli")
    dl2.download_button(
        "⬇️ Filtrelenmiş Excel",
        data=buf.getvalue(),
        file_name=f"KAP_Filtreli_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )


# ── TAB 2: Zaman Serisi ───────────────────────────────────
with tab2:
    if "_islem_dt" not in flt.columns or flt["_islem_dt"].isna().all():
        st.info("Zaman serisi için İşlem Tarihi verisi gerekli.")
    else:
        ts = flt.dropna(subset=["_islem_dt"]).copy()

        # Şirket seçimi
        ilgili_uniq = sorted(
            ts["İlgili Şirket"].dropna()
            .apply(lambda x: [s.strip() for s in str(x).split(",")])
            .explode().replace("", pd.NA).dropna().unique().tolist()
        ) if "İlgili Şirket" in ts.columns else []

        sel_ts = st.multiselect(
            "Grafiklerde gösterilecek şirketler",
            ilgili_uniq,
            default=ilgili_uniq[:8] if len(ilgili_uniq) > 8 else ilgili_uniq,
            key="ts_sel"
        )

        if sel_ts:
            # Her satırı ilgili şirket bazında explode et
            ts_exp = ts.copy()
            ts_exp["_ilgili"] = ts_exp["İlgili Şirket"].apply(
                lambda x: [s.strip() for s in str(x).split(",")]
            )
            ts_exp = ts_exp.explode("_ilgili")
            ts_exp = ts_exp[ts_exp["_ilgili"].isin(sel_ts)]

            # ── Grafik 1: Günlük Alım/Satım Nominal ──
            st.markdown("#### 📈 Günlük Alım & Satım Nominal (TL)")
            daily = ts_exp.groupby(["_islem_dt", "_ilgili"]).agg(
                alim=("_alim", "sum"),
                satim=("_satim", "sum"),
            ).reset_index()

            if not daily.empty:
                fig1 = make_subplots(specs=[[{"secondary_y": False}]])
                colors_alim  = px.colors.qualitative.Set2
                colors_satim = px.colors.qualitative.Pastel1

                for idx, sirket in enumerate(sel_ts):
                    d = daily[daily["_ilgili"] == sirket]
                    if d.empty:
                        continue
                    c = colors_alim[idx % len(colors_alim)]
                    fig1.add_trace(go.Bar(
                        x=d["_islem_dt"], y=d["alim"],
                        name=f"{sirket} Alım",
                        marker_color=c, opacity=0.85,
                        hovertemplate="%{x|%d.%m.%Y}<br>Alım: %{y:,.0f} TL",
                    ))
                    if d["satim"].sum() > 0:
                        fig1.add_trace(go.Bar(
                            x=d["_islem_dt"], y=-d["satim"],
                            name=f"{sirket} Satım",
                            marker_color=colors_satim[idx % len(colors_satim)],
                            opacity=0.75,
                            hovertemplate="%{x|%d.%m.%Y}<br>Satım: %{y:,.0f} TL",
                        ))

                fig1.update_layout(
                    barmode="relative", height=380,
                    legend=dict(orientation="h", y=-0.2),
                    xaxis_title="İşlem Tarihi",
                    yaxis_title="Nominal Tutar (TL)",
                    hovermode="x unified",
                    margin=dict(t=20, b=60),
                )
                fig1.update_xaxes(tickformat="%d.%m.%Y")
                st.plotly_chart(fig1, use_container_width=True)

            # ── Grafik 2: Sermaye Oranı Zaman Serisi ──
            st.markdown("#### 📊 Sermaye Oranı Değişimi (%)")
            serm = ts_exp.dropna(subset=["_sermaye"]).copy()
            serm = serm.sort_values("_islem_dt")

            if not serm.empty:
                fig2 = go.Figure()
                for idx, sirket in enumerate(sel_ts):
                    d = serm[serm["_ilgili"] == sirket].sort_values("_islem_dt")
                    if d.empty:
                        continue
                    c = colors_alim[idx % len(colors_alim)]
                    fig2.add_trace(go.Scatter(
                        x=d["_islem_dt"], y=d["_sermaye"],
                        name=sirket, mode="lines+markers",
                        line=dict(color=c, width=2),
                        marker=dict(size=7),
                        hovertemplate="%{x|%d.%m.%Y}<br>Sermaye: %{y:.2f}%",
                    ))

                fig2.update_layout(
                    height=340,
                    legend=dict(orientation="h", y=-0.2),
                    xaxis_title="İşlem Tarihi",
                    yaxis_title="Sermaye Oranı (%)",
                    yaxis_ticksuffix="%",
                    hovermode="x unified",
                    margin=dict(t=20, b=60),
                )
                fig2.update_xaxes(tickformat="%d.%m.%Y")
                st.plotly_chart(fig2, use_container_width=True)

            # ── Grafik 3: Kümülatif Net Pozisyon ──
            st.markdown("#### 📉 Kümülatif Net Nominal (TL)")
            net = ts_exp.dropna(subset=["_net"]).sort_values("_islem_dt")

            if not net.empty:
                fig3 = go.Figure()
                for idx, sirket in enumerate(sel_ts):
                    d = net[net["_ilgili"] == sirket].sort_values("_islem_dt")
                    if d.empty:
                        continue
                    d = d.copy()
                    d["_cumnet"] = d["_net"].cumsum()
                    c = colors_alim[idx % len(colors_alim)]
                    fig3.add_trace(go.Scatter(
                        x=d["_islem_dt"], y=d["_cumnet"],
                        name=sirket, mode="lines+markers",
                        line=dict(color=c, width=2),
                        fill="tozeroy", fillcolor=c.replace("rgb","rgba").replace(")",",0.08)") if "rgb" in c else c,
                        hovertemplate="%{x|%d.%m.%Y}<br>Kümülatif Net: %{y:,.0f} TL",
                    ))

                fig3.update_layout(
                    height=320,
                    legend=dict(orientation="h", y=-0.2),
                    xaxis_title="İşlem Tarihi",
                    yaxis_title="Kümülatif Net (TL)",
                    hovermode="x unified",
                    margin=dict(t=20, b=60),
                )
                fig3.update_xaxes(tickformat="%d.%m.%Y")
                st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("Grafik için en az bir şirket seçin.")


# ── TAB 3: Dağılım ───────────────────────────────────────
with tab3:
    if flt.empty:
        st.info("Veri yok.")
    else:
        r1, r2 = st.columns(2)

        with r1:
            if "İlgili Şirket" in flt.columns:
                vc = (flt["İlgili Şirket"]
                      .dropna()
                      .apply(lambda x: [s.strip() for s in str(x).split(",")])
                      .explode()
                      .value_counts()
                      .head(15)
                      .reset_index())
                vc.columns = ["Şirket","Bildirim"]
                fig = px.bar(vc, x="Şirket", y="Bildirim",
                             title="Şirkete Göre Bildirim Sayısı",
                             color="Bildirim", color_continuous_scale="Blues")
                fig.update_layout(height=320, showlegend=False, margin=dict(t=40))
                st.plotly_chart(fig, use_container_width=True)

        with r2:
            if "_sermaye" in flt.columns:
                tmp = flt.dropna(subset=["_sermaye"])
                if "İlgili Şirket" in tmp.columns:
                    tmp2 = tmp.copy()
                    tmp2["_ilgili"] = tmp2["İlgili Şirket"].apply(
                        lambda x: str(x).split(",")[0].strip()
                    )
                    grp = tmp2.groupby("_ilgili")["_sermaye"].last().reset_index()
                    grp.columns = ["Şirket","Sermaye Oranı (%)"]
                    grp = grp.sort_values("Sermaye Oranı (%)", ascending=False).head(15)
                    fig2 = px.bar(grp, x="Şirket", y="Sermaye Oranı (%)",
                                  title="Son Sermaye Oranı (%) — Şirket Bazında",
                                  color="Sermaye Oranı (%)",
                                  color_continuous_scale="RdYlGn")
                    fig2.update_layout(height=320, showlegend=False, margin=dict(t=40))
                    st.plotly_chart(fig2, use_container_width=True)

        # Alım vs Satım scatter
        if "_alim" in flt.columns and "_satim" in flt.columns:
            tmp3 = flt.dropna(subset=["_alim","_satim"]).copy()
            tmp3 = tmp3[(tmp3["_alim"] > 0) | (tmp3["_satim"] > 0)]
            if not tmp3.empty:
                lbl = "İlgili Şirket" if "İlgili Şirket" in tmp3.columns else "Hisse Kodu"
                tmp3["_lbl"] = tmp3[lbl].apply(lambda x: str(x).split(",")[0].strip())
                fig3 = px.scatter(
                    tmp3, x="_alim", y="_satim",
                    color="_lbl", text="_lbl",
                    size_max=20,
                    title="Alım vs Satım Nominal Dağılımı",
                    labels={"_alim":"Alım (TL)","_satim":"Satım (TL)","_lbl":"Şirket"},
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig3.update_traces(textposition="top center", marker=dict(size=10, opacity=0.7))
                fig3.update_layout(height=380, showlegend=False, margin=dict(t=40))
                st.plotly_chart(fig3, use_container_width=True)


# ── TAB 4: Detay ─────────────────────────────────────────
with tab4:
    if flt.empty:
        st.info("Veri yok.")
    else:
        lbl_col = "İlgili Şirket" if "İlgili Şirket" in flt.columns else "Hisse Kodu"
        opts = [
            f"#{r.get('No','?')} | {r.get('Hisse Kodu','')} → {r.get(lbl_col,'')} | {r.get('İşlem Tarihi','')}"
            for _, r in flt.iterrows()
        ]
        idx = st.selectbox("Bildirim Seçin", range(len(opts)), format_func=lambda i: opts[i])
        row = flt.iloc[idx]

        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### 🏢 Kurum Bilgileri")
            for f in ["Aracı Kurum","Hisse Kodu","İlgili Şirket","Yayın Tarihi","Konu","Özet"]:
                if f in row.index and pd.notna(row[f]) and str(row[f]).strip():
                    st.markdown(f"**{f}:** {row[f]}")
        with c2:
            st.markdown("##### 📊 İşlem Detayları")
            for f in [
                "İşlem Tarihi","Ort. Fiyat (TL)",
                "Alım Nominal (TL)","Satım Nominal (TL)","Net Nominal (TL)",
                "Gün Başı Nominal (TL)","Gün Sonu Nominal (TL)",
                "Sermaye Oranı Gün Başı (%)","Oy Hakları Gün Başı (%)",
                "Sermaye Oranı Gün Sonu (%)","Oy Hakları Gün Sonu (%)",
            ]:
                if f in row.index and pd.notna(row[f]) and str(row[f]).strip():
                    st.markdown(f"**{f}:** {row[f]}")

        link = row.get("KAP Linki","")
        if pd.notna(link) and str(link).startswith("http"):
            st.link_button("🔗 KAP'ta Görüntüle", str(link))

# ── Footer ────────────────────────────────────────────────
st.divider()
st.caption(f"Kaynak: [KAP](https://www.kap.org.tr) | {datetime.now().strftime('%d.%m.%Y %H:%M')}")
