#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import yfinance as yf
import requests
import ftplib
import io
import ta as ta
from ta.volatility import BollingerBands
from datetime import date
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error,r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import Lasso
from termcolor import colored
import requests, time, urllib3, xlsxwriter
from datetime import datetime
from tqdm import tqdm
import mplfinance as mpf
import plotly.graph_objects as go
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_auth
import plotly.subplots as ms


# In[2]:


from dash.dependencies import Input,Output


# In[3]:


app =dash.Dash()


# In[4]:


server = app.server


# In[ ]:





# In[5]:


plt.rcParams.update({'figure.max_open_warning': 0})


# In[6]:


today = date.today()
print("Today's date:", today)


# In[ ]:





# In[7]:


IMKB=["AKBNK.IS","ALBRK.IS","GARAN.IS","HALKB.IS","ISCTR.IS","SKBNK.IS","TSKB.IS","ICBCT.IS","KLNMA.IS",
            "VAKBN.IS","YKBNK.IS","AKGRT.IS","ANHYT.IS","ANSGR.IS","AGESA.IS","TURSG.IS","RAYSG.IS","CRDFA.IS","GARFA.IS","ISFIN.IS",
        "LIDFA.IS","SEKFK.IS","ULUFA.IS","VAKFN.IS","A1CAP.IS","GEDIK.IS","GLBMD.IS","INFO.IS","ISMEN.IS","OSMEN.IS",
        "OYYAT.IS","TERA.IS","ALMAD.IS","CVKMD.IS","IPEKE.IS","KOZAL.IS","KOZAA.IS","PRKME.IS","ALCAR.IS","BIENY.IS","BRSAN.IS","CUSAN.IS",
            "DNISI.IS","DOGUB.IS","EGSER.IS","ERBOS.IS","QUAGR.IS","INTEM.IS","KLKIM.IS","KLSER.IS","KLMSN.IS","KUTPO.IS",
            "PNLSN.IS","SAFKR.IS","ERCB.IS","SISE.IS","USAK.IS","YYAPI.IS","AFYON.IS","AKCNS.IS","BTCIM.IS","BSOKE.IS",
            "BOBET.IS","BUCIM.IS","CMBTN.IS","CMENT.IS","CIMSA.IS","GOLTS.IS","KONYA.IS","OYAKC.IS","NIBAS.IS","NUHCM.IS",
            "ARCLK.IS","ARZUM.IS","SILVR.IS","VESBE.IS","VESTL.IS","BMSCH.IS","BMSTL.IS","EREGL.IS","IZMDC.IS","KCAER.IS",
            "KRDMA.IS","KRDMB.IS","KRDMD.IS","TUCLK.IS","YKSLN.IS","AHGAZ.IS","AKENR.IS","AKFYE.IS","AKSEN.IS","AKSUE.IS","ALFAS.IS","ASTOR.IS","ARASE.IS","AYDEM.IS","AYEN.IS",
        "BASGZ.IS","BIOEN.IS","CONSE.IS","CWENE.IS","CANTE.IS","EMKEL.IS","ENJSA.IS","ENERY.IS","ESEN.IS","GWIND.IS",
        "GEREL.IS","HUNER.IS","IZENR.IS","KARYE.IS","NATEN.IS","NTGAZ.IS","MAGEN.IS","ODAS.IS","SMRTG.IS","TATEN.IS",
        "ZEDUR.IS","ZOREN.IS","ATAKP.IS","AVOD.IS","AEFES.IS","BANVT.IS","BYDNR.IS","BIGCH.IS","CCOLA.IS","DARDL.IS","EKIZ.IS","EKSUN.IS","ELITE.IS",
      "ERSU.IS","FADE.IS","FRIGO.IS","GOKNR.IS","KAYSE.IS","KENT.IS","KERVT.IS","KNFRT.IS","KRSTL.IS","KRVGD.IS","KTSKR.IS",
      "MERKO.IS","OFSYM.IS","ORCAY.IS","OYLUM.IS","PENGD.IS","PETUN.IS","PINSU.IS","PNSUT.IS","SELGD.IS","SELVA.IS","SOKE.IS",
      "TBORG.IS","TATGD.IS","TUKAS.IS","ULKER.IS","ULUUN.IS","YYLGD.IS","BIMAS.IS","KIMMR.IS","GMTAS.IS","SOKM.IS","BIZIM.IS","CRFSA.IS","MGROS.IS","AKYHO.IS","ALARK.IS","MARKA.IS","ATSYH.IS","BRYAT.IS","COSMO.IS","DAGHL.IS","DOHOL.IS","DERHL.IS","ECZYT.IS",
         "ENKAI.IS","EUHOL.IS","GLYHO.IS","GLRYH.IS","GSDHO.IS","HEDEF.IS","IEYHO.IS","IHLAS.IS",
         "INVES.IS","KERVN.IS","KLRHO.IS","KCHOL.IS","BERA.IS","MZHLD.IS","MMCAS.IS","METRO.IS","NTHOL.IS","OSTIM.IS",
         "POLHO.IS","RALYH.IS","SAHOL.IS","TAVHL.IS","TKFEN.IS","UFUK.IS","VERUS.IS","AGHOL.IS","YESIL.IS","UNLU.IS","ADESE.IS","AKFGY.IS","AKMGY.IS","AKSGY.IS","ALGYO.IS","ASGYO.IS","ATAGY.IS","AGYO.IS","AVGYO.IS","DAPGM.IS",
     "DZGYO.IS","DGGYO.IS","EDIP.IS","EYGYO.IS","EKGYO.IS","FZLGY.IS","IDGYO.IS","IHLGM.IS","ISGYO.IS",
     "KZBGY.IS","KLGYO.IS","KRGYO.IS","KUYAS.IS","MSGYO.IS","NUGYO.IS","OZKGY.IS","OZGYO.IS","PAGYO.IS","PSGYO.IS",
     "PEKGY.IS","RYGYO.IS","SEGYO.IS","SRVGY.IS","SNGYO.IS","TRGYO.IS","TDGYO.IS","TSGYO.IS","TURGG.IS",
     "VKGYO.IS","YGGYO.IS","YGYO.IS","ZRGYO.IS","ALCTL.IS","ARDYZ.IS","ARENA.IS","INGRM.IS","ASELS.IS","ATATP.IS","AZTEK.IS","DGATE.IS","DESPC.IS","EDATA.IS",
         "FORTE.IS","HTTBT.IS","KFEIN.IS","SDTTR.IS","SMART.IS","ESCOM.IS","FONET.IS","INDES.IS","KAREL.IS","KRONT.IS",
         "LINK.IS","LOGO.IS","MANAS.IS","MTRKS.IS","MIATK.IS","MOBTL.IS","NETAS.IS","OBASE.IS","PENTA.IS","TKNSA.IS",
         "VBTYZ.IS","ARSAN.IS","BLCYT.IS","BRKO.IS","BRMEN.IS","BOSSA.IS","DAGI.IS","DERIM.IS","DESA.IS","DIRIT.IS",
         "EBEBK.IS","ENSRI.IS","HATEK.IS","ISSEN.IS","KRTEK.IS","LUKSK.IS","MNDRS.IS","RUBNS.IS","SKTAS.IS",
         "SNPAM.IS","SUNTK.IS","YATAS.IS","YUNSA.IS","ADEL.IS","ANGEN.IS","ANELE.IS","BNTAS.IS","BRKVY.IS","BRLSM.IS","BURCE.IS","BURVA.IS","BVSAN.IS","CEOEM.IS",
       "DGNMO.IS","EMNIS.IS","EUPWR.IS","ESCAR.IS","FORMT.IS","FLAP.IS","GESAN.IS","GLCVY.IS","GENTS.IS",
       "HKTM.IS","IHEVA.IS","IHAAS.IS","IMASM.IS","KTLEV.IS","KLSYN.IS","KONTR.IS","MACKO.IS","MAVI.IS","MAKIM.IS",
       "MAKTK.IS","MEPET.IS","ORGE.IS","PARSN.IS","TGSAS.IS","PRKAB.IS","PAPIL.IS","PCILT.IS","PKART.IS",
       "PSDTC.IS","SANEL.IS","SNICA.IS","SANKO.IS","SARKY.IS","SNKRN.IS","KUVVA.IS","OZSUB.IS","SONME.IS","SUMAS.IS",
       "SUWEN.IS","TLMAN.IS","ULUSE.IS","VAKKO.IS","YAPRK.IS","YAYLA.IS","YEOTK.IS","AVHOL.IS","BEYAZ.IS","DENGE.IS",
       "IZFAS.IS","IZINV.IS","MEGAP.IS","OZRDN.IS","PASEU.IS","PAMEL.IS","POLTK.IS","RODRG.IS","ASUZU.IS","DOAS.IS","FROTO.IS","KARSN.IS","OTKAR.IS","TOASO.IS","TMSN.IS","TTRAK.IS","BFREN.IS","BRISA.IS",
          "CELHA.IS","CEMAS.IS","CEMTS.IS","DOKTA.IS","DMSAS.IS","DITAS.IS","EGEEN.IS","FMIZP.IS","GOODY.IS","JANTS.IS",
          "KATMR.IS","AYGAZ.IS","CASA.IS","TUPRS.IS","TRCAS.IS","ACSEL.IS","AKSA.IS","ALKIM.IS","BAGFS.IS","BAYRK.IS","BRKSN.IS",
       "DYOBY.IS","EGGUB.IS","EGPRO.IS","EPLAS.IS","EUREN.IS","GUBRF.IS","ISKPL.IS","KMPUR.IS","KOPOL.IS",
       "KORDS.IS","KRPLS.IS","MRSHL.IS","MERCN.IS","PETKM.IS","RNPOL.IS","SANFM.IS","SASA.IS","TARKM.IS","ALKA.IS","BAKAB.IS","BARMA.IS","DURDO.IS","GEDZA.IS","GIPTA.IS","KAPLM.IS","KARTN.IS","KONKA.IS","MNDTR.IS",
         "PRZMA.IS","SAMAT.IS","TEZOL.IS","VKING.IS","HUBVC.IS","GOZDE.IS","HDFGS.IS","ISGSY.IS","PRDGS.IS",
         "VERTU.IS","DOBUR.IS","HURGZ.IS","IHGZT.IS","IHYAY.IS","AYCES.IS","AVTUR.IS","ETILR.IS","MAALT.IS","METUR.IS","PKENT.IS","TEKTU.IS","ULAS.IS","CLEBI.IS","GSDDE.IS",
           "GRSEL.IS","GZNMI.IS","PGSUS.IS","PLTUR.IS","RYSAS.IS","LIDER.IS","TUREX.IS","THYAO.IS","TCELL.IS","TTKOM.IS","DEVA.IS","ECILC.IS","GENIL.IS","MEDTR.IS","MPARK.IS","EGEPO.IS","ONCSM.IS","RTALB.IS","SELEC.IS",
      "TNZTP.IS","TRILC.IS","ATLAS.IS","MTRYO.IS","EUKYO.IS","ETYAT.IS","EUYO.IS","GRNYO.IS","ISYAT.IS","OYAYO.IS",
      "VKFYO.IS"]


# In[8]:


HisseAdet=0
for i in IMKB:
    HisseAdet+=1


# In[9]:


HisseAl=[]
q=0
for a in IMKB:
    q+=1
    Hisse=yf.download(a,
                     start="2023-01-06",
                     end=today,
                     progress=False)
    Hisse.index=pd.to_datetime(Hisse.index)
    Hisse["Return"]=Hisse["Close"].diff()
    Hisse["Return_pct"]=Hisse["Close"].pct_change()
    Hisse["Target_Cls"]=np.where(Hisse.Return>0,1,0)
    Hisse["Vol_diff"]=Hisse["Volume"].diff()
    Hisse["Vol_change"]=Hisse["Volume"].pct_change()
    indicator_bb= BollingerBands(close=Hisse["Close"],window=20,window_dev=2)
    Hisse["bb_bbm"]=indicator_bb.bollinger_mavg()
    Hisse["bb_bbh"]=indicator_bb.bollinger_hband()
    Hisse["bb_bbl"]=indicator_bb.bollinger_lband()
    Hisse["MACD"] = ta.trend.macd(Hisse["Close"], window_slow = 26, window_fast= 12, fillna=False)
    Hisse["MACDS"] = ta.trend.macd_signal(Hisse["Close"], window_sign= 9, fillna=False)
    Hisse["Diff"]=Hisse["MACD"]-Hisse["MACDS"]
    Hisse["Buy_MACD"]=np.where((Hisse["MACD"]>Hisse["MACDS"]),1,0)
    Hisse["Buy_MACDS"]=np.where((Hisse["Buy_MACD"]>Hisse["Buy_MACD"].shift(1)),1,0)
    Hisse["Buy_MACDS2"]=np.where((Hisse["Diff"]>0) & (Hisse["Buy_MACDS"]==1),2,Hisse["Buy_MACDS"])
    Hisse['VSMA15'] = ta.trend.sma_indicator(Hisse['Volume'], window=15)
    
    Hisse['OBV'] = ta.volume.on_balance_volume(Hisse['Close'], Hisse['Volume'])
    Hisse["RSI"]=ta.momentum.rsi(Hisse["Close"],window= 14, fillna= False)
    Hisse["Buy_RSI"]=np.where((Hisse["RSI"]>30),1,0)
    Hisse["Buy_RSIS"]=np.where((Hisse["Buy_RSI"]>Hisse["Buy_RSI"].shift(1)),1,0)
    Hisse["AO"]=ta.momentum.awesome_oscillator(Hisse["High"],Hisse["Low"],window1=5,window2=34,fillna=True)
    Hisse["Buy_AO"]=np.where((Hisse["AO"]>0),1,0)
    Hisse["Buy_AOS"]=np.where((Hisse["Buy_AO"]>Hisse["Buy_AO"].shift(1)),1,0)
    Hisse["CCI"]=ta.trend.cci(Hisse["High"],Hisse["Low"],Hisse["Close"],window=20,fillna=False)
    Hisse["Buy_CCI"]=np.where((Hisse["CCI"]>0),1,0)
    Hisse["Buy_CCIS"]=np.where((Hisse["Buy_CCI"]>Hisse["Buy_CCI"].shift(1)),1,0)
    Hisse["EMA10"]=ta.trend.ema_indicator(Hisse["Close"],window=10,fillna=False)
    Hisse["EMA30"]=ta.trend.ema_indicator(Hisse["Close"],window=30,fillna=False)
    Hisse["Buy_EMA10"]=np.where((Hisse["Close"]>Hisse["EMA10"]),1,0)
    Hisse["Buy_EMA10S"]=np.where((Hisse["Buy_EMA10"]>Hisse["Buy_EMA10"].shift(1)),1,0)
    Hisse["Buy_EMA10_EMA30"]=np.where((Hisse["EMA10"]>Hisse["EMA30"]),1,0)
    Hisse["Buy_EMA10_EMA30S"]=np.where((Hisse["Buy_EMA10_EMA30"]>Hisse["Buy_EMA10_EMA30"].shift(1)),1,0)
    Hisse["Stochastic"]=ta.momentum.stoch_signal(Hisse["High"],Hisse["Low"],Hisse["Close"],window=3,fillna=False)
    Hisse["Stochastic_Buy"]=np.where((Hisse["Stochastic"]>20),1,0)
    Hisse["Stochastic_BuyS"]=np.where((Hisse["Stochastic_Buy"]>Hisse["Stochastic_Buy"].shift(1)),1,0)
    Hisse["KAMA"]=ta.momentum.kama(Hisse["Close"],window=10,pow1=2,pow2=30, fillna=False)
    Hisse["Buy_KAMA"]=np.where((Hisse["Close"]>Hisse["KAMA"]),1,0)
    Hisse["Buy_KAMAS"]=np.where((Hisse["Buy_KAMA"]>Hisse["Buy_KAMA"].shift(1)),1,0)
    Hisse['SMA5'] = ta.trend.sma_indicator(Hisse['Close'], window=5)
    Hisse['SMA22'] = ta.trend.sma_indicator(Hisse['Close'], window=22)
    Hisse['SMA50'] = ta.trend.sma_indicator(Hisse['Close'], window=50)
    Hisse["Buy_SMA5"]=np.where((Hisse["Close"]>Hisse["SMA5"]),1,0)
    Hisse["Buy_SMA22"]=np.where((Hisse["Close"]>Hisse["SMA22"]),1,0)
    Hisse["Buy_SMA50"]=np.where((Hisse["Close"]>Hisse["SMA50"]),1,0)
    Hisse["Buy_SMA5S"]=np.where((Hisse["Buy_SMA5"]>Hisse["Buy_SMA5"].shift(1)),1,0)
    Hisse["Buy_SMA22S"]=np.where((Hisse["Buy_SMA22"]>Hisse["Buy_SMA22"].shift(1)),1,0)
    Hisse["Buy_SMA50S"]=np.where((Hisse["Buy_SMA50"]>Hisse["Buy_SMA50"].shift(1)),1,0)
    Hisse["CMF"]=ta.volume.chaikin_money_flow(Hisse["High"],Hisse["Low"],Hisse["Close"],Hisse["Volume"],window=20,fillna=False)
    Hisse["Buy_CMF"]=np.where((Hisse["CMF"]>0),1,0)
    Hisse["Buy_CMFS"]=np.where((Hisse["Buy_CMF"]>Hisse["Buy_CMF"].shift(1)),1,0)
    HisseDeger=Hisse.tail(1).squeeze()
    f=(HisseDeger["Volume"]/HisseDeger["VSMA15"])
    Score1=HisseDeger["Buy_MACDS2"]+HisseDeger["Buy_AOS"]+HisseDeger["Buy_SMA5S"]+HisseDeger["Buy_SMA22S"]+HisseDeger["Buy_RSIS"]+HisseDeger["Stochastic_BuyS"]+HisseDeger["Buy_CCIS"]+HisseDeger["Buy_KAMAS"]+HisseDeger["Buy_CMFS"]
    Score2=f
    print(a,Score1,Score2)
    if Score1>=3 and Score2 >0.75:
        HisseAl.append(a)
       


# In[10]:


app.layout = html.Div([
    html.H1("📈 Teknik Yükselişte Olan Hisseler", style={"text-align": "center", "color": "#1f77b4"}),

    html.Div([
        html.Label("Hisse Seçiniz", style={"font-weight": "bold", "font-size": "16px"}),
        dcc.Dropdown(id="Stocks",
                    options=[{"label":i,"value":i}for i in HisseAl],
                    value='Stocks')
    ], style={"width": "48%", "margin": "0 auto", "padding": "20px"}),

    html.Div([
        dcc.Graph(id="gauge1", style={"display": "inline-block", "width": "48%", "height": "300px"}, figure=go.Figure(layout={"title": "Indicator Score Gauge"})),
        dcc.Graph(id="gauge2", style={"display": "inline-block", "width": "48%", "height": "300px"}, figure=go.Figure(layout={"title": "Volume Score Gauge"})),
    ], style={"text-align": "center"}),

    html.Div([
        dcc.Graph(id="stock-chart-1", style={"height": "400px"}, config={'displayModeBar': True}, figure=go.Figure(layout={"title": {"text": "Price Candlestick Chart"}})),
        dcc.Graph(id="stock-chart-2", style={"height": "400px"}, config={'displayModeBar': True}, figure=go.Figure(layout={"title": {"text": "Volume Bar Chart"}})),
        dcc.Graph(id="stock-chart-3", style={"height": "400px"}, config={'displayModeBar': True}, figure=go.Figure(layout={"title": {"text": "MACD Analysis"}})),
        dcc.Graph(id="stock-chart-4", style={"height": "400px"}, config={'displayModeBar': True}, figure=go.Figure(layout={"title": {"text": "RSI Chart"}}))
    ])
    
])

# Callback Function
@app.callback(
    [Output("gauge1", "figure"),
     Output("gauge2", "figure"),
     Output("stock-chart-1", "figure"),
     Output("stock-chart-2", "figure"),
     Output("stock-chart-3", "figure"),
     Output("stock-chart-4", "figure")],
    [Input("Stocks", "value")]
)
def update_graph(stock):
    if stock is None:
        empty_fig = go.Figure()
        return empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig

    try:
        # Fetch stock data
        Hisse = yf.download(stock, start="2023-01-01", end=today, progress=False)
        if Hisse.empty:
            print(f"UYARI: Veri gelmiyor, {stock} için grafikler boş olabilir!")
            empty_fig = go.Figure()
            return empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig

        # Process data
        Hisse.index=pd.to_datetime(Hisse.index)
        Hisse["Return"]=Hisse["Close"].diff()
        Hisse["Return_pct"]=Hisse["Close"].pct_change()
        Hisse["Target_Cls"]=np.where(Hisse.Return>0,1,0)
        Hisse["Vol_diff"]=Hisse["Volume"].diff()
        Hisse["Vol_change"]=Hisse["Volume"].pct_change()
        indicator_bb= BollingerBands(close=Hisse["Close"],window=20,window_dev=2)
        Hisse["bb_bbm"]=indicator_bb.bollinger_mavg()
        Hisse["bb_bbh"]=indicator_bb.bollinger_hband()
        Hisse["bb_bbl"]=indicator_bb.bollinger_lband()
        Hisse["MACD"] = ta.trend.macd(Hisse["Close"], window_slow = 26, window_fast= 12, fillna=False)
        Hisse["MACDS"] = ta.trend.macd_signal(Hisse["Close"], window_sign= 9, fillna=False)
        Hisse["Diff"]=Hisse["MACD"]-Hisse["MACDS"]
        Hisse["Buy_MACD"]=np.where((Hisse["MACD"]>Hisse["MACDS"]),1,0)
        Hisse["Buy_MACDS"]=np.where((Hisse["Buy_MACD"]>Hisse["Buy_MACD"].shift(1)),1,0)
        Hisse["Buy_MACDS2"]=np.where((Hisse["Diff"]>0) & (Hisse["Buy_MACDS"]==1),2,Hisse["Buy_MACDS"])
        Hisse['VSMA15'] = ta.trend.sma_indicator(Hisse['Volume'], window=15)
    
        Hisse['OBV'] = ta.volume.on_balance_volume(Hisse['Close'], Hisse['Volume'])
        Hisse["RSI"]=ta.momentum.rsi(Hisse["Close"],window= 14, fillna= False)
        Hisse["Buy_RSI"]=np.where((Hisse["RSI"]>30),1,0)
        Hisse["Buy_RSIS"]=np.where((Hisse["Buy_RSI"]>Hisse["Buy_RSI"].shift(1)),1,0)
        Hisse["AO"]=ta.momentum.awesome_oscillator(Hisse["High"],Hisse["Low"],window1=5,window2=34,fillna=True)
        Hisse["Buy_AO"]=np.where((Hisse["AO"]>0),1,0)
        Hisse["Buy_AOS"]=np.where((Hisse["Buy_AO"]>Hisse["Buy_AO"].shift(1)),1,0)
        Hisse["CCI"]=ta.trend.cci(Hisse["High"],Hisse["Low"],Hisse["Close"],window=20,fillna=False)
        Hisse["Buy_CCI"]=np.where((Hisse["CCI"]>0),1,0)
        Hisse["Buy_CCIS"]=np.where((Hisse["Buy_CCI"]>Hisse["Buy_CCI"].shift(1)),1,0)
        Hisse["CCI"]=ta.trend.cci(Hisse["High"],Hisse["Low"],Hisse["Close"],window=20,fillna=False)
        Hisse["Buy_CCI"]=np.where((Hisse["CCI"]>0),1,0)
        Hisse["Buy_CCIS"]=np.where((Hisse["Buy_CCI"]>Hisse["Buy_CCI"].shift(1)),1,0)
        Hisse["EMA10"]=ta.trend.ema_indicator(Hisse["Close"],window=10,fillna=False)
        Hisse["EMA30"]=ta.trend.ema_indicator(Hisse["Close"],window=30,fillna=False)
        Hisse["Buy_EMA10"]=np.where((Hisse["Close"]>Hisse["EMA10"]),1,0)
        Hisse["Buy_EMA10S"]=np.where((Hisse["Buy_EMA10"]>Hisse["Buy_EMA10"].shift(1)),1,0)
        Hisse["Buy_EMA10_EMA30"]=np.where((Hisse["EMA10"]>Hisse["EMA30"]),1,0)
        Hisse["Buy_EMA10_EMA30S"]=np.where((Hisse["Buy_EMA10_EMA30"]>Hisse["Buy_EMA10_EMA30"].shift(1)),1,0)
        Hisse["Stochastic"]=ta.momentum.stoch_signal(Hisse["High"],Hisse["Low"],Hisse["Close"],window=3,fillna=False)
        Hisse["Stochastic_Buy"]=np.where((Hisse["Stochastic"]>20),1,0)
        Hisse["Stochastic_BuyS"]=np.where((Hisse["Stochastic_Buy"]>Hisse["Stochastic_Buy"].shift(1)),1,0)
        Hisse["KAMA"]=ta.momentum.kama(Hisse["Close"],window=10,pow1=2,pow2=30, fillna=False)
        Hisse["Buy_KAMA"]=np.where((Hisse["Close"]>Hisse["KAMA"]),1,0)
        Hisse["Buy_KAMAS"]=np.where((Hisse["Buy_KAMA"]>Hisse["Buy_KAMA"].shift(1)),1,0)
        Hisse['SMA5'] = ta.trend.sma_indicator(Hisse['Close'], window=5)
        Hisse['SMA22'] = ta.trend.sma_indicator(Hisse['Close'], window=22)
        Hisse['SMA50'] = ta.trend.sma_indicator(Hisse['Close'], window=50)
        Hisse["Buy_SMA5"]=np.where((Hisse["Close"]>Hisse["SMA5"]),1,0)
        Hisse["Buy_SMA22"]=np.where((Hisse["Close"]>Hisse["SMA22"]),1,0)
        Hisse["Buy_SMA50"]=np.where((Hisse["Close"]>Hisse["SMA50"]),1,0)
        Hisse["Buy_SMA5S"]=np.where((Hisse["Buy_SMA5"]>Hisse["Buy_SMA5"].shift(1)),1,0)
        Hisse["Buy_SMA22S"]=np.where((Hisse["Buy_SMA22"]>Hisse["Buy_SMA22"].shift(1)),1,0)
        Hisse["Buy_SMA50S"]=np.where((Hisse["Buy_SMA50"]>Hisse["Buy_SMA50"].shift(1)),1,0)
        Hisse["CMF"]=ta.volume.chaikin_money_flow(Hisse["High"],Hisse["Low"],Hisse["Close"],Hisse["Volume"],window=20,fillna=False)
        Hisse["Buy_CMF"]=np.where((Hisse["CMF"]>0),1,0)
        Hisse["Buy_CMFS"]=np.where((Hisse["Buy_CMF"]>Hisse["Buy_CMF"].shift(1)),1,0)
        Hisse["Target_Close_Next_3Day"]=round((Hisse["Target_Cls"].shift(-1)+Hisse["Target_Cls"].shift(-2)+Hisse["Target_Cls"].shift(-3))/3,0)
        Hisse["Target_Close_Next_3Day"].fillna(1,inplace=True)
        Hisse["Return_pct_next_day"]=Hisse["Return_pct"].shift(-1)
        Hisse["Return_pct_next_day"].fillna(Hisse["Return_pct_next_day"].mean(),inplace=True)
        HisseDeger=Hisse.tail(1).squeeze()
        f=(HisseDeger["Volume"]/HisseDeger["VSMA15"])
        Score1=HisseDeger["Buy_MACDS2"]+HisseDeger["Buy_AOS"]+HisseDeger["Buy_EMA10_EMA30S"]+HisseDeger["Buy_SMA5S"]+HisseDeger["Buy_SMA22S"]+HisseDeger["Buy_RSIS"]+HisseDeger["Stochastic_BuyS"]+HisseDeger["Buy_CCIS"]+HisseDeger["Buy_KAMAS"]+HisseDeger["Buy_CMFS"]
        Score2=f

        # Create charts
        # Gauge Charts
        gauge1 = go.Figure(go.Indicator(
            mode="gauge+number",
            value=Score1,
            title={'text': "Indıcator Score"},
            gauge={'axis': {'range': [None, 5]}, 'bar': {'color': "#1f77b4"}}
        ))

        gauge2 = go.Figure(go.Indicator(
            mode="gauge+number",
            value=Score2,
            title={'text': "Volume Score"},
            gauge={'axis': {'range': [None, 5]}, 'bar': {'color': "#ff7f0e"}}
        ))

        # Stock Charts
        stock_chart_1 = go.Figure(go.Candlestick(
            x=Hisse.index, open=Hisse['Open'], high=Hisse['High'],
            low=Hisse['Low'], close=Hisse['Close'], name='Price'))
        stock_chart_1.update_layout(
            title={
            'text': "Candlestick Chart for Price Movement",
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'
            },
        title_font=dict(size=20, color='black')
        )

        stock_chart_2 = go.Figure(go.Bar(
            x=Hisse.index, y=Hisse['Volume'], name='Volume'))
        
        stock_chart_2.update_layout(
            title={
            'text': "Volume Bar Chart",
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'
            },
        title_font=dict(size=20, color='black')
        )

        stock_chart_3 = go.Figure()
        stock_chart_3.add_trace(go.Scatter(x=Hisse.index, y=Hisse['MACD'], name='MACD'))
        stock_chart_3.add_trace(go.Scatter(x=Hisse.index, y=Hisse['MACDS'], name='Signal Line'))
        
        stock_chart_3.update_layout(
            title={
            'text': "MACD Indicator Chart",
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'
            },
        title_font=dict(size=20, color='black')
        )
        

        stock_chart_4 = go.Figure(go.Scatter(
            x=Hisse.index, y=Hisse['RSI'], name='RSI'))
        
        stock_chart_4.update_layout(
            title={
            'text': "RSI Indicator Chart",
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'
            },
        title_font=dict(size=20, color='black')
        )

        return gauge1, gauge2, stock_chart_1, stock_chart_2, stock_chart_3, stock_chart_4

    except Exception as e:
        print(f"HATA: {e}")
        empty_fig = go.Figure()
        return empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig
        
        
    
        
        
       
        
    
        

    


# In[ ]:


if __name__== "__main__":
    app.run_server(port=8058)


# In[ ]:





# In[ ]:





# In[ ]:




