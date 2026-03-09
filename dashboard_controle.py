"""
dashboard_controle.py — Dashboard em Streamlit para controle de trocas de HD.
"""

import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import os
import sys

# Adiciona o diretório execution ao path para importar o módulo de processamento
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "execution"))

from processar_dados import carregar_ligacoes, carregar_leitura, carregar_os, cruzar_dados, detectar_duplicatas_os, processar_tudo

@st.cache_data
def carregar_dados_iniciais(arq_lig, arq_leit, arq_os):
    with st.spinner("Processando planilhas..."):
        return processar_tudo(arq_lig, arq_leit, arq_os)

st.title("💧 Controle de Trocas de Hidrômetros por Telemetria (Marialva)")
st.markdown("Acompanhamento do status das ordens de serviço e identificação de pendências.")

# --- BARRA LATERAL (UPLOAD E FILTROS) ---
with st.sidebar:
    st.header("📂 Atualizar Dados")
    st.markdown("Faça o upload das planilhas mais recentes para atualizar os números.")
    
    upload_lig = st.file_uploader("1️⃣ Planilha de Cadastro (Ligações.csv)", type=["csv"])
    upload_leit = st.file_uploader("2️⃣ Planilha de Transmissão (Leitura atual.xlsx)", type=["xlsx"])
    upload_os = st.file_uploader("3️⃣ Planilha de Serviços (Lista de OS.xlsx)", type=["xlsx"])
    
    st.markdown("---")
    st.header("⚙️ Painel de Filtros")

# Verifica se os arquivos foram subidos, se não, usa os locais (para ambiente de dev)
if upload_lig and upload_leit and upload_os:
    df_result, df_dup, df_leit, df_lig_dups, df_os = carregar_dados_iniciais(upload_lig, upload_leit, upload_os)
else:
    # Fallback silencioso para arquivos locais
    try:
        df_result, df_dup, df_leit, df_lig_dups, df_os = carregar_dados_iniciais(None, None, None)
    except FileNotFoundError:
        st.warning("⚠️ Aguardando upload das 3 planilhas na barra lateral para iniciar o processamento.")
        st.stop()

    st.markdown("---")
    
    # Busca Direta
    st.subheader("Buscador")
    busca_mat = st.text_input("Buscar Matrícula (Ligação):", placeholder="Ex: 1281")
    
    st.markdown("---")
    st.subheader("Filtros Gerais")

    # Filtro de Grupo (Cadastro)
    grupos_disponiveis = sorted([g for g in df_result["Grupo - Nome"].unique() if pd.notna(g)])
    grupos_selec = st.multiselect("Grupo Cadastro:", grupos_disponiveis, default=[])

    # Filtro de Rota
    rotas_disponiveis = sorted(df_result["Rota"].dropna().unique().tolist())
    rotas_selecionadas = st.multiselect("Selecione a Rota:", rotas_disponiveis, default=[])

    # Filtro de Tipo de OS
    tipos_os = sorted([t for t in df_os["TIPO DE SERVIÇO"].unique() if pd.notna(t)])
    tipos_selec = st.multiselect("Tipo de OS:", tipos_os, default=[])

    # Filtro de Status da OS
    status_os_disp = sorted([s for s in df_os["Status"].unique() if pd.notna(s)])
    status_os_selec = st.multiselect("Status da OS:", status_os_disp, default=[])

    st.markdown("---")
    # Filtro: Apenas com telemetria ativa
    apenas_telemetria = st.toggle("Apenas transmissão ativa (Leitura Atual)", value=False)


# --- APLICAÇÃO DOS FILTROS ---
df_filtrado = df_result.copy()

# Busca por Matrícula específica
if busca_mat.strip():
    if busca_mat.isdigit():
        df_filtrado = df_filtrado[df_filtrado["Matrícula"] == int(busca_mat)]
    else:
        st.sidebar.error("A matrícula deve conter apenas números.")

if grupos_selec:
    df_filtrado = df_filtrado[df_filtrado["Grupo - Nome"].isin(grupos_selec)]

if rotas_selecionadas:
    df_filtrado = df_filtrado[df_filtrado["Rota"].isin(rotas_selecionadas)]

# Para filtrar por Tipo ou Status da OS, precisamos cruzar os filtros com a tabela de OS real
if tipos_selec or status_os_selec:
    df_os_filtro = df_os.copy()
    if tipos_selec:
        df_os_filtro = df_os_filtro[df_os_filtro["TIPO DE SERVIÇO"].isin(tipos_selec)]
    if status_os_selec:
        df_os_filtro = df_os_filtro[df_os_filtro["Status"].isin(status_os_selec)]
    
    mats_validas_os = set(df_os_filtro["Matrícula_OS"].dropna())
    df_filtrado = df_filtrado[df_filtrado["Matrícula"].isin(mats_validas_os)]

if apenas_telemetria:
    df_filtrado = df_filtrado[df_filtrado["Tem_Telemetria"] == True]


# --- ESTÉTICA E CARDS (KPIs) ---
st.markdown("""
<style>
    div[data-testid="metric-container"] {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 15px;
        border-left: 5px solid #0068c9;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.05);
    }
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 0px;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #6b7280;
        margin-bottom: 30px;
    }
</style>
""", unsafe_allow_html=True)

total_filtrado = len(df_filtrado)
concluidas = len(df_filtrado[df_filtrado["Classificação"] == "Troca Concluída"])
pendentes = len(df_filtrado[df_filtrado["Classificação"] == "Pendente de OS"])
andamento = len(df_filtrado[df_filtrado["Classificação"] == "OS em Andamento"])

col1, col2, col3, col4 = st.columns(4)
col1.metric("📊 Total de Ligações", total_filtrado)
col2.metric("✅ Trocas Concluídas", f"{concluidas}", f"{concluidas/total_filtrado*100:.1f}%" if total_filtrado > 0 else "0%")
col3.metric("⏳ OS em Andamento", andamento)
col4.metric("🚨 Pendentes de OS", pendentes)
st.markdown("<br>", unsafe_allow_html=True)


# --- GRÁFICOS ---
st.markdown("### Resumo por Status")
colA, colB = st.columns(2)

with colA:
    resumo_status = df_filtrado["Classificação"].value_counts().reset_index()
    resumo_status.columns = ["Status da Troca", "Quantidade"]
    st.bar_chart(resumo_status.set_index("Status da Troca"))

with colB:
    st.markdown("#### Progresso por Rota (Top 10 rotas com mais ligações)")
    resumo_rota = df_filtrado.groupby(["Rota", "Classificação"]).size().unstack(fill_value=0)
    
    # Pegar as 10 rotas com mais movimentação
    resumo_rota["Total"] = resumo_rota.sum(axis=1)
    resumo_rota = resumo_rota.sort_values(by="Total", ascending=False).head(10)
    resumo_rota = resumo_rota.drop(columns=["Total"])
    
    st.bar_chart(resumo_rota)


# --- ALERTAS E DUPLICADAS ---
colW, colZ = st.columns(2)

with colW:
    if not df_lig_dups.empty:
        st.markdown("### ⚠️ Matrículas Duplicadas no Cadastro (Ligações)")
        st.error(f"Foram identificadas {df_lig_dups['Matrícula'].nunique()} matrículas repetidas na planilha de Ligações. O sistema está considerando apenas a 1ª ocorrência.")
        colunas_dup_lig = ["Matrícula", "Rota", "Status", "Endereço"]
        st.dataframe(df_lig_dups[colunas_dup_lig].sort_values("Matrícula"), use_container_width=True)
    else:
        st.markdown("### ✅ Cadastro de Ligações OK")
        st.success("Não há matrículas duplicadas na base atual.")

with colZ:
    if not df_dup.empty:
        st.markdown("### ⚠️ OS Duplicadas (Abertas na mesma matrícula)")
        st.error(f"Existem {df_dup['Matrícula_OS'].nunique()} matrículas com mais de uma OS ABERTA/AGUARDANDO PROGRAMAR.")
        st.dataframe(df_dup[["Matrícula_OS", "NRO SS", "Status", "TIPO DE SERVIÇO", "Data" if "Data" in df_dup.columns else "Últ. Leitura"]], use_container_width=True)


# --- TABELA DE PENDENTES ---
st.markdown("### 📋 Unidades Pendentes para Abertura de OS")
st.markdown("Unidades que precisam de troca mas não possuem OS concluída ou em andamento.")

df_pendentes = df_filtrado[df_filtrado["Classificação"] == "Pendente de OS"]

colunas_exibir = ["Matrícula", "Rota", "Andar", "Categoria - Nome", "Endereço", "Status", "Tem_Telemetria", "Total_OS"]
colunas_exibir = [c for c in colunas_exibir if c in df_pendentes.columns]

st.dataframe(df_pendentes[colunas_exibir].sort_values(by=["Rota", "Matrícula"]), use_container_width=True)


# --- MAPA ---
st.markdown("### 🗺️ Mapa das Ligações (Pendências)")
st.markdown("Exibindo unidades pendentes (vermelho) e em andamento (laranja) na rota selecionada.")

# Limitar a quantidade de pontos no mapa para não travar (max 1000)
df_mapa = df_filtrado[
    (df_filtrado["Classificação"].isin(["Pendente de OS", "OS em Andamento"])) & 
    (df_filtrado["Latitude"].notna()) & 
    (df_filtrado["Longitude"].notna())
].head(1000)

if not df_mapa.empty:
    m = folium.Map(location=[df_mapa["Latitude"].mean(), df_mapa["Longitude"].mean()], zoom_start=14)
    
    for idx, row in df_mapa.iterrows():
        if row["Classificação"] == "Pendente de OS":
            color = "red"
        else:
            color = "orange"
            
        folium.CircleMarker(
            location=[row["Latitude"], row["Longitude"]],
            radius=5,
            popup=f"Mat: {row['Matrícula']}<br>Rota: {row['Rota']}",
            tooltip=f"{row['Classificação']} - {row['Matrícula']}",
            color=color,
            fill=True,
            fill_color=color
        ).add_to(m)
        
    st_folium(m, height=500, width=1200)
else:
    st.info("Nenhuma unidade com coordenadas encontrada nos filtros atuais.")

