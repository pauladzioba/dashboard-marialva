"""
processar_dados.py — Módulo de processamento de dados para o dashboard de controle de trocas.

Carrega e cruza os 3 arquivos (Ligações, Leitura atual, Lista de OS)
para classificar cada ligação quanto ao status da troca de HD com telemetria.

Autor: Agente IA
Data: 2026-03-09
"""

import pandas as pd
import os
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Diretório base do projeto para testes locais
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def carregar_ligacoes(arquivo_ou_caminho=None):
    """
    Carrega Ligações.csv — base cadastral.
    Pode receber um caminho de arquivo (string) ou um arquivo em memória (UploadedFile).
    """
    if arquivo_ou_caminho is None:
        arquivo_ou_caminho = os.path.join(BASE_DIR, "Ligações.csv")

    # Lê o CSV usando utf-8-sig
    df = pd.read_csv(arquivo_ou_caminho, encoding="utf-8-sig", sep=";", low_memory=False)

    # Converter Matrícula para inteiro
    df["Matrícula"] = pd.to_numeric(df["Matrícula"], errors="coerce")

    # Converter coordenadas de formato brasileiro para float
    for col in ["Latitude", "Longitude"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", ".", regex=False)
                .apply(pd.to_numeric, errors="coerce")
            )

    # Converter Rota para inteiro
    if "Rota" in df.columns:
        df["Rota"] = pd.to_numeric(df["Rota"], errors="coerce")

    return df


def carregar_leitura(arquivo_ou_caminho=None):
    """
    Carrega Leitura atual.xlsx.
    """
    if arquivo_ou_caminho is None:
        arquivo_ou_caminho = os.path.join(BASE_DIR, "Leitura atual.xlsx")

    df = pd.read_excel(arquivo_ou_caminho, header=3)

    # Converter Matrícula para inteiro
    df["Matrícula"] = pd.to_numeric(df["Matrícula"], errors="coerce")

    # Converter Leitura Atual (formato brasileiro)
    if "Leitura Atual" in df.columns:
        df["Leitura Atual"] = (
            df["Leitura Atual"]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .apply(pd.to_numeric, errors="coerce")
        )

    return df


def carregar_os(arquivo_ou_caminho=None):
    """
    Carrega Lista de OS.xlsx. Renomeia MATRÍCULA para Matrícula_OS.
    """
    if arquivo_ou_caminho is None:
        arquivo_ou_caminho = os.path.join(BASE_DIR, "Lista de OS.xlsx")

    df = pd.read_excel(arquivo_ou_caminho, header=2)

    # A coluna MATRÍCULA (maiúscula) é a chave correta
    if "MATRÍCULA" in df.columns:
        df = df.rename(columns={"MATRÍCULA": "Matrícula_OS"})
    elif "Matrícula" in df.columns:
        # Fallback
        df = df.rename(columns={"Matrícula": "Matrícula_OS"})

    df["Matrícula_OS"] = pd.to_numeric(df["Matrícula_OS"], errors="coerce")

    return df


def classificar_os_por_matricula(df_os):
    """
    Para cada matrícula, determina o status consolidado das OS:
    - 'Concluída' se tem pelo menos 1 OS CONCLUÍDA
    - 'Em Andamento' se tem OS ABERTA/EXECUTANDO/AGUARDANDO PROGRAMAR/AGUARDANDO APROVAÇÃO
    - 'Não Executada' se todas as OS são NÃO EXECUTADA
    - 'Cancelada' se todas as OS são CANCELADO
    """
    resultados = []

    for mat, grupo in df_os.groupby("Matrícula_OS"):
        status_set = set(grupo["Status"].dropna().str.upper())
        total_os = len(grupo)

        if "CONCLUÍDA" in status_set:
            status_final = "Concluída"
        elif status_set & {"ABERTA", "EXECUTANDO", "AGUARDANDO PROGRAMAR", "AGUARDANDO APROVAÇÃO"}:
            status_final = "Em Andamento"
        elif status_set == {"NÃO EXECUTADA"}:
            status_final = "Não Executada"
        elif status_set == {"CANCELADO"}:
            status_final = "Cancelada"
        else:
            status_final = "Outros"

        resultados.append({
            "Matrícula": mat,
            "Status_OS_Consolidado": status_final,
            "Total_OS": total_os,
            "Status_OS_Detalhado": ", ".join(sorted(status_set)),
        })

    return pd.DataFrame(resultados)


def detectar_duplicatas_os(df_os):
    """
    Detecta matrículas com mais de 1 OS com status ABERTA ou AGUARDANDO PROGRAMAR.
    """
    status_alerta = ["ABERTA", "AGUARDANDO PROGRAMAR"]
    df_alerta = df_os[df_os["Status"].str.upper().isin(status_alerta)].copy()

    contagem = df_alerta.groupby("Matrícula_OS").size().reset_index(name="Qtd_OS_Abertas")
    duplicadas = contagem[contagem["Qtd_OS_Abertas"] > 1]

    if len(duplicadas) == 0:
        return pd.DataFrame()

    mats_dup = duplicadas["Matrícula_OS"].tolist()
    df_dup = df_alerta[df_alerta["Matrícula_OS"].isin(mats_dup)].copy()
    df_dup = df_dup.merge(duplicadas, on="Matrícula_OS", how="left")

    return df_dup


def cruzar_dados(df_lig, df_leit, df_os):
    """
    Cruza as 3 tabelas para classificar cada ligação.
    """
    # 1. Marcar quem tem telemetria (aparece em Leitura atual)
    mats_leitura = set(df_leit["Matrícula"].dropna().unique())
    df_lig = df_lig.copy()
    df_lig["Tem_Telemetria"] = df_lig["Matrícula"].isin(mats_leitura)

    # 2. Consolidar status das OS por matrícula (considera apenas 1 status por matrícula validando prioridade)
    df_os_consolidado = classificar_os_por_matricula(df_os)

    # 3. Merge ligações com OS consolidado
    df_result = df_lig.merge(df_os_consolidado, on="Matrícula", how="left")

    # Preencher ligações sem OS
    df_result["Status_OS_Consolidado"] = df_result["Status_OS_Consolidado"].fillna("Sem OS")
    df_result["Total_OS"] = df_result["Total_OS"].fillna(0).astype(int)

    # 4. Classificação final
    def classificar(row):
        # Se foi excluído do cadastro da base
        if row["Status"] in ["EXCLUIDO"]:
            return "Excluída"
            
        # NOVA REGRA: Se a unidade já está transmitindo telemetria, a troca ESTÁ FEITA.
        if row["Tem_Telemetria"]:
            return "Troca Concluída"
            
        # Se não tem leitura ainda, olhamos para a OS
        if row["Status_OS_Consolidado"] == "Concluída":
            return "Troca Concluída"
        if row["Status_OS_Consolidado"] == "Em Andamento":
            return "OS em Andamento"
        if row["Status_OS_Consolidado"] == "Não Executada":
            return "OS Não Executada"
        if row["Status_OS_Consolidado"] == "Cancelada":
            return "OS Cancelada"
            
        # Não tem telemetria e não tem OS (ou Sem OS)
        return "Pendente de OS"

    df_result["Classificação"] = df_result.apply(classificar, axis=1)

    return df_result


def processar_tudo(arq_lig=None, arq_leit=None, arq_os=None):
    """
    Pipeline completo. Permite injeção de arquivos do Streamlit Web.
    """
    print("Carregando Ligações...")
    df_lig = carregar_ligacoes(arq_lig)
    
    # Identificar e remover duplicatas de matrícula na base de Ligações
    df_lig_dups = df_lig[df_lig.duplicated(subset=["Matrícula"], keep=False)].copy()
    if not df_lig_dups.empty:
        df_lig = df_lig.drop_duplicates(subset=["Matrícula"], keep="first")
    
    print(f"  → {len(df_lig)} ligações carregadas (únicas por matrícula)")

    print("Carregando Leitura atual...")
    df_leit = carregar_leitura(arq_leit)
    print(f"  → {len(df_leit)} leituras carregadas")

    print("Carregando Lista de OS...")
    df_os = carregar_os(arq_os)
    print(f"  → {len(df_os)} OS carregadas")

    print("Cruzando dados...")
    df_result = cruzar_dados(df_lig, df_leit, df_os)

    print("Detectando duplicatas de OS...")
    df_dup = detectar_duplicatas_os(df_os)

    return df_result, df_dup, df_leit, df_lig_dups, df_os


if __name__ == "__main__":
    df_result, df_dup, df_leit, df_lig_dups, df_os = processar_tudo()
