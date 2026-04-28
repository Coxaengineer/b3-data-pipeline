"""
Script: ingest_b3.py
Camada: Bronze
Função: Buscar cotações da B3 via API brapi.dev e salvar como JSON particionado por data
Executado pelo Airflow todo dia útil às 19h UTC (16h BRT)

LIMITAÇÃO DO PLANO GRATUITO BRAPI:
- Máximo 1 ticker por requisição
- Solução: fazemos 1 chamada por ticker e juntamos os resultados
"""

# ── IMPORTS ──────────────────────────────────────────────────────────────────
import os           # Para criar pastas e lidar com caminhos de arquivo
import json         # Para trabalhar com o formato JSON
import time         # ← NOVO: para dar uma pausa entre as chamadas à API
import requests     # Para fazer pedidos HTTP (falar com a API)
from datetime import datetime   # Para pegar a data de hoje
from dotenv import load_dotenv  # Para ler as variáveis do arquivo .env


# ── 1. CARREGAR O TOKEN DO ARQUIVO .env ──────────────────────────────────────
load_dotenv()
TOKEN = os.getenv("BRAPI_TOKEN")


# ── 2. DEFINIR QUAIS AÇÕES VAMOS BUSCAR ──────────────────────────────────────
TICKERS = [
    "PETR4",  # Petrobras
    "VALE3",  # Vale
    "ITUB4",  # Itaú Unibanco
    "BBDC4",  # Bradesco
    "ABEV3",  # Ambev
    "WEGE3",  # WEG
    "MGLU3",  # Magazine Luiza
    "BBAS3",  # Banco do Brasil
]


# ── 3. BUSCAR COTAÇÃO DE UM ÚNICO TICKER ─────────────────────────────────────
# MUDANÇA: antes tentávamos buscar todos de uma vez
# Agora temos uma função que busca 1 ticker por vez
def buscar_cotacao(ticker: str) -> dict:
    """
    Faz o pedido HTTP GET para a API e retorna os dados de UM ticker.

    - ticker: código da ação (ex: "PETR4")
    - retorna: dicionário com os dados desse ticker
    """
    # Monta a URL com apenas 1 ticker
    url = f"https://brapi.dev/api/quote/{ticker}?token={TOKEN}"

    print(f"[INFO] Buscando {ticker}...")

    # Faz o pedido à API
    response = requests.get(url, timeout=30)

    # Lança erro se a API retornar algo diferente de 200
    response.raise_for_status()

    # Converte a resposta em dicionário Python
    dados = response.json()

    # A API retorna os dados dentro de uma lista "results"
    # Pegamos o primeiro (e único) item da lista: results[0]
    return dados["results"][0]


# ── 4. BUSCAR TODOS OS TICKERS ────────────────────────────────────────────────
# MUDANÇA: agora fazemos um loop — chamamos buscar_cotacao() para cada ticker
def buscar_todas_cotacoes(tickers: list) -> list:
    """
    Percorre a lista de tickers e busca a cotação de cada um.

    - tickers: lista de códigos (ex: ["PETR4", "VALE3", ...])
    - retorna: lista com os dados de todos os tickers
    """
    resultados = []  # Lista vazia que vai recebendo cada resultado

    for ticker in tickers:
        try:
            cotacao = buscar_cotacao(ticker)  # Busca 1 ticker
            resultados.append(cotacao)        # Adiciona na lista de resultados
            time.sleep(0.5)  # Espera 0.5 segundos antes da próxima chamada
                             # Boa prática: não sobrecarregar a API com chamadas muito rápidas
        except Exception as e:
            # Se der erro em 1 ticker, não para tudo — apenas avisa e continua
            print(f"[AVISO] Erro ao buscar {ticker}: {e}")

    print(f"[INFO] {len(resultados)} cotações recebidas com sucesso.")
    return resultados


# ── 5. SALVAR OS DADOS COMO JSON PARTICIONADO POR DATA ───────────────────────
def salvar_bronze(resultados: list, base_path: str = "/opt/airflow/data/bronze") -> str:
    """
    Salva os dados em um arquivo JSON particionado por data.
    Caminho: data/bronze/cotacoes/year=YYYY/month=MM/day=DD/

    - resultados: lista com as cotações de todos os tickers
    - base_path: pasta raiz onde os arquivos serão salvos
    - retorna: caminho completo do arquivo salvo
    """
    agora = datetime.now()

    # Particionamento no padrão Hive: year=2026/month=04/day=22
    # Esse padrão é reconhecido automaticamente pelo PySpark e Delta Lake
    pasta = os.path.join(
        base_path,
        "cotacoes",
        f"year={agora.strftime('%Y')}",
        f"month={agora.strftime('%m')}",
        f"day={agora.strftime('%d')}",
    )

    # Cria a pasta se não existir
    os.makedirs(pasta, exist_ok=True)

    # Nome do arquivo com timestamp para não sobrescrever execuções anteriores
    # Ex: 20260422T190000Z.json
    nome_arquivo = agora.strftime("%Y%m%dT%H%M%SZ") + ".json"
    caminho_arquivo = os.path.join(pasta, nome_arquivo)

    # Monta o dicionário final no mesmo formato que a API retornaria
    payload = {
        "results": resultados,
        "requestedAt": agora.isoformat(),
        "total": len(resultados),
    }

    # Salva o JSON formatado
    with open(caminho_arquivo, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"[INFO] Dados salvos em: {caminho_arquivo}")
    return caminho_arquivo


# ── 6. FUNÇÃO PRINCIPAL ───────────────────────────────────────────────────────
def main():
    print("[START] Iniciando ingestão B3...")

    # Valida se o token foi carregado
    if not TOKEN:
        raise ValueError("[ERRO] BRAPI_TOKEN não encontrado!")

    # Passo 1: busca todos os tickers (1 por vez)
    resultados = buscar_todas_cotacoes(TICKERS)

    # Valida se veio pelo menos 1 resultado
    if not resultados:
        raise ValueError("[ERRO] Nenhuma cotação foi retornada!")

    # Passo 2: salva na camada Bronze
    salvar_bronze(resultados)

    print("[DONE] Ingestão concluída com sucesso!")


# ── PONTO DE ENTRADA ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()