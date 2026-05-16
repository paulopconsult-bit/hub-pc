# ==============================================================================
# BIBLIOTECAS (As ferramentas da nossa faxina técnica)
# v3 — qtd_inicial declarada antes dos blocos try (fix UnboundLocalError).
#      Print de rastreio do PIPELINE_RUN_ID adicionado. Alias desnecessário removido.
# ==============================================================================
import os                      # Gestão de caminhos e variáveis de ambiente
import sys                     # Controle de saídas do sistema e erros críticos
import json                    # Para registrar os manifestos atômicos isolados
from datetime import datetime  # Para carimbos de tempo do processo
import pandas as pd            # Motor principal para transformação de dados

# Importações Condicionais da Nuvem (Garante portabilidade absoluta)
try:
    from google.cloud import storage
    from google.auth import default
except ImportError:
    storage = None

# ==============================================================================
# STEP 0: CONTRATO DE AMBIENTE E ABSTRAÇÃO DE I/O (PORTABILIDADE PURA)
# ==============================================================================
EXECUTION_MODE   = os.environ.get("EXECUTION_MODE", "local").lower()
TENANT_ID        = os.environ.get("TENANT_ID", "00-cliente")
PIPELINE_RUN_ID  = os.environ.get("PIPELINE_RUN_ID", f"run-{datetime.now().strftime('%Y%m%d-%H%M%S')}")
BUCKET_LAKEHOUSE = os.environ.get("BUCKET_LAKEHOUSE", "stg-tf-hub-pc-dev")

# ABSTRAÇÃO DA V1: Ponto de montagem idêntico para Dev Local e Container Linux
BASE_PATH = os.getenv('ROOT_PROJETO', r"D:\cloud\ls")

if EXECUTION_MODE == "local":
    PATH_BRONZE    = os.path.join(BASE_PATH, "02-01-bronze", TENANT_ID)
    PATH_SILVER    = os.path.join(BASE_PATH, "03-silver-cleaned", TENANT_ID)
    PATH_MANIFESTO = os.path.join(BASE_PATH, "99-metadata-manifests", TENANT_ID)
else:
    PATH_BRONZE    = f"gs://{BUCKET_LAKEHOUSE}/02-01-bronze/{TENANT_ID}"
    PATH_SILVER    = f"gs://{BUCKET_LAKEHOUSE}/03-silver-cleaned/{TENANT_ID}"
    PATH_MANIFESTO = f"gs://{BUCKET_LAKEHOUSE}/99-metadata-manifests/{TENANT_ID}"

def obter_cliente_gcs():
    if storage is None:
        raise ImportError("SDK do Google Cloud Storage não encontrado no ambiente.")
    credenciais, projeto_id = default()
    return storage.Client(credentials=credenciais, project=projeto_id)

def quebrar_uri_gcs(caminho_uri):
    if caminho_uri.startswith("gs://"):
        partes = caminho_uri.replace("gs://", "").split("/", 1)
        return partes[0], partes[1] if len(partes) > 1 else ""
    return None, caminho_uri

def salvar_manifesto_silver(manifesto_dict):
    nome_manifesto = f"silver-cleaned-{PIPELINE_RUN_ID}-cliente.json"
    conteudo_json = json.dumps(manifesto_dict, ensure_ascii=False, indent=2)

    if EXECUTION_MODE == "local":
        os.makedirs(PATH_MANIFESTO, exist_ok=True)
        caminho = os.path.join(PATH_MANIFESTO, nome_manifesto)
        with open(caminho, 'w', encoding='utf-8') as f:
            f.write(conteudo_json)
    else:
        client = obter_cliente_gcs()
        bucket_nome, prefixo_nome = quebrar_uri_gcs(PATH_MANIFESTO)
        bucket = client.bucket(bucket_nome)
        caminho_blob = os.path.join(prefixo_nome, nome_manifesto).replace("\\", "/")
        blob = bucket.blob(caminho_blob)
        blob.upload_from_string(conteudo_json, content_type='application/json')

# ==============================================================================
# STEP 1: PROCESSO DE HIGIENIZAÇÃO (A VASSOURA TÉCNICA)
# ==============================================================================
def step_silver_clean():
    print(f"{'='*80}\n[STEP 02 -> 03] SILVER CLEAN CONSOLIDADA: {TENANT_ID.upper()}\n{'='*80}")
    print(f"[DEBUG RASTRO] PIPELINE_RUN_ID ativo neste processo: {PIPELINE_RUN_ID}")
    print(f"[INFO] Lendo dados da Bronze em: {PATH_BRONZE}")

    # FIX: Declarado antes dos blocos try para garantir escopo no except do segundo bloco
    qtd_inicial = 0

    try:
        df = pd.read_parquet(PATH_BRONZE, engine='pyarrow')
        qtd_inicial = len(df)
        print(f"Bronze lida com sucesso: {qtd_inicial} registros consolidados.")
    except Exception as e:
        print(f"Erro Crítico: Impossível ler o diretório Bronze ou tabela vazia. Detalhe: {e}")
        sys.exit(1)

    try:
        print("Higienizando caracteres de controle (\\n, \\r, \\t)...")
        df = df.replace(r'\r+|\n+', ' [NL] ', regex=True)
        df = df.replace(r'\t+', ' ', regex=True)

        # FIX COMPLIANCE: Adiciona 'string' junto com 'object' para sanar o Pandas4Warning
        cols_texto = df.select_dtypes(include=['object', 'string']).columns
        for col in cols_texto:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True)

        print("Higienizando strings de data (Mantendo tipo object para a Enriched)...")
        df['data_nascimento'] = df['data_nascimento'].astype(str).str.strip()

        if 'estado' in df.columns:
            df['estado'] = df['estado'].str.upper()
        if 'nome' in df.columns:
            df['nome'] = df['nome'].str.title()

        df['_at_higienizacao']       = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['_silver_pipeline_run_id'] = PIPELINE_RUN_ID

        if EXECUTION_MODE == "local":
            os.makedirs(PATH_SILVER, exist_ok=True)
            destino_parquet = os.path.join(PATH_SILVER, "cliente.parquet")
        else:
            destino_parquet = f"{PATH_SILVER}/cliente.parquet".replace("\\", "/")

        print(f"[INFO] Gravando arquivo Parquet purificado em: {destino_parquet}")
        df.to_parquet(destino_parquet, index=False, engine='pyarrow', compression='snappy')

        print(f"Sucesso: {len(df)} linhas limpas e entregues na Silver Clean.")

        manifesto = {
            "pipeline_run_id":    PIPELINE_RUN_ID,
            "tenant_id":          TENANT_ID,
            "camada_alvo":        "SILVER-CLEANED",
            "status_execucao":    "SUCESSO",
            "data_processamento": datetime.now().isoformat(),
            "linhas_lidas":       qtd_inicial,
            "linhas_gravadas":    len(df),
            "linhas_rejeitadas":  0,
            "origem_caminho":     PATH_BRONZE,
            "destino_caminho":    destino_parquet,
            "erro_detalhe":       "NA"
        }
        salvar_manifesto_silver(manifesto)

    except Exception as e:
        print(f"Erro catastrófico na transformação Silver Clean: {e}")
        manifesto_erro = {
            "pipeline_run_id":    PIPELINE_RUN_ID,
            "tenant_id":          TENANT_ID,
            "camada_alvo":        "SILVER-CLEANED",
            "status_execucao":    "ERRO_TRANSFORMACAO",
            "data_processamento": datetime.now().isoformat(),
            "linhas_lidas":       qtd_inicial,
            "linhas_gravadas":    0,
            "linhas_rejeitadas":  0,
            "origem_caminho":     PATH_BRONZE,
            "destino_caminho":    "NA",
            "erro_detalhe":       str(e)
        }
        salvar_manifesto_silver(manifesto_erro)
        sys.exit(1)

if __name__ == "__main__":
    step_silver_clean()