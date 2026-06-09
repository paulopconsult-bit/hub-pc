# ==============================================================================
# BIBLIOTECAS (O motor final de entrega para IA e BI)
# v3 — qtd_lidas declarada antes dos blocos try (fix UnboundLocalError).
#      return substituído por sys.exit(1) nas falhas de infraestrutura.
#      Print de rastreio do PIPELINE_RUN_ID adicionado.
# ==============================================================================
import os                      # Mapeamento de ambiente e variáveis de contexto
import sys                     # Controle de saídas do sistema e encerramento rápido
import json                    # Registro de manifestos atômicos e padronizados
from datetime import datetime  # Carimbos de tempo corporativos de faturamento
import pandas as pd            # Motor analítico de alta performance

# Importações Condicionais da Nuvem (Garante portabilidade absoluta V2)
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
BUCKET_DW        = os.environ.get("BUCKET_DW", "stg-dw-hub-pc-dev")        # Storage 2 (DW)
BUCKET_LAKEHOUSE = os.environ.get("BUCKET_LAKEHOUSE", "stg-tf-hub-pc-dev")  # Storage 1 (Lakehouse)

# ABSTRAÇÃO DA V1: Ponto de montagem idêntico para Dev Local e Container Linux
BASE_PATH = os.getenv('ROOT_PROJETO', r"D:\cloud\ls")

if EXECUTION_MODE == "local":
    PATH_ENRICHED_INPUT = os.path.join(BASE_PATH, "04-silver-enriched", TENANT_ID, "approved")
    PATH_GOLD_OUTPUT    = os.path.join(BASE_PATH, "05-gold-business", TENANT_ID)
    PATH_MANIFESTO      = os.path.join(BASE_PATH, "99-metadata-manifests", TENANT_ID)
else:
    PATH_ENRICHED_INPUT = f"gs://{BUCKET_LAKEHOUSE}/04-silver-enriched/{TENANT_ID}/approved"
    PATH_GOLD_OUTPUT    = f"gs://{BUCKET_DW}/05-gold-business/{TENANT_ID}"
    PATH_MANIFESTO      = f"gs://{BUCKET_LAKEHOUSE}/99-metadata-manifests/{TENANT_ID}"

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

def salvar_manifesto_gold(manifesto_dict):
    """SAL Sênior: Garante logs individuais de entrega final prefixados com 'gold-'"""
    nome_manifesto = f"gold-{PIPELINE_RUN_ID}-cliente.json"
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
# STEP 1: CAMADA SOBERANA DE PRODUTO FINAL (SÓ PASSA FILÉ)
# ==============================================================================
def step_gold_business():
    print(f"{'='*80}\n[STEP 04 -> 05] GOLD BUSINESS DELIVERY: tb_cliente\n{'='*80}")
    print(f"[DEBUG RASTRO] PIPELINE_RUN_ID ativo neste processo: {PIPELINE_RUN_ID}")
    print(f"[INFO] Lendo o File Analitico da Enriched em: {PATH_ENRICHED_INPUT}")

    # FIX: Declarado antes dos blocos try para garantir escopo no except do segundo bloco
    qtd_lidas = 0

    if EXECUTION_MODE == "local":
        origem = os.path.join(PATH_ENRICHED_INPUT, "tb_cliente.parquet")
    else:
        origem = f"{PATH_ENRICHED_INPUT}/tb_cliente.parquet".replace("\\", "/")

    # Verificação de existência — local e cloud
    if EXECUTION_MODE == "local":
        if not os.path.exists(origem):
            print(f"[AVISO] RAW vazia. Arquivo nao disponibilizado nesta janela. Monitorar regularidade da fonte.")
            sys.exit(0)
    else:
        client = obter_cliente_gcs()
        bucket_nome, prefixo_nome = quebrar_uri_gcs(origem)
        if not client.bucket(bucket_nome).blob(prefixo_nome).exists():
            print(f"[AVISO] RAW vazia. Arquivo nao disponibilizado nesta janela. Monitorar regularidade da fonte.")
            sys.exit(0)

    try:
        df = pd.read_parquet(origem, engine='pyarrow')
        qtd_lidas = len(df)
        print(f"[SUCESSO] File analitico carregado com sucesso: {qtd_lidas} registros de alta qualidade.")
    except Exception as e:
        print(f"[ERRO CRITICO] Falha ao ler os dados aprovados da Enriched. Detalhe: {e}")
        # FIX: sys.exit(1) para sinalizar falha ao orquestrador (era return silencioso)
        sys.exit(1)

    if qtd_lidas == 0:  # Se não houver dados, encerra o processo com aviso
        print(f"[INFO] Camada de origem vazia. Sem dados para processar nesta janela.")
        sys.exit(0)

    # Watermark — verifica se Silver Enriched foi atualizada neste run
    if df['_enriched_pipeline_run_id'].iloc[0] != PIPELINE_RUN_ID:
        print(f"[AVISO] RAW vazia. Arquivo nao disponibilizado nesta janela. Monitorar regularidade da fonte.")
        sys.exit(0)

    try:
        # --- 1.1: AGREGAÇÃO DE NEGÓCIO DE MARKETING (MAPPING DE MACRO REGIAO) ---
        print("[INFO] Aplicando regra de negocio de Marketing (Mapeamento de Macro Regiao)...")
        mapeamento_regioes = {
            'SP': 'SUDESTE',
            'RJ': 'SUDESTE',
            'MG': 'SUDESTE',
            'ES': 'SUDESTE',
            'PR': 'SUL',
            'SC': 'SUL',
            'RS': 'SUL'
        }
        df['macro_regiao'] = df['uf'].map(mapeamento_regioes).fillna('OUTROS')

        # --- 1.2: GARANTIA DE ORDENAÇÃO FÍSICA PREVISÍVEL ---
        print("[INFO] Ordenando registros por id_cliente para otimizar leitura de BI...")
        df = df.sort_values('id_cliente')

        # --- 1.2.1: METADADOS DE ENTREGA V2 (AUDITORIA SOBERANA DA GOLD) ---
        df['_at_entrega_gold']      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['_gold_pipeline_run_id'] = PIPELINE_RUN_ID

        # --- 1.3: PERSISTÊNCIA FINAL (O PRODUTO ACABADO NO SANTUÁRIO DO DW) ---
        if EXECUTION_MODE == "local":
            os.makedirs(PATH_GOLD_OUTPUT, exist_ok=True)
            destino_parquet = os.path.join(PATH_GOLD_OUTPUT, "tb_cliente.parquet")
        else:
            destino_parquet = f"{PATH_GOLD_OUTPUT}/tb_cliente.parquet".replace("\\", "/")

        print(f"[INFO] Gravando produto final soberano em Parquet Snappy no DW: {destino_parquet}")
        df.to_parquet(destino_parquet, index=False, engine='pyarrow', compression='snappy')

        # --- 1.4: PROTOCOLO DE MANIFESTO SIMÉTRICO V2 (CONTRATO DE 11 CHAVES FIXAS) ---
        print(f"[SUCESSO] Tabela 'tb_cliente' disponivel na Gold com {len(df)} registros de elite.")

        manifesto = {
            "pipeline_run_id":    PIPELINE_RUN_ID,
            "tenant_id":          TENANT_ID,
            "camada_alvo":        "GOLD-BUSINESS",
            "status_execucao":    "SUCESSO",
            "data_processamento": datetime.now().isoformat(),
            "linhas_lidas":       qtd_lidas,
            "linhas_gravadas":    len(df),
            "linhas_rejeitadas":  0,
            "origem_caminho":     origem,
            "destino_caminho":    destino_parquet,
            "erro_detalhe":       "NA"
        }
        salvar_manifesto_gold(manifesto)

    except Exception as e:
        print(f"[ERRO CATASTROFICO] Erro na entrega soberana da Gold: {e}")
        manifesto_erro = {
            "pipeline_run_id":    PIPELINE_RUN_ID,
            "tenant_id":          TENANT_ID,
            "camada_alvo":        "GOLD-BUSINESS",
            "status_execucao":    "ERRO_DELIVERY",
            "data_processamento": datetime.now().isoformat(),
            "linhas_lidas":       qtd_lidas,
            "linhas_gravadas":    0,
            "linhas_rejeitadas":  0,
            "origem_caminho":     origem,
            "destino_caminho":    "NA",
            "erro_detalhe":       str(e)
        }
        salvar_manifesto_gold(manifesto_erro)
        sys.exit(1)

if __name__ == "__main__":
    step_gold_business()