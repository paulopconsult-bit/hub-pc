# ==============================================================================
# BIBLIOTECAS (O motor de inteligência e transformação)
# v3 — qtd_lidas declarada antes dos blocos try (fix UnboundLocalError).
#      return substituído por sys.exit(1) nas falhas de infraestrutura.
#      Print de rastreio do PIPELINE_RUN_ID adicionado.
# ==============================================================================
import os                      # Mapeamento de ambiente e variáveis de contexto
import sys                     # Controle de saídas do sistema e encerramento rápido
import json                    # Registro de manifestos atômicos e padronizados
from datetime import datetime  # Cálculos de idade e carimbos de tempo corporativos
import pandas as pd            # Transformação e tipagem forte de matrizes de dados

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
BUCKET_LAKEHOUSE = os.environ.get("BUCKET_LAKEHOUSE", "stg-tf-hub-pc-dev")

# ABSTRAÇÃO DA V1: Ponto de montagem idêntico para Dev Local e Container Linux
BASE_PATH = os.getenv('ROOT_PROJETO', r"D:\cloud\ls")

if EXECUTION_MODE == "local":
    PATH_CLEAN     = os.path.join(BASE_PATH, "03-silver-cleaned", TENANT_ID)
    PATH_ENRICHED  = os.path.join(BASE_PATH, "04-silver-enriched", TENANT_ID)
    PATH_MANIFESTO = os.path.join(BASE_PATH, "99-metadata-manifests", TENANT_ID)
else:
    PATH_CLEAN     = f"gs://{BUCKET_LAKEHOUSE}/03-silver-cleaned/{TENANT_ID}"
    PATH_ENRICHED  = f"gs://{BUCKET_LAKEHOUSE}/04-silver-enriched/{TENANT_ID}"
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

def salvar_manifesto_enriched(manifesto_dict):
    nome_manifesto = f"silver-enriched-{PIPELINE_RUN_ID}-cliente.json"
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
# STEP 1: JURISDIÇÃO DE NEGÓCIO E TIPAGEM FORTE
# ==============================================================================
def step_silver_enriched():
    print(f"{'='*80}\n[STEP 03 -> 04] SILVER ENRICHED CONSOLIDADA: tb_cliente\n{'='*80}")
    print(f"[DEBUG RASTRO] PIPELINE_RUN_ID ativo neste processo: {PIPELINE_RUN_ID}")
    print(f"[INFO] Lendo dados da Silver Cleaned em: {PATH_CLEAN}")

    # FIX: Declarado antes dos blocos try para garantir escopo no except do segundo bloco
    qtd_lidas = 0

    if EXECUTION_MODE == "local":
        origem = os.path.join(PATH_CLEAN, "cliente.parquet")
    else:
        origem = f"{PATH_CLEAN}/cliente.parquet".replace("\\", "/")

    # FIX: sys.exit(1) para sinalizar falha ao orquestrador (era return silencioso)
    if not os.path.exists(origem) and EXECUTION_MODE == "local":
        print(f"[ERRO CRITICO] Origem Clean nao encontrada no caminho: {origem}")
        sys.exit(1)

    try:
        df = pd.read_parquet(origem, engine='pyarrow')
        qtd_lidas = len(df)
        print(f"Tabela Silver Cleaned lida com sucesso: {qtd_lidas} registros.")
    except Exception as e:
        print(f"[ERRO CRITICO] Falha ao ler os dados da Silver Cleaned. Detalhe: {e}")
        # FIX: sys.exit(1) para sinalizar falha ao orquestrador (era return silencioso)
        sys.exit(1)

    if qtd_lidas == 0:  # Se não houver dados, encerra o processo com aviso
        print(f"[INFO] Camada de origem vazia. Sem dados para processar nesta janela.")
        sys.exit(0)

    try:
        # --- 1.1: DEDUPLICAÇÃO (REGRA DE NEGÓCIO: SNAPSHOT ATUAL) ---
        print("Removendo duplicatas (Mantendo a versão mais recente em _at_captura)...")
        df = df.sort_values('_at_captura').drop_duplicates(subset=['id_cliente'], keep='last')

        # --- 1.2: NORMALIZAÇÃO DE CHAVE (ID DE ELITE V1) ---
        print("Aplicando máscara de ID corporativo (Ex: c00001)...")
        df['id_cliente'] = 'c' + df['id_cliente'].astype(str).str.replace('.0', '', regex=False).str.zfill(5)

        # --- 1.3: TRADUÇÃO DE NOMENCLATURA (SNAKE_CASE) ---
        print("Renomeando colunas para padrão analítico tb_cliente...")
        dicionario_nomes = {
            'nome': 'nm_cliente',
            'estado': 'uf',
            'data_nascimento': 'dt_nascimento'
        }
        df = df.rename(columns=dicionario_nomes)

        # --- 1.4: TIPAGEM E GESTÃO DE DATAS IMPOSSÍVEIS (DESARMANDO A BOMBA DO ANO 19923) ---
        print("Convertendo dt_nascimento para tipo DATE (errors='coerce' neutraliza anomalias)...")
        df['dt_nascimento'] = pd.to_datetime(df['dt_nascimento'], dayfirst=True, errors='coerce').dt.date

        # --- 1.4.1: SEGREGAÇÃO ATÔMICA POR SUBPASTAS DE DOMÍNIO FÍSICO (APPROVED VS REJECTED) ---
        filtro_valido = df['dt_nascimento'].notnull()

        df_rejeitados = df[~filtro_valido].copy()  # Linhas anômalas (data impossível)
        df_validos    = df[filtro_valido].copy()   # Linhas puras analíticas (O "Filé")

        linhas_rejeitadas_negocio = len(df_rejeitados)
        print(f"[DATA QUALITY] Segregacao concluida. Validos: {len(df_validos)} | Rejeitados: {linhas_rejeitadas_negocio}")

        # --- 1.5: ENRIQUECIMENTO DE VALOR AGREGADO (APENAS NOS VÁLIDOS) ---
        print("Calculando idade com compensação de mês e dia...")
        hoje = datetime.now()
        df_validos['nr_idade'] = df_validos['dt_nascimento'].apply(
            lambda x: hoje.year - x.year - ((hoje.month, hoje.day) < (x.month, x.day))
        )
        df_validos['nr_idade'] = df_validos['nr_idade'].astype('Int64')

        # Metadados de Auditoria Enriched
        df_validos['_at_enriquecimento']        = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df_validos['_enriched_pipeline_run_id'] = PIPELINE_RUN_ID

        # --- 1.6: PERSISTÊNCIA ISOLADA POR DIRETÓRIO (APPROVED VS REJECTED) ---
        if EXECUTION_MODE == "local":
            dir_approved = os.path.join(PATH_ENRICHED, "approved")
            dir_rejected = os.path.join(PATH_ENRICHED, "rejected")

            os.makedirs(dir_approved, exist_ok=True)
            destino_parquet    = os.path.join(dir_approved, "tb_cliente.parquet")
            caminho_rejeitados = os.path.join(dir_rejected, "tb_cliente.parquet")
        else:
            destino_parquet    = f"{PATH_ENRICHED}/approved/tb_cliente.parquet".replace("\\", "/")
            caminho_rejeitados = f"{PATH_ENRICHED}/rejected/tb_cliente.parquet".replace("\\", "/")

        # Se houver rejeitados por negócio no lote, materializa fisicamente a Dead Letter Table
        if linhas_rejeitadas_negocio > 0:
            if EXECUTION_MODE == "local":
                os.makedirs(dir_rejected, exist_ok=True)
            df_rejeitados.to_parquet(caminho_rejeitados, index=False, engine='pyarrow', compression='snappy')
            print(f"{linhas_rejeitadas_negocio} registros anomalos isolados na subpasta em: {caminho_rejeitados}")

        print(f"[INFO] Gravando produto analitico final limpo em: {destino_parquet}")
        df_validos.to_parquet(destino_parquet, index=False, engine='pyarrow', compression='snappy')

        # --- 1.7: PROTOCOLO DE MANIFESTO SIMÉTRICO V2 (CONTRATO DE 11 CHAVES FIXAS) ---
        print(f"Sucesso: {len(df_validos)} registros únicos entregues na Silver Enriched.")

        manifesto = {
            "pipeline_run_id":    PIPELINE_RUN_ID,
            "tenant_id":          TENANT_ID,
            "camada_alvo":        "SILVER-ENRICHED",
            "status_execucao":    "SUCESSO",
            "data_processamento": datetime.now().isoformat(),
            "linhas_lidas":       qtd_lidas,
            "linhas_gravadas":    len(df_validos),
            "linhas_rejeitadas":  int(linhas_rejeitadas_negocio),
            "origem_caminho":     origem,
            "destino_caminho":    destino_parquet,
            "erro_detalhe":       "NA"
        }
        salvar_manifesto_enriched(manifesto)

    except Exception as e:
        print(f"[ERRO CATASTROFICO] Processamento da Enriched falhou: {e}")
        manifesto_erro = {
            "pipeline_run_id":    PIPELINE_RUN_ID,
            "tenant_id":          TENANT_ID,
            "camada_alvo":        "SILVER-ENRICHED",
            "status_execucao":    "ERRO_ENRIQUECIMENTO",
            "data_processamento": datetime.now().isoformat(),
            "linhas_lidas":       qtd_lidas,
            "linhas_gravadas":    0,
            "linhas_rejeitadas":  0,
            "origem_caminho":     origem,
            "destino_caminho":    "NA",
            "erro_detalhe":       str(e)
        }
        salvar_manifesto_enriched(manifesto_erro)
        sys.exit(1)

if __name__ == "__main__":
    step_silver_enriched()