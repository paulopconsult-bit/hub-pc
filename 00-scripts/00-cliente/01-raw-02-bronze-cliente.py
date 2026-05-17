# ==============================================================================
# BIBLIOTECAS (As ferramentas herméticas do nosso robô)
# Host Dev
# Adição de Campos de Controle (Bronze) - Metadados de Aplicação (Inseridos Manualmente)
# v3 — Imports não utilizados removidos (pa, pq). Rastreio de RUN_ID adicionado.
# ==============================================================================
import os                      # Captura de variáveis de ambiente do SO
import sys                     # Controle de saídas do sistema e encerramento rápido
import json                    # Leitura do Schema-as-Code e geração de manifestos
import uuid                    # Geração do id_registro imutável e universal por linha
import glob                    # Varredura de arquivos coringas no host local
import shutil                  # Movimentação física de arquivos em escopo local
import pandas as pd            # Engenharia e transformação de matrizes de dados
from datetime import datetime  # Gestão de carimbos de tempo padronizados (ISO)

try:
    from google.cloud import storage
    from google.auth import default
except ImportError:
    storage = None

# ==============================================================================
# STEP 0: O CONTRATO DE AMBIENTE E ABSTRAÇÃO DE I/O (ARQUITETURA PURA V2)
# ==============================================================================
EXECUTION_MODE  = os.environ.get("EXECUTION_MODE", "local").lower()
TENANT_ID       = os.environ.get("TENANT_ID", "00-cliente")

# Orquestrador injeta este ID em todos os filhos — manifestos cross-layer rastreáveis por 1 chave.
# Execução isolada (dev/debug): cada script gera o seu próprio ID. 
# Quando orquestrado, o ambiente de execução (ex: Airflow) deve garantir a propagação deste ID sendo igual para todos os scripts filhos via variável de ambiente PIPELINE_RUN_ID.
PIPELINE_RUN_ID = os.environ.get("PIPELINE_RUN_ID", f"run-{datetime.now().strftime('%Y%m%d-%H%M%S')}")

# Variáveis globais exigidas pela volumetria e nuvem
LIMITE_PARTICIONAMENTO_BYTES = 1 * 1024 * 1024 * 1024
BUCKET_LAKEHOUSE             = os.environ.get("BUCKET_LAKEHOUSE", "stg-tf-hub-pc-dev")

# A ABSTRAÇÃO DA V1: Lê a variável do container ou adota o padrão do Windows
BASE_PATH = os.getenv('ROOT_PROJETO', r"D:\cloud\ls")

if EXECUTION_MODE == "local":
    # 100% LIMPO: Aponta estritamente para a taxonomia corporativa oficial de Dev
    PATH_RAW       = os.path.join(BASE_PATH, "01-00-raw", TENANT_ID)
    PATH_BACKUP    = os.path.join(BASE_PATH, "02-00-raw-backup", TENANT_ID)
    PATH_BRONZE    = os.path.join(BASE_PATH, "02-01-bronze", TENANT_ID)
    PATH_ERRO      = os.path.join(BASE_PATH, "02-02-quarantine-erros", TENANT_ID)
    PATH_MANIFESTO = os.path.join(BASE_PATH, "99-metadata-manifests", TENANT_ID)
    PATH_CONTRATO  = os.path.join(BASE_PATH, "77-schema-controls", TENANT_ID, "schema-controls.json")
else:
    # Estrutura de Nuvem (GCP)
    PATH_RAW       = f"gs://{BUCKET_LAKEHOUSE}/01-00-raw/{TENANT_ID}"
    PATH_BACKUP    = f"gs://{BUCKET_LAKEHOUSE}/02-00-raw-backup/{TENANT_ID}"
    PATH_BRONZE    = f"gs://{BUCKET_LAKEHOUSE}/02-01-bronze/{TENANT_ID}"
    PATH_ERRO      = f"gs://{BUCKET_LAKEHOUSE}/02-02-quarantine-erros/{TENANT_ID}"
    PATH_MANIFESTO = f"gs://{BUCKET_LAKEHOUSE}/99-metadata-manifests/{TENANT_ID}"
    PATH_CONTRATO  = f"gs://{BUCKET_LAKEHOUSE}/77-schema-controls/{TENANT_ID}/schema-controls.json"

def obter_cliente_gcs():
    if storage is None:
        raise ImportError("SDK do Google Cloud Storage não encontrado.")
    credenciais, projeto_id = default()
    return storage.Client(credentials=credenciais, project=projeto_id)

def quebrar_uri_gcs(caminho_uri):
    if caminho_uri.startswith("gs://"):
        partes = caminho_uri.replace("gs://", "").split("/", 1)
        return partes[0], partes[1] if len(partes) > 1 else ""
    return None, caminho_uri

# ==============================================================================
# STEP 1: CAMADA DE ABSTRAÇÃO DE ARMAZENAMENTO (S.A.L. - MOTOR DE PORTABILIDADE)
# ==============================================================================
def ler_contrato_schema():
    if EXECUTION_MODE == "local":
        print(f"[DEBUG RASTRO] Lendo contrato local em: {PATH_CONTRATO}")
        with open(PATH_CONTRATO, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        client = obter_cliente_gcs()
        bucket_nome, prefixo_nome = quebrar_uri_gcs(PATH_CONTRATO)
        bucket = client.bucket(bucket_nome)
        blob = bucket.blob(prefixo_nome)
        return json.loads(blob.download_as_text(encoding='utf-8'))

def listar_arquivos_input():
    if EXECUTION_MODE == "local":
        padrao = os.path.join(PATH_RAW, "*.csv")
        print(f"[DEBUG RASTRO] Varrendo a pasta RAW com a mascara: {padrao}")
        encontrados = glob.glob(padrao)
        print(f"[DEBUG RASTRO] Arquivos físicos encontrados no Windows: {encontrados}")
        return [{"nome": os.path.basename(f), "caminho": f} for f in encontrados]
    else:
        client = obter_cliente_gcs()
        bucket_nome, prefixo_nome = quebrar_uri_gcs(PATH_RAW)
        bucket = client.bucket(bucket_nome)
        blobs = bucket.list_blobs(prefix=prefixo_nome)

        arquivos = []
        for blob in blobs:
            if blob.name.endswith(".csv"):
                arquivos.append({
                    "nome": os.path.basename(blob.name),
                    "caminho": f"gs://{bucket_nome}/{blob.name}",
                    "blob_obj": blob
                })
        return arquivos

def mover_arquivo_raw(origem_dict, destino_dir_base, novo_nome_arquivo):
    if EXECUTION_MODE == "local":
        os.makedirs(destino_dir_base, exist_ok=True)
        caminho_final = os.path.join(destino_dir_base, novo_nome_arquivo)
        print(f"[DEBUG RASTRO] Tentando mover fisicamente de {origem_dict['caminho']} para {caminho_final}")
        shutil.move(origem_dict["caminho"], caminho_final)
        return caminho_final
    else:
        client = obter_cliente_gcs()
        bucket_dest_nome, prefixo_dest_base = quebrar_uri_gcs(destino_dir_base)
        caminho_blob_destino = os.path.join(prefixo_dest_base, novo_nome_arquivo).replace("\\", "/")
        bucket_origem = origem_dict["blob_obj"].bucket
        bucket_destino = client.bucket(bucket_dest_nome)
        bucket_origem.copy_blob(origem_dict["blob_obj"], bucket_destino, caminho_blob_destino)
        origem_dict["blob_obj"].delete()
        return f"gs://{bucket_dest_nome}/{caminho_blob_destino}"

def salvar_manifesto_execucao(manifesto_dict, arquivo_nome):
    nome_manifesto = f"bronze-{PIPELINE_RUN_ID}-{arquivo_nome.replace('.csv', '.json')}"
    conteudo_json = json.dumps(manifesto_dict, ensure_ascii=False, indent=2)

    if EXECUTION_MODE == "local":
        os.makedirs(PATH_MANIFESTO, exist_ok=True)
        caminho = os.path.join(PATH_MANIFESTO, nome_manifesto)
        print(f"[DEBUG RASTRO] Gravando manifesto de auditoria local em: {caminho}")
        with open(caminho, 'w', encoding='utf-8') as f:
            f.write(conteudo_json)
    else:
        client = obter_cliente_gcs()
        bucket_nome, prefixo_nome = quebrar_uri_gcs(PATH_MANIFESTO)
        bucket = client.bucket(bucket_nome)
        caminho_blob = os.path.join(prefixo_nome, nome_manifesto).replace("\\", "/")
        blob = bucket.blob(caminho_blob)
        blob.upload_from_string(conteudo_json, content_type='application/json')

def gravar_output_parquet(df, destino_dir_base, nome_base_arquivo):
    tamanho_estimado_bytes = df.memory_usage(deep=True).sum()

    if EXECUTION_MODE == "local":
        os.makedirs(destino_dir_base, exist_ok=True)
        if tamanho_estimado_bytes > LIMITE_PARTICIONAMENTO_BYTES:
            df["_part_ano"] = datetime.now().strftime("%Y")
            df["_part_mes"] = datetime.now().strftime("%m")
            df["_part_dia"] = datetime.now().strftime("%d")
            df.to_parquet(destino_dir_base, partition_cols=["_part_ano", "_part_mes", "_part_dia"], index=False, compression="snappy")
            return destino_dir_base
        else:
            caminho_final = os.path.join(destino_dir_base, nome_base_arquivo.replace(".csv", ".parquet"))
            print(f"[DEBUG RASTRO] Gravando Parquet Bronze local em: {caminho_final}")
            df.to_parquet(caminho_final, index=False, compression="snappy")
            return caminho_final
    else:
        if tamanho_estimado_bytes > LIMITE_PARTICIONAMENTO_BYTES:
            df["_part_ano"] = datetime.utcnow().strftime("%Y")
            df["_part_mes"] = datetime.utcnow().strftime("%m")
            df["_part_dia"] = datetime.utcnow().strftime("%d")
            caminho_gcs = destino_dir_base.replace("\\", "/")
            df.to_parquet(caminho_gcs, partition_cols=["_part_ano", "_part_mes", "_part_dia"], index=False, compression="snappy")
            return caminho_gcs
        else:
            caminho_final = f"{destino_dir_base}/{nome_base_arquivo.replace('.csv', '.parquet')}".replace("\\", "/")
            df.to_parquet(caminho_final, index=False, compression="snappy")
            return caminho_final

# ==============================================================================
# STEP 2: PIPELINE CORE - PROCESSAMENTO COMPLIANCE BRONZE
# ==============================================================================
def executar_esteira_bronze():
    print(f"{'='*80}\n[MODO ESTEIRA V2] EXTRACT & LOAD BRONZE AGNOSTICO: {TENANT_ID.upper()}\n{'='*80}")
    print(f"[DEBUG RASTRO] PIPELINE_RUN_ID ativo neste processo: {PIPELINE_RUN_ID}")

    try:
        contrato = ler_contrato_schema()
        colunas_esperadas = contrato['colunas_esperadas']
        print(f"[DEBUG RASTRO] Colunas contratuais esperadas: {colunas_esperadas}")
    except Exception as e:
        print(f"ERRO CRITICO: Impossivel carregar contrato de schema. Abortando. Detalhe: {e}")
        sys.exit(1)

    arquivos = listar_arquivos_input()
    if not arquivos:
        print("Camada RAW limpa. Sem novos arquivos para processar nesta janela.")
        return

    for arquivo in arquivos:
        nome_original = arquivo["nome"]
        print(f"\nComeçando analise de objeto no loop: {nome_original}")

        if EXECUTION_MODE == "local":
            mtime = os.path.getmtime(arquivo["caminho"])
            dt_modificacao_origem = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
        else:
            dt_modificacao_origem = arquivo["blob_obj"].updated.strftime('%Y-%m-%d %H:%M:%S')

        carimbo_timestamp = datetime.now().strftime('%Y-%m-%d-%H%M%S')

        try:
            print(f"[DEBUG RASTRO] Tentando abrir arquivo via contexto with: {arquivo['caminho']}")
            if EXECUTION_MODE == "local":
                with open(arquivo["caminho"], 'r', encoding='utf-8') as f:
                    df_processamento = pd.read_csv(f, dtype=str)
            else:
                df_processamento = pd.read_csv(arquivo["caminho"], dtype=str)

            print(f"[DEBUG RASTRO] Colunas lidas do arquivo real: {list(df_processamento.columns)}")

            if list(df_processamento.columns) != colunas_esperadas:
                raise ValueError(f"Quebra de Contrato! Recebido: {list(df_processamento.columns)} | Esperado: {colunas_esperadas}")

            df_processamento['_id_registro']            = [str(uuid.uuid4()) for _ in range(len(df_processamento))]
            df_processamento['_at_captura']             = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df_processamento['_arquivo_origem']         = nome_original
            df_processamento['_arquivo_dt_modificacao'] = dt_modificacao_origem
            df_processamento['_pipeline_run_id']        = PIPELINE_RUN_ID

            caminho_bronze_gravado = gravar_output_parquet(df_processamento, PATH_BRONZE, nome_original)

            nome_arquivo_backup    = f"{carimbo_timestamp}-{nome_original}"
            caminho_backup_gravado = mover_arquivo_raw(arquivo, PATH_BACKUP, nome_arquivo_backup)

            print(f"Sucesso integrado e persistido de forma imutavel.")

            manifesto = {
                "pipeline_run_id":    PIPELINE_RUN_ID,
                "tenant_id":          TENANT_ID,
                "camada_alvo":        "BRONZE",
                "status_execucao":    "SUCESSO",
                "data_processamento": datetime.now().isoformat(),
                "linhas_lidas":       len(df_processamento),
                "linhas_gravadas":    len(df_processamento),
                "linhas_rejeitadas":  0,
                "origem_caminho":     arquivo["caminho"],
                "destino_caminho":    caminho_bronze_gravado,
                "erro_detalhe":       "NA"
            }
            salvar_manifesto_execucao(manifesto, nome_original)

        except Exception as erro_processamento:
            print(f"ANOMALIA IDENTIFICADA em {nome_original}: {erro_processamento}")

            nome_arquivo_erro          = f"error-{carimbo_timestamp}-{nome_original}"
            caminho_final_quarentena   = mover_arquivo_raw(arquivo, PATH_ERRO, nome_arquivo_erro)

            print(f"Objeto corrompido ou fora de contrato isolado na QUARENTENA.")

            manifesto_erro = {
                "pipeline_run_id":    PIPELINE_RUN_ID,
                "tenant_id":          TENANT_ID,
                "camada_alvo":        "BRONZE",
                "status_execucao":    "ERRO_SCHEMA",
                "data_processamento": datetime.now().isoformat(),
                "linhas_lidas":       0,
                "linhas_gravadas":    0,
                "linhas_rejeitadas":  1,
                "origem_caminho":     arquivo["caminho"],
                "destino_caminho":    caminho_final_quarentena,
                "erro_detalhe":       str(erro_processamento)
            }
            salvar_manifesto_execucao(manifesto_erro, nome_original)

if __name__ == "__main__":
    executar_esteira_bronze()