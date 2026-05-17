# ==============================================================================
# BIBLIOTECAS
# ==============================================================================
import subprocess  # Para disparar os scripts como processos independentes
import os          # Para mapeamento de caminhos e variáveis de ambiente
import sys         # Para garantir o uso do interpretador Python correto
from datetime import datetime  # Para geração do PIPELINE_RUN_ID único

# ==============================================================================
# STEP 0: AMBIENTE E MAPEAMENTO DE SCRIPTS (ABSTRAÇÃO)
# ==============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# FIX: ID único gerado aqui e injetado em todos os filhos via variável de ambiente.
# Antes cada script gerava o seu próprio ID — manifestos de um mesmo run
# ficavam com IDs diferentes, quebrando a rastreabilidade entre camadas.
PIPELINE_RUN_ID = f"run-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

PIPELINE_CLIENTE = [
    "01-raw-02-bronze-cliente.py",
    "02-bronze-03-silver-cleaned-cliente.py",
    "03-silver-clean-04-silver-enriched-cliente.py",
    "04-silver-enriched-05-gold-cliente.py"
]

# ==============================================================================
# STEP 1: EXECUÇÃO DA ESTEIRA (FLOW CONTROL)
# ==============================================================================
def rodar_pipeline():
    print(f"{'='*60}")
    print(f"[ORQUESTRADOR] DOMINIO: 00-CLIENTE")
    print(f"[LOCALIZACAO]  {SCRIPT_DIR}")
    print(f"[RUN_ID]       {PIPELINE_RUN_ID}")
    print(f"{'='*60}\n")

    # Herda o ambiente atual e injeta o PIPELINE_RUN_ID para todos os filhos
    env_filhos = os.environ.copy()
    env_filhos["PIPELINE_RUN_ID"] = PIPELINE_RUN_ID

    for script in PIPELINE_CLIENTE:
        caminho_script = os.path.join(SCRIPT_DIR, script)

        print(f"[INICIANDO] {script}")

        processo = subprocess.run(
            [sys.executable, caminho_script],
            capture_output=False,
            env=env_filhos        # FIX: injeta o env com o RUN_ID único
        )

        if processo.returncode == 0:
            print(f"[SUCESSO] {script} concluido.\n")
        else:
            print(f"\n{'!'*60}")
            print(f"[ERRO] Falha no passo: {script}")
            print(f"[ABORTANDO] Esteira interrompida.")
            print(f"{'!'*60}")
            sys.exit(1)

    print(f"{'='*60}")
    print(f"[CONCLUIDO] Esteira 00-cliente finalizada com sucesso.")
    print(f"[RUN_ID]    {PIPELINE_RUN_ID}")
    print(f"{'='*60}")

if __name__ == "__main__":
    rodar_pipeline()