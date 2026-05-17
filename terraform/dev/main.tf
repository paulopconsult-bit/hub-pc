# ==============================================================================
# HUB-PC (V2) - TERRAFORM: INFRAESTRUTURA GCP DEV
# Responsável: sa-terraform-dev
# Projeto GCP: hub-pc-dev
# Região: southamerica-east1 (São Paulo)
# Backend (estado remoto): gs://stg-tf-hub-pc-dev/
# ==============================================================================

# ==============================================================================
# STEP 0: BACKEND REMOTO (ONDE O TERRAFORM GUARDA O ESTADO DO QUE JÁ CRIOU)
# O estado nunca fica no host local nem no Git — fica no bucket blindado do GCP.
# ==============================================================================
terraform {
  required_version = ">= 1.5.0"

  backend "gcs" {
    bucket = "stg-tf-hub-pc-dev"       # Bucket de estado — criado manualmente (pré-requisito)
    prefix = "terraform/state"          # Pasta lógica dentro do bucket
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# ==============================================================================
# STEP 1: PROVEDOR GCP (AUTENTICAÇÃO VIA CHAVE DA CONTA DE SERVIÇO)
# sa-terraform-dev é o único robô autorizado a criar recursos no hub-pc-dev.
# ==============================================================================
provider "google" {
  credentials = file(var.caminho_chave_sa)   # Lê a chave JSON da conta de serviço
  project     = var.project_id
  region      = var.regiao
}

# ==============================================================================
# STEP 2: HABILITAR APIs DO GCP (PRÉ-REQUISITO PARA CRIAR RECURSOS)
# O GCP exige que cada serviço seja habilitado antes de ser usado.
# ==============================================================================
resource "google_project_service" "artifact_registry" {
  project            = var.project_id
  service            = "artifactregistry.googleapis.com"
  disable_on_destroy = false   # Não desabilita a API ao destruir — evita impacto em outros recursos
}

resource "google_project_service" "cloud_run" {
  project            = var.project_id
  service            = "run.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  project            = var.project_id
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

# ==============================================================================
# STEP 3: BUCKETS DO LAKEHOUSE (ARQUITETURA MEDALLION)
# storage1 = hub-pc-dev-stg-lakehouse — todas as camadas exceto Gold
# storage2 = hub-pc-dev-stg-dw        — Gold (produto final para BI e IA)
# ==============================================================================

# Bucket principal do Lakehouse (RAW → Bronze → Silver → Quality → Manifests → Logs → Schema)
resource "google_storage_bucket" "lakehouse" {
  name                        = "hub-pc-dev-stg-lakehouse"
  project                     = var.project_id
  location                    = var.regiao
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true       # Controle de acesso unificado — padrão Big Tech
  public_access_prevention    = "enforced" # Bloqueia acesso público — segurança obrigatória

  versioning {
    enabled = true   # Guarda versões anteriores dos objetos — imutabilidade histórica
  }

  lifecycle_rule {
    action { type = "Delete" }
    condition {
      num_newer_versions = 5   # Apaga versões antigas quando existem mais de 5 versões mais recentes
    }
  }

  lifecycle_rule {
    action { type = "Delete" }
    condition {
      days_since_noncurrent_time = 7   # Apaga versões não-atuais após 7 dias
    }
  }

  depends_on = [google_project_service.storage]
}

# Bucket do Data Warehouse — soberano para Gold (Analytics + IA)
resource "google_storage_bucket" "dw" {
  name                        = "hub-pc-dev-stg-dw"
  project                     = var.project_id
  location                    = var.regiao
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action { type = "Delete" }
    condition {
      num_newer_versions = 5
    }
  }

  lifecycle_rule {
    action { type = "Delete" }
    condition {
      days_since_noncurrent_time = 7
    }
  }

  depends_on = [google_project_service.storage]
}

# ==============================================================================
# STEP 4: BUCKETS DE FERRAMENTAS (ANSIBLE, DATADOG, DATAHUB)
# Cada ferramenta tem seu próprio bucket isolado — princípio de menor privilégio.
# ==============================================================================
resource "google_storage_bucket" "ansible" {
  name                        = "hub-pc-dev-stg-ansible"
  project                     = var.project_id
  location                    = var.regiao
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  depends_on                  = [google_project_service.storage]
}

resource "google_storage_bucket" "datadog" {
  name                        = "hub-pc-dev-stg-datadog"
  project                     = var.project_id
  location                    = var.regiao
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  depends_on                  = [google_project_service.storage]
}

resource "google_storage_bucket" "datahub" {
  name                        = "hub-pc-dev-stg-datahub"
  project                     = var.project_id
  location                    = var.regiao
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  depends_on                  = [google_project_service.storage]
}

# ==============================================================================
# STEP 5: ARTIFACT REGISTRY (REPOSITÓRIO DE IMAGENS DOCKER)
# Onde a imagem hub-pc/microsvc-cliente será armazenada no GCP.
# ==============================================================================
resource "google_artifact_registry_repository" "lakehouse" {
  project       = var.project_id
  location      = var.regiao
  repository_id = "repo-lakehouse-cliente"
  format        = "DOCKER"
  description = "Repositorio de imagens Docker - microsservico 00-cliente"

  depends_on = [google_project_service.artifact_registry]
}

# ==============================================================================
# STEP 6: CONTA DE SERVIÇO DO ENGINE (ROBÔ QUE EXECUTA O PIPELINE)
# sa-engine-00-cliente-dev — identidade do container em execução no GCP.
# Permissões mínimas: leitura/escrita nos buckets do lakehouse e dw.
# ==============================================================================
resource "google_service_account" "engine" {
  project      = var.project_id
  account_id   = "sa-engine-00-cliente-dev"
  display_name = "sa-engine-00-cliente-dev"
  description  = "Conta de serviço do pipeline 00-cliente — ambiente dev"
}

# Permissão de leitura e escrita no lakehouse
resource "google_storage_bucket_iam_member" "engine_lakehouse" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.objectAdmin"   # Lê, escreve e deleta objetos no bucket
  member = "serviceAccount:${google_service_account.engine.email}"
}

# Permissão de leitura e escrita no DW (Data Warehouse)
resource "google_storage_bucket_iam_member" "engine_dw" {
  bucket = google_storage_bucket.dw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.engine.email}"
}

# Permissoes para CI/CD — sa-engine faz push de imagem e atualiza Cloud Run Job
resource "google_project_iam_member" "engine_artifact_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${google_service_account.engine.email}"
}

resource "google_project_iam_member" "engine_run_developer" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.engine.email}"
}

resource "google_project_iam_member" "engine_sa_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.engine.email}"
}

# ==============================================================================
# STEP 7: PERMISSAO PARA SA-ENGINE INVOCAR CLOUD RUN JOBS
# A mesma conta de servico do pipeline recebe permissao de invocar o job.
# Em Big Tech: conta separada por responsabilidade (invoker vs executor).
# ==============================================================================
resource "google_project_iam_member" "engine_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.engine.email}"
}

# ==============================================================================
# STEP 8: CLOUD RUN JOB (EXECUTOR DO PIPELINE NO GCP)
# Aponta para a imagem no Artifact Registry e executa o orquestrador.
# A imagem e atualizada pelo CI/CD a cada push no GitHub.
# ==============================================================================
resource "google_cloud_run_v2_job" "pipeline_cliente" {
  project  = var.project_id
  name     = "job-lakehouse-00-cliente-dev"
  location = var.regiao

  template {
    template {
      service_account = google_service_account.engine.email

      containers {
        image = "southamerica-east1-docker.pkg.dev/${var.project_id}/repo-lakehouse-cliente/microsvc-00-cliente:latest"

        env {
          name  = "EXECUTION_MODE"
          value = "cloud"
        }
        env {
          name  = "TENANT_ID"
          value = "00-cliente"
        }
        env {
          name  = "BUCKET_LAKEHOUSE"
          value = "hub-pc-dev-stg-lakehouse"
        }
        env {
          name  = "BUCKET_DW"
          value = "hub-pc-dev-stg-dw"
        }
      }
    }
  }

  depends_on = [google_artifact_registry_repository.lakehouse]
}

# ==============================================================================
# STEP 9: CLOUD SCHEDULER (AGENDADOR DO JOB)
# Dispara o job automaticamente conforme o cron definido.
# Cron atual: todo dia as 13:00 horario de Brasilia (16:00 UTC).
# Em Big Tech: ajustar conforme SLA de entrega de dados do negocio.
# ==============================================================================
resource "google_cloud_scheduler_job" "scheduler_cliente" {
  project   = var.project_id
  name      = "scheduler-lakehouse-00-cliente-dev"
  region    = var.regiao
  schedule  = "0 16 * * *"
  time_zone = "America/Sao_Paulo"

  http_target {
    http_method = "POST"
    uri         = "https://${var.regiao}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/job-lakehouse-00-cliente-dev:run"

    oauth_token {
      service_account_email = google_service_account.engine.email
    }
  }

  depends_on = [google_cloud_run_v2_job.pipeline_cliente]
}