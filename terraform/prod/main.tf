# ==============================================================================
# HUB-PC (V2) - TERRAFORM: INFRAESTRUTURA GCP PROD
# Responsável: sa-terraform-prod
# Projeto GCP: hub-pc-prd
# Região: southamerica-east1 (São Paulo)
# Backend (estado remoto): gs://stg-tf-hub-pc-prod/
# ==============================================================================

# ==============================================================================
# STEP 0: BACKEND REMOTO (ONDE O TERRAFORM GUARDA O ESTADO DO QUE JÁ CRIOU)
# O estado nunca fica no host local nem no Git — fica no bucket blindado do GCP.
# ==============================================================================
terraform {
  required_version = ">= 1.5.0"

  backend "gcs" {
    bucket = "stg-tf-hub-pc-prod"      # Bucket de estado — criado manualmente (pré-requisito)
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
# sa-terraform-prod é o único robô autorizado a criar recursos no hub-pc-prd.
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
# storage1 = hub-pc-prod-stg-lakehouse — todas as camadas exceto Gold
# storage2 = hub-pc-prod-stg-dw        — Gold (produto final para BI e IA)
# ==============================================================================

# Bucket principal do Lakehouse (RAW → Bronze → Silver → Quality → Manifests → Logs → Schema)
resource "google_storage_bucket" "lakehouse" {
  name                        = "hub-pc-prod-stg-lakehouse"
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
  name                        = "hub-pc-prod-stg-dw"
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
  name                        = "hub-pc-prod-stg-ansible"
  project                     = var.project_id
  location                    = var.regiao
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  depends_on                  = [google_project_service.storage]
}

resource "google_storage_bucket" "datadog" {
  name                        = "hub-pc-prod-stg-datadog"
  project                     = var.project_id
  location                    = var.regiao
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  depends_on                  = [google_project_service.storage]
}

resource "google_storage_bucket" "datahub" {
  name                        = "hub-pc-prod-stg-datahub"
  project                     = var.project_id
  location                    = var.regiao
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  depends_on                  = [google_project_service.storage]
}

# ==============================================================================
# STEP 5: ARTIFACT REGISTRY (REPOSITÓRIO DE IMAGENS DOCKER)
# Onde a imagem hub-pc/microsvc-cliente será armazenada no GCP prod.
# ==============================================================================
resource "google_artifact_registry_repository" "lakehouse" {
  project       = var.project_id
  location      = var.regiao
  repository_id = "repo-lakehouse-cliente"
  format        = "DOCKER"
  description   = "Repositório de imagens Docker — microsserviço 00-cliente (prod)"

  depends_on = [google_project_service.artifact_registry]
}

# ==============================================================================
# STEP 6: CONTA DE SERVIÇO DO ENGINE (ROBÔ QUE EXECUTA O PIPELINE)
# sa-engine-00-cliente-prod — identidade do container em execução no GCP prod.
# Permissões mínimas: leitura/escrita nos buckets do lakehouse e dw.
# ==============================================================================
resource "google_service_account" "engine" {
  project      = var.project_id
  account_id   = "sa-engine-00-cliente-prod"
  display_name = "sa-engine-00-cliente-prod"
  description  = "Conta de serviço do pipeline 00-cliente — ambiente prod"
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