# ==============================================================================
# HUB-PC (V2) - TERRAFORM: VARIÁVEIS DO AMBIENTE PROD
# Valores reais ficam no terraform.tfvars (gitignored — nunca sobe para o Git)
# ==============================================================================

# ==============================================================================
# STEP 0: IDENTIDADE DO PROJETO GCP
# ==============================================================================
variable "project_id" {
  description = "ID do projeto GCP onde os recursos serão criados"
  type        = string
}

variable "regiao" {
  description = "Região GCP para criação dos recursos — southamerica-east1 (São Paulo)"
  type        = string
  default     = "southamerica-east1"
}

# ==============================================================================
# STEP 1: AUTENTICAÇÃO DA CONTA DE SERVIÇO TERRAFORM
# Caminho local da chave JSON do sa-terraform-prod — nunca hardcoded aqui.
# ==============================================================================
variable "caminho_chave_sa" {
  description = "Caminho absoluto para a chave JSON da conta de serviço sa-terraform-prod"
  type        = string
  sensitive   = true   # Marca como sensível — Terraform omite o valor nos logs de execução
}
