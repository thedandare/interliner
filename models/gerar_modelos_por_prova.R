#!/usr/bin/env Rscript

# ============================================================
# USO:
#   Rscript gerar_modelos_por_prova.R <arquivoCSV> <pastaSaida>
#
# Exemplo:
#   Rscript gerar_modelos_por_prova.R itens_parametros.csv modelos/
#
# OBS:
#   - Este script lê arquivo CSV com sep=";", contendo colunas:
#       CO_PROVA, NU_PARAM_A, NU_PARAM_B, NU_PARAM_C, ...
#   - Cria um modelo fixo (3PL) para cada CO_PROVA encontrado.
#   - Salva em <pastaSaida>/modelo_<CO_PROVA>_3PL.rds
# ============================================================

suppressMessages(library(mirt))
suppressMessages(library(dplyr))

# Load our model building functions
source("build_mirt_model.R")

# 1) Lê parâmetros da linha de comando
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  cat("Uso: Rscript gerar_modelos_por_prova.R <arquivoCSV> <pastaSaida>\n")
  quit(status = 1)
}

arquivoCSV  <- args[1]  # Ex: "itens_parametros.csv"
pastaSaida  <- args[2]  # Ex: "modelos/"

# Cria a pasta de saída se não existir
if (!dir.exists(pastaSaida)) {
  dir.create(pastaSaida, recursive = TRUE)
}

# 2) Ler dados do CSV, com sep=';'
cat("Lendo arquivo CSV:", arquivoCSV, "\n")
df <- read.csv2(arquivoCSV, stringsAsFactors = FALSE)

# Verificar se colunas necessárias existem
colunas_necessarias <- c("CO_PROVA", "NU_PARAM_A", "NU_PARAM_B", "NU_PARAM_C")
falta <- setdiff(colunas_necessarias, names(df))
if (length(falta) > 0) {
  stop("Faltam colunas no CSV: ", paste(falta, collapse = ", "))
}

# 3) Agrupar por CO_PROVA
todas_as_provas <- unique(df$CO_PROVA)
cat("Encontradas", length(todas_as_provas), "provas distintas.\n")

# 4) Função auxiliar para criar o modelo fixo de uma prova
criar_modelo_por_prova <- function(subdf, idProva, pastaOut) {
  # subdf tem as linhas de itens para uma CO_PROVA específica
  k <- nrow(subdf)
  if (k < 1) {
    cat("Prova", idProva, "sem itens?\n")
    return(NULL)
  }
  
  # Clean and validate parameters
  params <- clean_irt_parameters(
    a_params = subdf$NU_PARAM_A, 
    b_params = subdf$NU_PARAM_B, 
    c_params = subdf$NU_PARAM_C,
    item_ids = seq_len(k)
  )
  
  # Build the model using our utility function
  fixed_model <- build_fixed_3pl_model(
    a_vec = params$a,
    b_vec = params$b,
    c_vec = params$c,
    item_names = paste0("item", seq_len(k))
  )
  
  # Salvar o modelo
  arquivo_saida <- file.path(pastaOut, paste0("modelo_", idProva, "_3PL.rds"))
  saveRDS(fixed_model, arquivo_saida)
  cat("Modelo salvo em:", arquivo_saida, "\n")
  
  return(fixed_model)
}

# 5) Loop em cada CO_PROVA e gerar modelo
for (prov in todas_as_provas) {
  subset_prova <- df %>% filter(CO_PROVA == prov)
  cat("\nProcessando prova:", prov, "com", nrow(subset_prova), "itens\n")
  
  tryCatch({
    criar_modelo_por_prova(subset_prova, prov, pastaSaida)
  }, error = function(e) {
    cat("ERRO ao processar prova", prov, ":", e$message, "\n")
  })
}

cat("\nProcessamento concluído.\n")
