# ------------------------------------------------------------------------------
# File: estimate_multidim_3pl.R
# Description: Estimate 3PL item parameters using all ENEM responses
#              in a four-dimensional MIRT model (one factor per knowledge area).
#
# Usage:
#   Rscript estimate_multidim_3pl.R <csv_path> <output_csv>
# ------------------------------------------------------------------------------

suppressMessages(library(data.table))
suppressMessages(library(mirt))

args <- commandArgs(trailingOnly = TRUE)
csv_path   <- ifelse(length(args) >= 1, args[1], "MICRODADOS_ENEM_2023.csv")
output_csv <- ifelse(length(args) >= 2, args[2], "estimated_item_params.csv")

# --------------------------- Helper functions ---------------------------------

# Convert response and gabarito strings to a 0/1 vector
resp_to_binary <- function(resp, gab) {
  v <- integer(nchar(resp))
  for (i in seq_len(nchar(resp))) {
    r <- substr(resp, i, i)
    g <- substr(gab, i, i)
    if (r %in% c("*", ".")) {
      v[i] <- 0L
    } else {
      v[i] <- as.integer(r == g)
    }
  }
  v
}

# Build a response matrix for a specific combination of exam codes
build_response_matrix <- function(df) {
  resp_ch <- t(mapply(resp_to_binary, df$TX_RESPOSTAS_CH, df$TX_GABARITO_CH))
  resp_cn <- t(mapply(resp_to_binary, df$TX_RESPOSTAS_CN, df$TX_GABARITO_CN))
  resp_mt <- t(mapply(resp_to_binary, df$TX_RESPOSTAS_MT, df$TX_GABARITO_MT))
  resp_lc <- t(mapply(resp_to_binary, df$TX_RESPOSTAS_LC, df$TX_GABARITO_LC))

  colnames(resp_ch) <- sprintf("CH_%02d", seq_len(ncol(resp_ch)))
  colnames(resp_cn) <- sprintf("CN_%02d", seq_len(ncol(resp_cn)))
  colnames(resp_mt) <- sprintf("MT_%02d", seq_len(ncol(resp_mt)))
  colnames(resp_lc) <- sprintf("LC_%02d", seq_len(ncol(resp_lc)))

  cbind(resp_ch, resp_cn, resp_mt, resp_lc)
}

# Create a 4-factor model specification given item counts per domain
create_spec <- function(n_ch, n_cn, n_mt, n_lc) {
  total <- n_ch + n_cn + n_mt + n_lc
  mirt::mirt.model(paste0(
    "F1 = 1-", n_ch, "\n",
    "F2 = ", n_ch + 1, "-", n_ch + n_cn, "\n",
    "F3 = ", n_ch + n_cn + 1, "-", n_ch + n_cn + n_mt, "\n",
    "F4 = ", n_ch + n_cn + n_mt + 1, "-", total
  ))
}

# ------------------------------ Main routine ----------------------------------

cat("[LOAD] Reading CSV...\n")
students <- fread(
  csv_path,
  sep = ";",
  encoding = "Latin-1",
  colClasses = list(character = "NU_INSCRICAO")
)

# Keep only rows with all exam codes present
students <- students[!is.na(CO_PROVA_CH) & !is.na(CO_PROVA_CN) &
                     !is.na(CO_PROVA_MT) & !is.na(CO_PROVA_LC)]

# Group by unique combination of exam codes
comb_cols <- c("CO_PROVA_CH", "CO_PROVA_CN", "CO_PROVA_MT", "CO_PROVA_LC")
combs <- unique(students[, ..comb_cols])

results_list <- list()

for (i in seq_len(nrow(combs))) {
  comb <- combs[i]
  cat("[FIT] Exam combo:", as.character(comb), "\n")

  subset_df <- students[CO_PROVA_CH == comb$CO_PROVA_CH &
                         CO_PROVA_CN == comb$CO_PROVA_CN &
                         CO_PROVA_MT == comb$CO_PROVA_MT &
                         CO_PROVA_LC == comb$CO_PROVA_LC]

  resp_mat <- build_response_matrix(subset_df)

  n_ch <- nchar(subset_df$TX_GABARITO_CH[1])
  n_cn <- nchar(subset_df$TX_GABARITO_CN[1])
  n_mt <- nchar(subset_df$TX_GABARITO_MT[1])
  n_lc <- nchar(subset_df$TX_GABARITO_LC[1])

  spec <- create_spec(n_ch, n_cn, n_mt, n_lc)

  fit <- mirt(resp_mat, model = spec, itemtype = "3PL")

  coefs <- coef(fit, simplify = TRUE)$items
  a <- coefs[, "a1"]
  b <- -coefs[, "d"] / coefs[, "a1"]
  c <- coefs[, "g"]

  items <- data.frame(
    domain = rep(c("CH", "CN", "MT", "LC"),
                 c(n_ch, n_cn, n_mt, n_lc)),
    co_prova = rep(unlist(comb), times = c(n_ch, n_cn, n_mt, n_lc)),
    item = unlist(lapply(list(seq_len(n_ch), seq_len(n_cn),
                              seq_len(n_mt), seq_len(n_lc)),
                         function(x) sprintf("%02d", x))),
    a = a,
    b = b,
    c = c,
    stringsAsFactors = FALSE
  )

  results_list[[i]] <- items
}

final_df <- rbindlist(results_list)

fwrite(final_df, output_csv)
cat("[DONE] Saved parameter estimates to", output_csv, "\n")

