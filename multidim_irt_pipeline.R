# ------------------------------------------------------------------------------
# File: multidim_irt_pipeline.R
# Description: Estimate four-dimensional theta vectors from ENEM CSV data
# ------------------------------------------------------------------------------

suppressMessages(library(data.table))
suppressMessages(library(mirt))
suppressMessages(library(future.apply))

# ------------------------------ CONFIG ---------------------------------------
# Path to CSV with all responses
csv_path  <- "B:/Corpus/enem/microdados_enem_2023/DADOS/MICRODADOS_ENEM_2023.csv"
model_dir <- "models"                                 # directory with *_3PL.rds

# Number of workers for parallel processing
num_workers <- 4

# -------------------------- PARAMETER HELPERS --------------------------------

# Extract a,b,c vectors from a unidimensional model file
extract_params <- function(rds_file) {
  mod <- readRDS(rds_file)
  coefs <- coef(mod, simplify = TRUE)$items
  a <- coefs[, "a1"]
  b <- -coefs[, "d"] / coefs[, "a1"]
  c <- coefs[, "g"]
  list(a = a, b = b, c = c)
}

# Build a fixed 4-factor 3PL model given parameter lists for each domain
build_fixed_4f_model <- function(par_ch, par_cn, par_mt, par_lc) {
  a_vec <- c(par_ch$a, par_cn$a, par_mt$a, par_lc$a)
  b_vec <- c(par_ch$b, par_cn$b, par_mt$b, par_lc$b)
  c_vec <- c(par_ch$c, par_cn$c, par_mt$c, par_lc$c)

  n_ch <- length(par_ch$a)
  n_cn <- length(par_cn$a)
  n_mt <- length(par_mt$a)
  n_lc <- length(par_lc$a)
  k    <- n_ch + n_cn + n_mt + n_lc

  dummy <- matrix(rbinom(3 * k, 1, 0.5), nrow = 3)
  colnames(dummy) <- paste0("item", seq_len(k))

  spec <- mirt::mirt.model(paste0(
    "F1 = 1-", n_ch, "\n",
    "F2 = ", n_ch + 1, "-", n_ch + n_cn, "\n",
    "F3 = ", n_ch + n_cn + 1, "-", n_ch + n_cn + n_mt, "\n",
    "F4 = ", n_ch + n_cn + n_mt + 1, "-", k
  ))

  tab <- mirt(dummy, model = spec, itemtype = "3PL", pars = "values", verbose = FALSE)

  offset <- 0
  for (j in seq_len(k)) {
    rows <- which(tab$item == j)
    fac_rows <- grep("^a", tab$name[rows])
    d_row  <- rows[tab$name[rows] == "d"]
    g_row  <- rows[tab$name[rows] == "g"]

    # zero all discriminations
    tab$value[rows[fac_rows]] <- 0
    tab$est[rows[fac_rows]] <- FALSE

    if (j <= n_ch) {
      tab$value[rows[fac_rows[1]]] <- par_ch$a[j]
    } else if (j <= n_ch + n_cn) {
      idx <- j - n_ch
      tab$value[rows[fac_rows[2]]] <- par_cn$a[idx]
    } else if (j <= n_ch + n_cn + n_mt) {
      idx <- j - n_ch - n_cn
      tab$value[rows[fac_rows[3]]] <- par_mt$a[idx]
    } else {
      idx <- j - n_ch - n_cn - n_mt
      tab$value[rows[fac_rows[4]]] <- par_lc$a[idx]
    }

    # difficulty and guessing
    a_val <- tab$value[rows[fac_rows[tab$name[rows[fac_rows]] != "a4"]][1]]  # active a
    if (j <= n_ch) {
      b_val <- par_ch$b[j]
      c_val <- par_ch$c[j]
    } else if (j <= n_ch + n_cn) {
      idx <- j - n_ch
      b_val <- par_cn$b[idx]
      c_val <- par_cn$c[idx]
    } else if (j <= n_ch + n_cn + n_mt) {
      idx <- j - n_ch - n_cn
      b_val <- par_mt$b[idx]
      c_val <- par_mt$c[idx]
    } else {
      idx <- j - n_ch - n_cn - n_mt
      b_val <- par_lc$b[idx]
      c_val <- par_lc$c[idx]
    }
    tab$value[d_row] <- -a_val * b_val
    tab$value[g_row] <- c_val
    tab$est[d_row] <- FALSE
    tab$est[g_row] <- FALSE
  }

  # Fix latent means/covariances
  lat_rows <- grep("MEAN|COV", tab$name)
  tab$est[lat_rows] <- FALSE
  tab$value[grep("MEAN", tab$name)] <- 0
  diag_idx <- which(tab$name %in% paste0("COV_", 1:4, 1:4))
  tab$value[diag_idx] <- 1
  tab$value[setdiff(grep("COV", tab$name), diag_idx)] <- 0

  mirt(dummy, model = spec, itemtype = "3PL", pars = tab, verbose = FALSE)
}

# ------------------------- MODEL CACHE ---------------------------------------

uni_models <- list()
model_cache <- list()

load_unidim <- function(co_prova) {
  key <- as.character(co_prova)
  if (!is.null(uni_models[[key]])) return(uni_models[[key]])
  file <- file.path(model_dir, paste0("modelo_", key, "_3PL.rds"))
  uni_models[[key]] <<- readRDS(file)
  uni_models[[key]]
}

get_4d_model <- function(ch, cn, mt, lc) {
  key <- paste(ch, cn, mt, lc, sep = "_")
  if (!is.null(model_cache[[key]])) return(model_cache[[key]])

  par_ch <- extract_params(load_unidim(ch))
  par_cn <- extract_params(load_unidim(cn))
  par_mt <- extract_params(load_unidim(mt))
  par_lc <- extract_params(load_unidim(lc))

  model_cache[[key]] <<- build_fixed_4f_model(par_ch, par_cn, par_mt, par_lc)
  model_cache[[key]]
}

# ----------------------- STUDENT PROCESSING ----------------------------------

build_resp <- function(resp, gab) {
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

score_student <- function(st_row) {
  ch <- st_row$CO_PROVA_CH
  cn <- st_row$CO_PROVA_CN
  mt <- st_row$CO_PROVA_MT
  lc <- st_row$CO_PROVA_LC

  model <- get_4d_model(ch, cn, mt, lc)

  resp_ch <- build_resp(st_row$TX_RESPOSTAS_CH, st_row$TX_GABARITO_CH)
  resp_cn <- build_resp(st_row$TX_RESPOSTAS_CN, st_row$TX_GABARITO_CN)
  resp_mt <- build_resp(st_row$TX_RESPOSTAS_MT, st_row$TX_GABARITO_MT)
  resp_lc <- build_resp(st_row$TX_RESPOSTAS_LC, st_row$TX_GABARITO_LC)

  pattern <- matrix(c(resp_ch, resp_cn, resp_mt, resp_lc), nrow = 1)
  colnames(pattern) <- colnames(model@Data$data)

  fs <- fscores(model, response.pattern = pattern, method = "EAP",
                full.scores = TRUE, full.scores.SE = TRUE)

  list(nu_inscricao = st_row$NU_INSCRICAO,
       theta = as.numeric(fs[1, c("F1", "F2", "F3", "F4")]),
       se = as.numeric(fs[1, c("SE_F1", "SE_F2", "SE_F3", "SE_F4")]))
}

# ----------------------- MAIN PIPELINE ---------------------------------------

# Read CSV data
students_df <- fread(csv_path, sep = ";", encoding = "Latin-1",
                     colClasses = list(character = "NU_INSCRICAO"))

# Filter rows that have all CO_PROVA columns
students_df <- students_df[!is.na(CO_PROVA_CH) & !is.na(CO_PROVA_CN) &
                           !is.na(CO_PROVA_MT) & !is.na(CO_PROVA_LC)]

plan(multisession, workers = num_workers)

results <- future_lapply(split(students_df, seq_len(nrow(students_df))), score_student)

# Convert to data.frame and save
out_df <- data.frame(
  nu_inscricao = sapply(results, `[[`, "nu_inscricao"),
  theta_CH = sapply(results, function(x) x$theta[1]),
  theta_CN = sapply(results, function(x) x$theta[2]),
  theta_MT = sapply(results, function(x) x$theta[3]),
  theta_LC = sapply(results, function(x) x$theta[4]),
  se_CH = sapply(results, function(x) x$se[1]),
  se_CN = sapply(results, function(x) x$se[2]),
  se_MT = sapply(results, function(x) x$se[3]),
  se_LC = sapply(results, function(x) x$se[4])
)

fwrite(out_df, "theta_results_multidim.csv")

cat("Processed ", nrow(out_df), " students. Results saved to theta_results_multidim.csv\n")

