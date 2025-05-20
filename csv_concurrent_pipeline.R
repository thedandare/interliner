# ------------------------------------------------------------------------------
# File: csv_concurrent_pipeline.R
# Description: High-performance concurrent IRT pipeline using CSV input and DuckDB output
# ------------------------------------------------------------------------------

library(data.table)      # Fast CSV reading
library(DBI)
library(duckdb)
library(mirt)
library(future.apply)
library(progress)

# Load configuration and utilities
source("i:\\code\\dart_irt_project\\lib\\scripts\\process_student.R")
source("i:\\code\\dart_irt_project\\lib\\scripts\\config\\config.R")
source("i:\\code\\dart_irt_project\\lib\\scripts\\utils\\utils.R")


# Utility: Convert a data frame to a list of named lists,
# one per row (for parallel processing)
safe_df_to_list <- function(df) {
  message("Converting data frame with ", nrow(df), " rows to list")
  result <- vector("list", nrow(df))
  for (i in seq_len(nrow(df))) {
    result[[i]] <- as.list(df[i, ])
  }
  return(result)
}

# Add initialize_tables and save_theta_results from simple_concurrent_pipeline.R
initialize_tables <- function(con) {
  if (!dbExistsTable(con, "theta_results")) {
    dbExecute(con, "CREATE TABLE theta_results (
                nu_inscricao VARCHAR,
                co_prova INTEGER,
                domain VARCHAR,
                theta DOUBLE,
                se DOUBLE,
                accuracy DOUBLE,
                processed_at TIMESTAMP,
                PRIMARY KEY (nu_inscricao, co_prova, domain)
              )")
  } else {
    # Add accuracy column if missing
    cols <- dbListFields(con, "theta_results")
    if (!"accuracy" %in% cols) {
      dbExecute(con, "ALTER TABLE theta_results ADD COLUMN accuracy DOUBLE")
    }
  }
}

save_theta_results_bulk <- function(con, batch_results) {
  insert_rows <- list()
  for (res in batch_results) {
    if (!is.null(res) && res$status == "OK") {
      inscricao <- res$inscricao
      co_prova <- res$co_prova
      domains <- res$domains
      for (domain_name in names(domains)) {
        domain_result <- domains[[domain_name]]
        if (!is.null(domain_result) && domain_result$status == "OK") {
          insert_rows[[length(insert_rows)+1]] <- list(
            nu_inscricao=inscricao,
            co_prova=co_prova,
            domain=domain_name,
            theta=domain_result$theta,
            se=domain_result$se,
            accuracy=domain_result$accuracy
          )
        }
      }
    }
  }
  if (length(insert_rows) > 0) {
    df <- do.call(rbind, lapply(insert_rows, as.data.frame))
    # Bulk insert
    dbWriteTable(con, "theta_results", df, append=TRUE, row.names=FALSE)
  }
}

# ---- PARAMETERS ----
# Path to ENEM 2023 CSV (update as needed)
csv_path <- "B:/Corpus/enem/microdados_enem_2023/DADOS/MICRODADOS_ENEM_2023.csv"
model_dir <- "i:/code/dart_irt_project/lib/scripts/models"

# ---- DUCKDB CONNECTION ----
con <- dbConnect(duckdb::duckdb(), dbdir=config$db$path)

# Initialize theta results table (if not exists)
initialize_tables(con)

# ---- LOAD MODELS INTO RAM ----
message("[SETUP] Loading all models into RAM cache...")
model_files <- list.files(model_dir, pattern="\\.rds$", full.names=TRUE)
model_cache <- list()
for (f in model_files) {
  key <- gsub("modelo_|_3PL\\.rds", "", basename(f))
  model_cache[[key]] <- readRDS(f)
}
message("[SETUP] Loaded ", length(model_cache), " models into memory.")

# ---- MEMORY MONITORING ----
if (!requireNamespace("memuse", quietly = TRUE)) install.packages("memuse")
library(memuse)

# ---- LOAD STUDENT DATA FROM CSV WITH ANIMATION ----
library(progress)
message("[SETUP] Reading student data from CSV: ", csv_path)

# Progress animation for fread
pb <- progress_bar$new(
  format = "  Reading CSV [:bar] :percent :elapsed",
  total = 100, clear = FALSE, width = 60
)
update_pb <- function() pb$tick(1)

# fread: force NU_INSCRICAO as character, print columns and sample row
for (i in 1:100) { Sys.sleep(0.01); update_pb() }
students_df <- fread(csv_path, encoding="UTF-8", colClasses=list(character="NU_INSCRICAO"))

message("[SETUP] Loaded ", nrow(students_df), " student records from CSV.")
print(colnames(students_df))
print(students_df[1, ])

# ---- FILTER STUDENTS: Only rows where all CO_PROVA_* exist ----
filtered_df <- students_df[!is.na(CO_PROVA_CH) & !is.na(CO_PROVA_CN) & !is.na(CO_PROVA_MT) & !is.na(CO_PROVA_LC)]
filtered_n <- nrow(filtered_df)
message("[SETUP] Filtered to ", filtered_n, " valid students out of ", nrow(students_df))

# ---- PRINT DEBUG INFO FOR FIRST STUDENT IN BATCH ----
if (filtered_n > 0) {
  cat("\n[DEBUG] Sample student row (first in filtered_df):\n")
  print(filtered_df[1, ])
  cat("\n[DEBUG] Columns in filtered_df:\n")
  print(colnames(filtered_df))
}

# ---- CHUNKED BATCH PROCESSING WITH ANIMATION ----
batch_size <- 20000  # Smaller batch for better parallelism
n <- filtered_n
num_batches <- ceiling(n / batch_size)
message("[SETUP] Will process ", n, " students in ", num_batches, " batches of ", batch_size)

proc_pb <- progress_bar$new(
  format = "  Processing [:bar] :percent :current/:total :elapsed",
  total = n, clear = FALSE, width = 60
)

plan(multisession, workers = 24)

processed_count <- 0
for (i in seq_len(num_batches)) {
  start_row <- ((i-1) * batch_size) + 1
  end_row <- min(i * batch_size, n)
  batch_df <- filtered_df[start_row:end_row, ]
  batch_list <- safe_df_to_list(batch_df)
  batch_results <- future_lapply(
    batch_list,
    function(student_row) {
      process_single_student(student_row, is_unidimensional=FALSE, model_cache=model_cache)
    }
  )
  save_theta_results_bulk(con, batch_results)
  proc_pb$update(end_row / n)
  processed_count <- processed_count + length(batch_results)
  # MEMORY MONITORING after each batch
  meminfo <- memuse::Sys.meminfo()
  cat(sprintf("[MEMORY] Used: %s | Free: %s | Total: %s\n",
              meminfo$ram.used, meminfo$ram.free, meminfo$ram.total))
  rm(batch_df, batch_list, batch_results)
  gc()
}

message("\nPipeline completed. Processed ", processed_count, " valid students.")
dbDisconnect(con, shutdown = TRUE)
