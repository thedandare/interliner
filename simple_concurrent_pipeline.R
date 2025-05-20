# ------------------------------------------------------------------------------
# File: simple_concurrent_pipeline.R
# Description: Production-safe concurrent pipeline for IRT processing (DuckDB-safe)
# ------------------------------------------------------------------------------

library(DBI)
library(duckdb)
library(mirt)
library(future.apply)

# Load configuration and utilities
source("i:\\code\\dart_irt_project\\lib\\scripts\\process_student.R")
source("i:\\code\\dart_irt_project\\lib\\scripts\\config\\config.R")
source("i:\\code\\dart_irt_project\\lib\\scripts\\utils\\utils.R")

# Initialize theta results table
initialize_tables <- function(con) {
  if (!dbExistsTable(con, "theta_results")) {
    dbExecute(con, "CREATE TABLE theta_results (
                nu_inscricao VARCHAR,
                co_prova INTEGER,
                sg_area VARCHAR,
                domain VARCHAR,
                theta DOUBLE,
                se DOUBLE,
                processed_at TIMESTAMP,
                PRIMARY KEY (nu_inscricao, co_prova, domain)
              )")
  }
}

# Save theta results to DuckDB (main process only)
save_theta_results <- function(con, result) {
  if (result$status == "OK") {
    domains <- result$domains
    inscricao <- result$inscricao
    student_row <- result$student_row

    for (domain_name in names(domains)) {
      domain_result <- domains[[domain_name]]
      if (domain_result$status == "OK") {
        # Use the domain-specific co_prova value
        domain_co_prova <- domain_result$co_prova
        
        query <- "INSERT OR REPLACE INTO theta_results (
          nu_inscricao, co_prova, domain, theta, se,
          processed_at
        ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)"
        params <- list(
          as.character(inscricao),
          as.integer(domain_co_prova),
          domain_name,
          as.numeric(domain_result$theta),
          as.numeric(domain_result$se)
        )
        tryCatch(
          {
            dbExecute(con, query, params = params)
            message(sprintf(
              "Saved theta result for student %s, prova %s, domain %s: θ=%.2f",
              inscricao, domain_co_prova, domain_name, domain_result$theta
            ))
          },
          error = function(e) {
            warning("Error saving theta results for domain ", domain_name, ": ", e$message)
          }
        )
      } else {
        message(sprintf(
          "Skipping domain %s for student %s: %s",
          domain_name, inscricao, domain_result$message
        ))
      }
    }
    return(TRUE)
  } else {
    warning("Cannot save results, status is not OK: ", result$message)
    return(FALSE)
  }
}

# Convert dataframe to list safely
safe_df_to_list <- function(df) {
  message("Converting data frame with ", nrow(df), " rows to list")
  result <- list()
  for (i in 1:nrow(df)) {
    result[[i]] <- as.list(df[i, ])
  }
  return(result)
}

# Set the correct absolute path for the models directory
model_dir <- "i:/code/dart_irt_project/lib/scripts/models"

# Restore: establish DuckDB connection at the top-level and close at the very end
con <- dbConnect(duckdb::duckdb(), dbdir=config$db$path)

# Initialize theta results table
initialize_tables(con)

message("[SETUP] Loading all models into RAM cache...")
model_files <- list.files(model_dir, pattern="\\.rds$", full.names=TRUE)
model_cache <- list()
for (f in model_files) {
  key <- gsub("modelo_|_3PL\\.rds", "", basename(f))
  model_cache[[key]] <- readRDS(f)
}
message("[SETUP] Loaded ", length(model_cache), " models into memory.")

# Prepare students data
query <- paste0(
  "SELECT * FROM ALUNOS_PRESENTES ",
  "ORDER BY nu_inscricao ",
  "LIMIT ", 10000
)
students_df <- dbGetQuery(con, query)
message("Fetched ", nrow(students_df), " students for prova ")
students <- safe_df_to_list(students_df)

# Register parallel plan (use as many workers as you have RAM/cores for)
plan(multisession, workers = 16)  # Adjust to your RAM/CPU

# Parallel processing: each worker gets a student and the shared model_cache
results <- future_lapply(
  students,
  function(student_row) {
    process_single_student(student_row, is_unidimensional=FALSE, model_cache=model_cache)
  }
)

# Save results to DuckDB
for (res in results) {
  if (res$status == "OK") {
    save_theta_results(con, res)
  }
}

message("\nPipeline completed")

# At the very end, close the connection
dbDisconnect(con, shutdown = TRUE)
