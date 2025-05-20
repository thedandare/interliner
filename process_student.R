# ------------------------------------------------------------------------------
# File: process_student.R
# Description: Function to process a single student record for IRT scoring
# ------------------------------------------------------------------------------

# Load required libraries
library(mirt)

# Load configuration
source("i:\\code\\dart_irt_project\\lib\\scripts\\config\\config.R")

# Load utilities
source("i:\\code\\dart_irt_project\\lib\\scripts\\utils\\utils.R")


build_responses_by_domain <- function(st_row, domain) {
    # Print debug info for the requested domain and columns
    resp_col <- paste0("TX_RESPOSTAS_", domain)
    gab_col <- paste0("TX_GABARITO_", domain)
    cat(sprintf("[DEBUG] build_responses_by_domain: domain=%s, resp_col=%s, gab_col=%s\n", domain, resp_col, gab_col))
    cat("[DEBUG] st_row[resp_col]: ", st_row[[resp_col]], "\n")
    cat("[DEBUG] st_row[gab_col]: ", st_row[[gab_col]], "\n")
    check_correct <- function(resp, gab) {
      if (resp %in% c("*", ".")) {
        return(0L)
      }
      return(as.integer(resp == gab))
    }

    responses <- integer(0)
    # Process only the specified domain
    if (!is.null(st_row[[resp_col]]) && !is.null(st_row[[gab_col]])) {
      for (i in seq_len(nchar(st_row[[resp_col]]))) {
        resp <- substr(st_row[[resp_col]], i, i)
        gab <- substr(st_row[[gab_col]], i, i)
        responses <- c(responses, check_correct(resp, gab))
      }
    }
    return(responses)
  }


process_single_student <- function(student_row, is_unidimensional, model_cache = NULL) {
  # Explicitly load the mirt package in the child process
  if (!"mirt" %in% .packages()) {
    suppressPackageStartupMessages(library(mirt))
  }
  # Initialize results collection
  student_id <- student_row$NU_INSCRICAO
  message("Processing student: ", student_id)
  # Process each knowledge domain separately
  results <- list()
  theta_results <- character(0)

  # Use model_dir from global env if available, else fallback to config
  model_dir <- if (exists("model_dir", envir = .GlobalEnv)) get("model_dir", envir = .GlobalEnv) else config$processing$model_path
  message("[DEBUG] model_dir set to: ", model_dir)
  co_prova_ch <- student_row$CO_PROVA_CH
  co_prova_cn <- student_row$CO_PROVA_CN
  co_prova_mt <- student_row$CO_PROVA_MT
  co_prova_lc <- student_row$CO_PROVA_LC
  message("[DEBUG] CO_PROVA values: CH=", co_prova_ch, ", CN=", co_prova_cn, ", MT=", co_prova_mt, ", LC=", co_prova_lc)
  
  domain_co_provas <- list(
    CH = co_prova_ch,
    CN = co_prova_cn,
    MT = co_prova_mt,
    LC = co_prova_lc
  )

  for (domain in names(domain_co_provas)) {
    co_prova <- domain_co_provas[[domain]]
    message("[STEP] Processing domain '", domain, "' with CO_PROVA=", co_prova)
    # Use model_cache if provided
    general_model <- NULL
    if (!is.null(model_cache)) {
      cache_key_general <- paste0(co_prova, "_general")
      cache_key_domain <- paste0(co_prova, "_domain")
      if (!is.null(model_cache[[cache_key_general]])) {
        message("[DEBUG] Loaded general model from cache for CO_PROVA=", co_prova)
        general_model <- model_cache[[cache_key_general]]
      }
    }
    general_model_file <- file.path(model_dir, paste0("modelo_", co_prova, "_3PL.rds"))
    message("[DEBUG] Looking for general model file: ", general_model_file)
    if (is.null(general_model) && file.exists(general_model_file)) {
      message("[DEBUG] General model file found: ", general_model_file)
      general_model <- readRDS(general_model_file)
    } else if (is.null(general_model)) {
      warning("[WARN] General model not found for CO_PROVA=", co_prova, " (expected file: ", general_model_file, ")")
      general_model <- NULL
    }

    # Build responses for this domain
    domain_responses <- build_responses_by_domain(student_row, domain)
    message("[DEBUG] Responses for domain '", domain, "': ", paste(domain_responses, collapse=","))
    # Skip if no responses for this domain
    if (length(domain_responses) == 0) {
      warning("[WARN] No responses found for domain '", domain, "' (CO_PROVA=", co_prova, ")")
      results[[domain]] <- list(
        status = "ERROR",
        inscricao = student_row$NU_INSCRICAO,
        domain = domain,
        reason = "No responses found",
        accuracy = 0.0
      )
      theta_results <- c(theta_results, paste0(domain, ": N/A"))
      next
    }

    # Try to load domain-specific model
    fixed_model <- NULL
    if (!is.null(model_cache) && !is.null(model_cache[[cache_key_domain]])) {
      message("[DEBUG] Loaded domain-specific model from cache for CO_PROVA=", co_prova)
      fixed_model <- model_cache[[cache_key_domain]]
    }
    model_file <- file.path(model_dir, paste0("modelo_", co_prova, "_", co_prova, "_3PL.rds"))
    message("[DEBUG] Looking for domain-specific model file: ", model_file)
    if (is.null(fixed_model) && file.exists(model_file)) {
      message("[DEBUG] Domain-specific model file found: ", model_file)
      fixed_model <- readRDS(model_file)
    } else if (is.null(fixed_model) && !is.null(general_model)) {
      message("[DEBUG] Using general model for domain '", domain, "' (CO_PROVA=", co_prova, ")")
      fixed_model <- general_model
    } else if (is.null(fixed_model)) {
      warning("[WARN] No model found for domain '", domain, "' (CO_PROVA=", co_prova, ")")
      results[[domain]] <- list(
        status = "ERROR",
        inscricao = student_row$NU_INSCRICAO,
        domain = domain,
        reason = paste0("No model for CO_PROVA=", co_prova),
        accuracy = 0.0
      )
      theta_results <- c(theta_results, paste0(domain, ": N/A"))
      next
    }
    
    # Create response pattern
    response_pattern <- matrix(domain_responses, nrow=1)
    
    # Make sure the response_pattern has the same number of columns as the model data
    if (length(domain_responses) != ncol(fixed_model@Data$data)) {
      # Adjust response pattern to match model dimensions
      if (length(domain_responses) < ncol(fixed_model@Data$data)) {
        # Pad with zeros if we have fewer responses than model items
        response_pattern <- matrix(c(domain_responses, rep(0, ncol(fixed_model@Data$data) - length(domain_responses))), nrow=1)
      } else {
        # Truncate if we have more responses than model items
        response_pattern <- matrix(domain_responses[1:ncol(fixed_model@Data$data)], nrow=1)
      }
    }
    colnames(response_pattern) <- colnames(fixed_model@Data$data)
    
    # Score the student (EAP, MAP, or ML)
    theta_est <- 0
    se_est <- 1
    
    tryCatch({
      # Use fully qualified namespace calls for mirt functions
      fs <- mirt::fscores(
        fixed_model,
        response.pattern = response_pattern,
        method = "EAP",  #config$processing_config$response_method,    # or "MAP", "ML"
        full.scores = TRUE,
        full.scores.SE = TRUE,
        quadpts = 21
      
      )
      
      # Extract the factor score (theta) and SE
      theta_est <- fs[1, "F1"]
      se_est <- fs[1, "SE_F1"]
    }, error = function(e) {
      warning("[WARN] Error in fscores for domain ", co_prova, ": ", e$message)
    })
    
    # Calculate accuracy
    accuracy <- 0.0
    if (length(domain_responses) > 0) {
      accuracy <- sum(domain_responses) / length(domain_responses)
    }
    
    # Format theta result for this domain
    theta_results <- c(theta_results, paste0(domain, ": θ=", sprintf("%.2f", theta_est), 
                                          " (SE=", sprintf("%.2f", se_est), ", Acc=", sprintf("%.2f", accuracy), ")"))
    
    # Store results for this domain
    results[[domain]] <- list(
      status = "OK",
      inscricao = student_row$NU_INSCRICAO,
      domain = domain,
      co_prova = co_prova,
      accuracy = accuracy,
      theta = theta_est,
      se = se_est
    )
  }
  
  # Print a single line with all theta results
  message(paste0("Student ", student_id, ": ", paste(theta_results, collapse = " | ")))
  
  # Return combined results for all domains
  return(list(
    status = "OK",
    inscricao = student_row$NU_INSCRICAO,
    # Removed generic co_prova which was using the last domain's value
    student_row = student_row,
    domains = results
  ))
}
