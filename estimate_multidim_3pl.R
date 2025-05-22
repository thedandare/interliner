# ------------------------------------------------------------------------------
# File: estimate_multidim_3pl.R
# Description: Estimate 3PL item parameters using all ENEM responses
#              in a four-dimensional MIRT model (one factor per knowledge area).
#
# Usage:
#   Rscript estimate_multidim_3pl.R <csv_path> <output_csv>
# ------------------------------------------------------------------------------

# Memory management - print diagnostic information
cat(paste0("[INFO] R Version: ", R.version.string, "\n"))
# R 4.5.0 doesn't use memory.limit anymore

# Load required packages with error handling
suppressMessages(library(data.table))
suppressMessages(library(mirt))

# Set up parallel processing
suppressMessages(library(parallel))
suppressMessages(library(foreach))
suppressMessages(library(doParallel))

# Check and install missing packages if needed
required_packages <- c("stringi", "parallel", "foreach", "doParallel")
for (pkg in required_packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    cat(paste0("[INFO] Installing required package: ", pkg, "\n"))
    install.packages(pkg, repos = "https://cloud.r-project.org", quiet = TRUE)
    suppressMessages(library(pkg, character.only = TRUE))
  }
}

# Load stringi after checking installation
suppressMessages(library(stringi))

# Configure parallelization
num_cores <- 24  # Use all 24 cores available on the system
cat(sprintf("[INFO] Setting up parallel processing with %d cores\n", num_cores))

# Set a relatively large BLAS thread count to 1
# (we'll handle parallelization ourselves)
if (exists("setMKLthreads")) setMKLthreads(1)

# Parse command line arguments more carefully to handle R-specific args
all_args <- commandArgs()
trailing_args <- commandArgs(trailingOnly = TRUE)

# Filter out any R-specific arguments (those starting with --)
user_args <- trailing_args[!grepl("^--", trailing_args)]

csv_path <- ifelse(length(user_args) >= 1, user_args[1], "B:\\Corpus\\enem\\microdados_enem_2023\\DADOS\\MICRODADOS_ENEM_2023.csv")
output_csv <- ifelse(length(user_args) >= 2, user_args[2], "estimated_item_params.csv")

# Check for memory size argument
mem_size_arg <- grep("--max-mem-size", all_args, value = TRUE)
if (length(mem_size_arg) > 0) {
  cat(paste0("[INFO] Found memory size argument: ", mem_size_arg, "\n"))
}

# --------------------------- Helper functions ---------------------------------

# Optimized helper function to build response matrix with better memory management
# and filtering for zero-variance items (which can't be estimated)
build_response_matrix_optimized <- function(df) {
  process_domain <- function(responses_col, gabarito_col, domain_prefix) {
    # Ensure df is not empty and columns exist
    if (nrow(df) == 0) return(matrix(0L, nrow = 0, ncol = 0))
    
    # Get number of items from the first non-NA gabarito string
    first_valid_gab <- first(na.omit(gabarito_col))
    if (is.na(first_valid_gab)) return(matrix(0L, nrow = nrow(df), ncol = 0)) # No valid gabarito
    num_items <- nchar(first_valid_gab)
    if (num_items == 0) return(matrix(0L, nrow = nrow(df), ncol = 0))

    domain_resp_mat <- matrix(0L, nrow = nrow(df), ncol = num_items)
    item_has_variance <- logical(num_items)

    for (j in 1:num_items) {
      if (length(gabarito_col) > 0 && length(responses_col) > 0) {
        # Use substr if stringi isn't loaded, otherwise use stri_sub
        if ("package:stringi" %in% search()) {
          item_resps <- stri_sub(responses_col, j, j)
          item_gabs  <- stri_sub(gabarito_col, j, j)
        } else {
          item_resps <- substr(responses_col, j, j)
          item_gabs  <- substr(gabarito_col, j, j)
        }
        
        # Scoring logic: '*' or '.' is 0 (incorrect), otherwise 1 if resp==gab, else 0.
        scores <- ifelse(item_resps %in% c("*", ".") | is.na(item_resps) | is.na(item_gabs), 
                         0L, 
                         as.integer(item_resps == item_gabs))
        domain_resp_mat[, j] <- scores
        
        # Check if this item has variance (critical for IRT estimation)
        # Need at least one 0 and one 1 to be estimable
        has_zero <- any(scores == 0, na.rm = TRUE)
        has_one <- any(scores == 1, na.rm = TRUE)
        item_has_variance[j] <- has_zero && has_one
      } else {
        # If no data, mark as no variance
        item_has_variance[j] <- FALSE
      }
    }
    
    # Only keep items with variance
    valid_columns <- which(item_has_variance)
    if (length(valid_columns) == 0) {
      cat(paste0("[WARN] All ", domain_prefix, " items lack variance and will be filtered out\n"))
      return(matrix(0L, nrow = nrow(df), ncol = 0))
    } else if (length(valid_columns) < num_items) {
      cat(paste0("[INFO] Filtered out ", num_items - length(valid_columns), " ", 
                 domain_prefix, " items with no variance\n"))
      domain_resp_mat <- domain_resp_mat[, valid_columns, drop = FALSE]
    }
    
    # Assign proper column names based on the original positions
    colnames(domain_resp_mat) <- sprintf("%s_%02d", domain_prefix, valid_columns)
    domain_resp_mat
  }

  # Process each domain separately
  resp_ch <- process_domain(df$TX_RESPOSTAS_CH, df$TX_GABARITO_CH, "CH")
  resp_cn <- process_domain(df$TX_RESPOSTAS_CN, df$TX_GABARITO_CN, "CN")
  resp_mt <- process_domain(df$TX_RESPOSTAS_MT, df$TX_GABARITO_MT, "MT")
  resp_lc <- process_domain(df$TX_RESPOSTAS_LC, df$TX_GABARITO_LC, "LC")
  
  all_matrices <- list(resp_ch, resp_cn, resp_mt, resp_lc)
  valid_matrices <- Filter(function(m) ncol(m) > 0, all_matrices)
  
  if (length(valid_matrices) == 0) {
    return(matrix(0L, nrow = nrow(df), ncol = 0))
  }
  
  # Combine all valid matrices
  result <- do.call(cbind, valid_matrices)
  return(result)
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

# Process a single exam combination
process_exam_combination <- function(subset_data, exam_combo_values) {
  # subset_data is the data for the current group
  # exam_combo_values contains the values of the grouping columns
  
  current_combo_str <- paste(unlist(exam_combo_values), collapse = ", ")
  cat(sprintf("[FIT] Exam combo: %s (Students: %d)\n", 
              current_combo_str, nrow(subset_data)))

  # Skip if too few students for reliable estimation
  if (nrow(subset_data) < 15) { # Increased minimum to 15 for better stability
    cat("[SKIP] Insufficient data (N =", nrow(subset_data), ")\n")
    return(NULL)
  }
  
  # Force garbage collection before large operations
  gc(verbose = FALSE)
  
  # Build response matrix with the optimized function
  resp_mat <- build_response_matrix_optimized(subset_data)
  
  # Skip if response matrix is empty
  if (nrow(resp_mat) == 0 || ncol(resp_mat) == 0) {
    cat("[SKIP] Empty response matrix\n")
    return(NULL)
  }

  # Check how many items we have in each domain after filtering
  # Count the number of columns for each domain prefix
  count_by_prefix <- function(colnames, prefix) {
    sum(grepl(paste0("^", prefix), colnames))
  }
  
  n_ch <- count_by_prefix(colnames(resp_mat), "CH_")
  n_cn <- count_by_prefix(colnames(resp_mat), "CN_")
  n_mt <- count_by_prefix(colnames(resp_mat), "MT_")
  n_lc <- count_by_prefix(colnames(resp_mat), "LC_")
  total_items <- n_ch + n_cn + n_mt + n_lc
  
  cat(paste0("[INFO] After filtering, using ", n_ch, " CH items, ", n_cn, " CN items, ", 
              n_mt, " MT items, and ", n_lc, " LC items (total: ", total_items, ")\n"))

  # Skip if any domain has zero items or fewer than 3 items total per domain
  # This helps prevent model convergence issues
  min_items_per_domain <- 3
  if (n_ch > 0 && n_ch < min_items_per_domain || 
      n_cn > 0 && n_cn < min_items_per_domain || 
      n_mt > 0 && n_mt < min_items_per_domain || 
      n_lc > 0 && n_lc < min_items_per_domain) {
    cat("[SKIP] Insufficient items in at least one domain\n")
    return(NULL)
  }

  # Skip if total items is too low
  if (total_items < 10) {
    cat("[SKIP] Too few total items (", total_items, ") after filtering\n")
    return(NULL)
  }

  # Create model specification - only include factors with items
  # This more carefully builds the model specification
  model_parts <- character(0)
  current_index <- 1
  
  if (n_ch > 0) {
    model_parts <- c(model_parts, paste0("F1 = ", current_index, "-", current_index + n_ch - 1))
    current_index <- current_index + n_ch
  }
  if (n_cn > 0) {
    model_parts <- c(model_parts, paste0("F2 = ", current_index, "-", current_index + n_cn - 1))
    current_index <- current_index + n_cn
  }
  if (n_mt > 0) {
    model_parts <- c(model_parts, paste0("F3 = ", current_index, "-", current_index + n_mt - 1))
    current_index <- current_index + n_mt
  }
  if (n_lc > 0) {
    model_parts <- c(model_parts, paste0("F4 = ", current_index, "-", current_index + n_lc - 1))
  }
  
  if (length(model_parts) == 0) {
    cat("[SKIP] No valid model specification could be created\n")
    return(NULL)
  }
  
  # Construct the complete model specification
  spec <- mirt::mirt.model(paste(model_parts, collapse = "\n"))
  
  # Set up different model configurations to try, from simplest to most complex
  # Use MCEM method for high-dimensional models as suggested by mirt
  models_to_try <- list(
    # 1PL (Rasch model) is the simplest, fastest, and least memory intensive
    list(name = "1PL", itemtype = "Rasch", 
         technical = list(NCYCLES = 500), 
         method = "MCEM"),
    
    # 2PL is more flexible but still relatively efficient
    list(name = "2PL", itemtype = "2PL", 
         technical = list(NCYCLES = 500, TOL = 0.01), 
         method = "MCEM", 
         control = list(TOL = 0.01))
    
    # 3PL is most memory intensive and slower to converge, only try if others work
    # list(name = "3PL", itemtype = "3PL", 
    #      technical = list(NCYCLES = 500), 
    #      method = "MCEM")
  )
  
  fit <- NULL
  success_model <- NULL
  
  # Try models in order of increasing complexity
  for (model in models_to_try) {
    # Force garbage collection before model fitting
    gc(verbose = FALSE)
    
    tryCatch({
      cat(paste0("[MIRT] Trying ", model$name, " model estimation...\n"))
      
      # For large datasets and high-dimensional models, use appropriate settings
      # These settings should help prevent hanging and reduce memory usage
      fit <- mirt(resp_mat, model = spec, 
                 itemtype = model$itemtype, 
                 verbose = FALSE,
                 method = model$method,                # Use MCEM method for high dimensions
                 technical = model$technical,         # Cycle limits and tolerances
                 control = model$control,             # Additional control parameters
                 TOL = 0.01,                         # Looser tolerance for faster convergence
                 max_theta = 3,                      # Limit integration range
                 dentype = 'Gaussian',                # Gaussian density for MCEM
                 theta_lim = c(-3, 3),               # Limit integration range more strictly
                 ncore = num_cores)                  # Use parallel processing with specified cores
      
      cat(paste0("[MIRT] ", model$name, " model estimation completed successfully\n"))
      success_model <- model$name
      break  # Exit the loop if successful
    }, error = function(e) {
      cat(paste0("[ERROR] in ", model$name, " estimation: ", e$message, "\n"))
      fit <<- NULL  # Ensure fit is NULL if there's an error
    })
  }

  # If all models failed, return NULL
  if (is.null(fit)) {
    cat("[SKIP] All model estimation attempts failed\n")
    return(NULL)
  }
  
  # Extract coefficients with error handling
  result <- tryCatch({
    coefs <- coef(fit, simplify = TRUE)$items
    
    # Collect domain and item information based on column names
    extract_item_info <- function(colname) {
      # Parse format like 'CH_01', 'CN_02', etc.
      parts <- strsplit(colname, "_")[[1]]
      list(domain = parts[1], item_num = as.integer(parts[2]))
    }
    
    # Process each column name
    item_info <- lapply(rownames(coefs), extract_item_info)
    domains <- sapply(item_info, function(x) x$domain)
    item_nums <- sapply(item_info, function(x) x$item_num)
    
    # Get parameters based on the model that was successfully fit
    if (success_model == "3PL") {
      a_val <- coefs[, "a1"]  # Discrimination
      c_val <- coefs[, "g"]   # Guessing parameter
    } else { # 2PL model
      a_val <- coefs[, "a1"]  # Discrimination
      c_val <- rep(0, nrow(coefs))  # No guessing parameter in 2PL
    }
    
    # Calculate difficulty parameter (b) from the threshold parameter (d)
    # Prevent division by zero/NA for b parameter
    b_val <- ifelse(is.na(a_val) | a_val == 0, NA_real_, -coefs[, "d"] / a_val)

    # Create result data.table, using the actual domain and item number from column names
    items_df <- data.table(
      domain = domains,
      co_prova = unlist(exam_combo_values)[1],  # Just use the first value for each domain
      item = sprintf("%02d", item_nums),
      model = success_model,  # Add which model was used
      a = a_val,
      b = b_val,
      c = c_val
    )
    
    # Explicit cleanup of large objects
    rm(resp_mat, fit, coefs)
    gc(verbose = FALSE)
    
    return(items_df)
  }, error = function(e) {
    cat("[ERROR] in coefficient extraction:", e$message, "\n")
    return(NULL)
  })
  
  return(result)
}

# ------------------------------ Main routine ----------------------------------

cat("[LOAD] Reading CSV...\n")
# Only read the columns we need
needed_cols <- c(
  "NU_INSCRICAO",
  "CO_PROVA_CH", "CO_PROVA_CN", "CO_PROVA_MT", "CO_PROVA_LC",
  "TX_RESPOSTAS_CH", "TX_GABARITO_CH",
  "TX_RESPOSTAS_CN", "TX_GABARITO_CN",
  "TX_RESPOSTAS_MT", "TX_GABARITO_MT",
  "TX_RESPOSTAS_LC", "TX_GABARITO_LC"
)

# Use a tryCatch block in case the file can't be read
students <- tryCatch({
  # Try to reduce memory usage while reading
  options(datatable.optimize = Inf)
  
  # Read the CSV file with optimized settings
  df <- fread(
    csv_path,
    sep = ";",
    encoding = "Latin-1",
    colClasses = list(character = "NU_INSCRICAO"),
    select = needed_cols,
    showProgress = FALSE,  # Disable progress bar to save console output
    nThread = 2           # Limit threads to reduce memory consumption
  )
  
  # Print the total number of students loaded
  cat(sprintf("[INFO] Loaded %d students from CSV file\n", nrow(df)))
  
  df
}, error = function(e) {
  cat("[ERROR] Failed to read CSV file:", e$message, "\n")
  NULL
})

# Exit if we couldn't read the file
if (is.null(students)) {
  cat("[ERROR] Exiting due to CSV read failure\n")
  quit(status = 1)
}

# Perform garbage collection after loading large data
cat("[INFO] Initial data loaded, performing garbage collection...\n")
gc(verbose = TRUE)  # Show GC stats once

# Subset to only rows with complete exam codes - this creates a new, smaller data.table
cat("[INFO] Filtering students with all exam codes present...\n")
students_filtered <- students[!is.na(CO_PROVA_CH) & !is.na(CO_PROVA_CN) &
                     !is.na(CO_PROVA_MT) & !is.na(CO_PROVA_LC)]

# Report number of students after filtering
cat(sprintf("[INFO] %d students remain after filtering (%.1f%% of original)\n", 
            nrow(students_filtered), 100 * nrow(students_filtered) / nrow(students)))

# Remove the original larger data.table to free memory
rm(students)
gc(verbose = FALSE)

# Define grouping columns
comb_cols <- c("CO_PROVA_CH", "CO_PROVA_CN", "CO_PROVA_MT", "CO_PROVA_LC")

# Check if students data.table is empty after filtering
if (nrow(students_filtered) == 0) {
  cat("[WARN] No student data remains after filtering\n")
  final_df <- data.table()
} else {
  # Set a parallel approach for processing exam combinations
  combs <- unique(students_filtered[, ..comb_cols])
  num_combs <- nrow(combs)
  
  cat(sprintf("[INFO] Processing %d unique exam combinations using %d parallel cores\n", 
              num_combs, num_cores))
  
  # Filter out small combinations first to avoid wasting resources
  # Calculate the number of students for each combination
  combo_counts <- students_filtered[, .N, by = comb_cols]
  
  # Only keep combinations with sufficient student counts
  valid_combos <- combo_counts[N >= 15]
  
  # Report on filtering
  cat(sprintf("[INFO] Filtering out %d combinations with fewer than 15 students\n", 
              nrow(combo_counts) - nrow(valid_combos)))
  cat(sprintf("[INFO] Processing %d valid combinations\n", nrow(valid_combos)))
  
  # Use a simpler parallelization approach that should avoid serialization issues
  # We'll still use parallel processing through internal mirt mechanisms
  # The mirt package handles parallelization internally via the ncores parameter
  
  # Note: We'll set the number of cores when calling the mirt function instead of using mirt_options
  cat(sprintf("[INFO] Will use %d cores for internal mirt parallelization\n", num_cores))
  
  cat("[INFO] Starting processing with mirt internal parallelization...\n")
  
  # Initialize results storage
  results_list <- list()
  successful_fits <- 0
  
  # Set up memory monitoring function
  memory_usage <- function() {
    gc_info <- gc(full = FALSE, verbose = FALSE)
    mem_used <- sum(gc_info[,2])  # Sum of Vcells and Ncells in MB
    cat(sprintf("[MEMORY] Current usage: %.1f MB\n", mem_used))
    return(mem_used)
  }
  
  # Create incremental results filename
  create_incremental_filename <- function(base_filename, increment) {
    output_parts <- strsplit(base_filename, "\\.")[[1]]
    if (length(output_parts) > 1) {
      base_name <- paste(output_parts[1:(length(output_parts)-1)], collapse = ".")
      extension <- output_parts[length(output_parts)]
      return(sprintf("%s_part%03d.%s", base_name, increment, extension))
    } else {
      return(sprintf("%s_part%03d", base_filename, increment))
    }
  }
  
  # Process combinations in a regular loop, but with internal parallel processing
  increment_counter <- 1  # For incremental filenames
  save_interval <- 5      # Save every 5 successful fits
  
  for (i in 1:nrow(valid_combos)) {
    # Print progress and memory usage regularly
    if (i %% 5 == 0 || i == 1) {
      cat(sprintf("[INFO] Processing combination %d/%d (%d successful so far)\n", 
                i, nrow(valid_combos), successful_fits))
      memory_usage()
    }
    
    comb <- valid_combos[i]
    
    # Create subset for this combination
    subset_df <- students_filtered[
      CO_PROVA_CH == comb$CO_PROVA_CH &
      CO_PROVA_CN == comb$CO_PROVA_CN &
      CO_PROVA_MT == comb$CO_PROVA_MT &
      CO_PROVA_LC == comb$CO_PROVA_LC
    ]
    
    # Process this combination with error handling
    result <- NULL
    tryCatch({
      result <- process_exam_combination(subset_df, comb)
      
      # Add successful result to the list
      if (!is.null(result)) {
        results_list[[length(results_list) + 1]] <- result
        successful_fits <- successful_fits + 1
        
        # Save results incrementally
        if (length(results_list) %% save_interval == 0) {
          # Combine current results
          current_results <- rbindlist(results_list, fill = TRUE)
          
          # Add timestamp for tracking
          current_results[, timestamp := Sys.time()]
          
          # Create incremental filename
          incr_filename <- create_incremental_filename(output_csv, increment_counter)
          increment_counter <- increment_counter + 1
          
          # Save incremental results
          fwrite(current_results, incr_filename)
          cat(sprintf("[SAVE] Saved %d item parameters to %s\n", nrow(current_results), incr_filename))
        }
        
        # Force garbage collection after each successful fit
        rm(subset_df)
        gc(verbose = FALSE)
      }
    }, error = function(e) {
      cat(sprintf("[ERROR] Failed processing combination %d: %s\n", i, e$message))
    })
  }
  
  # Note: Using mirt's internal parallelization, no cluster to shut down
  
  # Filter out NULL results
  results_list <- results_list[!sapply(results_list, is.null)]
  successful_fits <- length(results_list)
  
  # Force garbage collection
  gc(verbose = FALSE, full = TRUE)
  
  # Create final results file if we have any remaining results
  cat(sprintf("[INFO] Completed processing with %d successful model fits\n", successful_fits))
  
  if (length(results_list) > 0) {
    # One final save of any remaining results
    final_df <- rbindlist(results_list, fill = TRUE)
    
    # Add timestamp for tracking
    final_df[, timestamp := Sys.time()]
    
    # Create final filename
    final_filename <- sprintf("%s_final_%d_items.csv", 
                            gsub("\\.csv$", "", output_csv), 
                            nrow(final_df))
    
    # Save final results
    fwrite(final_df, final_filename)
    cat(sprintf("[SAVE] Saved final %d item parameters to %s\n", 
                nrow(final_df), final_filename))
    
    # Clean up large objects
    rm(results_list, final_df)
    gc(verbose = FALSE)
  } else {
    cat("[INFO] No additional results to save in final file\n")
  }
}

# Save a summary of all incremental files
if (increment_counter > 1) {
  cat("[INFO] Creating summary of all result files...\n")
  
  # Get all part files
  file_pattern <- gsub("\\.csv$", "_part\\d+\\.csv", output_csv)
  part_files <- list.files(path = ".", pattern = file_pattern, full.names = TRUE)
  
  if (length(part_files) > 0) {
    summary_filename <- sprintf("%s_SUMMARY.txt", gsub("\\.csv$", "", output_csv))
    summary_file <- file(summary_filename, "w")
    
    cat(sprintf("Processing summary for %s\n", Sys.time()), file = summary_file)
    cat(sprintf("Total successful fits: %d\n", successful_fits), file = summary_file)
    cat("\nIncremental result files:\n", file = summary_file)
    
    # List all result files with item counts
    for (file in part_files) {
      items <- try(nrow(fread(file, nrows = 1)), silent = TRUE)
      if (inherits(items, "try-error")) items <- "UNKNOWN"
      cat(sprintf("  %s (%s items)\n", basename(file), items), file = summary_file)
    }
    
    close(summary_file)
    cat(sprintf("[DONE] Created summary in %s\n", summary_filename))
  } else {
    cat("[WARN] No part files found for summary\n")
  }
} else {
  cat("[INFO] No incremental files were created\n")
}

# Final cleanup
rm(list = ls())
gc(verbose = TRUE)
cat("[INFO] Script completed successfully\n")

