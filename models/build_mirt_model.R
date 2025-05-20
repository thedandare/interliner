# ------------------------------------------------------------------------------
# File: build_mirt_model.R
# Description: Functions to build MIRT models with fixed parameters
# ------------------------------------------------------------------------------

library(mirt)

#' Build a fixed 3PL IRT model with specified parameters
#' 
#' @param a_vec Vector of discrimination parameters
#' @param b_vec Vector of difficulty parameters
#' @param c_vec Vector of guessing parameters
#' @param item_names Optional vector of item names (default: "item1", "item2", etc.)
#' @return A fitted mirt model object with fixed parameters
build_fixed_3pl_model <- function(a_vec, b_vec, c_vec, item_names = NULL) {
  # Validate inputs
  k <- length(a_vec)
  if (length(b_vec) != k || length(c_vec) != k) {
    stop("Parameter vectors must all have the same length")
  }
  
  # Create item names if not provided
  if (is.null(item_names)) {
    item_names <- paste0("item", seq_len(k))
  }
  
  # Create minimal dummy data: 3 rows, k columns
  set.seed(123)
  dummy_data <- matrix(rbinom(n = 3*k, size = 1, prob = 0.5), nrow = 3, ncol = k)
  
  # Force some variation if any item is all-0 or all-1
  for (j in seq_len(k)) {
    if (length(unique(dummy_data[, j])) == 1) {
      # Flip one row to ensure variation
      dummy_data[sample(1:3, 1), j] <- 1 - dummy_data[1, j]
    }
  }
  
  # Set column names
  colnames(dummy_data) <- item_names
  
  # Generate initial parameter table for 3PL
  init_tab <- mirt(dummy_data, model = 1, itemtype = "3PL", pars="values", verbose = FALSE)
  
  # Overwrite each item's parameters (a, d, g)
  # In some mirt versions, 3PL has 4 rows per item: a1, d, g, u
  for (j in seq_len(k)) {
    # For 3PL with 'u' param, each item has 4 rows
    row_a <- (j - 1)*4 + 1  # a1
    row_d <- row_a + 1      # d
    row_g <- row_a + 2      # g
    # row_u <- row_a + 3    # often fixed at 1, no need to change
    
    a_val <- a_vec[j]
    b_val <- b_vec[j]
    c_val <- c_vec[j]
    d_val <- -a_val * b_val  # mirt's difficulty param is d = -a*b
    
    init_tab$value[row_a] <- a_val
    init_tab$est[row_a]   <- FALSE
    
    init_tab$value[row_d] <- d_val
    init_tab$est[row_d]   <- FALSE
    
    init_tab$value[row_g] <- c_val
    init_tab$est[row_g]   <- FALSE
  }
  
  # Fix latent distribution to mean=0, var=1 so nothing is estimated
  idx_mean <- which(init_tab$name == "MEAN_1")
  idx_cov  <- which(init_tab$name == "COV_11")
  if (length(idx_mean) == 1 && length(idx_cov) == 1) {
    init_tab$est[idx_mean]   <- FALSE
    init_tab$value[idx_mean] <- 0  # fix latent mean
    init_tab$est[idx_cov]    <- FALSE
    init_tab$value[idx_cov]  <- 1  # fix latent var
  }
  
  # Create the fixed model
  fixed_model <- mirt(
    dummy_data,
    model = 1,
    itemtype = "3PL",
    pars = init_tab,
    verbose = FALSE
  )
  
  return(fixed_model)
}

#' Validate and clean IRT parameters
#' 
#' @param a_params Vector of discrimination parameters
#' @param b_params Vector of difficulty parameters
#' @param c_params Vector of guessing parameters
#' @param item_ids Optional vector of item IDs for error reporting
#' @return A list with cleaned a, b, and c parameter vectors
clean_irt_parameters <- function(a_params, b_params, c_params, item_ids = NULL) {
  # Convert to numeric if needed
  a_params <- as.numeric(a_params)
  b_params <- as.numeric(b_params)
  c_params <- as.numeric(c_params)
  
  # Fix invalid a parameters (should be positive)
  invalid_a <- which(is.na(a_params) | a_params <= 0)
  if (length(invalid_a) > 0) {
    if (!is.null(item_ids)) {
      warning("Invalid a parameters for items: ", paste(item_ids[invalid_a], collapse=", "))
    } else {
      warning("Invalid a parameters at positions: ", paste(invalid_a, collapse=", "))
    }
    a_params[invalid_a] <- 1.0
  }
  
  # Fix invalid b parameters
  invalid_b <- which(is.na(b_params))
  if (length(invalid_b) > 0) {
    if (!is.null(item_ids)) {
      warning("Invalid b parameters for items: ", paste(item_ids[invalid_b], collapse=", "))
    } else {
      warning("Invalid b parameters at positions: ", paste(invalid_b, collapse=", "))
    }
    b_params[invalid_b] <- 0.0
  }
  
  # Fix invalid c parameters (should be between 0 and 1)
  invalid_c <- which(is.na(c_params) | c_params < 0 | c_params >= 1)
  if (length(invalid_c) > 0) {
    if (!is.null(item_ids)) {
      warning("Invalid c parameters for items: ", paste(item_ids[invalid_c], collapse=", "))
    } else {
      warning("Invalid c parameters at positions: ", paste(invalid_c, collapse=", "))
    }
    c_params[invalid_c] <- 0.2
  }
  
  return(list(a = a_params, b = b_params, c = c_params))
}

# Example usage:
a_vec <- c(1.5, 0.9, 2.0, 1.1, 1.7)
b_vec <- c(-0.5, 0.3, 1.2, -0.8, 0.0)
c_vec <- c(0.2, 0.15, 0.25, 0.2, 0.2)
model <- build_fixed_3pl_model(a_vec, b_vec, c_vec)
saveRDS(model, "my_fixed_model.rds")
