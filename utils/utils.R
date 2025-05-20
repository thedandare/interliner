# ------------------------------------------------------------------------------
# File: utils.R
# Description: Utility functions for the IRT pipeline
# ------------------------------------------------------------------------------

# Function to get model filename
get_model_filename <- function(co_prova) {
  return(file.path(
    config$processing$model_path,
    paste0("modelo_", co_prova, config$processing$model_suffix)
  ))
}

# Function to validate responses
validate_responses <- function(responses, model) {
  if (length(responses) == 0) {
    warning("No valid responses found")
    return(FALSE)
  }
  
  if (length(responses) != ncol(model@Data$data)) {
    warning(paste0("Response length (", length(responses), ") doesn't match model dimensions (", 
                   ncol(model@Data$data), ")"))
    return(FALSE)
  }
  return(TRUE)
}

# Function to create response pattern
create_response_pattern <- function(responses, model) {
  response_pattern <- matrix(responses, nrow=1)
  colnames(response_pattern) <- colnames(model@Data$data)
  return(response_pattern)
}

# Function to calculate accuracy
calculate_accuracy <- function(responses) {
  if (length(responses) == 0) return(0)
  return(sum(responses) / length(responses))
}

# Function to handle errors
handle_error <- function(e, context) {
  if (config$error$log_errors) {
    warning(paste0("Error in ", context, ": ", e$message))
  }
  if (config$error$max_retries > 0) {
    Sys.sleep(config$error$retry_delay)
  }
  return(NULL)
}

# Export all functions
utils <- list(
  get_model_filename = get_model_filename,
  validate_responses = validate_responses,
  create_response_pattern = create_response_pattern,
  calculate_accuracy = calculate_accuracy,
  handle_error = handle_error
)
