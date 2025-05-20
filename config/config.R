# ------------------------------------------------------------------------------
# File: config.R
# Description: Configuration settings for the IRT pipeline
# ------------------------------------------------------------------------------

# Database configuration
db_config <- list(
  path = "B:\\Corpus\\enem\\2023.db",  # Update this path to your actual database location
  query_limit = 50,  # Number of rows per query
  offset = 0L        # Starting offset
)

# Processing configuration
processing_config <- list(
  max_parallel_r = 60,    # Maximum concurrent processes
  is_unidimensional = FALSE,  # Dimensionality of the model
  model_path = "models",     # Directory for model files
  model_suffix = "_3PL.rds",  # Suffix for model files
  response_method = "MAP",    # Scoring method (EAP, MAP, ML)

  debug = TRUE             # Enable debug logging
)

# Error handling configuration
error_config <- list(
  max_retries = 3,        # Maximum retries for failed processes
  retry_delay = 5,        # Delay between retries (seconds)
  log_errors = TRUE       # Whether to log errors
)

# Export the configuration
config <- list(
  db = db_config,
  processing = processing_config,
  error = error_config
)
