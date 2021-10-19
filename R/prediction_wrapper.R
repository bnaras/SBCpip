#%%%%%%%%%%%%%%%%% Prediction Helper Functions %%%%%%%%%%%%%%%%%

#' Generate seed database for use in prediction
#'
#' @param conn the active database connection object
#' @param pred_start_date, the start date for the prediction, used to specify
#'     date range for the seed dataset.
#' @param config the site-specific configuration
#' @return nothing (saves an RDS file with the seed data for 1 day before prediction date)
#' @importFrom DBI dbRemoveTable
#' @importFrom loggit set_logfile loggit
#' @importFrom magrittr %<>%
#' @importFrom dplyr copy_to
#' @export
sbc_build_and_save_seed_db <- function(conn, pred_start_date, config) {
  
  if (!DBI::dbIsValid(conn)) stop("Database connection is invalid. Please reconnect.")
  
  # Remove the model and data_scaling tables because we are setting up a new set of seed data.
  overwrite <- readline(prompt="This function will overwrite existing data tables. Continue? ")
  if (tolower(overwrite) != "yes" & tolower(overwrite) != "y") {
    stop("Seed data build stopped.")
  }
  try(DBI::dbRemoveTable(conn, "model"), TRUE)
  try(DBI::dbRemoveTable(conn, "data_scaling"), TRUE)
  try(DBI::dbRemoveTable(conn, "pred_cache"), TRUE)
  
  seed_start_date <- pred_start_date - config$history_window - 1L
  seed_end_date <- pred_start_date - 1L
  all_seed_dates <- seq.Date(from = seed_start_date, to = seed_end_date, by = 1L)
  
  seed_data <- list(cbc = NULL, census = NULL, transfusion = NULL, surgery = NULL, inventory = NULL)
  
  for (i in seq_along(all_seed_dates)) {
    date_str <- as.character(all_seed_dates[i])
    data_single_day <- process_data_for_date(config = config, date = date_str) # include create_cbc_features in here
    
    # Is there a way around this rbind?
    seed_data$cbc %<>% rbind(data_single_day$cbc)
    seed_data$census %<>% rbind(data_single_day$census)
    seed_data$surgery %<>% rbind(data_single_day$surgery)
    seed_data$transfusion %<>% rbind(data_single_day$transfusion)
    seed_data$inventory %<>% rbind(data_single_day$inventory)
    
  }
  
  # Write a table for every type of data file in the seed window
  for (i in seq_along(seed_data)) {
    table_name <- names(seed_data)[i]
    dplyr::copy_to(conn, data.frame(seed_data[[table_name]], check.names = FALSE), 
                   name = table_name, 
                   overwrite = TRUE,
                   temporary = FALSE)
  }
}

# Helper function to obtain a list of unique dates in the folder following each filename pattern
gather_dates_by_pattern <- function(data_folder, pattern) {
  files <- list.files(path = data_folder, pattern = pattern, full.names = TRUE)

  # Extract the date from each filename (assume date immediately follows pattern in YYYY-MM-DD format)
  dates <- sapply(files, function(x) {
    date <- substring(x, first = stringr::str_length(data_folder) + stringr::str_length(pattern) + 1L, 
                      last = stringr::str_length(data_folder) + stringr::str_length(pattern) + 10L)
    })
  unique(dates)
}

#' Preprocess all available files and add to database
#'
#' @param conn the active database connection object 
#' @param config the site-specific configuration
#' @param updateProgress the callback function to display progress in the shiny app
#' 
#' @return nothing (saves an RDS file with the seed data for 1 day before prediction date)
#' @importFrom loggit set_logfile loggit
#' @importFrom DBI dbRemoveTable
#' @importFrom magrittr %<>%
#' @importFrom dplyr copy_to
#' @export
sbc_build_and_save_full_db <- function(conn, config, updateProgress = NULL) {
  
  if (!DBI::dbIsValid(conn)) stop("Database connection is invalid. Please reconnect.")
  
  try(DBI::dbRemoveTable(conn, "model"), TRUE)
  try(DBI::dbRemoveTable(conn, "data_scaling"), TRUE)
  try(DBI::dbRemoveTable(conn, "pred_cache"), TRUE)
  
  # Use filename prefixes instead of hardcoding the patterns here
  cbc_dates <- gather_dates_by_pattern(config$data_folder, paste0(config$cbc_filename_prefix, "*"))
  census_dates <- gather_dates_by_pattern(config$data_folder, paste0(config$census_filename_prefix, "*"))
  surgery_dates <- gather_dates_by_pattern(config$data_folder, paste0(config$surgery_filename_prefix, "*"))
  transfusion_dates <- gather_dates_by_pattern(config$data_folder, paste0(config$transfusion_filename_prefix, "*"))
  inventory_dates <- gather_dates_by_pattern(config$data_folder, paste0(config$inventory_filename_prefix, "*"))
  
  all_dates <- Reduce(intersect, list(cbc_dates, census_dates, surgery_dates, transfusion_dates, inventory_dates))
  print(all_dates)
  
  # Make sure we have files that are contiguous in time. Break at the first non-contiguous point
  cut_point <- length(all_dates)
  for (i in seq_len(length(all_dates) - 1L)) {
    if (as.Date(all_dates[i]) + 1 != as.Date(all_dates[i + 1L])) {
      cut_point <- i
      warning(sprintf("Gap in data_folder dates found after %s, Excluding data after this point.", 
                      all_dates[i]))
      break
    }
  }
  all_dates <- all_dates[1:(max(cut_point, 4) - 3)] # Exclude last 3 files because surgery requires next 3 days.
  # Likely should remove the minus 3 ^ later

  full_data <- list(cbc = NULL, census = NULL, transfusion = NULL, surgery = NULL, inventory = NULL)
  
  for (i in seq_along(all_dates)) {
    date_str <- all_dates[i]
    data_single_day <- process_data_for_date(config = config, date = date_str)

    full_data$cbc %<>% rbind(data_single_day$cbc)
    full_data$census %<>% rbind(data_single_day$census)
    full_data$surgery %<>% rbind(data_single_day$surgery)
    full_data$transfusion %<>% rbind(data_single_day$transfusion)
    full_data$inventory %<>% rbind(data_single_day$inventory)
    
    if (is.function(updateProgress)) {
      updateProgress(detail = sprintf("Last processed date: %s (%.2f %%)", date_str, 100 * i / length(all_dates)))
    }
  }
  
  # Write a table for every type of data file in the seed window
  for (i in seq_along(full_data)) {
    table_name <- names(full_data)[i]
    conn %>% dplyr::copy_to(data.frame(full_data[[table_name]], check.names = FALSE), 
                            name = table_name, 
                            overwrite = TRUE,
                            temporary = FALSE)
  }
  # Return first and last date processed.
  c(all_dates[1], all_dates[2])
}

#' Database version of range prediction
#'
#' @param conn, the database connection object
#' @param pred_start_date, the start date for the prediction, used to specify
#'     date range for the seed dataset.
#' @param num_days, the number of days to predict from pred_start_date
#' @param config the site-specific configuration
#' @param updateProgress a callback function to update progress for the shiny app (optional)
#' @return a prediction tibble named prediction_df with a column for date and the prediction.
#'     This function is essentially a wrapper for SBCpip::predict_for_date across a specific
#'     date range.
#' @note Assumes that the config output folder contains a seed dataset labeled one day prior
#'     to the prediction date range.
#' @importFrom loggit loggit
#' @importFrom DBI dbDisconnect dbIsValid
#' @export
sbc_predict_for_range_db <- function(conn, pred_start_date, num_days, config, updateProgress = NULL) {
  
  if (!DBI::dbIsValid(conn)) stop("Database connection is invalid. Please reconnect.")
  
  # offset by 1 bc the files we process are for the next day1
  pred_end_date <- pred_start_date + num_days 
  all_pred_dates <- seq.Date(from = pred_start_date, to = pred_end_date, by = 1L) 
  
  prediction_df <- NULL
  for (i in seq_along(all_pred_dates)) {
    date_str <- as.character(all_pred_dates[i])
    loggit::loggit(log_lvl = "INFO", log_msg = paste0("Predicting for date: ", date_str))
    result <- conn %>% predict_for_date_db(config = config, date = date_str)
    prediction_df <- rbind(prediction_df,result)
    
    if (is.function(updateProgress)) {
      updateProgress(detail = sprintf("Last predicted date: %s (%.2f %%)", date_str, 100 * i / num_days))
    }
    
  }
  
  prediction_df
}

#' Given the configuration and prediction data frame, build a prediction table
#'
#' @param conn the database connection obejct
#' @param config the site configuration
#' @param start_date the starting date in YYYY-mm-dd format
#' @param end_date the end date in YYYY-mm-dd format, by default today plus 2 days
#' @param pred_tbl a table of predictions generated by predict_for_date
#' @param offset the day before we start evaluating the model (i.e. collecting units)
#' @param min_inventory the minimum that needs to be in inventory,
#'     by default what was used in the training, which is config$c0.
#' @return a tibble of several variables, including all columns of
#'     prediction_df, plus units expiring in a day (r1), units
#'     expiring in 2 days (r2), waste (w), collection units (x),
#'     shortage (s) and y prediction and the platelet usage for that
#'     date, suggested values in case of inventory minimum is not met,
#'     inventory columns if available
#' @importFrom magrittr %>%
#' @importFrom rlang .data
#' @importFrom dplyr select left_join lead
#' @importFrom tibble as_tibble
#' @importFrom DBI dbDisconnect dbIsValid dbListTables
#' @export
build_prediction_table_db <- function(conn, 
                                      config, 
                                      start_date, 
                                      end_date = Sys.Date() + 2,
                                      pred_tbl = NULL,
                                      offset = config$start - 1,
                                      min_inventory = config$min_inventory) {

  if (!DBI::dbIsValid(conn)) stop("Database connection is invalid. Please reconnect.")
  db_tablenames <- DBI::dbListTables(conn)
  if (!("transfusion" %in% db_tablenames) | !("inventory" %in% db_tablenames)) {
    stop("Database must contain at least transfusion and inventory tables.")
  }
  
  # If no prediction table is provided, check the database for last prediction
  if (is.null(pred_tbl)) {
    if (!("pred_cache" %in% db_tablenames)) {
      stop("No prediction table found as input or in cache.")
    } else {
      pred_tbl <- conn %>% dplyr::tbl("pred_cache") %>% dplyr::collect()
    }
  }
  
  if ( !(as.Date(start_date) %in% pred_tbl$date) | !(as.Date(end_date) %in% pred_tbl$date))
    stop("The prediction table must contain the requested dates.")
  
  dates <- seq.Date(from = start_date, to = end_date, by = 1L)
  
  # IMPORTANT:
  # d$dataset only includes the "history_window" used to retrain the model + 7 days
  # d$prediction_df includes all dates in the prediction range
  # d$transfusion, d$census, etc. include all seed dates + prediction dates
  d2 <- conn %>% dplyr::tbl("transfusion") %>%
    dplyr::collect() %>%
    dplyr::filter(date >= as.Date(start_date) & date <= as.Date(end_date)) %>%
    dplyr::rename(plt_used = .data$used) %>% 
    dplyr::distinct(date, .keep_all = TRUE)
  
  # Important to replace plt_used and t_pred NA values with 0
  tibble::tibble(date = dates) %>%
    dplyr::left_join(d2, by = "date") %>%
    dplyr::left_join(pred_tbl, by = "date") %>%
    dplyr::distinct(date, .keep_all = TRUE) %>%
    tidyr::replace_na(list(plt_used = 0, t_pred = 0)) ->
    pred_tbl_joined
  
  N <- nrow(pred_tbl_joined)
  if (offset >= N) {
    loggit::loggit(log_lvl = "ERROR", log_msg = "Not enough predictions!")
    stop("Not enough predictions!")
  }
  
  y <- pred_tbl_joined$plt_used
  t_pred <- pred_tbl_joined$t_pred
  initial_expiry_data <- config$initial_expiry_data
  pred_mat <- matrix(0, nrow = N + 3, ncol = 11)
  colnames(pred_mat) <- c("Alert", "r1", "r2", "w", "x", "s",
                          "r1_adj", "r2_adj","w_adj", "x_adj", "s_adj")
  
  pred_mat[offset + (1:3), "x"] <- config$initial_collection_data
  pred_mat[offset + (1:3), "x_adj"] <- config$initial_collection_data
  index <- offset + 1
  
  # Moving average usage that we use to diagnose "high" levels of r1 (impending waste)
  yma <- ma(y, window_size = config$start)
  
  pred_mat[index, "w"] <- pred_mat[index, "w_adj"] <- pip::pos(initial_expiry_data[1] - y[index])
  pred_mat[index, "r1"] <- pred_mat[index, "r1_adj"] <- pip::pos(initial_expiry_data[1] + initial_expiry_data[2] - y[index] - pred_mat[index, "w"])
  pred_mat[index, "s"] <- pred_mat[index, "s_adj"] <- pip::pos(y[1] - initial_expiry_data[1] - initial_expiry_data[2] - pred_mat[index, "x"])
  pred_mat[index, "r2"] <- pred_mat[index, "r2_adj"] <- pip::pos(pred_mat[1, "x"] - pip::pos(y[1]- initial_expiry_data[1] - initial_expiry_data[2]))
  pred_mat[index + 3, "x"] <- pred_mat[index + 3, "x_adj"] <- floor(pip::pos(t_pred[index] - pred_mat[index + 1, "x"] - pred_mat[index + 2, "x"] - min(pred_mat[index, "r1"], yma[index]) - pred_mat[index, "r2"] + 1))
  
  for (i in seq.int(index + 1L, N)) {
    # These are the constraint parameters without adjusting for the minimum inventory
    pred_mat[i, "w"] <- pip::pos(pred_mat[i - 1 , "r1"] - y[i])
    pred_mat[i, "r1"] <- pip::pos(pred_mat[i - 1, "r1"] + pred_mat[i - 1, "r2"] - y[i] - pred_mat[i, "w"])
    pred_mat[i, "s"] <- pip::pos(y[i] - pred_mat[i - 1, "r1"] - pred_mat[i - 1, "r2"] - pred_mat[i, "x"])
    pred_mat[i, "r2"] <- pip::pos(pred_mat[i, "x"] - pip::pos(y[i] - pred_mat[i - 1, "r1"] - pred_mat[i - 1, "r2"]))
    pred_mat[i+3, "x"] <- floor(pip::pos(t_pred[i] - pred_mat[i + 1, "x"] - pred_mat[i + 2, "x"] - min(pred_mat[i, "r1"], yma[i]) - pred_mat[i, "r2"] + 1))
                                
    
    # This set ensures that we have collected not only enough to satisfy our prediction, but
    # also enough to replenish to our minimum inventory.
    pred_mat[i, "w_adj"] <- pip::pos(pred_mat[i - 1 , "r1_adj"] - y[i])
    pred_mat[i, "r1_adj"] <- pip::pos(pred_mat[i - 1, "r1_adj"] + pred_mat[i - 1, "r2_adj"] - y[i] - pred_mat[i, "w_adj"])
    pred_mat[i, "s_adj"] <- pip::pos(y[i] - pred_mat[i - 1, "r1_adj"] - pred_mat[i - 1, "r2_adj"] - pred_mat[i, "x_adj"])
    pred_mat[i, "r2_adj"] <- pip::pos(pred_mat[i, "x_adj"] - pip::pos(y[i] - pred_mat[i - 1, "r1_adj"] - pred_mat[i - 1, "r2_adj"]))
    pred_mat[i+3,"x_adj"] <- floor(pip::pos(t_pred[i] + pip::pos(min_inventory - min(pred_mat[i, "r1_adj"], yma[i]) - pred_mat[i,"r2_adj"]) 
                                                - pred_mat[i + 1, "x_adj"] - pred_mat[i + 2, "x_adj"] - min(pred_mat[i, "r1_adj"], yma[i]) - pred_mat[i, "r2_adj"]))
  }
  
  pred_mat[, "Alert"] <- (pred_mat[, "r1"] + pred_mat[, "r2"] <= config$c0)
  
  conn %>% 
    dplyr::tbl("inventory") %>% 
    dplyr::collect() %>%
    dplyr::mutate(date = as.Date(date)) ->
    inventory
  
  tibble::as_tibble(cbind(pred_tbl_joined, pred_mat[seq_len(N), ])) %>%
    dplyr::mutate(t_true = dplyr::lead(.data$plt_used, 1L) + dplyr::lead(.data$plt_used, 2L) + dplyr::lead(.data$plt_used, 3L)) %>%
    dplyr::relocate(.data$t_true, .after = .data$plt_used) %>%
    dplyr::left_join(inventory, by = "date") ->
    pred_table
  
  names(pred_table) <- c("date",
                         "Platelet usage",
                         "Three-day actual usage",
                         "Three-day prediction",
                         "Alert",
                         "No. expiring in 1 day",
                         "No. expiring in 2 days",
                         "Waste",
                         "No. to collect per prediction",
                         "Shortage",
                         "Adj. no. expiring in 1 day",
                         "Adj. no. expiring in 2 days",
                         "Adj. waste",
                         "Adj. no. to collect",
                         "Adj. shortage",
                         "Inv. count",
                         "Inv. expiring in 1 day",
                         "Inv. expiring in 2 days",
                         "Inv. expiring in 2+ days")
  
  # Compute "true" values for waste, fresh collections, and shortage
  pred_table %>%
    dplyr::mutate(`True Waste` = pip::pos(.data$`Inv. expiring in 1 day` - .data$`Platelet usage`)) %>%
    dplyr::mutate(`Fresh Units Collected` = ifelse(dplyr::lead(.data$`Inv. expiring in 2 days`, 1) > 0, 
                                                  (dplyr::lead(.data$`Inv. count`, 1) - ((.data$`Inv. count` - .data$`Inv. expiring in 2+ days`) -.data$`Platelet usage` - .data$`True Waste`)),
                                                  ceiling((dplyr::lead(.data$`Inv. expiring in 1 day`, 1) - (.data$`Inv. count` -.data$`Platelet usage` - .data$`True Waste`)))) # Largest value it could possibly be
                  ) %>%
    dplyr::mutate(`True Shortage` = pip::pos(.data$`Platelet usage` - (.data$`Inv. count` - `Inv. expiring in 2+ days`) - .data$`Fresh Units Collected`)) ->
    pred_table
  
  pred_table
}

#' Build a table of coefficients over the prediction period based on the database model table
#' 
#' @param conn the active database connection object
#' @param pred_start_date, the start date for the prediction, used to specify
#'     date range for the seed dataset.
#' @param num_days, the number of days to predict from pred_start_date
#' @return a data frame containing the coefficients generated during the prediction
#'
#' @importFrom loggit loggit
#' @importFrom DBI dbIsValid dbListTables
#' @importFrom dplyr relocate tbl select collect
#' @importFrom magrittr %>%
#' @export
build_coefficient_table_db <- function(conn, pred_start_date, num_days) {
  # Collect model coefficients over all output files (only need one set per week)
  
  if (!DBI::dbIsValid(conn)) stop("Database connection is invalid. Please reconnect.")
  db_tablenames <- DBI::dbListTables(conn)
  if (!("model" %in% db_tablenames)) {
    stop("Database must contain at least model table.")
  }
  
  pred_end_date <- pred_start_date + num_days
  all_pred_dates <- seq.Date(from = pred_start_date, to = pred_end_date, by = 1L)
  
  model_tbl <- conn %>% dplyr::tbl("model") %>% 
    dplyr::filter(as.character(date) %in% all_pred_dates) %>%
    dplyr::collect()
  
  model_tbl
}

#' Compute the Proxy Loss
#'
#' Since we cannot observe shortage, or the true predicted response, the proxy 
#' shortage refers to the total number of inventory platelets below c0
#'
#' @param w a vector of waste determined by pip::compute_prediction_statistics
#' @param r1 a vector of remaining inventory determined by pip::compute_prediction_statistics
#' @param r2 a vector of remaining inventory determined by pip::compute_prediction_statistics
#' @param s a vector of shortage determined by pip::compute_prediction_statistics
#' @param penalty_factor factor to additionally penalize shortage terms over waste
#' @param c0 the minimum inventory level below which we penalize.
#' @return a vector of losses computed for each hyperparameter
#' @importFrom dplyr lead
#' @importFrom pip pos
compute_proxy_loss <- function(w, r1, r2, s, penalty_factor, c0) {
  
  if (nrow(w) != nrow(r1) || nrow(w) != nrow(r2)) {
    stop("Waste, inventory, and/or shortage vectors of different lengths.")
  }
  
  waste_loss <- apply(w, 2, function(x) sum(x)) # total waste
  inv_loss <- apply(r1 + r2, 2, function(x) sum(pip::pos(c0 - x))) # total shortage
  short_loss <- apply(s, 2, function(x) sum(x))
  
  loss <- (waste_loss + penalty_factor * (short_loss + inv_loss)) / nrow(w)
  loss
}


#%%%%%%%%%%%%%%%%% Prediction Table Analysis Wrapper Functions %%%%%%%%%%%%%%%%%
#' Compute the loss value on a validation or test dataset from the original
#' and adjusted waste and inventory levels based on model projections.
#'
#' The loss formula is given by the sum of the waste plus the sum of the squared positive
#' difference between the desired inventory level and the remaining inventory, as per
#' pip::build_model. This can be used to standardize and compare the efficacy of different
#' models.
#'
#' @param pred_table, the prediction table generated from SBCpip::build_prediction_table
#' @param config the site-specific configuration
#' @return a list containing the projected average daily loss and adjusted average daily loss
#' @importFrom pip pos
#' @export
projection_loss <- function(pred_table, config) {
  ## (from pip/func.R) Since we skip for start days and then we see the results
  ## only three days later, we see waste is start + 5
  first_day <- config$start + 6L
  last_day <- nrow(pred_table) - 3L
  pred_table_trunc <- pred_table[seq(first_day, last_day), , drop = FALSE]
  
  loss <- compute_proxy_loss(w = matrix(pred_table_trunc$Waste, ncol = 1),
                             r1 = matrix(pred_table_trunc$`No. expiring in 1 day`, ncol = 1),
                             r2 = matrix(pred_table_trunc$`No. expiring in 2 days`, ncol = 1),
                             s = matrix(pred_table_trunc$Shortage, ncol = 1),
                             penalty_factor = config$penalty_factor,
                             c0 = config$c0)
  
  list(`Avg. Daily Loss` = signif(loss, 5))
  
}

#' Compute the true average daily loss based on actual inventory levels during the
#' prediction period.
#'
#' The loss formula is given by the sum of the waste plus the sum of the squared positive
#' difference between the desired inventory level and the remaining inventory, as per
#' pip::build_model. This can be used to standardize and compare the efficacy of different
#' models.
#'
#' @param pred_table, the prediction table generated from SBCpip::build_prediction_table
#' @param config the site-specific configuration
#' @return a list containing the true average daily loss
#' @importFrom pip pos
#' @export
real_loss <- function(pred_table, config) {
  first_day <- config$start + 6L
  last_day <- nrow(pred_table) - 3L
  pred_table_trunc <- pred_table[seq(first_day,last_day), , drop = FALSE]
  
  remaining_inventory <- pred_table_trunc$`Inv. count` +
    pred_table_trunc$`Fresh Units Collected` -
    pred_table_trunc$`Platelet usage`
  
  
  loss <- compute_proxy_loss(w = matrix(pred_table_trunc$`True Waste`, ncol = 1),
                             r1 = matrix(remaining_inventory, ncol = 1),
                             r2 = matrix(remaining_inventory, ncol = 1), 
                             s = matrix(pred_table_trunc$`True Shortage`, ncol = 1),
                             penalty_factor = config$penalty_factor,
                             c0 = config$c0)
  
  list(`Real Avg. Daily Loss` = signif(loss, 5))
  
}

#' Compute the RMSE of predicted vs. actual next-3-day product usage by day of week
#'
#' Note: Even though wastage minimization is the end-goal of the model (and not just
#' usage prediction), this will help understand the effects of varying config hyperparameters
#' such as c0 and penalty_factor. We can also explore the effects of varying the
#' day(s) of week on which the model is updated, as well as the model_update_frequency.
#'
#' @param pred_table, the prediction table generated from SBCpip::build_prediction_table
#' @param config the site-specific configuration
#' @return a list containing the RMSE for next-3-day product usage by day of week
#' @importFrom dplyr filter
#' @importFrom magrittr %>%
#' @export
prediction_error <- function(pred_table, config) {
  
  first_day <- pred_table$date[config$start + 6L]
  last_day <- pred_table$date[nrow(pred_table) - 3L]
  
  prediction_error <- list("Overall" = NA, "Pos." = NA, "Neg." = NA,
                           "Sunday" = NA, "Monday" = NA, "Tuesday" = NA,
                           "Wednesday" = NA, "Thursday" = NA,
                           "Friday" = NA, "Saturday" = NA)
  
  # Truncate the prediction table
  pred_table_trunc <- pred_table %>%
    dplyr::filter(date >= first_day) %>%
    dplyr::filter(date <= last_day)
  
  # Overall RMSE
  errs <- (pred_table_trunc$`Three-day actual usage` - pred_table_trunc$`Three-day prediction`)^2
  pos_err <- sum(errs[pred_table_trunc$`Three-day actual usage` < pred_table_trunc$`Three-day prediction`])
  neg_err <- sum(errs[pred_table_trunc$`Three-day actual usage` > pred_table_trunc$`Three-day prediction`])
  tot_err <- pos_err + neg_err
  
  prediction_error[["Overall"]] <- signif(sqrt(tot_err / nrow(pred_table_trunc)), 5)
  prediction_error[["Pos."]] <- if (pos_err > 0) {
    signif(sqrt(pos_err / sum(pred_table_trunc$`Three-day actual usage` < pred_table_trunc$`Three-day prediction`)), 5)
  } else 0
  prediction_error[["Neg."]] <- if (neg_err > 0) {
    signif(sqrt(neg_err / sum(pred_table_trunc$`Three-day actual usage` > pred_table_trunc$`Three-day prediction`)), 5)
  } else 0
  
  # RMSE by Day of Week
  for (dow in c("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")) {
    pred_table_weekly <- pred_table_trunc %>% dplyr::filter(weekdays(date) == dow)
    n <- nrow(pred_table_weekly)
    pred_rmse <- signif(sqrt(sum((pred_table_weekly$`Three-day actual usage` - 
                                   pred_table_weekly$`Three-day prediction`)^2) / n), 5)
    prediction_error[[dow]] <- pred_rmse
  }
  
  prediction_error
}

#' Combine projection loss and prediction error calculation into a single evaluation.
#'
#' @param pred_table, the prediction table generated from SBCpip::build_prediction_table
#' @param config the site-specific configuration
#' @return a list containing a summary of the projection loss and prediction error (i.e.
#'         the overall efficacy of the model)
#' @export
pred_table_analysis <- function(pred_table, config) {
  initial_mask <- seq_len(config$start + 5L)
  final_mask <- (nrow(pred_table) - 2L):nrow(pred_table)
  
  pred_stats <- list(pred_start = as.character(pred_table$date[length(initial_mask) + 1L]),
       pred_end = as.character(pred_table$date[final_mask[1L] - 1L]),
       num_days = final_mask[1L] - length(initial_mask) - 1L, # effective number of days
       total_model_waste = sum(pred_table$Waste[-c(initial_mask, final_mask)]),
       total_model_short = sum(pred_table$Shortage[-c(initial_mask, final_mask)]),
       total_real_waste = sum(pred_table$`True Waste`[-c(initial_mask, final_mask)]),
       proj_loss = projection_loss(pred_table, config),
       real_loss = real_loss(pred_table, config),
       three_day_pred_rmse = prediction_error(pred_table, config))
  
  pred_stats
}

#' Combine projection loss and prediction error calculation into a single evaluation.
#'
#' @param coef_table, the coefficient table generated from SBCpip::build_coefficient_table
#' @param config the site-specific configuration
#' @param top_n the number of coefficients that are reported in the table
#' @return a list containing the top 20 coefficient by sum of magnitudes over the analyzed period, 
#' as well as the mean and standard deviation of their values.
#' @importFrom dplyr select mutate group_by summarize arrange slice_head
#' @importFrom tidyr pivot_longer 
#' @importFrom dplyr desc
#' @export
coef_table_analysis <- function(coef_table, config, top_n = 25L) {
  dows <- c("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")
  # sum the absolute values of all of the coefficients (except intercept). 
  # Display 20 coefficients in order of largest absolute sums and state average value over period
  coef_table %>% 
    dplyr::select(-c(.data$intercept, .data$lag_bound, .data$age)) %>%
    tidyr::pivot_longer(-c(date), names_to = "feat") %>%
    dplyr::mutate(abs_val = abs(.data$value)) %>%
    dplyr::group_by(.data$feat) %>%
    dplyr::summarize(abs_sum = sum(.data$abs_val), avg = mean(.data$value), sd = sd(.data$value)) %>% 
    dplyr::arrange(desc(.data$abs_sum)) %>%
    dplyr::slice_head(n = top_n) %>%
    dplyr::select(c(.data$feat, .data$avg, .data$sd)) -> top_coefs
  
  # separate day of week and other features
  list(dow = top_coefs %>% filter(.data$feat %in% dows), 
       other = top_coefs %>% filter(!(.data$feat %in% dows)))
}

#' Generate a recommendation for number of platelets to collect today based on 
#' predicted usage.
#'
#' @param conn the active database connection object
#' @param config the configuration object
#' @param date the date on which we expect to collect new platelets
#' @param pred predicted total platelet usage over the next [product_type + 1] days
#' @param product_type either 1 or 2 depending on the number of days taken to 
#'        prepare the blood product. 
#' @param x_plus1 the number of platelets collected on day [date - product_type], 
#'        which will be available on day [date]
#' @param x_plus2 the number of platelets collected on day [date + 1 - product_type],
#'        which will be available on day [date + 1]. Irrelevant if product_type == 1
#' @param r_1 the number of platelets considered expired after [date]
#' @param r_2 the number of platelets considered expired after [date + 1]
#' @return recommended number of platelets to collect on [date]
#' @importFrom DBI dbIsValid dbListTables
#' @importFrom pip pos 
#' @importFrom dplyr tbl collect filter
#' @export
recommend_collection <- function(conn, config, date, pred, product_type = 2L, 
                                 x_plus1 = 30, x_plus2 = 30, r_1 = 0, 
                                 r_2 = 0) {
  
  if (!DBI::dbIsValid(conn)) stop("Database connection is invalid. Please reconnect.")
  db_tablenames <- DBI::dbListTables(conn)
  if (!("transfusion" %in% db_tablenames)) {
    stop("Database must contain at least the transfusion table.")
  }
  
  transfusion_tbl <- conn %>% 
    dplyr::tbl("transfusion") %>%
    dplyr::collect() %>%
    dplyr::filter(date > as.Date(date) - config$start & date <= as.Date(date))
  
  mean_y_recent <- as.integer(mean(as.numeric(transfusion_tbl$used)))
  
  x_2 <- x_plus2
  if (product_type != 2) x2 <- 0 
  
  floor(pip::pos(pred - x_plus1 - x_2 - min(r_1, mean_y_recent) - r_2 + 1))
}
