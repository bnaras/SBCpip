#%%%%%%%%%%%%%%%%% Prediction Helper Functions %%%%%%%%%%%%%%%%%

#' Generate .RDS file for seed data to be used in prediction
#'
#' @param pred_start_date, the start date for the prediction, used to specify
#'     date range for the seed dataset.
#' @param config the site-specific configuration
#' @return nothing (saves an RDS file with the seed data for 1 day before prediction date)
#' @importFrom loggit set_logfile loggit
#' @importFrom magrittr %<>%
#' @export
sbc_build_and_save_seed_data <- function(pred_start_date, config) {
  
  seed_start_date <- pred_start_date - config$history_window - 1L
  seed_end_date <- pred_start_date - 1L
  all_seed_dates <- seq.Date(from = seed_start_date, to = seed_end_date, by = 1L)
  
  seed_data <- list(cbc = NULL, census = NULL, transfusion = NULL, surgery = NULL, inventory = NULL)
  
  for (i in seq_along(all_seed_dates)) {
    date_str <- as.character(all_seed_dates[i])
    data_single_day <- process_data_for_date(config = config, date = date_str)
    seed_data$cbc %<>% rbind(data_single_day$cbc)
    seed_data$census %<>% rbind(data_single_day$census)
    seed_data$surgery %<>% rbind(data_single_day$surgery)
    seed_data$transfusion %<>% rbind(data_single_day$transfusion)
    seed_data$inventory %<>% rbind(data_single_day$inventory)
  }
  
  # Save the seed dataset
  saveRDS(object = seed_data,
          file = file.path(config$output_folder,
                           sprintf(config$output_filename_prefix, as.character(seed_end_date))))
}

#' Predict product usage and corresponding order schedule over specified date range
#'
#' @param pred_start_date, the start date for the prediction, used to specify
#'     date range for the seed dataset.
#' @param num_days, the number of days to predict from pred_start_date
#' @param config the site-specific configuration
#' @return a prediction tibble named prediction_df with a column for date and the prediction.
#'     This function is essentially a wrapper for SBCpip::predict_for_date across a specific
#'     date range.
#' @note Assumes that the config output folder contains a seed dataset labeled one day prior
#'     to the prediction date range.
#' @importFrom loggit loggit
#' @export
sbc_predict_for_range <- function(pred_start_date, num_days, config) {
  
  pred_end_date <- pred_start_date + num_days
  all_pred_dates <- seq.Date(from = pred_start_date, to = pred_end_date, by = 1L)
  
  prediction_df <- NULL
  for (i in seq_along(all_pred_dates)) {
    date_str <- as.character(all_pred_dates[i])
    loggit::loggit(log_lvl = "INFO", log_msg = paste0("Predicting for date: ", date_str))
    result <- predict_for_date(config = config, date = date_str)
    prediction_df <- rbind(prediction_df,result)
  }
  prediction_df
}

#' Fetch model coefficients based on generated output files
#' @param pred_start_date, the start date for the prediction, used to specify
#'     date range for the seed dataset.
#' @param num_days, the number of days to predict from pred_start_date
#' @param config the site-specific configuration
#' @return a data frame containing the coefficients generated during the prediction
#'
#' @importFrom loggit loggit
#' @importFrom dplyr relocate
#' @importFrom magrittr %>%
#' @export
build_coefficient_table <- function(pred_start_date, num_days, config) {
  # Collect model coefficients over all output files (only need one set per week)
  
  model_coefs <- NULL
  pred_end_date <- pred_start_date + num_days
  all_pred_dates <- seq.Date(from = pred_start_date, to = pred_end_date, by = 1L)
  
  # Read model coefs according to cadence of output frequency
  for (i in seq_along(all_pred_dates)) {
    date <- all_pred_dates[i]
    output_file <- list.files(path = config$output_folder,
                              pattern = sprintf(config$output_filename_prefix, date),
                              full.names = TRUE)
    
    if (length(output_file) == 0L) {
      print(paste0("No output files generated for date ", date))
    }
    else {
      d <- readRDS(output_file)
      model_coefs %>% rbind(c(d$model$coefs, 
                              l1_bound = d$model$l1_bound, 
                              lag_bound = d$model$lag_bound)) -> model_coefs
      print(paste("Added model coefs for day", i))
    }
  }
  coef.tbl <- tibble::as_tibble(model_coefs)
  coef.tbl$date <- all_pred_dates
  coef.tbl %>% dplyr::relocate(date) -> coef.tbl
  coef.tbl
}

#' Generate the full dataset used for prediction without relying on output RDS files
#'
#' Useful for applying different prediction techniques to the same data. Note that
#' this has the side-effect of producing reports in the report folder for each date due
#' to the process_data_for_date function.
#'
#' @param pred_start_date, the start date for the prediction, used to specify
#'     date range for the seed dataset.
#' @param num_days, the number of days to predict from pred_start_date
#' @param config the site-specific configuration
#' @return a dataset tibble with date, product usage (transfusion), CBC, census, and inventory
#' @importFrom dplyr left_join rename mutate
#' @importFrom magrittr %>% %<>%
#' @export
generate_full_dataset <- function(pred_start_date, num_days, config) {
  pred_end_date <- pred_start_date + num_days
  pred_inputs <- list(cbc = NULL, census = NULL, transfusion = NULL, inventory = NULL)
  all_pred_dates <- seq.Date(from = pred_start_date, to = pred_end_date, by = 1L)
  
  for (i in seq_along(all_pred_dates)) {
    date_str <- as.character(all_pred_dates[i])
    inputs_single_day <- process_data_for_date(config = config, date = date_str)
    pred_inputs$cbc %<>% rbind(inputs_single_day$cbc)
    pred_inputs$census %<>% rbind(inputs_single_day$census)
    pred_inputs$transfusion %<>% rbind(inputs_single_day$transfusion)
    pred_inputs$inventory %<>% rbind(inputs_single_day$inventory)
  }
  
  full_dataset <- create_cbc_features(pred_inputs$cbc, config$cbc_quantiles)
  pred_inputs$inventory$date %<>% as.Date()
  full_dataset %>%
    dplyr::left_join(pred_inputs$census, by="date") %>%
    dplyr::left_join(pred_inputs$transfusion, by="date") %>%
    dplyr::left_join(pred_inputs$inventory, by="date") %>%
    dplyr::mutate(dow = weekdays(date)) -> full_dataset
  
  full_dataset %>% distinct() %>%
    dplyr::rename(plt_used = .data$used) %>%
    dplyr::mutate(lag = ma(.data$plt_used, window_size = 7L)) -> full_dataset
  
  full_dataset
}

#%%%%%%%%%%%%%%%%% Prediction Table Analysis Wrapper Functions %%%%%%%%%%%%%%%%%
#' Plot the predicted vs. true product usage over the appropriate date range
#'
#' @param pred_table, the prediction table generated from SBCpip::build_prediction_table
#'
#' @importFrom dplyr filter
#' @importFrom ggplot2 ggplot geom_line aes
#' @importFrom magrittr %>%
#' @export
plot_pred_vs_true_usage <- function(pred_table) {
  
  first_day <- pred_table$date[1L]
  last_day <- pred_table$date[nrow(pred_table) - 3L] # predictions end 3 days early
  pred_table_trunc <- pred_table %>%
    dplyr::filter(date >= first_day) %>%
    dplyr::filter(date < last_day)
  
  ggplot2::ggplot(data=pred_table_trunc) +
    ggplot2::geom_line(ggplot2::aes(x=date, y=`Three-day actual usage`)) +
    ggplot2::geom_line(ggplot2::aes(x=date, y=`Three-day prediction`))
}

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
  first_day <- config$start + 5L
  last_day <- nrow(pred_table) - 1L
  n <- last_day - first_day
  
  remaining_inventory <- pred_table$`No. expiring in 1 day` + pred_table$`No. expiring in 2 days`
  adj_remaining_inventory <- pred_table$`Adj. no. expiring in 1 day` + pred_table$`Adj. no. expiring in 2 days`
  
  loss <- sum( pred_table$Waste[seq(first_day, last_day)]) +
    sum((pip::pos(config$penalty_factor - remaining_inventory)^2) [seq(first_day, last_day)]) +
    sum((pred_table$Shortage^2)[seq(first_day, last_day)])
  
  adj_loss <- sum( pred_table$`Adj. waste`[seq(first_day, last_day)]) +
    sum((pip::pos(config$penalty_factor - adj_remaining_inventory)^2) [seq(first_day, last_day)]) +
    sum((pred_table$`Adj. shortage`^2)[seq(first_day, last_day)])
  
  list(`Avg. Daily Loss` = loss / n, `Adj. Avg. Daily Loss` = adj_loss / n)
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
  first_day_waste_seen <- config$start + 5L
  last_day_waste_seen <- nrow(pred_table) - 1L
  n <- last_day_waste_seen - first_day_waste_seen
  
  remaining_inventory <- pred_table$`Inv. count` +
    pred_table$`Fresh Units Ordered` -
    pred_table$`Platelet usage`
  
  loss <- sum(pred_table$`True Waste`[seq(first_day_waste_seen, last_day_waste_seen)]) +
    sum((pip::pos(config$penalty_factor - remaining_inventory)^2) [seq(first_day_waste_seen, last_day_waste_seen)]) +
    sum((pred_table$`True Shortage`^2)[seq(first_day_waste_seen, last_day_waste_seen)])
  
  list(`Real Avg. Daily Loss` = loss / n)
  
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
  
  first_day <- pred_table$date[config$start + 5L]
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
  
  prediction_error[["Overall"]] <- sqrt(tot_err / nrow(pred_table_trunc))
  prediction_error[["Pos."]] <- if (pos_err > 0) sqrt(pos_err / sum(pred_table_trunc$`Three-day actual usage` < pred_table_trunc$`Three-day prediction`))
  prediction_error[["Neg."]] <- if (neg_err > 0) sqrt(neg_err / sum(pred_table_trunc$`Three-day actual usage` > pred_table_trunc$`Three-day prediction`))
  
  # RMSE by Day of Week
  for (dow in c("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")) {
    pred_table_weekly <- pred_table_trunc %>% dplyr::filter(weekdays(date) == dow)
    n <- nrow(pred_table_weekly)
    pred_rmse <- sqrt(sum((pred_table_weekly$`Three-day actual usage` - pred_table_weekly$`Three-day prediction`)^2) / n)
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
  list(pred_start = pred_table$date[config$start + 5L],
       pred_end = pred_table$date[nrow(pred_table) - 1L],
       num_days = nrow(pred_table) - config$start - 6L, # effective number of days
       total_adj_waste = sum(pred_table$`Adj. waste`),
       total_adj_short = sum(pred_table$`Adj. shortage`),
       proj_loss = projection_loss(pred_table, config),
       real_loss = real_loss(pred_table, config),
       three_day_pred_rmse = prediction_error(pred_table, config))
}

#' Combine projection loss and prediction error calculation into a single evaluation.
#'
#' @param coef_table, the coefficient table generated from SBCpip::build_coefficient_table
#' @param config the site-specific configuration
#' @return a list containing the top 20 coefficient by sum of magnitudes over the analyzed period, 
#' as well as the mean and standard deviation of their values.
#' @export
coef_table_analysis <- function(coef_table, config) {
  dows <- c("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")
  # sum the absolute values of all of the coefficients (except intercept). 
  # Display 20 coefficients in order of largest absolute sums and state average value over period
  coef_table %>% 
    dplyr::select(-c(intercept, l1_bound)) %>%
    tidyr::pivot_longer(-c(date), names_to = "feat") %>%
    dplyr::mutate(abs_val = abs(value)) %>%
    dplyr::group_by(feat) %>%
    dplyr::summarize(abs_sum = sum(abs_val), avg = mean(value), sd = sd(value)) %>% 
    dplyr::arrange(desc(abs_sum)) %>%
    #dplyr::slice_head(n = 20L) %>%
    dplyr::select(c(feat, avg, sd)) -> top_coefs
  
  # separate day of week and other features
  list(dow = top_coefs %>% filter(feat %in% dows), 
       other = top_coefs %>% filter(!(feat %in% dows)))
}
