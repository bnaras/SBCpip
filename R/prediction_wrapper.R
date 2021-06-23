#' Generate .RDS file for seed data to be used in prediction
#' @param pred_start_date, the start date for the prediction, used to specify
#'     date range for the seed dataset.
#' @param config the site-specific configuration
#' @return nothing (saves an RDS file with the seed data for 1 day before prediction date)
#' @importFrom readr cols col_integer col_character col_datetime read_tsv
#' @importFrom loggit set_logfile loggit
#' @export
sbc_build_and_save_seed_data <- function(pred_start_date, config) {

  seed_start_date <- pred_start_date - config$history_window - 1
  seed_end_date <- pred_start_date - 1
  all_seed_dates <- seq.Date(from = seed_start_date, to = seed_end_date, by = 1)

  seed_data <- list(cbc = NULL, census = NULL, transfusion = NULL, inventory = NULL)
  for (i in seq_along(all_seed_dates)) {
    date_str <- as.character(all_seed_dates[i])
    data_single_day <- process_data_for_date(config = config, date = date_str)
    seed_data$cbc %<>% rbind(data_single_day$cbc)
    seed_data$census %<>% rbind(data_single_day$census)
    seed_data$transfusion %<>% rbind(data_single_day$transfusion)
    seed_data$inventory %<>% rbind(data_single_day$inventory)
  }

  # Save the seed dataset
  saveRDS(object = seed_data,
          file = file.path(config$output_folder,
                           sprintf(config$output_filename_prefix, as.character(seed_end_date))))
}

#' Predict product usage and corresponding order schedule over specified date range
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
  all_pred_dates <- seq.Date(from = pred_start_date, to = pred_end_date, by = 1)
  all_pred_dates
  prediction_df <- NULL
  for (i in seq_along(all_pred_dates)) {
    date_str <- as.character(all_pred_dates[i])
    loggit::loggit(log_lvl = "INFO", log_msg = paste("Predicting for date:", date))
    result <- predict_for_date(config = config, date = date_str)
    prediction_df <- rbind(prediction_df,result)
  }
  prediction_df
}

#' Retrieve true transfusion data from the files (this fetches data from all files in folder)
#' @param
#' @param
#' @return a tibble with the true transfusion/product usage data.
#' @importFrom dplyr distinct, arrange
#' @export
process_all_transfusion_files_no_reports <- function(data_folder,
                                                     pattern = "LAB-BB-CSRP-Transfused*") {
  fileList <- list.files(data_folder, pattern = pattern , full.names = TRUE)
  names(fileList) <- basename(fileList)
  raw_transfusion <- lapply(fileList, read_one_transfusion_file)

  Reduce(f = rbind,
         lapply(raw_transfusion, function(x) x$transfusion_data)) %>%
    dplyr::distinct() %>%
    dplyr::arrange(date)
}

#' Construct a tibble of predicted vs. true usage over specified prediction date range.
#' @param pred_start_date, the start date for the prediction, used to specify
#'     date range for the seed dataset.
#' @param num_days, the number of days to predict from pred_start_date
#' @param config the site-specific configuration
#' @return a tibble of predicted vs. true usage
#' @importFrom loggit loggit
#' @export
get_pred_vs_true <- function(pred_start_date, num_days, config)  {

  pred_end_date <- pred_start_date + num_days
  transfusion_inc <- process_all_transfusion_files_no_reports(data_folder = config$data_folder)
  transfusion_dates <- sapply(transfusion_inc$date, function(x) {
    as.character(x, format = "%Y-%m-%d")
  })
  transfusion_used <- as.numeric(transfusion_inc$used)
  plt_used_df <- data.frame(transfusion_dates, transfusion_used)
  plt_used_tbl <- as_tibble(plt_used_df[1:nrow(plt_used_df),])
  plt_used_tbl$transfusion_dates %<>% as.Date
  names(plt_used_tbl) = c("date", "d_true")

  plt_used_tbl %<>% filter(date >= pred_start_date) %<>% filter(date <= pred_end_date)

  prediction %>% left_join(plt_used_tbl, by="date") -> pred_and_true

  # The "true" usage (t_true) is the sum of the usage over the ensuing 3 days (i+1, i+2, i+3)
  pred_and_true %>%
    mutate(dlead1 = lead(pred_and_true$d_true, 1)) %>%
    mutate(dlead2 = lead(pred_and_true$d_true, 2)) %>%
    mutate(dlead3 = lead(pred_and_true$d_true, 3)) %>%
    mutate(t_true = dlead1 + dlead2 + dlead3)
  pred_and_true %>% filter(is.na(t_true))
  pred_and_true <- pred_and_true[2:(nrow(pred_and_true)-2),] # remove NAs
}

#' Fetch model coefficients based on generated output files
#' @param pred_start_date, the start date for the prediction, used to specify
#'     date range for the seed dataset.
#' @param num_days, the number of days to predict from pred_start_date
#' @param config the site-specific configuration
#' @return a data frame containing the coefficients generated during the prediction
#'
#' @importFrom loggit loggit
#' @export
get_coefs_over_time <- function(pred_start_date, num_days, config) {
  # Collect model coefficients over all output files (only need one set per week)
  # [Would be good to allow for start and end date specifications - throw error if file not available]
  #output_files <- list.files(path = config$output_folder,
  #                           pattern = paste0("^",
  #                                            substring(config$output_filename_prefix, first = 1, last = 10)),
  #                           full.names = TRUE)

  model_coefs <- NULL
  #loggit::loggit(log_lvl = "INFO", log_msg = paste("Number of days to process:", length(output_files)))
  pred_end_date <- pred_start_date + num_days
  all_pred_dates <- seq.Date(from = pred_start_date, to = pred_end_date,
                             by = config$model_update_frequency)

  # Read model coefs according to cadency of output frequency
  for (i in seq_along(all_pred_dates)) {
    date <- all_pred_dates[i]
    output_file <- list.files(path = config$output_folder,
               pattern = sprintf(config$output_filename_prefix, date),
               full.names = TRUE)
    if (length(output_file) == 0) {
      print("No output files generated for date", date)
      return()
    }
    d <- readRDS(output_file)
    model_coefs %>% rbind(c(d$model$coefs, lambda = d$model$lambda)) -> model_coefs
    print(paste("Added model coefs for day", i))
  }

  data.frame(model_coefs)
}


evaluate_model_over_range <- function(config) {

}
