## -----------------------------------------------------------------------------
library(SBCpip)
options(warn = 1) ## Report warnings as they appear

## ---- eval = FALSE------------------------------------------------------------
#  ## Specify the data location
#  config <- set_config_param(param = "data_folder",
#                             value = "full_path_to_historical_data")
#  
#  ## Specify output folder; existing output files will be overwritten
#  set_config_param(param = "output_folder",
#                   value = "full_path_to_output_folder")
#  
#  ## Specify report folder; existing output files will be overwritten
#  set_config_param(param = "report_folder",
#                   value = "full_path_to_report_folder")
#  
#  ## Specify log folder; existing log files will be overwritten
#  ## Also get a handle on the invisibly returned updated configuration
#  config <- set_config_param(param = "log_folder",
#                             value = "full_path_to_log_folder")
#  

## ---- eval = FALSE------------------------------------------------------------
#  ## This is for creating the seed dataset
#  cbc <- process_all_cbc_files(data_folder = config$data_folder,
#                               cbc_abnormals = config$cbc_abnormals,
#                               cbc_vars = config$cbc_vars,
#                               verbose = config$verbose)
#  
#  census <- process_all_census_files(data_folder = config$data_folder,
#                                     locations = config$census_locations,
#                                     verbose = config$verbose)
#  
#  transfusion <- process_all_transfusion_files(data_folder = config$data_folder,
#                                               verbose = config$verbose)
#  
#  ## Save the SEED dataset so that we can begin the process of building the model.
#  ## This gets us to date 2018-04-09-08-01-54.txt, so we save data using that date
#  saveRDS(list(cbc = cbc, census = census, transfusion = transfusion),
#          file = file.path(config$output_folder,
#                           sprintf(config$output_filename_prefix, "2018-04-09")))

## ---- eval = FALSE------------------------------------------------------------
#  
#  ## Now process the incremental files
#  ## Point to folder containing incrementals
#  config <- set_data_folder("full_path_to_incremental_data")
#  ## One may or may not choose to set the incremental reports to go
#  ## to another location as shown below.
#  config <- set_report_folder("full_path_to_incremental_data_reports")
#  ## Same for output although not shown.
#  
#  By specifying, the starting and ending dates, one can process all files in one swoop.
#  

## ---- eval = FALSE------------------------------------------------------------
#  start_date <- as.Date("2018-04-10")
#  end_date <- as.Date("2018-05-29")
#  all_dates <- seq.Date(from = start_date, to = end_date, by = 1)
#  
#  prediction <- NULL
#  
#  for (i in seq_along(all_dates)) {
#      date_str <- as.character(all_dates[i])
#      ##print(date_str)
#      result <- predict_for_date(config = config, date = date_str)
#      prediction <- c(prediction, result$t_pred)
#  }

## ---- eval = FALSE------------------------------------------------------------
#  result <- predict_for_date(config = config)

## ---- eval = FALSE------------------------------------------------------------
#  result <- predict_for_date(config = config, date = "2018-04-30")

