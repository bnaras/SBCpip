library(SBCpip)
library(dplyr)
library(ggplot2)
library(tidyverse)
library(magrittr)
options(warn = 1)

# Set my params
set_config_param(param = "output_folder",
                 value = "/Users/kaiokada/Desktop/Research/platelet_data_full/Blood_Center_Outputs")
set_config_param(param = "log_folder",
                 value = "/Users/kaiokada/Desktop/Research/platelet_data_full/Blood_Center_Logs")


set_config_param(param = "report_folder",
                 value = "/Users/kaiokada/Desktop/Research/platelet_data_full/Blood_Center_Reports")

config <- set_config_param(param = "data_folder",
                           value = "/Users/kaiokada/Desktop/Research/platelet_data_full/Blood_Center_inc")

config$history_window <- 200
config$start <- 5
config$initial_collection_data <- c(60, 60, 60)
config$initial_expiry_data <- c(0,0)

### Forward Stepwise Selection over limited dates ###
all_locations <- c("B1", "B2", "B3", "C1", "C2", "C3", "CDU-CLIN DEC UNIT",
                   "E1", "E2-ICU", "E3", "EGR", "F3", "FGR", "G2P", "G2S",
                   "H2", "J2", "J4", "J5", "J6", "J7", "K4", "K5", "K6", "K7",
                   "L4", "L5", "L6", "L7", "M4", "M5", "M6", "M7",
                   "VCL SKILLED NURSING FACILITY", "VCP 1 WEST", "VCP 2 NORTH",
                   "VCP 2 WEST", "VCP 3 WEST", "VCP CCU 1",  "VCP CCU 2",
                   "VCP EMERGENCY DEPARTMENT", "VCP LABOR AND DELIVERY", "VCP NICU",
                   "VCP NURSERY", "VCP PEDIATRICS")
config$census_locations <- all_locations

pred_start_date <- as.Date("2020-06-19")
num_days <- 23
pred_end_date <- pred_start_date + num_days

sbc_build_and_save_seed_data(pred_start_date, config)
config$census_locations <- NULL
prev_locations <- NULL

losses <- rep(NA, 20)
errors <- rep(NA, 20)

min_loss <- 10
min_pred_rmse <- 60

for (i in seq(20)) {
  print(paste0("Adding variable ", i))

  # Append a single NA to the end of the locations
  config$census_locations <- c(config$census_locations, NA)
  min_loss <- 10
  min_pred_rmse <- 60
  curr_min_error <- min_pred_rmse
  curr_min_loss <- min_loss

  for (var in setdiff(all_locations, prev_locations)) {

    # assign a variable to the end of the census locations
    config$census_locations[i] <- var
    print(config$census_locations)
    #sbc_build_and_save_seed_data(pred_start_date, config)

    # Use the configuration to make predictions
    sbc_predict_for_range(pred_start_date, num_days, config)
    pred_table <- build_prediction_table(config, pred_start_date, pred_end_date)
    analysis <- pred_table_analysis(pred_table, config)

    # If the augmented model has more accurate predictions, update the current
    # minimum RMSE and additional variable
    if (analysis$three_day_pred_rmse$Overall < curr_min_error) {
      curr_min_error <- analysis$three_day_pred_rmse$Overall
      curr_min_loss <- analysis$proj_loss$`Adj. Avg. Daily Loss`
      min_var <- config$census_locations[length(config$census_locations)]
    }
  }

  errors[i] <- curr_min_error
  losses[i] <- curr_min_loss
  config$census_locations <- c(prev_locations, min_var)
  prev_locations <- config$census_locations
}
var_select <- tibble(num = 1:20, vars = config$census_locations, errors = errors, adj_losses = losses)
write.csv(var_select, "census_var_select_0620.csv")
