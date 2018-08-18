## SBC site-specific parameters
## return site specific parameters such as quantile functions, abnormal values, locations,
## cbc_variables etc.
SBC_config <- function() {
    result <- list(
        cbc_quantiles = list(
            'HGB' = function(x) quantile(x, probs = 0.25, na.rm = TRUE),   ## q_0.25
            'LYMAB' = function(x) quantile(x, probs = 0.25, na.rm = TRUE), ## q_0.25
            'MCH' = function(x) quantile(x, probs = 0.25, na.rm = TRUE),   ## q_0.25
            'MCHC' = function(x) 32,    ## g/dL
            'MCV' = function(x) quantile(x, probs = 0.25, na.rm = TRUE),   ## q_0.25
            'PLT' = function(x) 150,    ## K/muL
            'RBC' = function(x) 4.4,    ## MIL/muL
            'RDW' = function(x) 12,     ## %
            'WBC' = function(x) quantile(x, probs = 0.25, na.rm = TRUE),   ## q_0.25
            'HCT' = function(x) 40
        ),

        cbc_abnormals = list(
            'HGB' = function(x) x < 9.0,    ## g/dL
            'LYMAB' = function(x) x < 0.69, ## K/muL
            'MCH' = function(x) x < 29.0,   ## pg/rbc
            'MCHC' = function(x) x < 32,    ## g/dL
            'MCV' = function(x) x < 87.0,   ## fL/rbc
            'PLT' = function(x) x < 150,    ## K/muL
            'RBC' = function(x) x < 4.4,    ## MIL/muL
            'RDW' = function(x) x < 11.5,   ## %
            'WBC' = function(x) x > 10.3,   ## K/muL
            'HCT' = function(x) x < 34 ## NARAS ADDED THIS!!
        ),
        census_locations = c('B1','B2', 'B3', 'C1', 'C2', 'C3',
                             'CAPR XFER OVERFL', 'CATH PACU', 'CDU-CLIN DEC UNIT',
                             'D1CC', 'D1CS', 'D2', 'D3', 'DGR',
                             'E1', 'E2-ICU', 'E29-ICU', 'E3',
                             'EMERGENCY DEPARTMENT',
                             'F3', 'FGR', 'G1', 'G2P', 'G2S', 'H1', 'H2'),

        c0 = 30, ## value for c0 to use in training model
        min_inventory = 30, ## the minimum inventory
        history_window = 200,  ## how many days to use in training
        penalty_factor = 15,   ## penalty factor to use in training
        start = 10, ## the day we start the model evaluation
        initial_collection_data = c(60, 60, 60), ## the initial number that will be collected for the first three days
        initial_expiry_data = c(0, 0), ## the number of units that expire a day after, and two days after respectively
        data_folder = "E:/platelet_predict_daily_data", ## Shared folder for Blood Center
        cbc_filename_prefix = "LAB-BB-CSRP-CBC_Daily%s-",
        census_filename_prefix = "LAB-BB-CSRP-Census_Daily%s-",
        transfusion_filename_prefix = "LAB-BB-CSRP-Transfused Product Report_Daily%s-",
        inventory_filename_prefix = "Daily_Product_Inventory_Report_Morning_To_Folder%s-",
        output_filename_prefix = "pip-output-%s.RDS",
        log_filename_prefix = "SBCpip_%s.json",
        model_update_frequency = 7L ## every 7 days
    )
    result$cbc_vars <- names(result$cbc_quantiles)[seq_len(9L)] ## Ignore HCT
    result$report_folder <- "E:/Blood_Center_Reports"
    result$output_folder  <- "E:/Blood_Center_Output"
    result$log_folder <- "E:/Blood_Center_Logs"
    result
}

##
## Global variable for package
##
sbc_config <- SBC_config()

#' Get the global package variable \code{sbc_config}.
#'
#' The configuration is a list of items.
#' \describe{
#'   \item{\code{cbc_quantiles}}{a named list of site-specific quantile functions for each CBC of interest}
#'   \item{\code{cbc_abnormals}}{a named list of site-specific functions that flag values as abnormal or not}
#'   \item{\code{cbc_vars}}{a list of names of CBC variables of interest, i.e. specific values of \code{BASE_NAME}}
#'   \item{\code{census_locations}}{a character vector locations that need to be used for modeling}
#'   \item{\code{c0}}{a value to use in training for c0}
#'   \item{\code{min_inventory}}{a value to use as offset to ensure a minimum threshold}
#'   \item{\code{history_window}}{how many days of history to use in training, default 200}
#'   \item{\code{penalty_factor}}{the penalty factor in training, default 15}
#'   \item{\code{start}}{the day when the model evaluation begins, default 10}
#'   \item{\code{initial_expiry_data}}{the number of units expiring a day after prediction begins and one day after that, i.e. a 2-vector}
#'   \item{\code{initial_collection_data}}{the number of units to collect on the prediction day, a day after, and another day after, i.e. a 3-vector}
#'   \item{\code{data_folder}}{full path of location of raw data files}
#'   \item{\code{report_folder}}{full path of where reports should go, must exist}
#'   \item{\code{output_folder}}{full path of where processed output data should go, must exist}
#'   \item{\code{log_folder}}{full path of where logs should go, must exist}
#'   \item{\code{model_update_frequency}}{how often to update the model, default 7 days}
#'   \item{\code{cbc_filename_prefix}}{a character expression describing the prefix of daily CBC file name, this is combined with the date is substituted with the date in YYYY-dd-mm format}
#'   \item{\code{census_filename_prefix}}{a character expression describing the prefix of daily census file name, this is combined with the date is substituted with the date in YYYY-dd-mm format}
#'   \item{\code{transfusion_filename_prefix}}{a character expression describing the prefix of daily transfusion file name, this is combined with the date is substituted with the date in YYYY-dd-mm format}
#'   \item{\code{output_filename_prefix}}{a character expression describing the prefix of daily output file name, this is combined with the date is substituted with the date in YYYY-dd-mm format}
#'   \item{\code{log_filename_prefix}}{a character expression describing the prefix of log file name, this is combined with the date is substituted with the date in YYYY-dd-mm format}
#'   \item{\code{inventory_filename_prefix}}{a character expression describing the prefix of the inventory file name, this is combined with the date is substituted with the date in YYYY-dd-mm format}
#' }
#' @return the value of package global \code{sbc_config}.
#' @importFrom utils assignInMyNamespace head tail
#' @export
#' @examples
#' get_SBC_config()
#'
get_SBC_config <- function() {
    sbc_config
}

#' Reset the global package variable \code{sbc_config}
#'
#' @return the default value value of testpack package global \code{sbc_config}
#' @export
#' @examples
#' \dontrun{
#'   reset_config()
#' }
#'
reset_config <- function() {
    assignInMyNamespace("sbc_config", SBC_config())
    invisible(sbc_config)
}

#' Set SBC configuration parameter to a specified value
#'
#' @param param the name of the variable as a string
#' @param value the value to assign
#' @return the changed value of the package global \code{sbc_config} invisibly
#' @export
#'
set_config_param <- function(param, value) {
    result <- sbc_config
    if (! (param %in% names(result)) ) {
        loggit::loggit(log_lvl = "ERROR", log_msg = paste("Unknown parameter ", param))
        stop(paste("Unknown parameter ", param))
    }
    result[[param]] <- value
    assignInMyNamespace("sbc_config", result)
    invisible(result)
}

## Some functions below specifically to ensure validation

#' Set the SBC initial number of units that will expire in a day and
#' the next
#'
#' @param value the value to assign, should be a non-negative 2-vector
#' @return the changed value of the package global \code{sbc_config}
#'     invisibly
#' @export
#'
set_initial_expiry_data <- function(value) {
    if (length(value) != 2L || any(value < 0)) {
        stop("Need a non-negative two-vector")
    }
    result <- sbc_config
    result$initial_expiry_data <- value
    assignInMyNamespace("sbc_config", result)
    invisible(result)
}

#' Set the SBC initial number of units that will be collected for the
#' first three days
#'
#' @param value the value to assign, should be a non-negative 3-vector
#' @return the changed value of the package global \code{sbc_config}
#'     invisibly
#' @export
#'
set_initial_collection_data <- function(value) {
    if (length(value) != 3L || any(value < 0)) {
        stop("Need a non-negative three-vector")
    }
    result <- sbc_config
    result$initial_collection_data <- value
    assignInMyNamespace("sbc_config", result)
    invisible(result)
}
