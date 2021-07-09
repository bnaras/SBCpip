#' Read a single cbc file data and return it, as is
#' @param filename the fully qualified path of the file
#' @param cbc_abnormals, a named list of functions of a single vector
#'     returning TRUE for abnormal values
#' @param cbc_vars, the names of fields to include; others are excluded
#' @return a list of four items, filename, raw_data (tibble), report a
#'     list of missing values tibble and a summary tibble, cbc_data
#'     (tibble)
#' @importFrom readr cols col_integer col_character col_datetime read_tsv
#' @importFrom loggit set_logfile loggit
#' @export
read_one_cbc_file <- function(filename, cbc_abnormals, cbc_vars) {
    ## ORD_VALUE can have values like "<0.1", so we read as char
    ## and convert as needed
    col_types <- list(
        ORDER_PROC_ID = readr::col_integer(),
        LINE = readr::col_integer(),
        PAT_ENC_CSN_ID = readr::col_double(),
        ADMIT_CONF_STAT_C = readr::col_integer(),
        DISCH_CONF_STAT_C = readr::col_integer(),
        PAT_ID = readr::col_character(),
        PAT_MRN_ID = readr::col_character(),
        PROC_CODE = readr::col_character(),
        DESCRIPTION = readr::col_character(),
        BASE_NAME = readr::col_character(),
        COMMON_NAME = readr::col_character(),
        ORD_VALUE = readr::col_character(), ## Needs to be coerced to double
        ORDERING_DATE = readr::col_datetime("%m/%d/%Y  %I:%M:%S%p"),
        ORDER_TIME = readr::col_datetime("%d-%b-%y %H:%M:%S"),
        RESULT_TIME = readr::col_datetime("%d-%b-%y %H:%M:%S"),
        ORDER_STATUS_C = readr::col_integer(),
        RESULT_STATUS_C = readr::col_integer(),
        SERV_AREA_ID = readr::col_character(),
        PARENT_HOSPITAL = readr::col_character()
    )

    loggit::loggit(log_lvl = "INFO", log_msg = paste("Processing", basename(path = filename)))

    raw_data <- readr::read_tsv(file = filename,
                                col_names = TRUE,
                                col_types = do.call(readr::cols, col_types),
                                progress = FALSE)
    ## Stop if no data
    if (nrow(raw_data) < 1) {
        loggit::loggit(log_lvl = "ERROR", log_msg = sprintf("No data in file %s", filename))
        stop(sprintf("No data in file %s", filename))
    }

    processed_data <- summarize_and_clean_cbc(raw_data, cbc_abnormals, cbc_vars)
    if (processed_data$errorCode != 0) {
        loggit::loggit(log_lvl = "ERROR", log_msg = processed_data$errorMessage)
        stop(processed_data$errorMessage)
    }
    list(filename = filename,
         raw_data = raw_data,
         report = list(missing_values = processed_data$missing_values,
                       summary = processed_data$summary),
         cbc_data = processed_data$data)
}

#' Summarize and clean the raw cbc data
#' @param raw_data the raw data tibble
#' @param cbc_abnormals, a named list of functions of a single vector
#'     returning TRUE for abnormal values
#' @param cbc_vars, the names of fields to include; others are excluded
#' @return a list of five items; errorCode (nonzero if error),
#'     errorMessage if any, missing_value_df the missing value data
#'     tibble, the summary data tibble, the data tibble filtered with
#'     relevant columns for us
#' @importFrom magrittr %>%
#' @importFrom dplyr filter mutate select group_by summarize distinct first
#' @importFrom rlang quo !! .data
#' @importFrom stats median quantile sd
#' @export
summarize_and_clean_cbc <- function(raw_data, cbc_abnormals, cbc_vars) {
    result <- list(errorCode = 0,
                   errorMessage = "")
    raw_data %>%
        dplyr::distinct(.keep_all = TRUE) %>%  # KO safeguard
        dplyr::filter(.data$BASE_NAME %in% cbc_vars) %>%
        dplyr::mutate(CBC_VALUE = as.numeric(gsub("^<0.1", "0", .data$ORD_VALUE))) %>%
        dplyr::mutate(RESULT_DATE = as.Date(.data$RESULT_TIME)) %>%
        dplyr::select(.data$RESULT_DATE, .data$BASE_NAME, .data$CBC_VALUE, .data$ORD_VALUE) ->
        cbc_data

    if (any(is.na(cbc_data$RESULT_DATE))) {
        result$errorCode <- 1
        result$errorMessage <- "Bad RESULT_TIME column!"
        return(result)
    }
    ## Report those missing values arising out of values other
    ## than "<0.1" (which we expect and have handled already!)
    cbc_data %>%
        dplyr::filter(is.na(.data$CBC_VALUE) & !grepl("^<0.1", .data$ORD_VALUE)) %>%
        dplyr::select(.data$RESULT_DATE, .data$BASE_NAME, .data$ORD_VALUE) %>%
        dplyr::group_by(.data$RESULT_DATE, .data$BASE_NAME) ->
        result$missing_values

    ## Report abnormal values per the limits provided
    quo_base_name <- quo(.data$BASE_NAME)
    cbc_data %>%
        dplyr::select(.data$RESULT_DATE, .data$BASE_NAME, .data$CBC_VALUE) %>%
        dplyr::group_by(.data$RESULT_DATE, .data$BASE_NAME) %>%
        dplyr::mutate(abnormal = cbc_abnormals[[dplyr::first(!!quo_base_name)]](.data$CBC_VALUE)) %>%
        dplyr::summarize(total_N = dplyr::n(), missing_N = sum(is.na(.data$CBC_VALUE)),
                         abnormal_N = sum(.data$abnormal, na.rm = TRUE),
                         min = min(.data$CBC_VALUE, na.rm = TRUE),
                         q25 = quantile(.data$CBC_VALUE, probs = 0.25, na.rm = TRUE),
                         median = median(.data$CBC_VALUE, na.rm = TRUE),
                         q75 = quantile(.data$CBC_VALUE, probs = 0.75, na.rm = TRUE),
                         max = max(.data$CBC_VALUE, na.rm = TRUE),
                         mean = mean(.data$CBC_VALUE, na.rm = TRUE),
                         sd = sd(.data$CBC_VALUE, na.rm = TRUE)) ->
        result$summary

    ## Now drop the ORD_VALUE column
    cbc_data %>%
        dplyr::select(.data$RESULT_DATE, .data$BASE_NAME, .data$CBC_VALUE) ->
        result$data
    result
}

#' Process all cbc files in a folder and generate qc reports
#' @param data_folder the folder containing the raw data
#' @param report_folder the folder to write reports to, default is
#'     data_folder with "_Reports" appended. Must exist.
#' @param cbc_abnormals, a named list of functions of a single vector
#'     returning TRUE for abnormal values
#' @param cbc_vars, the names of fields to include; others are excluded
#' @param pattern the pattern to distinguish CBC files, default
#'     "LAB-BB-CSRP-CBC*" appearing anywhere
#' @return a combined dataset
#' @importFrom tools file_path_sans_ext
#' @importFrom dplyr filter arrange distinct bind_rows
#' @importFrom writexl write_xlsx
#' @export
process_all_cbc_files <- function(data_folder,
                                  report_folder = file.path(dirname(data_folder),
                                                            paste0(basename(data_folder),
                                                                   "_Reports")),
                                  cbc_abnormals,
                                  cbc_vars,
                                  pattern = "LAB-BB-CSRP-CBC*") {
    fileList <- list.files(path = data_folder, pattern = pattern, full.names = TRUE)
    names(fileList) <- basename(fileList)
    raw_cbc <- lapply(fileList, read_one_cbc_file, cbc_abnormals = cbc_abnormals,
                      cbc_vars = cbc_vars)
    for (item in raw_cbc) {
        save_report_file(report_tbl = item$report,
                         report_folder = report_folder,
                         filename = item$filename)
    }

    ## Process the cbc data files and select relevant columns
    Reduce(f = rbind, lapply(raw_cbc, function(x) x$cbc_data)) %>%
        dplyr::filter(.data$BASE_NAME %in% cbc_vars) %>%
        dplyr::rename(date = .data$RESULT_DATE) %>%
        dplyr::distinct()
}


#' Read a single census file data and return it, as is
#' @param filename the fully qualified path of the file
#' @param locations a character vector locations of interest
#' @return a list of four items, filename, raw_data (tibble), report a
#'     list consisting of summary tibble, census_data (tibble)
#' @importFrom readr cols col_integer col_character col_datetime read_tsv
#' @export
read_one_census_file <- function(filename, locations) {
    col_types <- list(
        PAT_MRN_ID = readr::col_character(),
        PAT_ID = readr::col_character(),
        LOCATION_ID = readr::col_integer(),
        LOCATION_NAME = readr::col_character(),
        EVENT_TYPE = readr::col_character(),
        LOCATION_DT = readr::col_datetime("%m/%d/%Y  %I:%M:%S%p"),
        ADMIT_DT = readr::col_datetime("%m/%d/%Y  %I:%M:%S%p"),
        DISCH_DT = readr::col_character(),
        CENSUS_INCLUSN_YN = readr::col_character(),
        PAT_ENC_CSN_ID = readr::col_double(),
        PARENT_HOSPITAL = readr::col_character()
    )

    loggit::loggit(log_lvl = "INFO", log_msg = paste("Processing", basename(path = filename)))
    raw_data <- readr::read_tsv(file = filename,
                                col_names = TRUE,
                                col_types = do.call(readr::cols, col_types),
                                progress = FALSE)
    ## Stop if no data
    if (nrow(raw_data) < 1) {
        loggit::loggit(log_lvl = "ERROR", log_msg = sprintf("No data in file %s", filename))
        stop(sprintf("No data in file %s", filename))
    }

    processed_data <- summarize_and_clean_census(raw_data, locations)
    if (processed_data$errorCode != 0) {
        loggit::loggit(log_lvl = "ERROR", log_msg = processed_data$errorMessage)
        stop(processed_data$errorMessage)
    }
    list(filename = filename,
         raw_data = raw_data,
         report = list(summary = processed_data$summary),
         census_data = processed_data$data)
}

#' Summarize and clean the raw census data
#' @param raw_data the raw data tibble
#' @param locations a character vector locations of interest
#' @return a list of five items; errorCode (nonzero if error),
#'     errorMessage if any, missing_value_df the missing value data
#'     tibble, the summary data tibble, the data tibble filtered with
#'     relevant columns for us
#' @importFrom magrittr %>%
#' @importFrom dplyr filter mutate select group_by summarize distinct
#' @importFrom rlang quo !! .data
#' @export
summarize_and_clean_census <- function(raw_data, locations) {
    result <- list(errorCode = 0,
                   errorMessage = "")

    if (any(is.na(raw_data$LOCATION_DT))) {
        result$errorCode <- 1
        result$errorMessage <- "Bad LOCATION_DT column!"
        return(result)
    }

    raw_data %>%
        dplyr::distinct(.keep_all = TRUE) %>% # KO safeguard
        dplyr::mutate(LOCATION_DT = as.Date(.data$LOCATION_DT)) %>%
        dplyr::group_by(.data$LOCATION_DT, .data$LOCATION_NAME) %>%
        dplyr::summarize(count = dplyr::n()) %>%
        tidyr::spread(.data$LOCATION_NAME, .data$count) ->
        census_data

    if (length(intersect(names(census_data), locations)) != length(locations)) {
        ## I should flag this as an error, but not doing so now. Just make the count NA
        ##result$errorCode <- 1
        ##result$errorMessage <- "Census is missing some locations used in the model!"
        ##return(result)
        missing_locations <- setdiff(locations, names(census_data)[-1])
        #census_data[, missing_locations] <- NA
        census_data[, missing_locations] <- 0 # (KO) should probably be 0, not NA (helps with downstream prediction and)
    }
    ## This is also the summary
    result$summary <- census_data
    ## For analysis, just narrow to locations of interest
    census_data %>%
        dplyr::select(c("LOCATION_DT", locations)) %>%
        dplyr::rename(date = .data$LOCATION_DT) ->
        result$data
    result
}


#' Process all census files in a folder and generate qc reports
#' @param data_folder the folder containing the raw data
#' @param report_folder the folder to write reports to, default is
#'     data_folder with "_Reports" appended. Must exist.
#' @param pattern the pattern to distinguish CBC files, default
#'     "LAB-BB-CSRP-Census*" appearing anywhere
#' @param locations the character vector of locations to consider
#' @return a combined dataset
#' @importFrom tools file_path_sans_ext
#' @importFrom tidyr replace_na
#' @importFrom dplyr distinct arrange
#' @importFrom writexl write_xlsx
#' @export
process_all_census_files <- function(data_folder,
                                     report_folder = file.path(dirname(data_folder),
                                                            paste0(basename(data_folder),
                                                                   "_Reports")),
                                     locations,
                                     pattern = "LAB-BB-CSRP-Census*") {
    fileList <- list.files(data_folder, pattern = pattern, full.names = TRUE)
    names(fileList) <- basename(fileList)
    raw_census <- lapply(fileList, read_one_census_file, locations = locations)

    for (item in raw_census) {
        save_report_file(report_tbl = item$report,
                         report_folder = report_folder,
                         filename = item$filename)
    }

    census_data <- Reduce(f = rbind,
                          lapply(raw_census, function(x) x$census_data))
    replacement <- lapply(names(census_data)[-1], function(x) 0)
    names(replacement) <- names(census_data)[-1]
    census_data %>%
        tidyr::replace_na(replace = replacement) %>%
        dplyr::distinct() %>%
        dplyr::arrange(date)
}


#' Read a single transfusion file data and return it, as is
#' @param filename the fully qualified path of the file
#' @return a list of four items, filename, raw_data (tibble), report a
#'     list consisting of summary tibble, transfusion_data (tibble)
#' @importFrom readr cols col_integer col_character col_datetime read_tsv
#' @export
read_one_transfusion_file <- function(filename) {
    col_types <- list(
        Type = readr::col_character(),
        DIN = readr::col_character(),
        `Product Code` = readr::col_character(),
        `Donation Code` = readr::col_character(),
        Division = readr::col_character(),
        UIP = readr::col_integer(),
        UnitABO = readr::col_character(),
        UnitRh = readr::col_character(),
        `Exp. Date/Time` = readr::col_datetime("%m/%d/%Y  %I:%M:%S %p"),
        `Issue Date/Time` = readr::col_datetime("%m/%d/%Y  %I:%M:%S %p"),
        `Issue to Location` = readr::col_character(),
        `Issue to Sub Location` = readr::col_character(),
        `Recip. MRN` = readr::col_character(),
        `Recip. ABO` = readr::col_character(),
        `Recip. Rh` = readr::col_character()
    )

    loggit::loggit(log_lvl = "INFO", log_msg = paste("Processing", basename(path = filename)))

    raw_data <- readr::read_tsv(file = filename,
                                col_names = TRUE,
                                col_types = do.call(readr::cols, col_types),
                                progress = FALSE)
    #dates <- unique(sort(as.Date(raw_data$`Issue Date/Time`)))

    ## Stop if no data
    if (nrow(raw_data) < 1) {
        loggit::loggit(log_lvl = "ERROR", log_msg = sprintf("No data in file %s", filename))
        stop(sprintf("No data in file %s", filename))
    }
    processed_data <- summarize_and_clean_transfusion(raw_data)
    if (processed_data$errorCode != 0) {
        loggit::loggit(log_lvl = "ERROR", log_msg = processed_data$errorMessage)
        stop(processed_data$errorMessage)
    }
    list(filename = filename,
         raw_data = raw_data,
         report = list(summary = processed_data$summary),
         transfusion_data = processed_data$data)
}


#' Summarize and clean the raw transfusion data
#' @param raw_data the raw data tibble
#' @return a list of four items; errorCode (nonzero if error),
#'     errorMessage if any, the summary data tibble, the data tibble
#'     filtered with relevant columns for us
#' @importFrom magrittr %>%
#' @importFrom dplyr filter mutate select group_by summarize distinct
#' @importFrom rlang quo !! .data
#' @export
summarize_and_clean_transfusion <- function(raw_data) {
    result <- list(errorCode = 0,
                   errorMessage = "")

    if (any(is.na(raw_data$`Issue Date/Time`))) {
        result$errorCode <- 1
        result$errorMessage <- "Bad Issue Date/Time column!"
        return(result)
    }

    raw_data %>%
        dplyr::distinct(.keep_all = TRUE) %>% # KO safeguard
        dplyr::filter(.data$Type == "PLT") %>%
        dplyr::mutate(date = as.Date(.data$`Issue Date/Time`)) %>%
        dplyr::select(.data$date) %>%
        dplyr::group_by(.data$date) %>%
        dplyr::summarize(used = dplyr::n()) ->
        result$data ->
        result$summary  ## This is also the summary

    result
}


#' Process all transfusion files in a folder and generate qc reports
#' @param data_folder the folder containing the raw data
#' @param report_folder the folder to write reports to, default is
#'     data_folder with "_Reports" appended. Must exist.
#' @param pattern the pattern to distinguish Transfusion files, default
#'     "LAB-BB-CSRP-Transfused*" appearing anywhere
#' @return a combined dataset
#' @importFrom tools file_path_sans_ext
#' @importFrom tidyr replace_na
#' @importFrom writexl write_xlsx
#' @export
process_all_transfusion_files <- function(data_folder,
                                          report_folder = file.path(dirname(data_folder),
                                                                    paste0(basename(data_folder),
                                                                           "_Reports")),
                                          pattern = "LAB-BB-CSRP-Transfused*") {
    fileList <- list.files(data_folder, pattern = pattern , full.names = TRUE)
    names(fileList) <- basename(fileList)
    raw_transfusion <- lapply(fileList, read_one_transfusion_file)

    for (item in raw_transfusion) {
        save_report_file(report_tbl = item$report,
                         report_folder = report_folder,
                         filename = item$filename)
    }

    Reduce(f = rbind,
           lapply(raw_transfusion, function(x) x$transfusion_data)) %>%
        dplyr::distinct() %>%
        dplyr::arrange(date)
}

#' Read a single surgery file data and return tibble and summary
#' @param filename the fully qualified path of the file
#' @param services the list of surgery types considered as features
#' @return a list of four items, filename, raw_data (tibble), report a
#'     list consisting of summary tibble, surgery_data (tibble)
#' @importFrom readr cols col_integer col_character col_datetime read_tsv
#' @export
read_one_surgery_file <- function(filename, services) {

    col_types <- list(
        LOG_ID = readr::col_character(),
        PAT_ID = readr::col_character(),
        PAT_MRN_ID = readr::col_character(),
        SURGEON_NAME = readr::col_character(),
        OR_SERVICE = readr::col_character(),
        ROOM_NAME = readr::col_character(),
        SURGERY_DATE = readr::col_datetime("%m/%d/%Y  %I:%M:%S %p"),
        PROC1 = readr::col_character(),
        PROC2 = readr::col_character(),
        PROC3 = readr::col_character(),
        FIRST_SCHED_DATE = readr::col_datetime("%m/%d/%Y  %I:%M:%S %p"),
        LAST_SCHED_DATE = readr::col_datetime("%m/%d/%Y  %I:%M:%S %p"),
        CASE_CLASS = readr::col_character(),
        PARENT_HOSPITAL = readr::col_character()
    )

    loggit::loggit(log_lvl = "INFO", log_msg = paste("Processing", basename(path = filename)))

    raw_data <- readr::read_tsv(file = filename,
                                col_names = TRUE,
                                col_types = do.call(readr::cols, col_types),
                                progress = FALSE)

    ## Stop if no data
    if (nrow(raw_data) < 1) {
        loggit::loggit(log_lvl = "ERROR", log_msg = sprintf("No data in file %s", filename))
        stop(sprintf("No data in file %s", filename))
    }
    processed_data <- summarize_and_clean_surgery(raw_data, services)
    if (processed_data$errorCode != 0) {
        loggit::loggit(log_lvl = "ERROR", log_msg = processed_data$errorMessage)
        stop(processed_data$errorMessage)
    }
    list(filename = filename,
         raw_data = raw_data,
         report = list(summary = processed_data$summary),
         surgery_data = processed_data$data)
}

#' Summarize and clean the raw surgery data
#' @param raw_data the raw data tibble
#' @param services the list of surgery types considered as features
#' @return a list of four items; errorCode (nonzero if error),
#'     errorMessage if any, the summary data tibble, the data tibble
#'     filtered with relevant columns for us
#' @importFrom dplyr filter mutate select group_by summarize n left_join
#' @importFrom tidyr pivot_wider
#' @importFrom rlang quo !! .data
#' @export
summarize_and_clean_surgery <- function(raw_data, services) {
    result <- list(errorCode = 0,
                   errorMessage = "")

    if (any(is.na(raw_data$SURGERY_DATE))) {
        result$errorCode <- 1
        result$errorMessage <- "Bad Surgery Date/Time column!"
        return(result)
    }

    # Update this section depending on which features are deemed significant
    raw_data %>%
        dplyr::distinct(.data$LOG_ID, .keep_all = TRUE) %>% # There are many repeated log IDs
        dplyr::mutate(date = as.Date(.data$SURGERY_DATE),
                      case_class = .data$CASE_CLASS,
                      or_service = factor(x = .data$OR_SERVICE, levels = services)) %>%
        dplyr::select(.data$date, .data$case_class, .data$or_service) -> filtered_data

    # Look at counts for most common procedures
    filtered_data %>%
        dplyr::group_by(.data$date, .data$or_service, .drop = FALSE) %>%
        dplyr::tally() %>%
        tidyr::pivot_wider(names_from = .data$or_service, 
                           values_from = .data$n, 
                           values_fill = 0) -> 
        proc_data -> result$data -> result$summary

    # Look at number of procedures 
    #filtered_data %>%
    #    #dplyr::filter(.data$case_class == "Urgent") %>%
    #    dplyr::group_by(.data$date) %>%
    #    dplyr::summarize(count = dplyr::n()) ->
    #    count

    #proc_data %>% dplyr::left_join(count) -> result$data -> result$summary

    result
}

#' Process all surgery files in a folder and generate qc reports
#' @param data_folder the folder containing the raw data
#' @param services the operating room services (surgery types) to analyze
#' @param report_folder the folder to write reports to, default is
#'     data_folder with "_Reports" appended. Must exist.
#' @param pattern the pattern to distinguish surgery files, default
#'     "LAB-BB-CSRP-Surgery*" appearing anywhere
#' @return a combined dataset
#' @importFrom tools file_path_sans_ext
#' @importFrom tidyr replace_na
#' @importFrom writexl write_xlsx
#' @export
process_all_surgery_files <- function(data_folder,
                                      services,
                                      report_folder = file.path(dirname(data_folder),
                                                                paste0(basename(data_folder),
                                                                       "_Reports")),
                                      pattern = "LAB-BB-CSRP-Surgery*") {
    fileList <- list.files(data_folder, pattern = pattern , full.names = TRUE)
    names(fileList) <- basename(fileList)
    raw_surgery <- lapply(fileList, read_one_surgery_file, services = services)

    for (item in raw_surgery) {
        save_report_file(report_tbl = item$report,
                         report_folder = report_folder,
                         filename = item$filename)
    }

    Reduce(f = rbind,
           lapply(raw_surgery, function(x) x$surgery_data)) %>%
        dplyr::distinct() %>%
        dplyr::arrange(date) %>% replace(., is.na(.), 0)
}

#' Construct a tibble containing the quartiles of the CBC values and lagged features
#' @param cbc the cbc data tibble
#' @param cbc_quantiles a named list of site-specific quantile functions for each cbc
#' @return a tibble of containing cbc variables of interest grouped by
#'     date
#' @importFrom magrittr %>%
#' @importFrom dplyr inner_join mutate select group_by summarize ungroup distinct first
#' @importFrom tidyr spread replace_na
#' @importFrom tibble as_tibble
#' @importFrom rlang quo !! .data
#' @export
create_cbc_features <- function(cbc, cbc_quantiles) {
    quo_base_name <- rlang::quo(.data$BASE_NAME)
    cbc %>%
        dplyr::select(.data$date, .data$BASE_NAME, .data$CBC_VALUE) %>%
        dplyr::group_by(.data$date, .data$BASE_NAME) %>%
        dplyr::mutate(q25 = cbc_quantiles[[dplyr::first(!!quo_base_name)]](.data$CBC_VALUE)) %>%
        dplyr::mutate(below = (.data$CBC_VALUE < .data$q25),
                      above = (.data$CBC_VALUE >= .data$q25)) %>%
        dplyr::summarize(count = dplyr::n(),
                         below_q25 = sum(.data$below, na.rm = TRUE),
                         above_q25 = sum(.data$above, na.rm = TRUE)) %>%
        dplyr::mutate(multiplier = ifelse(.data$BASE_NAME == 'WBC', 0, 1)) %>%
        dplyr::ungroup(.data$date) %>%
        dplyr::mutate(nq25 = .data$multiplier * .data$below_q25 + (1 - .data$multiplier) * .data$above_q25) %>%
        dplyr::select(.data$date, .data$BASE_NAME, .data$count, .data$nq25) ->
        tmp

    tmp %>%
        dplyr::select(.data$date, .data$BASE_NAME, .data$count) %>%
        tidyr::spread(.data$BASE_NAME, .data$count) ->
        tmp1

    tmp %>%
        dplyr::select(.data$date, .data$BASE_NAME, .data$nq25) %>%
        tidyr::spread(key = .data$BASE_NAME, value = .data$nq25) ->
        tmp2

    tmp1 %>%
        dplyr::inner_join(tmp2, by = "date", suffix = c("_N", "_Nq")) ->
        cbc_data

    ## Finally replace all NAs with 0's, except for first column
    replacement <- lapply(names(cbc_data)[-1], function(x) 0)
    names(replacement) <- names(cbc_data)[-1]

    tidyr::replace_na(cbc_data, replacement) %>%
        dplyr::distinct() %>%
            dplyr::select(-ends_with("_N"))
}

#' Smooth the CBC features using a moving average
#' @param cbc_data with the date and features
#' @param window_size the window size to use in smoothing, default 7
#' @return the data with features smoothed as a tibble
#' @importFrom magrittr %>%
#' @importFrom dplyr select
#' @importFrom tibble as_tibble
#' @export
smooth_cbc_features <- function(cbc_data, window_size = 7L) {
    cbc_data %>%
        dplyr::select(-date) %>%
        apply(MARGIN = 2, FUN = ma, window_size = window_size) ->
        d
    data.frame(date = cbc_data$date, d) %>%
        tibble::as_tibble()
}

#' Add columns for days of the week to smoothed data
#' @param smoothed_cbc_features with the date and features
#' @return the data with days of the week columns added as a tibble
#' @importFrom magrittr %>%
#' @importFrom tibble as_tibble
#' @export
add_days_of_week_columns <- function(smoothed_cbc_features) {
    day_of_week_vector <- c(Mon = 0, Tue = 0, Wed = 0,
                            Thu = 0, Fri = 0, Sat = 0, Sun = 0)
    day_of_week <- t(sapply(base::weekdays(smoothed_cbc_features$date, abbreviate = TRUE),
                            function(x) {
                                y <- day_of_week_vector
                                y[x] <- 1
                                y
                            })
                     )
    cbind(smoothed_cbc_features, day_of_week) %>%
        tibble::as_tibble()
}
#' Construct a dataset for use in forecasting
#' @param config a site-specific configuration list
#' @param cbc_features the tibble of cbc features
#' @param census the census data
#' @param surgery the surgery data
#' @param transfusion the transfusion data
#' @return a single tibble that contains the response, features and date (1st col)
#' @importFrom magrittr %>%
#' @importFrom dplyr inner_join mutate select rename
#' @importFrom tidyselect ends_with starts_with
#' @importFrom tidyr spread replace_na
#' @importFrom rlang quo !!
#' @export
create_dataset <- function(config,
                           cbc_features, 
                           census, 
                           surgery, 
                           transfusion) {
    
    transfusion %>%
        dplyr::rename(plt_used = .data$used) %>%
        dplyr::mutate(lag = ma(.data$plt_used, window_size = config$lag_window)) %>%
        dplyr::inner_join({
            cbc_features %>%
                smooth_cbc_features(window_size = config$lag_window) %>%
                add_days_of_week_columns()
        }, by = "date") %>%
        dplyr::inner_join(census, by = "date") %>%
        dplyr::inner_join(surgery, by = "date") ->
        dataset
    
    if (nrow(dataset) != nrow(transfusion)) {
        loggit::loggit(log_lvl = "ERROR", log_msg = "Missing data for some dates in cbc_quartiles, census, surgery, or transfusion")
        stop("Missing data in for some dates in cbc_quartiles, census, surgery, or transfusion")
    }
    
    ## Order the columns as we expect
    ## date, followed by names of days of week, seven_lag, other predictors, response
    response <- "plt_used"
    days_of_week <- c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
    first_columns <- c("date", days_of_week, "lag")
    other_columns <- setdiff(names(dataset), c(first_columns, response))
    dataset[, c(first_columns, other_columns, response)]
}

#' Scale the dataset and return the scaling parameters
#' @param dataset the dataset of date column, plus features
#' @param center the values to use for centering if not NULL
#' @param scale the values to use for scaling if not NULL
#' @return a list consisting of scaled data (date not touched) and center and scale
#' @importFrom magrittr %>%
#' @importFrom dplyr select
#' @importFrom tibble as_tibble
#' @export
scale_dataset <- function(dataset, center = NULL, scale = NULL) {

    dataset %>%
        dplyr::select(-.data$date, -.data$plt_used) ->
        data_matrix -> scaled_data

    missing_center <- is.null(center)
    missing_scale <- is.null(scale)

    if (missing_center && missing_scale) {
        data_matrix %>%
            scale ->
            scaled_data
        scale_info <- attributes(scaled_data)
        return(list(scaled_data = tibble::as_tibble(data.frame(date = dataset$date,
                                                               scaled_data,
                                                               plt_used = dataset$plt_used,
                                                               check.names = FALSE)),
                    center = scale_info$`scaled:center`,
                    scale = scale_info$`scaled:scale`))
    }

    if (!missing_center) {
        scaled_data <- sweep(x = scaled_data, MARGIN = 2L, STATS = center)
    }
    if (!missing_scale) {
        scaled_data <- sweep(x = scaled_data, MARGIN = 2L, STATS = scale, FUN = "/")
    }

    list(scaled_data = tibble::as_tibble(data.frame(date = dataset$date,
                                                    scaled_data,
                                                    plt_used = dataset$plt_used,
                                                    check.names = FALSE)),
         center = center,
         scale = scale)
}

#' Process data for a particular date by reading files per configuration and date
#'
#' This has a side-effect of producing reports in the reports folder
#' specified in the site-specific configuration object as per
#' \code{\link{get_SBC_config}}
#' @param config a site-specific configuration list
#' @param date a string in YYYY-mm-dd format, default today
#' @return a list of cbc, census, and transfusion tibbles
#' @importFrom magrittr %>%
#' @importFrom dplyr filter mutate select group_by summarize distinct
#'     first
#' @importFrom rlang .data
#' @importFrom tools file_path_sans_ext
#' @importFrom tidyr replace_na
#' @importFrom writexl write_xlsx
#' @export
process_data_for_date <- function(config,
                                  date = as.character(Sys.Date(), format = "%Y-%m-%d")) {

    loggit::set_logfile(logfile = paste(config$log_folder, sprintf(config$log_filename_prefix, date), sep="/"))

    cbc_file <- list.files(path = config$data_folder,
                           pattern = sprintf(config$cbc_filename_prefix, date),
                           full.names = TRUE)

    census_file <- list.files(path = config$data_folder,
                              pattern = sprintf(config$census_filename_prefix, date),
                              full.names = TRUE)
    
    surgery_file <- list.files(path = config$data_folder,
                               pattern = sprintf(config$surgery_filename_prefix, date),
                               full.names = TRUE)

    transfusion_file <- list.files(path = config$data_folder,
                                   pattern = sprintf(config$transfusion_filename_prefix, date),
                                   full.names = TRUE)

    inventory_file <- list.files(path = config$data_folder,
                                 pattern = sprintf(config$inventory_filename_prefix, date),
                                 full.names = TRUE)
    
    if (length(cbc_file) != 1L || length(census_file) != 1L || length(surgery_file) != 1L ||
        length(transfusion_file) != 1L || length(inventory_file) > 1L) {
        loggit::loggit(log_lvl = "ERROR", log_msg = "Too few or too many files matching patterns!")
        stop("Too few or too many files matching patterns!")
    }

    # Process CBC data
    cbc_data <- read_one_cbc_file(cbc_file,
                                  cbc_abnormals = config$cbc_abnormals,
                                  cbc_vars = config$cbc_vars)

    save_report_file(report_tbl = cbc_data$report,
                     report_folder = config$report_folder,
                     filename = cbc_data$filename)

    cbc_data$cbc_data %>%
        dplyr::filter(.data$BASE_NAME %in% config$cbc_vars) %>%
        dplyr::rename(date = .data$RESULT_DATE) %>%
        dplyr::distinct() ->
        cbc

    # Process Census data
    census_data <- read_one_census_file(census_file,
                                        locations = config$census_locations)

    save_report_file(report_tbl = census_data$report,
                     report_folder = config$report_folder,
                     filename = census_data$filename)

    census_data <- census_data$census_data
    
    # (This is to set 0 as the NA replacement option for every census column,
    # as census appears to be the most likely group of features to change)
    # Not sure this step is necessary.
    replacement <- lapply(names(census_data)[-1], function(x) 0)
    names(replacement) <- names(census_data)[-1]
    census_data %>%
        tidyr::replace_na(replace = replacement) %>%
        dplyr::distinct() %>%
        dplyr::arrange(date) ->
        census
    
    # Process Surgery data
    surgery_data <- read_one_surgery_file(surgery_file,
                                          services = config$surgery_services)
    
    save_report_file(report_tbl = surgery_data$report,
                     report_folder = config$report_folder,
                     filename = surgery_data$filename)

    surgery_data$surgery_data %>%
        dplyr::distinct() %>%
        dplyr::arrange(date) ->
        surgery
    
    # Process Transfusion data
    transfusion_data <- read_one_transfusion_file(transfusion_file)

    save_report_file(report_tbl = transfusion_data$report,
                     report_folder = config$report_folder,
                     filename = transfusion_data$filename)

    transfusion_data$transfusion_data %>%
        dplyr::distinct() %>%
        dplyr::arrange(date) ->
        transfusion

    # Process inventory data
    if (length(inventory_file) > 0) {
        inventory_data <- read_one_inventory_file(inventory_file)
        save_report_file(report_tbl = inventory_data$report,
                         report_folder = config$report_folder,
                         filename = inventory_data$filename)

        inventory_data$inventory_data %>%
            dplyr::distinct() %>%
            dplyr::arrange(date) ->
            inventory
    } else {
        inventory = NULL
    }

    list(cbc = cbc,
         census = census,
         surgery = surgery,
         transfusion = transfusion,
         inventory = inventory)
}

#' Save a generated report file in the report folder with the appropriate name
#'
#' @param report_tbl the report tibble
#' @param report_folder the report folder
#' @param filename the name of the file
#' @importFrom tools file_path_sans_ext
#' @importFrom writexl write_xlsx
#' @export
save_report_file <- function(report_tbl, report_folder, filename) {
    basename <- basename(filename)
    xlsx_file <- file.path(report_folder,
                           paste0(tools::file_path_sans_ext(basename), "-summary.xlsx"))
    loggit::loggit(log_lvl = "INFO", log_msg = sprintf("Writing Report %s\n", xlsx_file))
    invisible(writexl::write_xlsx(report_tbl, path = xlsx_file))
}

#' Read a single inventory file and return it, as is
#'
#' @param filename the fully qualified path of the file
#' @param date an optional date of inventory in case the date is not
#'     to be automatically inferred as the day before the date in the
#'     filename
#' @return a list of four items, filename, raw_data (tibble), report a
#'     list consisting of summary tibble, census_data (tibble)
#' @importFrom readr cols col_integer col_character col_datetime
#'     read_tsv
#' @importFrom readxl read_excel
#' @importFrom lubridate ymd_hms ddays
#' @importFrom stringr str_match
#' @export
read_one_inventory_file <- function(filename, date = NULL) {
    if (is.null(date)) {
        date_string <- paste(
            substring(stringr::str_match(string = basename(filename),
                                         pattern = "[0-9\\-]+")[1, 1], 1, 10),
            "23:59:59")
        ## Inventory is for the day before
        date <- lubridate::ymd_hms(date_string, tz = "America/Los_Angeles") - lubridate::ddays(1)
    }

    loggit::loggit(log_lvl = "INFO", log_msg = paste("Processing", basename(path = filename),
                                                       "for", as.character(date), "inventory"))
    raw_data <- readxl::read_excel(path = filename)

    ## Stop if no data
    if (nrow(raw_data) < 1) {
        loggit::loggit(log_lvl = "ERROR", log_msg = sprintf("No data in file %s", filename))
        stop(sprintf("No data in file %s", filename))
    }
    processed_data <- summarize_and_clean_inventory(raw_data, date)
    if (processed_data$errorCode != 0) {
        loggit::loggit(log_lvl = "ERROR", log_msg = processed_data$errorMessage)
        stop(processed_data$errorMessage)
    }
    list(filename = filename,
         raw_data = raw_data,
         report = list(summary = processed_data$summary),
         inventory_data = processed_data$data)
}

#' Summarize and clean the raw transfusion data
#' @param raw_data the raw data tibble
#' @param date the date for which this is the inventory
#' @return a list of four items; errorCode (nonzero if error),
#'     errorMessage if any, the summary data tibble, the data tibble
#'     filtered with relevant columns for us
#' @importFrom magrittr %>%
#' @importFrom dplyr filter mutate select group_by summarize distinct
#' @importFrom rlang quo !! .data
#' @export
summarize_and_clean_inventory <- function(raw_data, date) {
    result <- list(errorCode = 0,
                   errorMessage = "")
    ## There are known issue with the format when CMV is produced, but
    ## the filtering of PLT records automatically cleans them up.

    raw_data %>%
        dplyr::distinct(.keep_all = TRUE) %>%
        dplyr::filter(.data$Type == "PLT") %>%
        dplyr::mutate(Days_To_Expiry = .data$`Days to Expire`) %>%
        dplyr::mutate(Already_Expired = (.data$Days_To_Expiry <= 0)) %>%
        dplyr::group_by(.data$Already_Expired) ->
        result$summary

    result$summary %>%
        dplyr::filter(!.data$Already_Expired) %>%
        dplyr::mutate(date = date,
                      Expiry_Time = lubridate::ymd_hms(paste0(.data$`Exp. Date`, ' ', .data$`Exp. Time`, '00'))) %>%
        group_by(.data$date) %>%
        summarize(count = dplyr::n(),
                  r1 = sum((.data$Days_To_Expiry > 0) & (.data$Days_To_Expiry <= 1)),
                  r2 = sum((.data$Days_To_Expiry > 1) & (.data$Days_To_Expiry <= 2)),
                  r3_plus = sum(.data$Days_To_Expiry > 2)) %>%
        dplyr::select(.data$date, .data$count, .data$r1, .data$r2, .data$r3_plus) ->
        result$data

    result
}

#' Process all inventory files in a folder and generate qc reports
#' @param data_folder the folder containing the raw data
#' @param report_folder the folder to write reports to, default is
#'     data_folder with "_Reports" appended. Must exist.
#' @param pattern the pattern to distinguish CBC files, default
#'     "Daily_Product_Inventory_Report_Morning_To_Folder*" appearing anywhere
#' @return a combined dataset
#' @importFrom tools file_path_sans_ext
#' @importFrom tidyr replace_na
#' @importFrom writexl write_xlsx
#' @export
process_all_inventory_files <- function(data_folder,
                                        report_folder = file.path(dirname(data_folder),
                                                                  paste0(basename(data_folder),
                                                                         "_Reports")),
                                        pattern = "Daily_Product_Inventory_Report_Morning_To_Folder*") {
    fileList <- list.files(data_folder, pattern = pattern , full.names = TRUE)
    names(fileList) <- basename(fileList)
    raw_inventory <- lapply(fileList, read_one_inventory_file)

    for (item in raw_inventory) {
        save_report_file(report_tbl = item$report,
                         report_folder = report_folder,
                         filename = item$filename)
    }

    Reduce(f = rbind,
           lapply(raw_inventory, function(x) x$inventory_data)) %>%
        dplyr::arrange(date)
}


#' Predict usage for a specified date
#'
#' This function updates the saved datasets (therefore, has
#' side-effects) by reading incremental data for a specified date. The
#' \code{prev_day} argument can be specified in case the pipeline
#' fails for some reason to catch up. Note that the default set up is
#' one where the prediction is made on the morning of day \eqn{i + 1}
#' for day \eqn{i}.
#'
#' @param config the site configuration
#' @param date the date string for which the data is to be processed in "YYYY-mm-dd" format
#' @param prev_day the previous date, default NA, which means it is computed from date
#' @importFrom pip build_model predict_three_day_sum
#' @importFrom magrittr %>%
#' @importFrom dplyr group_by summarize_all
#' @return a prediction tibble named prediction_df with a column for date and the prediction
#' @importFrom loggit set_logfile loggit
#' @export
predict_for_date <- function(config,
                             date = as.character(Sys.Date(), format = "%Y-%m-%d"),
                             prev_day = NA) {

    ## Previous date is one day before unless specified explicity
    if (is.na(prev_day))
        prev_day <- as.character(as.Date(date, format = "%Y-%m-%d") - 1, format = "%Y-%m-%d")

    loggit::loggit(log_lvl = "INFO", log_msg = paste("Step 1. Loading previously processed data on", prev_day))
    prev_data <- readRDS(file = file.path(config$output_folder,
                                          sprintf(config$output_filename_prefix, prev_day)))
    ## Process data for the date
    loggit::loggit(log_lvl = "INFO", log_msg = paste("Step 2. Processing incremental data for date", date))
    result <- process_data_for_date(config = config, date = date)

    ## If the incrementals only contain data for multiple dates, reduce via sum
    ## This only applies to census and transfusion since the cbc data is not summarized
    ## until the cbc_features are created, where the procedure will capture the additional data
    ## for a repeated date during grouping
    multiple_dates_in_increment <- FALSE
    unique_cbc_dates <- unique(result$cbc$date)
    if (length(unique_cbc_dates) > 1L) {
        loggit::loggit(log_lvl = "WARN", log_msg = "Multiple dates in cbc file, model retraining forced!")
        loggit::loggit(log_lvl = "WARN", log_msg = unique_cbc_dates)
        multiple_dates_in_increment <- TRUE
    }
    unique_census_dates <- unique(result$census$date)
    if (length(unique_census_dates) > 1L) {
        loggit::loggit(log_lvl = "WARN", log_msg = "Multiple dates in census file, model retraining forced!")
        loggit::loggit(log_lvl = "WARN", log_msg = unique_census_dates)
        multiple_dates_in_increment <- TRUE
    }
    
    unique_surgery_dates <- unique(result$surgery$date)
    if (length(unique_surgery_dates) > 1L) {
        loggit::loggit(log_lvl = "WARN", log_msg = "Multiple dates in surgery file, model retraining forced!")
        loggit::loggit(log_lvl = "WARN", log_msg = unique_surgery_dates)
        multiple_dates_in_increment <- TRUE
    }
    
    unique_transfusion_dates <- unique(result$transfusion$date)
    if (length(unique_transfusion_dates) > 1L) {
        loggit::loggit(log_lvl = "WARN", log_msg = "Multiple dates in transfusion file, model retraining forced!")
        loggit::loggit(log_lvl = "WARN", log_msg = unique_transfusion_dates)
        multiple_dates_in_increment <- TRUE
    }

    ## Update prev_data with increment along with the model_age
    loggit::loggit(log_lvl = "INFO", log_msg = "Step 3. Adding new increment to previous data")
    date_diff <- as.integer(as.Date(date) - as.Date(prev_day))

    ## For cbc, we don't need to do anything for data for previous dates since the summarization
    ## of cbc_features will automatically handle those dates.
    cbc <- prev_data$cbc <- dplyr::bind_rows(prev_data$cbc, result$cbc)

    ## For census, we need to add any new data for previous dates, using sum
    dplyr::bind_rows(prev_data$census, result$census) %>%
        dplyr::group_by(date) %>%
        dplyr::summarize_all(sum) ->
        census ->
        prev_data$census
    
    ## For surgery, we need to add any new data for previous dates, using sum
    dplyr::bind_rows(prev_data$surgery, result$surgery) %>%
        dplyr::group_by(date) %>%
        dplyr::summarize_all(sum) ->
        surgery ->
        prev_data$surgery

    ## For transfusion, we need to add any new data for previous dates, using sum
    dplyr::bind_rows(prev_data$transfusion, result$transfusion) %>%
        dplyr::group_by(date) %>%
        dplyr::summarize_all(sum) ->
        transfusion ->
        prev_data$transfusion

    prev_data$inventory <- inventory <- dplyr::bind_rows(prev_data$inventory, result$inventory)

    loggit::loggit(log_lvl = "INFO", log_msg = "Step 3a. Creating CBC features")

    ## Create dataset. We add the lag window to avoid NAs at the beginning of the dataset
    cbc_features <- tail(create_cbc_features(cbc = cbc, cbc_quantiles = config$cbc_quantiles),
                         config$history_window + config$lag_window + 1L)
    census <- tail(census, config$history_window + config$lag_window + 1L)
    surgery <- tail(surgery, config$history_window + config$lag_window + 1L)
    transfusion <- tail(transfusion, config$history_window + config$lag_window + 1L)

    loggit::loggit(log_lvl = "INFO", log_msg = "Step 3b. Creating training/prediction dataset")

    # define all variables (this allows mismatch between RDS columns and config)
    cbc_names <- sapply(config$cbc_vars, function(x) paste0(x, "_Nq"))
    all_vars <- c("date", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", "lag",
                  cbc_names, 
                  config$census_locations, 
                  config$surgery_services,
                  "plt_used")

    dataset <- prev_data$dataset <- create_dataset(config,
                                                   cbc_features = cbc_features,
                                                   census = census,
                                                   surgery = surgery,
                                                   transfusion = transfusion) %>% dplyr::select(all_vars)

    recent_data <- tail(dataset, n = config$history_window + 1L)
    training_data <- head(recent_data, n = config$history_window)
    new_data <- tail(recent_data, n = 1L)

    ## If it is time to update the model, do so
    ## One way that can happen...
    model_changed <- model_config_changed(prev_data$config, config)
    loggit::loggit(log_lvl = "INFO", log_msg = "Step 4. Checking model age")
    model_needs_updating <- (multiple_dates_in_increment ||
                             is.null(prev_data$model_age) ||
                             model_changed ||
                             (prev_data$model_age %% config$model_update_frequency == 0L) ||
                             (date_diff > config$model_update_frequency))
    if (model_needs_updating) {
        ## Provide informative log
        if (is.null(prev_data$model_age)) {
            prev_data$model_age <- 0L  ## Set age to 0 for first time
            loggit::loggit(log_lvl = "INFO", log_msg = "Step 4.1. First time, so building model")
        } else if (multiple_dates_in_increment) {
            loggit::loggit(log_lvl = "INFO", log_msg = "Step 4.1. Multiple dates in data increment, so model training forced")
        } else if (model_changed) {
            loggit::loggit(log_lvl = "INFO", log_msg = "Step 4.1. Model changed, so model rebuilding and training forced")
        } else {
            loggit::loggit(log_lvl = "INFO", log_msg = "Step 4.1. Model is stale, so updating model")
        }

        prev_data$scaled_dataset <- scaled_dataset <- scale_dataset(training_data) # center and scale are NULL

        # ensure that no NA values are fed into build_model
        data <- as.data.frame(scaled_dataset$scaled_data, optional=TRUE)

        if (sum(is.na(data)) > 0) {
            data[is.na(data)] <- 0
            loggit::loggit(log_lvl = "WARN", log_msg ='Warning: NA values found in scaled dataset - replacing with 0')
        }

        prev_data$model <- pip::build_model(data = data,
                                            c0 = config$c0,
                                            history_window = config$history_window,
                                            penalty_factor = config$penalty_factor,
                                            initial_expiry_data = config$initial_expiry_data,
                                            initial_collection_data = config$initial_collection_data,
                                            start = config$start,
                                            min_lambda = config$min_lambda
                                            )
    } else {
        loggit::loggit(log_lvl = "INFO", log_msg = "Step 4.1. Using previous model and scaling")
        ## use previous scaling which is available in the saved scaled_dataset

        prev_data$scaled_dataset <- scaled_dataset <- scale_dataset(training_data,
                                                                    center = prev_data$scaled_dataset$center,
                                                                    scale = prev_data$scaled_dataset$scale)
    }

    ## Make prediction and update dataset for prediction
    loggit::loggit(log_lvl = "INFO", log_msg = "Step 5. Predicting and bumping model age")
    new_scaled_data <- scale_dataset(new_data,
                                     center = scaled_dataset$center,
                                     scale = scaled_dataset$scale)$scaled_data
    
    prediction <- pip::predict_three_day_sum(model = prev_data$model,
                                             new_data = as.data.frame(new_scaled_data, optional=TRUE)) ## last row is what we  want to predict for
    # Make sure prediction is returning a valid response. No point in continuing otherwise.
    if (is.nan(prediction)) {
        stop(sprintf("Next three day prediction returned NaN for %s", date))
    }

    prediction_df <- tibble::tibble(date = new_data$date, t_pred = prediction)

    if (is.null(prev_data$prediction_df)) {
        prev_data$prediction_df <- prediction_df
    } else {
        prev_data$prediction_df <- rbind(prev_data$prediction_df, prediction_df)
    }

    prev_data$model_age <- prev_data$model_age + date_diff ## should it be by the diff?
    ## Save configuration as well
    prev_data$config <- config

    ## Save dataset back for next day
    loggit::loggit(log_lvl = "INFO", log_msg = "Step 6. Save results for next day")
    saveRDS(prev_data, file = file.path(config$output_folder,
                                        sprintf(config$output_filename_prefix, date)))
    prediction_df
}

#' Get the actual prediction and platelet usage data from saved files for each date
#'
#' This function reads a saved dataset and returns a tibble with a
#' date, platelet usage, and three day predicted sum ensuring that the
#' prediction and dates are lined up correctly.
#'
#' @param config the site configuration
#' @param start_date the starting date in YYYY-mm-dd format
#' @param end_date the end date in YYYY-mm-dd format
#' @return a tibble of three variables: date, the corresponding
#'     prediction and the platelet usage for that date
#' @importFrom magrittr %>%
#' @importFrom dplyr select left_join
#' @export
get_prediction_and_usage <- function(config, start_date, end_date) {
    dates <- seq.Date(from = start_date, to = end_date, by = 1L)
    output_files <- list.files(path = config$output_folder,
                               pattern = paste0("^",
                                                substring(config$output_filename_prefix, first = 1, last = 10)),
                               full.names = TRUE)
    d <- readRDS(tail(output_files, 1L))
    d$dataset %>%
        dplyr::select(.data$date, .data$plt_used) ->
        d2
    tibble::tibble(date = dates) %>%
        dplyr::left_join(d2, by = "date") %>%
        dplyr::left_join(d$prediction_df, by = "date")
}

#' Given the configuration and prediction data frame, build a prediction table
#'
#' @param config the site configuration
#' @param start_date the starting date in YYYY-mm-dd format
#' @param end_date the end date in YYYY-mm-dd format, by default today plus 2 days
#' @param offset the offset to use to line up the initial settings for
#'     expiry and collection units; default is config$start - 1, which
#'     matches the usage in training the model, but can be any
#'     non-negative number less than the number of predictions made
#' @param generate_report a flag indicating whether a report needs to
#'     be generated as a side effect
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
#' @export
build_prediction_table <- function(config, start_date, end_date = Sys.Date() + 2, generate_report = TRUE,
                                   offset = config$start - 1,
                                   min_inventory = config$min_inventory) {
    dates <- seq.Date(from = start_date, to = end_date, by = 1)
    output_files <- list.files(path = config$output_folder,
                               pattern = paste0("^",
                                                sprintf(config$output_filename_prefix, as.character(end_date))),
                               full.names = TRUE)

    if (length(output_files) == 0) {
        stop(sprintf("No output file found for the prediction end date: %s", as.character(end_date)))
    }
    d <- readRDS(tail(output_files, 1L))

    #d$dataset %>%
    #    dplyr::select(.data$date, .data$plt_used) ->
    #    d2

    # IMPORTANT:
    # d$dataset only includes the "history_window" used to retrain the model + 7 days
    # d$prediction_df includes all dates in the prediction range
    # d$transfusion, d$census, etc. include all seed dates + prediction dates
    d2 <- tail(d$transfusion, nrow(d$prediction_df)) %>%
        rename(plt_used = used) %>% distinct(date, .keep_all = TRUE)

    # Important to replace plt_used and t_pred NA values with 0
    tibble::tibble(date = dates) %>%
        dplyr::left_join(d2, by = "date") %>%
        dplyr::left_join(d$prediction_df, by = "date") %>%
        distinct(date, .keep_all = TRUE) %>%
        tidyr::replace_na(list(plt_used = 0, t_pred = 0)) ->
        prediction_df

    N <- nrow(prediction_df)
    if (offset >= N) {
        loggit::loggit(log_lvl = "ERROR", log_msg = "Not enough predictions!")
        stop("Not enough predictions!")
    }

    y <- prediction_df$plt_used
    t_pred <- prediction_df$t_pred
    initial_expiry_data <- config$initial_expiry_data
    pred_mat <- matrix(0, nrow = N + 3, ncol = 12)
    colnames(pred_mat) <- c("Alert", "r1", "r2", "w", "x", "s", "t_adj",
                            "r1_adj", "r2_adj","w_adj", "x_adj", "s_adj")

    pred_mat[offset + (1:3), "x"] <- config$initial_collection_data
    pred_mat[offset + (1:3), "x_adj"] <- config$initial_collection_data
    index <- offset + 1
    t_adj <- t_pred

    pred_mat[index, "w"] <- pip::pos(initial_expiry_data[1] - y[index])
    pred_mat[index, "r1"] <- pip::pos(initial_expiry_data[1] + initial_expiry_data[2] - y[index] - pred_mat[index, "w"])
    pred_mat[index, "s"] <- pip::pos(y[index] - initial_expiry_data[1] - initial_expiry_data[2] - pred_mat[index, "x"])
    # Do we need waste below? I think so.
    pred_mat[index, "r2"] <- pip::pos(pred_mat[index, "x"] - pip::pos(y[index] + pred_mat[index, "w"] - initial_expiry_data[1] - initial_expiry_data[2]))
    pred_mat[index + 3, "x"] <- floor(pip::pos(t_pred[index] - pred_mat[index + 1, "x"] - pred_mat[index + 2, "x"] - pred_mat[index, "r1"] - pred_mat[index, "r2"] + 1))
    pred_mat[index + 3, "x_adj"] <- floor(pip::pos(t_pred[index] - pred_mat[index + 1, "x"] - pred_mat[index + 2, "x"] - pred_mat[index, "r1"] - pred_mat[index, "r2"] + 1))

    for (i in seq.int(index + 1L, N)) {
        # These are the constraint parameters without adjusting for the minimum inventory
        pred_mat[i, "w"] <- pip::pos(pred_mat[i - 1 , "r1"] - y[i])
        pred_mat[i, "r1"] <- pip::pos(pred_mat[i - 1, "r1"] + pred_mat[i - 1, "r2"] - y[i] - pred_mat[i, "w"])
        pred_mat[i, "s"] <- pip::pos(y[i] - pred_mat[i - 1, "r1"] - pred_mat[i - 1, "r2"] - pred_mat[i, "x"])
        # Do we need waste below? I think so.
        pred_mat[i, "r2"] <- pip::pos(pred_mat[i, "x"] - pip::pos(y[i] + pred_mat[i, "w"] - pred_mat[i - 1, "r1"] - pred_mat[i - 1, "r2"]))
        pred_mat[i + 3, "x"] <- floor(pip::pos(t_pred[i] - pred_mat[i + 1, "x"] - pred_mat[i + 2, "x"] - pred_mat[i, "r1"] - pred_mat[i, "r2"] + 1))

        # This set ensures that we have ordered not only enough to satisfy our prediction, but
        # also enough to replenish to our minimum inventory.
        pred_mat[i, "w_adj"] <- pip::pos(pred_mat[i - 1 , "r1_adj"] - y[i])
        pred_mat[i, "r1_adj"] <- pip::pos(pred_mat[i - 1, "r1_adj"] + pred_mat[i - 1, "r2_adj"] - y[i] - pred_mat[i, "w_adj"])
        pred_mat[i, "s_adj"] <- pip::pos(y[i] - pred_mat[i - 1, "r1_adj"] - pred_mat[i - 1, "r2_adj"] - pred_mat[i, "x_adj"])
        # Do we need waste below? I think so.
        pred_mat[i, "r2_adj"] <- pip::pos(pred_mat[i, "x_adj"] - pip::pos(y[i] + pred_mat[i, "w_adj"] - pred_mat[i - 1, "r1_adj"] - pred_mat[i - 1, "r2_adj"]))
        pred_mat[i+3,"x_adj"] <- floor(pip::pos(t_pred[i] + pip::pos(min_inventory - pred_mat[i, "r1"] - pred_mat[i,"r2"]) - pred_mat[i + 1, "x_adj"] - pred_mat[i + 2, "x_adj"] - pred_mat[i, "r1_adj"] - pred_mat[i, "r2_adj"] + 1))

        # Why do we adjust the 3 day usage prediction? This seems independent of the inventory.
        t_adj[i] = t_adj[i] + pip::pos(min_inventory - pred_mat[i,"r1"] - pred_mat[i,"r2"])
    }
    pred_mat[1:N,"t_adj"] = t_adj

    pred_mat[, "Alert"] <- (pred_mat[, "r1"] + pred_mat[, "r2"] <= min_inventory)

    d$inventory %>%     ## Drop the time part!
        dplyr::mutate(date = as.Date(date)) ->
        inventory

    tibble::as_tibble(cbind(prediction_df, pred_mat[seq_len(N), ])) %>%
        dplyr::mutate(t_true = dplyr::lead(plt_used, 1) + dplyr::lead(plt_used, 2) + dplyr::lead(plt_used, 3)
                      ) %>%
        dplyr::relocate(t_true, .after = plt_used) %>%
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
                           "No. to order per prediction",
                           "Shortage",
                           "Adj. three-day prediction",
                           "Adj. no. expiring in 1 day",
                           "Adj. no. expiring in 2 days",
                           "Adj. waste",
                           "Adj. no. to order",
                           "Adj. shortage",
                           "Inv. count",
                           "Inv. expiring in 1 day",
                           "Inv. expiring in 2 days",
                           "Inv. expiring in 2+ days")

    # Compute "true" values for waste, fresh orders, and shortage
    pred_table %>%
        dplyr::mutate(`True Waste` = pip::pos(`Inv. expiring in 1 day` - `Platelet usage`)) %>%
        dplyr::mutate(`Fresh Units Ordered` = lead(`Inv. count`, 1) - `Inv. count` +
                          `Platelet usage` + `True Waste`) %>%
        dplyr::mutate(`True Shortage` = pip::pos(`Platelet usage` - `Inv. count` - `Fresh Units Ordered`)) ->
        pred_table

    if (generate_report) {
        todays_date <- as.character(Sys.Date(), format = "%Y-%m-%d")
        filename <- sprintf("prediction-report-%s", todays_date)
        save_report_file(report_tbl = pred_table,
                         report_folder = config$report_folder,
                         filename = filename)
    }

    pred_table
}

