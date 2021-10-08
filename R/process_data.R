#' Read a single cbc file data and return it, as is
#' @param filename the fully qualified path of the file
#' @param cbc_abnormals, a named list of functions of a single vector
#'     returning TRUE for abnormal values
#' @param cbc_vars, the names of values to include; others are excluded
#' @param org_cols, the names of columns at the target organization (specified in data mapping)
#' @return a list of four items, filename, raw_data (tibble), report a
#'     list of missing values tibble and a summary tibble, cbc_data
#'     (tibble)
#' @importFrom readr cols col_integer col_character col_datetime read_tsv
#' @importFrom dplyr rename
#' @importFrom loggit set_logfile loggit
#' @export
read_one_cbc_file <- function(filename, cbc_abnormals, cbc_vars, 
                              org_cols = c("ORDER_PROC_ID", "BASE_NAME", "RESULT_TIME", "ORD_VALUE")) {
    if (length(filename) != 1L){
        stop("Not enough CBC files found.")
    }
    
    ## ORD_VALUE can have values like "<0.1", so we read as char
    ## and convert as needed
    sbc_cols <- c("ORDER_PROC_ID", "BASE_NAME", "RESULT_TIME", "ORD_VALUE")
    
    ## Start with SBC names
    col_types <- list(
        ORDER_PROC_ID = readr::col_character(),
        BASE_NAME = readr::col_character(),
        RESULT_TIME = readr::col_datetime("%d-%b-%y %H:%M:%S"),
        ORD_VALUE = readr::col_character()
    )
    
    ## Rename essential columns using org-specific equivalents
    names(col_types) <- org_cols

    loggit::loggit(log_lvl = "INFO", log_msg = paste("Processing", basename(path = filename)))
    
    raw_data <- readr::read_tsv(file = filename, 
                                col_names = TRUE, 
                                col_types = do.call(readr::cols, col_types), 
                                col_select = org_cols, 
                                show_col_types = FALSE)
    
    ## Set column names as SBC equivalents
    names(raw_data) <- sbc_cols
    
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
#' @param org_cols list of target organization's column names (from data mapping)
#' @return a list of four items, filename, raw_data (tibble), report a
#'     list consisting of summary tibble, census_data (tibble)
#' @importFrom readr cols col_integer col_character col_datetime read_tsv
#' @export
read_one_census_file <- function(filename, locations, 
                                 org_cols = c("PAT_ID", "LOCATION_NAME", "LOCATION_DT")) {
    if (length(filename) != 1L){
        stop("Not enough census files found.")
    }
    
    sbc_cols <- c("PAT_ID", "LOCATION_NAME", "LOCATION_DT")
    
    ## Start with SBC names
    col_types <- list(
        PAT_ID = readr::col_character(),
        LOCATION_NAME = readr::col_character(),
        LOCATION_DT = readr::col_datetime("%m/%d/%Y  %I:%M:%S%p")
    )
    
    ## Rename essential columns using org-specific equivalents
    names(col_types) <- org_cols

    loggit::loggit(log_lvl = "INFO", log_msg = paste("Processing", basename(path = filename)))

    raw_data <- readr::read_tsv(file = filename, 
                                col_names = TRUE, 
                                col_types = do.call(readr::cols, col_types), 
                                col_select = org_cols, 
                                show_col_types = FALSE)
    
    ## Set column names as SBC equivalents
    names(raw_data) <- sbc_cols
    
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
#' @param org_cols list of target organization's column names (from data mapping)
#' @return a list of four items, filename, raw_data (tibble), report a
#'     list consisting of summary tibble, transfusion_data (tibble)
#' @importFrom readr cols col_integer col_character col_datetime read_tsv
#' @export
read_one_transfusion_file <- function(filename, 
                                      org_cols = c("DIN", "Product Code", "Type", "Issue Date/Time")) {
    if (length(filename) != 1L){
        stop("Not enough transfusion files found.")
    }
    
    sbc_cols <- c("DIN", "Product Code", "Type", "Issue Date/Time")

    col_types <- list(
        DIN = readr::col_character(),
        Type = readr::col_character(),
        `Product Code` = readr::col_character(),
        `Issue Date/Time` = readr::col_datetime("%m/%d/%Y  %I:%M:%S %p")
    )
    
    names(col_types) <- org_cols

    loggit::loggit(log_lvl = "INFO", log_msg = paste("Processing", basename(path = filename)))

    raw_data <- readr::read_tsv(file = filename, 
                                col_names = TRUE, 
                                col_types = do.call(readr::cols, col_types), 
                                col_select = org_cols, 
                                show_col_types = FALSE)
    
    ## Set column names as SBC equivalents
    names(raw_data) <- sbc_cols

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

#' Read a single day surgery file data and return tibble and summary
#' @param filenames the fully qualified path of the 3 files file
#' @param services the list of surgery types considered as features
#' @param org_cols list of target organization's column names (from data mapping)
#' @return a list of four items, filename, raw_data (tibble), report a
#'     list consisting of summary tibble, surgery_data (tibble)
#' @importFrom readr cols col_integer col_character col_datetime read_tsv
#' @export
read_one_surgery_file <- function(filename, services, 
                                  org_cols = c("LOG_ID", "OR_SERVICE", "SURGERY_DATE", "FIRST_SCHED_DATE", "CASE_CLASS")) {
    if (length(filename) != 1L){
        stop("Not enough surgery files found.")
    }
    
    sbc_cols <- c("LOG_ID", "OR_SERVICE", "SURGERY_DATE", "FIRST_SCHED_DATE", "CASE_CLASS")
    
    col_types <- list(
        LOG_ID = readr::col_character(),
        OR_SERVICE = readr::col_character(),
        SURGERY_DATE = readr::col_datetime("%m/%d/%Y  %I:%M:%S %p"),
        FIRST_SCHED_DATE = readr::col_datetime("%m/%d/%Y  %I:%M:%S %p"),
        CASE_CLASS = readr::col_character()
    )
    
    names(col_types) <- org_cols
    
    loggit::loggit(log_lvl = "INFO", log_msg = paste("Processing surgery file from", basename(path = filename)))
    
    raw_data <- readr::read_tsv(file = filename, 
                                col_names = TRUE, 
                                col_types = do.call(readr::cols, col_types), 
                                col_select = org_cols, 
                                show_col_types = FALSE)
    
    ## Set column names as SBC equivalents
    names(raw_data) <- sbc_cols
    
    ## Stop if no data
    if (nrow(raw_data) < 1) {
        loggit::loggit(log_lvl = "ERROR", log_msg = sprintf("No data in file %s", filename))
        stop(sprintf("No data in file %s", filename))
    }
    processed_data <- summarize_and_clean_surgery_single_day(raw_data, services)
    if (processed_data$errorCode != 0) {
        loggit::loggit(log_lvl = "ERROR", log_msg = processed_data$errorMessage)
        stop(processed_data$errorMessage)
    }
    list(filename = filename,
         raw_data = raw_data,
         report = list(summary = processed_data$summary),
         surgery_data = processed_data$data)
}

#' Read a surgery file data  for following 3 days and return tibble and summary
#' @param filenames the fully qualified path of the 3 files file
#' @param services the list of surgery types considered as features
#' @return a list of four items, filename, raw_data (tibble), report a
#'     list consisting of summary tibble, surgery_data (tibble)
#' @importFrom readr cols col_integer col_character col_datetime read_tsv
#' @export
read_next_three_surgery_files <- function(filenames, services, 
                                          org_cols = c("LOG_ID", "OR_SERVICE", "SURGERY_DATE", "FIRST_SCHED_DATE", "CASE_CLASS")) {
    if (length(filenames) != 3L){
        stop("Not enough future surgery files found.")
    }
    
    sbc_cols <- c("LOG_ID", "OR_SERVICE", "SURGERY_DATE", "FIRST_SCHED_DATE", "CASE_CLASS")
    
    col_types <- list(
        LOG_ID = readr::col_character(),
        OR_SERVICE = readr::col_character(),
        SURGERY_DATE = readr::col_datetime("%m/%d/%Y  %I:%M:%S %p"),
        FIRST_SCHED_DATE = readr::col_datetime("%m/%d/%Y  %I:%M:%S %p"),
        CASE_CLASS = readr::col_character()
    )
    
    names(col_types) <- org_cols

    loggit::loggit(log_lvl = "INFO", log_msg = paste("Processing files from", basename(path = filenames[1])))

    surgery_tbls <- filenames %>%
        lapply(readr::read_tsv,
               col_names = TRUE,
               col_types = do.call(readr::cols, col_types), 
               col_select = org_cols, 
               progress = FALSE,
               show_col_types = FALSE)
    
    raw_data <- do.call(rbind, surgery_tbls)
    
    ## Set column names as SBC equivalents
    names(raw_data) <- sbc_cols

    ## Stop if no data
    if (nrow(raw_data) < 1) {
        loggit::loggit(log_lvl = "ERROR", log_msg = sprintf("No data in file %s", filenames[1]))
        stop(sprintf("No data in file %s", filenames[1]))
    }
    processed_data <- summarize_and_clean_surgery_next_three_day(raw_data, services)
    if (processed_data$errorCode != 0) {
        loggit::loggit(log_lvl = "ERROR", log_msg = processed_data$errorMessage)
        stop(processed_data$errorMessage)
    }
    list(filenames = filenames,
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
summarize_and_clean_surgery_next_three_day <- function(raw_data, services) {
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
        dplyr::mutate(surgery_date = as.Date(.data$SURGERY_DATE),
                      sched_date = as.Date(.data$FIRST_SCHED_DATE),
                      case_class = .data$CASE_CLASS,
                      or_service = factor(x = .data$OR_SERVICE, levels = services)) %>%
        dplyr::filter(!is.na(or_service)) %>%
        dplyr::select(.data$surgery_date, 
                      .data$sched_date, 
                      .data$case_class, 
                      .data$or_service) -> filtered_data
    
    # Look at counts for most common procedures, known before today (i + 1)
    curr_date <- min(filtered_data$surgery_date)
    filtered_data %>%
        dplyr::group_by(.data$or_service, .drop = FALSE) %>%
        dplyr::filter( .data$case_class == "Elective"  ) %>% # Omit urgent for now
        dplyr::filter(.data$sched_date < curr_date) %>% # Scheduled before today
        dplyr::tally() %>%
        tidyr::pivot_wider(names_from = .data$or_service, 
                           values_from = .data$n, 
                           values_fill = 0) %>%
        dplyr::mutate(date = curr_date) %>%
        dplyr::relocate(date) -> 
        proc_data -> result$data -> result$summary

    result
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
summarize_and_clean_surgery_single_day <- function(raw_data, services) {
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
        dplyr::mutate(surgery_date = as.Date(.data$SURGERY_DATE),
                      sched_date = as.Date(.data$FIRST_SCHED_DATE),
                      case_class = .data$CASE_CLASS,
                      or_service = factor(x = .data$OR_SERVICE, levels = services)) %>%
        dplyr::select(.data$surgery_date, 
                      .data$sched_date, 
                      .data$case_class, 
                      .data$or_service) -> filtered_data
    
    filtered_data %>%
        dplyr::group_by(.data$or_service, .drop = FALSE) %>%
        #dplyr::filter( .data$case_class == "Elective"  ) %>% # Omit urgent for now
        dplyr::tally() %>%
        tidyr::pivot_wider(names_from = .data$or_service, 
                           values_from = .data$n, 
                           values_fill = 0) %>%
        dplyr::mutate(date = filtered_data$surgery_date[1]) %>%
        dplyr::relocate(date) %>%
        dplyr::rename(other = `NA`) -> 
        proc_data -> result$data -> result$summary
    
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
    raw_surgery <- lapply(fileList, read_next_three_surgery_files, services = services)

    for (item in raw_surgery) {
        save_report_file(report_tbl = item$report,
                         report_folder = report_folder,
                         filename = item$filenames)
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
    # make sure scale does not break the model
    if (!is.null(scale)) scale[scale < 1.0e-12] <- 1
    
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
#' @param date a string in YYYY-mm-dd format, default today (Day i + 1 vs. prediction for i + 1 - i + 3)
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
    
    # Grab yesterday (day i) CBC file obtained this morning (day i + 1)
    cbc_file <- list.files(path = config$data_folder,
                           pattern = sprintf(paste0(config$cbc_filename_prefix, "%s-"), date),
                           full.names = TRUE)

    # Grab yesterday (day i) Census file obtained this morning (day i + 1)
    census_file <- list.files(path = config$data_folder,
                              pattern = sprintf(paste0(config$census_filename_prefix, "%s-"), date),
                              full.names = TRUE)
    
    # Take surgery files from next 3 days (i + 1, i + 2, i + 3), obtained on day (i + 2, i + 3, i + 4)
    # Note that this will need to be swapped for single day surgery pre-process when we use files that
    # contain scheduled surgeries for the next 3 days as opposed to yesterday's surgeries
    d_dates <- as.Date(date) + c(1, 2, 3)

    surgery_files <- sapply(d_dates, function(x) {
        surg_file <- list.files(path = config$data_folder,
                                pattern = sprintf(paste0(config$surgery_filename_prefix, "%s-"), x),
                                full.names = TRUE)
        if (length(surg_file) != 1L) stop("Too few or too many surgery files matching patterns!")
        surg_file
        })

    # Grab yesterday (day i) Transfusion file obtained this morning (day i + 1)
    transfusion_file <- list.files(path = config$data_folder,
                                   pattern = sprintf(paste0(config$transfusion_filename_prefix, "%s-"), date),
                                   full.names = TRUE)

    # Grab yesterday (day i) Inventory file obtained this morning (day i + 1)
    inventory_file <- list.files(path = config$data_folder,
                                 pattern = sprintf(paste0(config$inventory_filename_prefix, "%s-"), date),
                                 full.names = TRUE)
    
    if (length(cbc_file) != 1L) stop("Too few or too many CBC files matching patterns!")
    if (length(census_file) != 1L) stop("Too few or too many census files matching patterns!")
    if (length(surgery_files) != 3L) stop("Too few or too many surgery files matching patterns!")
    if (length(transfusion_file) != 1L) stop("Too few or too many transfusion files matching patterns!")
    if (length(inventory_file) > 1L) stop("Too few or too many files matching patterns!")

    # Process CBC data
    cbc_data <- read_one_cbc_file(cbc_file,
                                  cbc_abnormals = config$cbc_abnormals,
                                  cbc_vars = config$cbc_vars,
                                  org_cols = config$org_cbc_cols)

    cbc_data$cbc_data %>%
        dplyr::filter(.data$BASE_NAME %in% config$cbc_vars) %>%
        dplyr::rename(date = .data$RESULT_DATE) %>%
        dplyr::distinct() ->
        cbc
    
    cbc <- create_cbc_features(cbc, config$cbc_quantiles)

    # Process Census data
    census_data <- read_one_census_file(census_file,
                                        locations = config$census_locations,
                                        org_cols = config$org_census_cols)
    
    census_data <- census_data$census_data
    
    # (This is to set 0 as the NA replacement option for every census column,
    # as census appears to be the most likely group of features to change)
    # Not sure this step is necessary.
    replacement <- lapply(names(census_data)[-1], function(x) 0)
    names(replacement) <- names(census_data)[-1]
    census_data %>%
        tidyr::replace_na(replace = replacement) %>%
        dplyr::distinct() %>% # part of original code
        dplyr::arrange(date) ->
        census
    
    
    # Process Surgery data
    surgery_data <- read_next_three_surgery_files(surgery_files,
                                                  services = config$surgery_services,
                                                  org_cols = config$org_surgery_cols)
    
    
    surgery_data$surgery_data %>%
        dplyr::distinct() %>%
        dplyr::arrange(date) ->
        surgery
    
    
    # Process Transfusion data
    transfusion_data <- read_one_transfusion_file(transfusion_file, 
                                                  org_cols = config$org_transfusion_cols)

    transfusion_data$transfusion_data %>%
        dplyr::distinct() %>% # part of original code
        dplyr::arrange(date) ->
        transfusion

    # Process inventory data
    if (length(inventory_file) > 0) {
        inventory_data <- read_one_inventory_file(inventory_file, config$org_inventory_cols)

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
#' @param org_cols the organization-specific column names corresponding to SBC columns
#' @param date an optional date of inventory in case the date is not
#'     to be automatically inferred as the day before the date in the
#'     filename
#' @return a list of four items, filename, raw_data (tibble), report a
#'     list consisting of summary tibble, census_data (tibble)
#' @importFrom readr cols col_integer col_character col_datetime
#'     read_tsv
#' @importFrom dplyr select
#' @importFrom readxl read_excel
#' @importFrom lubridate ymd_hms ddays
#' @importFrom stringr str_match
#' @export
read_one_inventory_file <- function(filename,
                                    org_cols = c("Inv. ID", "Type", "Days to Expire", "Exp. Date", "Exp. Time"),
                                    date = NULL) {
    
    if (is.null(date)) {
        date_string <- paste(
            substring(stringr::str_match(string = basename(filename),
                                         pattern = "[0-9\\-]+")[1, 1], 1, 10),
            "23:59:59")
        ## Inventory is for the day before
        date <- lubridate::ymd_hms(date_string, tz = "America/Los_Angeles") - lubridate::ddays(1)
    }
    
    sbc_cols <- c("Inv. ID", "Type", "Days to Expire", "Exp. Date", "Exp. Time")

    loggit::loggit(log_lvl = "INFO", log_msg = paste("Processing", basename(path = filename),
                                                       "for", as.character(date), "inventory"))
    
    # Filter the raw data to the columns of interest
    raw_data <- readxl::read_excel(path = filename)
    
    ## Stop if no data
    if (nrow(raw_data) < 1) {
        loggit::loggit(log_lvl = "ERROR", log_msg = sprintf("No data in file %s", filename))
        stop(sprintf("No data in file %s", filename))
    }
    
    key_data <- raw_data %>%
        dplyr::select(c(as.name(org_cols[1]), 
                        as.name(org_cols[2]), 
                        as.name(org_cols[3]), 
                        as.name(org_cols[4]),
                        as.name(org_cols[5])))
    
    names(key_data) <- sbc_cols

    processed_data <- summarize_and_clean_inventory(key_data, date)
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
        dplyr::mutate(date = as.Date(date),
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
    raw_inventory <- lapply(fileList, read_one_inventory_file, config$org_inventory_cols)

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
#' @param eval TRUE or FALSE value for whether to evaluate model during predictions
#' @importFrom pip build_model predict_three_day_sum evaluate_model
#' @importFrom magrittr %>%
#' @importFrom dplyr group_by summarize_all
#' @return a prediction tibble named prediction_df with a column for date and the prediction
#' @importFrom loggit set_logfile loggit
#' @export
predict_for_date <- function(config,
                             date = as.character(Sys.Date(), format = "%Y-%m-%d"),
                             prev_day = NA,
                             eval = FALSE) {

    ## Previous date is one day before unless specified explicity
    if (is.na(prev_day))
        prev_day <- as.character(as.Date(date, format = "%Y-%m-%d") - 1, format = "%Y-%m-%d")

    loggit::loggit(log_lvl = "INFO", log_msg = paste("Step 1. Loading previously processed data on", prev_day))
    
    ############## REPLACE WITH DATABASE CALL ######################
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
    cbc_features <- tail(cbc, config$history_window + config$lag_window + 1L)
    census <- tail(census, config$history_window + config$lag_window + 1L)
    surgery <- tail(surgery, config$history_window + config$lag_window + 1L)  # need lag_window for smoothing
    transfusion <- tail(transfusion, config$history_window + config$lag_window + 1L) # need lag_window for lag
    
    # Obtain collection and expiry data (add 1 for the additional previous day's inventory)
    inventory <- tail(inventory, config$history_window + 1L)
    
    inventory %>% 
        dplyr::mutate(date = as.Date(.data$date)) %>% # converting from dttm to date (not sure why it is a datetime)
        dplyr::left_join(transfusion, by="date") %>%
        dplyr::mutate(collection = dplyr::lead(.data$count, 1) - 
                          .data$count + .data$used + 
                          pip::pos(.data$r1 - .data$used)) %>% # Amt collected
        dplyr::mutate(expiry1 = .data$r1 + .data$r2) %>% # Expiring in 1 day
        dplyr::mutate(expiry2 = .data$r3_plus) -> inventory

    loggit::loggit(log_lvl = "INFO", log_msg = "Step 3b. Creating training/prediction dataset")

    # define all variables (this allows mismatch between RDS columns and config)
    cbc_names <- unname(sapply(config$cbc_vars, function(x) paste0(x, "_Nq")))
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
                                            lo_inv_limit = config$lo_inv_limit,
                                            hi_inv_limit = config$hi_inv_limit,
                                            start = config$start,
                                            l1_bounds = config$l1_bounds,
                                            lag_bounds = config$lag_bounds)
        if (eval) {
            # Take this opportunity to evaluate the model
            pip::evaluate_model(data = data,
                        c0 = config$c0,
                        train_window = config$history_window - 14L,
                        test_window = 14L,
                        penalty_factor = config$penalty_factor,
                        lo_inv_limit = config$lo_inv_limit,
                        hi_inv_limit = config$hi_inv_limit,
                        start = config$start,
                        l1_bounds = config$l1_bounds,
                        lag_bounds = config$lag_bounds)
        }
        
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
    
    ################ REPLACE WITH DATABASE CALL ##########################
    saveRDS(prev_data, file = file.path(config$output_folder,
                                        sprintf(config$output_filename_prefix, date)))
    prediction_df
}

# DB version of single day prediction
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
#' @param eval TRUE or FALSE value for whether to evaluate model during predictions
#' @importFrom pip build_model predict_three_day_sum evaluate_model
#' @importFrom magrittr %>%
#' @importFrom dplyr tbl collect rows_upsert filter select distinct copy_to mutate relocate
#' @importFrom DBI dbIsValid dbListTables
#' @importFrom tibble tibble
#' @return a prediction tibble named prediction_df with a column for date and the prediction
#' @importFrom loggit set_logfile loggit
#' @export
predict_for_date_db <- function(conn, config,
                                date = as.character(Sys.Date(), format = "%Y-%m-%d"),
                                prev_day = NA,
                                eval = FALSE) {
    
    if (!DBI::dbIsValid(conn)) stop("Database connection is invalid. Please reconnect.")
    
    ## Previous date is one day before unless specified explicity
    if (is.na(prev_day))
        prev_day <- as.character(as.Date(date, format = "%Y-%m-%d") - 1, format = "%Y-%m-%d")
    date_diff <- as.integer(as.Date(date) - as.Date(prev_day))
    
    ## Step 1. Process data for the new date
    result <- process_data_for_date(config = config, date = date) # from a file
    db_tablenames <- DBI::dbListTables(conn)
    if (length(intersect(names(result), db_tablenames)) != length(names(result))) {
        stop("Mismatch between new data sources and existing sources in DB.")
    }
    
    ## Step 2. Check to make sure that the newly processed data contains only one date/row
    multiple_dates_in_increment <- FALSE
    
    for (i in seq_along(result)) {
        unique_dates <- unique(result[i]$data)
        if (length(unique_dates) > 1L) {
            loggit::loggit(log_lvl = "WARN", log_msg = sprintf("Multiple dates in %s file, model retraining forced!", 
                                                               names(result)[i]))
            loggit::loggit(log_lvl = "WARN", log_msg = unique_dates)
            multiple_dates_in_increment <- TRUE
        }
    }
    
    # Step 3. Define all variables based on current config (this allows mismatch between database columns and config)
    cbc_names <- unname(sapply(config$cbc_vars, function(x) paste0(x, "_Nq")))
    all_hosp_vars <- c(cbc_names, config$census_locations, config$surgery_services)
    all_vars <- c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", "lag", all_hosp_vars)
    all_cols <- c("date", all_vars, "plt_used")
    
    # Step 4. Load required data from database and "upsert" the new rows for each table
    # (Note: This assumes that the previously processed data and the new data have the same number of
    # columns with potentially different names)
    updated_data <- vector("list", length(result))
    names(updated_data) <- names(result) 
    
    first_day_train <- as.Date(prev_day) - (config$history_window + config$lag_window)
    first_day_test <- as.Date(prev_day) # Day i 
    
    for (i in seq_along(result)) {
        
        table_name <- names(updated_data)[i]
        db_table <- conn %>% 
            dplyr::tbl(table_name) %>% 
            dplyr::collect()
        
        new_vars <- names(result[[table_name]])
        db_vars <- names(db_table)
        missing_vars <- new_vars[!(new_vars %in% db_vars)]
        missing_string <- paste(missing_vars, collapse = ", ")
        
        if (length(missing_vars) > 0L) {
            stop(sprintf("Database is missing some of the requested features: %s . Please rebuild the database using these features.", 
                         missing_string))
        }
        
        # Note that the upsert function is experimental. May need to replace with manual workaround
        # Note that all columns in db_table must exist in result (or vice versa) - throws vague error
        # Note that updated_date includes training and test data.
        updated_data[[table_name]] <- tidyr::as_tibble(db_table) %>% 
            dplyr::rows_upsert(tidyr::as_tibble(result[[table_name]]), by = c("date")) %>%
            dplyr::filter(date >= first_day_train & date <= first_day_test) %>% 
            dplyr::arrange(date) # Make sure dates are sorted because we take contiguous chunks next
    }
    
    # Step 5. Create the combined dataset
    dataset <- create_dataset(config,
                              cbc_features = updated_data$cbc,
                              census = updated_data$census,
                              surgery = updated_data$surgery,
                              transfusion = updated_data$transfusion) %>% 
        dplyr::select(all_of(all_cols))
    
    recent_data <- tail(dataset, n = config$history_window + 1L)
    training_data <- head(recent_data, n = config$history_window)
    new_data <- tail(recent_data, n = 1L)
    
    # Step 6. Determine whether model needs to be updated (config changed or outdated)
    model_row <- list(age = 0L)
    model_exists <- "model" %in% db_tablenames
    if (model_exists) {
        model_tbl <- conn %>% dplyr::tbl("model") %>% 
            dplyr::collect() # Note - must have collect statement (otherwise R aborts)
        model_df <- as.data.frame(model_tbl) 
        model_row <- model_df %>% 
            dplyr::filter(date == as.Date(prev_day)) %>% # Grab model row for day i
            dplyr::distinct()
        colnames(model_row) <- c("date", "intercept", all_vars, "l1_bound", "lag_bound", "age")
    }
    
    model_needs_updating <- (multiple_dates_in_increment || 
                                 (nrow(model_row) == 0L) ||
                                 (model_row$age %% config$model_update_frequency == 0L) ||
                                 (date_diff > config$model_update_frequency))
    
    # Step 7. Scale the dataset and retrain the model as needed
    scaled_dataset <- NULL
    model <- list(l1_bound = NA, lag_bound = NA, coefs = NULL, w = NULL, r = NULL, s = NULL)
    
    if (model_needs_updating) {
        
        ## Provide informative log
        if (multiple_dates_in_increment) {
            loggit::loggit(log_lvl = "INFO", log_msg = "Multiple dates in data increment so model training forced")
        } else {
            loggit::loggit(log_lvl = "INFO", log_msg = "Model is stale so updating model")
        }
        
        scaled_dataset <- scale_dataset(training_data) # rescale
        
        # ensure that no NA values are fed into build_model
        data <- as.data.frame(scaled_dataset$scaled_data, optional = TRUE)
        if (sum(is.na(data)) > 0) {
            data[is.na(data)] <- 0
            loggit::loggit(log_lvl = "WARN", log_msg ='Warning: NA values found in scaled dataset - replacing with 0')
        }
        
        model <- pip::build_model(data = data,
                                  c0 = config$c0,
                                  history_window = config$history_window,
                                  penalty_factor = config$penalty_factor,
                                  lo_inv_limit = config$lo_inv_limit,
                                  hi_inv_limit = config$hi_inv_limit,
                                  start = config$start,
                                  l1_bounds = config$l1_bounds,
                                  lag_bounds = config$lag_bounds)
        
        # Create a new row for model_df (by convention - assign day i + 1)
        model_row <- as.data.frame(t(model$coefs))
        names(model_row) <- names(model$coefs)
        model_row <- model_row %>% 
            dplyr::mutate(date = as.Date(date), 
                          l1_bound = model$l1_bound, 
                          lag_bound = as.numeric(model$lag_bound),
                          age = 1L) %>% 
            dplyr::relocate(date)
        
        if (eval) {
            # Take this opportunity to evaluate the model
            pip::evaluate_model(data = data,
                                c0 = config$c0,
                                train_window = config$history_window - 14L,
                                test_window = 14L,
                                penalty_factor = config$penalty_factor,
                                lo_inv_limit = config$lo_inv_limit,
                                hi_inv_limit = config$hi_inv_limit,
                                start = config$start,
                                l1_bounds = config$l1_bounds,
                                lag_bounds = config$lag_bounds)
        }
        
        
    } else {
        loggit::loggit(log_lvl = "INFO", log_msg = "Using previous model and scaling")
        
        # Load previous scaling and model
        prev_scaling <- conn %>% dplyr::tbl("data_scaling") %>% 
            dplyr::collect()
        
        scaled_dataset <- scale_dataset(training_data,
                                        center = prev_scaling$center,
                                        scale = prev_scaling$scale)
        
        # Convert model data.frame row into model "object" for prediction
        model$l1_bound <- model_row$l1_bound
        model$lag_bound <- model_row$lag_bound
        model$coefs <- as.numeric(model_row %>% dplyr::select(-c(l1_bound, lag_bound, date, age)))
        names(model$coefs) <- setdiff(colnames(model_row), c("date", "l1_bound", "lag_bound", "age"))
        
        #Update the date and model age
        model_row$date <- as.Date(date)
        model_row$age <- model_row$age + 1L
    }
    
    # Step 9. Scale the new data based on updated or previous scaling
    new_scaled_data <- scale_dataset(new_data,
                                     center = scaled_dataset$center,
                                     scale = scaled_dataset$scale)$scaled_data
    
    # Step 10. Predict the total platelet usage for the next 3 days
    prediction <- pip::predict_three_day_sum(model = model,
                                             new_data = as.data.frame(new_scaled_data, optional=TRUE)) ## last row is what we  want to predict for
    
    # Make sure prediction is returning a valid response. No point in continuing otherwise.
    if (is.nan(prediction)) {
        stop(sprintf("Next three day prediction returned NaN for %s. Make sure all features are present in the database", date))
    }
    
    # Recall that the input to this function is date i + 1. However, we want to obtain t_i
    prediction_df <- tibble::tibble(date = as.Date(prev_day), t_pred = prediction)
    
    # Step 11. Update the database with the new data
    if (!model_exists) {
        conn %>% DBI::dbWriteTable("model", model_row)
    } else {
        conn %>% upsert_db("model", model_row, by = "date")
    }
    
    
    # Simply replace the previous scaling (meed 2 separate tables for center and scaling at this rate)
    conn %>% dplyr::copy_to(df = data.frame(scaled_dataset[c("center", "scale")]), 
                            name = "data_scaling",
                            overwrite = TRUE, 
                            temporary = FALSE)
    
    
    # Upsert new data
    for (i in seq_along(result)) {
        table_name <- names(updated_data)[i]
        conn %>% upsert_db(table_name, result[[table_name]], by = "date")
    }
    print(prediction_df)
    
    # Update the pred_cache table
    pred_cache_exists <- "pred_cache" %in% db_tablenames
    if (!pred_cache_exists) {
        conn %>% DBI::dbWriteTable("pred_cache", prediction_df)
    } else {
        conn %>% upsert_db("pred_cache", prediction_df, by = "date")
    }
    
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
    
    ############## REPLACE WITH DATABASE CALL ###################
    # There is no database version of this function. 
    # The prediction_df is not stored in the database as of now.
    d <- readRDS(tail(output_files, 1L))
    d$dataset %>%
        dplyr::select(.data$date, .data$plt_used) ->
        d2
    tibble::tibble(date = dates) %>%
        dplyr::left_join(d2, by = "date") %>%
        dplyr::left_join(d$prediction_df, by = "date")
}


#' Helper Database upsert function
#'
#' @param conn the database connection object
#' @param df_name string name of the database table
#' @param new_row new complete row to add to the database table (data.frame)
#' @param by string name of column by which to match rows (default to date for this use case)
#' @importFrom magrittr %>%
#' @importFrom dplyr tbl collect
#' @importFrom DBI dbExecute
#' @export
upsert_db <- function(conn, df_name, new_row, by = "date") {
    
    if (!DBI::dbIsValid(conn)) stop("Database connection is invalid. Please reconnect.")
    
    tbl <- conn %>% dplyr::tbl(df_name) %>% dplyr::collect()
    if (ncol(tbl) != ncol(new_row)) {
        stop("Column names in new entry do not match column names in database table.")
    }
    
    insert_command <- sprintf("INSERT INTO %s VALUES (%s)", 
                              df_name, paste(rep("?", ncol(new_row)), collapse = ", "))
    
    row_exists <- new_row[[by]] %in% tbl[[by]]
    
    if (row_exists) {
        DBI::dbExecute(conn, sprintf("DELETE FROM %s WHERE %s = '%s'", df_name, by, as.character(new_row[[by]])))
    }
    DBI::dbExecute(conn, insert_command, as.list(new_row))
}

