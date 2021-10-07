library(shiny)
library(shinydashboard)
library(shinyWidgets)
library(shinyalert)
library(SBCpip)
library(tidyverse)
library(gridExtra)
library(bibtex)
library(DBI)
library(duckdb)

paper_citation <- bibtex::read.bib(system.file("extdata", "platelet.bib", package = "SBCpip"))
#db_path <- "/Users/kaiokada/Desktop/Research/pip.duckdb"
data_tables <- c("cbc", "census", "surgery", "transfusion", "inventory")

# Read the column mapping from the sbc_data_mapping file.
data_mapping_file <- system.file("extdata", "sbc_data_mapping.csv", package = "SBCpip")
data_mapping <- read.csv(data_mapping_file)

set_config_param("org_cbc_cols", 
                 (data_mapping %>% 
                   dplyr::filter(data_file == "CBC"))$org_data_column_name_to_edit)
set_config_param("org_census_cols", 
                 (data_mapping %>% 
                    dplyr::filter(data_file == "Census"))$org_data_column_name_to_edit)
set_config_param("org_surgery_cols",
                 (data_mapping %>% 
                    dplyr::filter(data_file == "Surgery"))$org_data_column_name_to_edit)
set_config_param("org_transfusion_cols", 
                 (data_mapping %>% 
                    dplyr::filter(data_file == "Transfusion"))$org_data_column_name_to_edit)
set_config_param("org_inventory_cols",
                 (data_mapping %>%
                    dplyr::filter(data_file == "Inventory"))$org_data_column_name_to_edit)

intro_line_1 <- 'An application implementing the method described by'
intro_line_2 <- a(href = sprintf("https://doi.org/%s", paper_citation$doi),
                  sprintf("%s et. al., PNAS %s(%s) %s",
                          paper_citation$author[1],
                          paper_citation$volume,
                          paper_citation$number,
                          paper_citation$year))

gg_color_hue <- function(n) {
  hues = seq(15, 375, length = n + 1)
  grDevices::hcl(h = hues, l = 65, c = 100)[1:n]
}

cols <- gg_color_hue(3L)

sidebar <- dashboardSidebar(
  sidebarMenu(
    id = "tabId"
    , menuItem("Dashboard", tabName = "dashboard", icon = icon("dashboard"))
    , menuItem("Database Settings", tabName = "data_settings", icon = icon("wrench"))
    , conditionalPanel(
      condition = "input.tabId == 'data_settings'"
      , actionButton(inputId = "setDBValues", label = "Apply Database Settings")
      , actionButton(inputId = "buildDatabase", label = "Build Database")
    )
    , menuItem("Reports", icon = icon("table")
               , menuSubItem("CBC Summary", tabName = "cbc")
               , conditionalPanel(
                 condition = "input.tabId == 'cbc'"
                 , dateInput(inputId = "cbcDateStart", label = "Start Date", value = as.character(Sys.Date()))
                 , dateInput(inputId = "cbcDateEnd", label = "End Date", value = as.character(Sys.Date()))
                 , actionButton(inputId = "cbcSummaryButton", "Summarize")
               )
               , menuSubItem("Census Summary", tabName = "census")
               , conditionalPanel(
                 condition = "input.tabId == 'census'"
                 , dateInput(inputId = "censusDateStart", label = "Start Date", value = as.character(Sys.Date()))
                 , dateInput(inputId = "censusDateEnd", label = "End Date", value = as.character(Sys.Date()))
                 , actionButton(inputId = "censusSummaryButton", "Summarize")
               )
               , menuSubItem("Surgery Summary", tabName = "surgery")
               , conditionalPanel(
                 condition = "input.tabId == 'surgery'"
                 , dateInput(inputId = "surgeryDateStart", label = "Start Date", value = as.character(Sys.Date()))
                 , dateInput(inputId = "surgeryDateEnd", label = "End Date", value = as.character(Sys.Date()))
                 , actionButton(inputId = "surgerySummaryButton", "Summarize")
               )
               , menuSubItem("Inventory Summary", tabName = "inventory")
               , conditionalPanel(
                 condition = "input.tabId == 'inventory'"
                 , dateInput(inputId = "inventoryDateStart", label = "Start Date", value = as.character(Sys.Date()))
                 , dateInput(inputId = "inventoryDateEnd", label = "End Date", value = as.character(Sys.Date()))
                 , actionButton(inputId = "inventorySummaryButton", "Summarize")
               )
    )
    , menuItem("Model Settings", tabName = "model_settings", icon = icon("wrench"))
    , conditionalPanel(
      condition = "input.tabId == 'model_settings'"
      , actionButton(inputId = "setModelValues", label = "Apply Model Settings")
      , actionButton(inputId = "clearPredCache", label = "Clear Prediction Cache")
    )
    , menuItem("Validate Model Over Range", icon = icon("caret-right"), tabName = "validation")
    , conditionalPanel(
      condition = "input.tabId == 'validation'"
      , dateInput(inputId = "startDate", label = "Start Date", value = "2018-04-10")
      , dateInput(inputId = "endDate", label = "End Date", value = as.character(Sys.Date()))
      , actionButton(inputId = "predictionSummaryButton", "Validate Model")
      , actionButton(inputId = "standaloneSummaryButton", "Summary Table")
    )
    , menuItem("Predict for Today", icon = icon("caret-right"), tabName = "prediction")
    , conditionalPanel(
      condition = "input.tabId == 'prediction'"
      , dateInput(inputId = "predictDate", label = "Date (i)", value = as.character(Sys.Date()))
      , numericInput(inputId = "collect1", label = "Units Collected Tomorrow (i + 1)", value = 0)
      , numericInput(inputId = "collect2", label = "Units Collected in 2 Days (i + 2)", value = 0)
      , numericInput(inputId = "expire1", label = "Remaining Units Expiring Tomorrow", value = 0)
      , numericInput(inputId = "expire2", label = "Remaining Units Expiring in 2 Days", value = 0)
      , actionButton(inputId = "predictButton", "Predict")
    )
    , menuItem("Prediction Plots", icon = icon("bar-chart-o"), tabName = "modelPlots")
    , conditionalPanel(
      condition = "input.tabId == 'modelPlots'"
      , dateInput(inputId = "plotDateStart", label = "Start Date", value = "2018-04-10")
      , dateInput(inputId = "plotDateEnd", label = "End Date", value = as.character(Sys.Date()))
      , actionButton(inputId = "predPlotButton", label = "Plot Predictions")
    )
    , menuItem("Coefficient Plots", icon = icon("bar-chart-o"), tabName = "coefPlots")
    , conditionalPanel(
      condition = "input.tabId == 'coefPlots'"
      , dateInput(inputId = "coefPlotDateStart", label = "Start Date", value = "2018-04-10")
      , dateInput(inputId = "coefPlotDateEnd", label = "End Date", value = as.character(Sys.Date()))
      , actionButton(inputId = "plotCoefButton", "Plot Coefficients")
    )
    , actionButton(inputId = "exitApp", "Exit")
  )
)

body <- dashboardBody(
  tags$head(
    tags$style(
      HTML(".shiny-notification {
              height: 100px;
              width: 800px;
              position:fixed;
              top: calc(50% - 50px);;
              left: calc(50% - 400px);;
            }
           "
      ))),
  useShinyalert(),
  tabItems(
    tabItem(tabName = "dashboard"
            , h2("Platelet Inventory Prediction")
            , p(em(intro_line_1, intro_line_2))
            , h3("Abstract")
            , p(paper_citation$abstract)
    )
    , tabItem(tabName = "data_settings"
              , fluidRow(
                h2("Database Settings")
                , box(
                  h3("Input and Output Locations")
                  , textInput(inputId = "database_path", label = "Database Path", value = "/Users/kaiokada/Desktop/Research/pip.duckdb")
                  , textInput(inputId = "data_folder", label = "Data Folder", value = "/Users/kaiokada/Desktop/Research/platelet_data_full/Blood_Center_inc/")
                  , textInput(inputId = "log_folder", label = "Log Folder", value = "/Users/kaiokada/Desktop/Research/platelet_data_full/Blood_Center_Logs/")
                  
                  , h3("Filename Patterns")
                  , textInput(inputId = "cbc_filename_prefix", label = "CBC Files", value = SBCpip::get_SBC_config()$cbc_filename_prefix)
                  , textInput(inputId = "census_filename_prefix", label = "Census Files", value = SBCpip::get_SBC_config()$census_filename_prefix)
                  , textInput(inputId = "transfusion_filename_prefix", label = "Transfusion Files", value = SBCpip::get_SBC_config()$transfusion_filename_prefix)
                  , textInput(inputId = "surgery_filename_prefix", label = "Surgery Files", value = SBCpip::get_SBC_config()$surgery_filename_prefix)
                  , textInput(inputId = "inventory_filename_prefix", label = "Inventory Files", value = SBCpip::get_SBC_config()$inventory_filename_prefix)
                  , textInput(inputId = "log_filename_prefix", label = "Log Files", value = SBCpip::get_SBC_config()$log_filename_prefix)
                )
                , box(
                  h3("Feature Names")
                  , actionButton(inputId = "refreshFeatures", label = "Refresh Features")
                  , textAreaInput(inputId = "cbc_features", label = "CBC Features List", value = paste0(SBCpip::get_SBC_config()$cbc_vars, collapse = ", "))
                  , textAreaInput(inputId = "census_features", label = "Census Features List", value = paste0(SBCpip::get_SBC_config()$census_locations, collapse = ", "))
                  , textAreaInput(inputId = "surgery_features", label = "Surgery Features List", value = paste0(SBCpip::get_SBC_config()$surgery_services, collapse = ", "))
                )
              )
    )
    , tabItem(tabName = "model_settings"
              , fluidRow(
                h2("Configuration Settings")
                , box(
                  h3("Inventory Requirements for Model")
                  , sliderInput(inputId = "c0"
                                , label = "Minimum Remaining Fresh Units at EOD - c0 (units):"
                                , min = 0
                                , max = 50
                                , value = get_SBC_config()$c0)
                  , sliderInput(inputId = "loss_inventory_range"
                                , label = "Cross-Validation Prediction Bias (units):"
                                , min = 0
                                , max = 100
                                , value = get_SBC_config()$lo_inv_limit)
                  , sliderInput(inputId = "penalty_factor"
                                , label = "Shortage Penalty Factor (X unit Wasted : 1 units Short):"
                                , min = 0
                                , max = 20
                                , value = get_SBC_config()$penalty_factor)
                )
                , box(
                  h3("Other Model Fitting Parameters")
                  , sliderInput(inputId = "history_window"
                                , label = "History Window (days):"
                                , min = 50
                                , max = 300
                                , value = 100)
                  , sliderInput(inputId = "lag_window"
                                , label = "Moving Average Lag Window (days)"
                                , min = 1
                                , max = 14
                                , value = 7)
                  , sliderInput(inputId = "start"
                                , label = "Skip Initial (days):"
                                , min = 1
                                , max = 15
                                , value = 10)
                  , sliderInput(inputId = "model_update_frequency"
                                , label = "Model Update Frequency (days):"
                                , min = 1
                                , max = 30
                                , value = 7)
                  , sliderInput(inputId = "l1_bound_range"
                                , label = "Range of L1 Bounds on Model Coefs:"
                                , min = 0
                                , max = 200
                                , value = c(0, 60)
                                , step = 2)
                  , sliderInput(inputId = "lag_bound"
                                , label = "Bound on Seven-Day Usage Lag Coef:"
                                , min = 5
                                , max = 20
                                , value = 10)
                )
              )
    )
    # Hide the below items until the settings have been applied?
    , tabItem(tabName = "prediction"
              , h2("Prediction Result")
              , verbatimTextOutput("predictionResult")
              , tableOutput("modelCoefs")
    )
    , tabItem(tabName = "cbc"
              , h2("CBC Summary")
              , tableOutput("cbcSummary")
    )
    , tabItem(tabName = "census"
              , h2("Census Summary")
              , tableOutput("censusSummary")
    )
    , tabItem(tabName = "surgery"
              , h2("Surgery Summary")
              , tableOutput("surgerySummary")
    )
    , tabItem(tabName = "inventory"
              , h2("Inventory Summary")
              , tableOutput("inventorySummary")
    )
    , tabItem(tabName = "validation"
              , h2("Prediction Analysis")
              , fluidRow(
                box(
                  h3("Prediction Statistics")
                  , tableOutput("predAnalysis")
                )
                , box(
                  h3("Coefficients Over Prediction Range")
                  , tableOutput("coefAnalysis")
                )
              )
    )
    , tabItem(tabName = "modelPlots"
              , h2("Waste and Remaining Histograms")
              , plotOutput("wrPlot")
    )
    , tabItem(tabName = "coefPlots"
              , h2("Coefficient Plots Over Time")
              , h4("Select Coefficients, Start Date, and End Date. Then click \"Plot Coefficients.\"")
              , shinyWidgets::multiInput(inputId = "coefList", 
                                         label = "Coefficients to Plot:", 
                                         choices = c("None"))
              , plotOutput("cfPlot")
    )
  )
)

dbHeader <- dashboardHeader()
logo_src <- system.file("webapps", "dashboard", "assets", "sbc.png", package = "SBCpip")
dbHeader$children[[2]]$children <-  tags$a(href='https://stanfordbloodcenter.org',
                                           tags$img(src = 'https://sbcdonor.org/client_assets/images/logos/logo_stanford.png',
                                                    alt = 'Stanford Blood Center Dashboard', height = '40'))
##dbHeader



ui <- dashboardPage(
  title = "SBC Dashboard",
  ##dashboardHeader(title = "Stanford Blood Center")
  header = dbHeader
  , sidebar = sidebar
  , body = body
  , skin = "green"
)

fetch_data_for_dates <- function(req_start_date, req_end_date, table_name, db_path) {
  # Connect to duckdb database hosted on a local file
  if (req_end_date < req_start_date) return("End date must be at or after start date.")
  
  db <- DBI::dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)
  if (!(table_name %in% DBI::dbListTables(db))) {
    return(sprintf("Data table %s is not available in database. Make sure to run 'Database Settings' >> 'Reset Database' first.", 
                   table_name))
  }
  full_tbl <- db %>% dplyr::tbl(table_name) %>% dplyr::collect()
  DBI::dbDisconnect(db, shutdown = TRUE)
  
  # If the database does not contain data for the requested dates, process on the fly
  all_dates <- seq.Date(req_start_date, req_end_date, by = 1L)
  missing_dates <- all_dates[!(all_dates %in% full_tbl$date)]
  if (length(missing_dates) > 0L) {
    full_tbl <- tryCatch(
      apply(all_dates, 1, function(x) {
        SBCpip::process_data_for_date(SBCpip::get_SBC_config(), 
                                      as.character(as.Date(date) + 1))[[table_name]]
      }),
      error = function(e) { 
        print(e)
        NULL
      })
  }
  if (is.null(full_tbl)) {
    return("Some or all data files are unavailable for the requested date.")
  }
  
  print(full_tbl)
  
  tbl <- full_tbl %>% dplyr::ungroup() %>%
    dplyr::filter(date %in% all_dates) %>%
    tidyr::pivot_longer(-c(date), names_to = "var", values_to = "count") %>%
    dplyr::group_by(var) %>%
    dplyr::summarize(`Mean Value` = mean(count), `Standard Deviation` = sd(count)) %>%
    dplyr::select(var, `Mean Value`, `Standard Deviation`) %>%
    dplyr::mutate(`Standard Deviation` = replace_na(`Standard Deviation`, 0))
  tbl
}

# Helper function for feature input parsing (probably will remove)

server <- function(input, output, session) {
  
  parse_vars <- function(v) unlist(lapply(strsplit(as.character(v), split = ","), trimws))
  
  all_active_features <- reactive({
    fixed_features <- c("intercept", "lag", "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")
    all_active_features <- c(fixed_features, 
                             sapply(parse_vars(input$cbc_features), function(x) paste0(x, "_Nq")),
                             parse_vars(input$census_features),
                             parse_vars(input$surgery_features))
  })
  
  # Exit the app
  observeEvent(input$exitApp, {
    stopApp(TRUE)
  })
  
  # Refresh possible features to choose from based on available data files
  observeEvent(input$refreshFeatures, {
    # Folder names
    set_config_param("data_folder", input$data_folder)
    set_config_param("log_folder", input$log_folder)
    
    # File prefixes
    set_config_param("cbc_filename_prefix", input$cbc_filename_prefix)
    set_config_param("census_filename_prefix", input$census_filename_prefix)
    set_config_param("surgery_filename_prefix", input$surgery_filename_prefix)
    set_config_param("transfusion_filename_prefix", input$transfusion_filename_prefix)
    set_config_param("inventory_filename_prefix", input$inventory_filename_prefix)
    
    config <- SBCpip::get_SBC_config()
    
    # Grab CBC Features
    cbcFilename <- tail(list.files(path = config$data_folder, 
                                   pattern = config$cbc_filename_prefix, 
                                   full.names = TRUE), 1L)
    
    cbcData <- readr::read_tsv(file = cbcFilename, 
                               col_names = TRUE, 
                               show_col_types = FALSE)
    
    cbcFeatures <- unique(cbcData[[config$org_cbc_cols[2]]])
    
    updateTextInput(session, inputId = "cbc_features", value = paste0(sort(cbcFeatures), collapse = ", "))
    
    # Grab Census Features
    censusFilename <- tail(list.files(path = config$data_folder, 
                                   pattern = config$census_filename_prefix, 
                                   full.names = TRUE), 1L)
    
    censusData <- readr::read_tsv(file = censusFilename, 
                               col_names = TRUE, 
                               show_col_types = FALSE)
    
    censusFeatures <- unique(censusData[[config$org_census_cols[2]]])
    
    updateTextInput(session, inputId = "census_features", value = paste0(sort(censusFeatures), collapse = ", "))
    
    # Grab Surgery Features
    surgeryFilename <- tail(list.files(path = config$data_folder, 
                                      pattern = config$surgery_filename_prefix, 
                                      full.names = TRUE), 1L)
    
    surgeryData <- readr::read_tsv(file = surgeryFilename, 
                                  col_names = TRUE, 
                                  show_col_types = FALSE)
    
    surgeryFeatures <- unique(surgeryData[[config$org_surgery_cols[2]]])
    
    updateTextInput(session, inputId = "surgery_features", value = paste0(sort(surgeryFeatures), collapse = ", "))
    
  })
  
  # Set the database build configurations
  observeEvent(input$setDBValues, {
    # Folder names
    set_config_param("data_folder", input$data_folder)
    set_config_param("log_folder", input$log_folder)
    
    # File prefixes
    set_config_param("cbc_filename_prefix", input$cbc_filename_prefix)
    set_config_param("census_filename_prefix", input$census_filename_prefix)
    set_config_param("surgery_filename_prefix", input$surgery_filename_prefix)
    set_config_param("transfusion_filename_prefix", input$transfusion_filename_prefix)
    set_config_param("inventory_filename_prefix", input$inventory_filename_prefix)
    
    # Variables (eventually would like some kind of picklist setup instead of free text)
    set_config_param("cbc_vars", parse_vars(input$cbc_features))
    set_config_param("census_locations", parse_vars(input$census_features))
    set_config_param("surgery_services", parse_vars(input$surgery_features))
    set_config_param("database_path", input$database_path)
    
    # Update the coefficients 
    updateMultiInput(session, 
                     inputId = "coefList", 
                     choices = all_active_features())
    
    shinyalert("Success", "The database settings have been saved successfully.")
  })
  
  # Save parameters from settings panel to config.
  observeEvent(input$setModelValues, {

    # Inventory parameters
    set_config_param("c0", input$c0) # for training and cross-validation
    set_config_param("lo_inv_limit", input$loss_inventory_range[1]) # for loss function
    set_config_param("hi_inv_limit", input$loss_inventory_range[2]) # for loss function
    set_config_param("min_inventory", 0) # for prediction table building
    set_config_param("penalty_factor", input$penalty_factor)
    
    # Other model/prediction/validation parameters
    set_config_param("history_window", input$history_window)
    set_config_param("lag_window", input$lag_window)
    set_config_param("start", input$start)
    set_config_param("model_update_frequency", input$model_update_frequency)
    set_config_param("l1_bounds", seq(from = input$l1_bound_range[2], to = input$l1_bound_range[1], by = -2))
    set_config_param("lag_bounds", c(-1, input$lag_bound))
    
    loggit::loggit(log_lvl = "INFO", log_msg = "Settings saved.")

    shinyalert("Success", "The model settings have been saved successfully.")
    
  })
  
  # Clear currently cached predictions
  observeEvent(input$clearPredCache, {
    config <- SBCpip::get_SBC_config()
    
    db <- DBI::dbConnect(duckdb::duckdb(), config$database_path, read_only = FALSE)
    on.exit({
      if (DBI::dbIsValid(db)) DBI::dbDisconnect(db, shutdown = TRUE) 
    }, add = TRUE)
    db_tables <- db %>% DBI::dbListTables()
    if ("pred_cache" %in% db_tables) db %>% DBI::dbRemoveTable("pred_cache")
    if ("model" %in% db_tables) db %>% DBI::dbRemoveTable("model")
    shinyalert("Success", "The cached predictions have been cleared.")
  })
  
  # Build the full database using files in config$data_folder.
  observeEvent(input$buildDatabase, {
    
    loggit::loggit(log_lvl = "INFO", log_msg = "Building database")
    config <- SBCpip::get_SBC_config()
    
    n_files <- length(list.files(config$data_folder))
    
    # Initialize shiny progress bar
    progress <- shiny::Progress$new()
    progress$set(message = "Database Build Progress", value = 0)
    on.exit(progress$close())
    
    # Callback function to update progress
    updateProgress <- function(detail = NULL) {
      progress$inc(amount = 5/n_files, detail = detail) # 5 types of files
    }
    
    db <- DBI::dbConnect(duckdb::duckdb(), config$database_path, read_only = FALSE)
    on.exit({
      if (DBI::dbIsValid(db)) DBI::dbDisconnect(db, shutdown = TRUE) 
    }, add = TRUE)
    
    showModal(modalDialog(sprintf("Building DuckDB database based on all available files in %s. 
                                    Estimated total time is %d minutes", config$data_folder, floor(n_files / 400)),
                          footer = NULL))
    date_range <- tryCatch(db %>% SBCpip::sbc_build_and_save_full_db(config, updateProgress), error = function(e) {
      print(e)
      NULL
    })
    DBI::dbDisconnect(db, shutdown = TRUE)
    removeModal()
    if (is.null(date_range)) return()
    showModal(modalDialog(sprintf("Database build finished! Initial Date: %s -- Final Date: %s", 
                                  date_range[1], date_range[2])))
    Sys.sleep(4)
    removeModal()
  })
  
  # Summarize CBC data for a specific date. Assumes that db has table named cbc
  cbcSummary <- eventReactive(input$cbcSummaryButton, {
    req(input$cbcDateStart)
    req(input$cbcDateEnd)
    
    config <- SBCpip::get_SBC_config()
    
    fetch_data_for_dates(input$cbcDateStart, 
                         input$cbcDateEnd, 
                         "cbc", 
                         config$database_path)
  })
  output$cbcSummary <- renderTable({
    cbcSummary()
  })
  
  # Summarize Census data for a specific date. Assumes that db has table named census
  censusSummary <- eventReactive(input$censusSummaryButton, {
    req(input$censusDateStart)
    req(input$censusDateEnd)
    
    config <- SBCpip::get_SBC_config()

    fetch_data_for_dates(input$censusDateStart, 
                         input$censusDateEnd, 
                         "census", 
                         config$database_path)
  })
  output$censusSummary <- renderTable({
    censusSummary()
  })
  
  # Summarize Surgery data for a specific date. Assumes that db has table named surgery
  surgerySummary <- eventReactive(input$surgerySummaryButton, {
    req(input$surgeryDateStart)
    req(input$surgeryDateEnd)
    
    config <- SBCpip::get_SBC_config()

    fetch_data_for_dates(input$surgeryDateStart, 
                         input$surgeryDateEnd, 
                         "surgery",
                         config$database_path)
  })
  output$surgerySummary <- renderTable({
    surgerySummary()
  })
  
  # Summarize Inventory data for a specific date. Assumes that db has table named surgery
  inventorySummary <- eventReactive(input$inventorySummaryButton, {
    req(input$inventoryDateStart)
    req(input$inventoryDateEnd)
    
    config <- SBCpip::get_SBC_config()

    fetch_data_for_dates(input$inventoryDateStart, 
                         input$inventoryDateEnd, 
                         "inventory",
                         config$database_path)
  })
  output$inventorySummary <- renderTable({
    inventorySummary()
  })
  
  # Predict platelet usage for a specific date.
  predictSingleDay <- eventReactive(input$predictButton, {
    opts <- options(warn = 1) ## Report warnings as they appear
    req(input$predictDate)
    req(input$collect1)
    req(input$collect2)
    req(input$expire1)
    req(input$expire2)
    
    config <- SBCpip::get_SBC_config()
    
    db <- DBI::dbConnect(duckdb::duckdb(), config$database_path, read_only = FALSE)
    on.exit({
      if (DBI::dbIsValid(db)) DBI::dbDisconnect(db, shutdown = TRUE) 
    }, add = TRUE)
    
    model_age <- list(age = 1L)
    if ("model" %in% DBI::dbListTables(db)) {
      model_tbl <- db %>% dplyr::tbl("model") %>% dplyr::collect()
      if (input$predictDate %in% model_tbl$date) {
        model_age <- model_tbl %>% 
          dplyr::filter(date == input$predictDate) %>%
          dplyr::select(age)
      }
    }
    
    if (model_age == 1L) {
      shinyalert("Note", "Model is retraining - this may take some time to complete.")
    }
    
    pr <- db %>% SBCpip::predict_for_date_db(config = config, date = input$predictDate) %>%
      dplyr::mutate_at("date", as.character)
    
    coefs <- db %>% dplyr::tbl("model") %>% 
      dplyr::collect() %>% 
      dplyr::filter(date == input$predictDate) %>%
      dplyr::select(-c(age, l1_bound, lag_bound)) %>%
      tidyr::pivot_longer(-c(date), names_to = "var") %>%
      dplyr::mutate(abs_coef = abs(value)) %>%
      dplyr::arrange(desc(abs_coef)) %>%
      dplyr::select(-c(date, abs_coef))  
    DBI::dbDisconnect(db, shutdown = TRUE)
    
    # Return the predicted usage and recommended number of units to collect in 3 days.
    list(output_txt = sprintf("Prediction Date: %s\nPredicted Usage for Next 3 Days: %d Units\nRecommended Amount to Collect in 3 Days: %d Units\nModel retraining in: %d Days",
                              input$predictDate,
                              pr$t_pred, 
                              max(floor(pip::pos(pr$t_pred - input$collect1 - input$collect2 - input$expire1 - input$expire2 + 1)), config$c0),
                              config$model_update_frequency - model_age$age),
         coef_tbl = coefs)
  })
  output$predictionResult <- renderText({
    predictSingleDay()$output_txt
  })
  output$modelCoefs <- renderTable({
    predictSingleDay()$coef_tbl
  })
  
  # The below are equired for async, not necessary otherwise.
  startDate <- reactive({
    input$startDate
  })
  endDate <- reactive({
    input$endDate
  })
  
  # Generate prediction table for a range of dates.
  generatePredTable <- eventReactive(input$predictionSummaryButton, {   
    opts <- options(warn = 1) ## Report warnings as they appear
    req(input$startDate)
    req(input$endDate)
    
    start_date <- as.Date(startDate())
    end_date <- as.Date(endDate())
    num_days <- as.integer(end_date - start_date + 1)
    
    config <- SBCpip::get_SBC_config()
    
    # Initialize shiny progress bar
    progress <- shiny::Progress$new()
    progress$set(message = "Model Validation Progress", value = 0)
    on.exit(progress$close())
    
    # Callback function to update progress
    updateProgress <- function(detail = NULL) {
      progress$inc(amount = 1/num_days, detail = detail)
    }
    
    # Eventually will want to add future_promise here for scalability. Not including this
    # for now as the resulting functionality is the same with a single session.
    db <- DBI::dbConnect(duckdb::duckdb(), config$database_path, read_only = FALSE)
    on.exit({
      if (DBI::dbIsValid(db)) DBI::dbDisconnect(db, shutdown = TRUE) 
    }, add = TRUE)

    if (sum(data_tables %in% DBI::dbListTables(db)) != length(data_tables)) {
      return("Data tables are not available in database. Make sure to run 'Database Settings' >> 'Reset Database' first.")
    }

    transfusion_tbl <- db %>% dplyr::tbl("transfusion") %>% dplyr::collect()
    
    all_dates <- seq.Date(start_date, end_date, by = 1L)
    missing_dates <- all_dates[!(all_dates %in% transfusion_tbl$date)]

    # Check to make sure all transfusion data is included in the range we are trying to predict
    if (length(missing_dates) > 0L) return("Data for some or all of the selected dates are missing in the database.")
    
    # Predict range that needs to be predicted
    prediction_df <- db %>% SBCpip::sbc_predict_for_range_db(start_date, num_days, config, updateProgress = updateProgress)
    pred_analysis <- db %>% SBCpip::build_prediction_table_db(config, start_date, end_date, 
                                                              prediction_df) %>% 
      SBCpip::pred_table_analysis(config)
    coef_analysis <- db %>% SBCpip::build_coefficient_table_db(start_date, num_days) %>%
      SBCpip::coef_table_analysis()
      
    db %>% DBI::dbDisconnect(shutdown = TRUE)
    
    # Output both the prediction and coefficient analyses
    list(pred_analysis = unlist(pred_analysis), coef_analysis = coef_analysis)
  })
  output$predAnalysis <- renderTable({
    generatePredTable() %>% 
      `$`(pred_analysis) %>%
      as.data.frame()
    }, rownames = TRUE)
  output$coefAnalysis<- renderTable({
    generatePredTable() %>%
      `$`(coef_analysis) %>%
      `$`(other) 
    })

  ## Plot a range of dates that have already been predicted (in the pred_cache)
  predPlot <- eventReactive(input$predPlotButton, {
    req(input$plotDateStart)
    req(input$plotDateEnd)
    
    config <- SBCpip::get_SBC_config()
    
    db <- DBI::dbConnect(duckdb::duckdb(), config$database_path, read_only = FALSE)
    
    on.exit({
      if (DBI::dbIsValid(db)) DBI::dbDisconnect(db, shutdown = TRUE) 
    }, add = TRUE)
    
    db %>% build_prediction_table_db(config, start_date = input$plotDateStart, 
                                     end_date = input$plotDateEnd) %>%
      dplyr::filter(date >= input$plotDateStart + config$start + 5L) %>%
      dplyr::filter(date <= input$plotDateEnd - 3L) ->
      d
    
    # Actual vs. predicted usage for the next 3 days
    p1 <- ggplot2::ggplot(data = d) +
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `Three-day actual usage`, col = "Actual Usage")) +
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `Three-day prediction`, col = "Predicted Usage"))
    ggplot2::labs(x = "Actual and Estimated 3-Day Usage", y = "Units") +
      ggplot2::theme(legend.position = "bottom")
    
    # The recommended number of units to collect per the prediction
    p2 <- ggplot2::ggplot(data = d) +
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `No. to collect per prediction`, col = "Collection"))

    # Waste generated by model predictions vs. truth (with inventory expiring in 1 day)
    p3 <- ggplot2::ggplot(data = d) +
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = Waste, col = "Waste")) + 
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `Adj. no. expiring in 1 day`, col = "Expiring in 1 Day"))
    
    # Shortage generated by model prediction vs. truth (with inventory expiring in 2 days)
    p4 <- ggplot2::ggplot(data = d) + 
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = Shortage, col = "Shortage")) + 
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `Adj. no. expiring in 2 days`, col = "Expiring in 2 Days"))
    
    # The actual waste generated per true protocol
    p5 <- ggplot2::ggplot(data = d) +
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `True Waste`, col = "True Waste")) + 
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `Inv. expiring in 1 day`, col = "Inv. expiring in 1 day"))
    
    # The actual shortage generated per true protocol
    p6 <- ggplot2::ggplot(data = d) +
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `True Shortage`, col = "True Shortage")) + 
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `Inv. expiring in 2 days`, col = "Inv. expiring in 2 days"))

    gridExtra::grid.arrange(grobs = lapply(list(p1, p2, p3, p4, p5, p6), ggplot2::ggplotGrob),
                            layout_matrix = matrix(c(1,3,5,2,4,6), nrow = 3))
  })
  output$wrPlot <- renderPlot({
    predPlot()
  })
  
  ## Cooefficient Plot functionality
  coefPlot <- eventReactive(input$plotCoefButton, {
    req(input$coefPlotDateStart)
    req(input$coefPlotDateEnd)
    req(input$coefList)
    
    config <- SBCpip::get_SBC_config()
    
    db <- DBI::dbConnect(duckdb::duckdb(), config$database_path, read_only = FALSE)
    
    on.exit({
      if (DBI::dbIsValid(db)) DBI::dbDisconnect(db, shutdown = TRUE) 
    }, add = TRUE)
    
    start_date <- as.Date(input$coefPlotDateStart)
    end_date <- as.Date(input$coefPlotDateEnd)
    num_days <- as.integer(end_date - start_date + 1)
    
    coef_table <- db %>% SBCpip::build_coefficient_table_db(start_date, num_days)
    
    pred_table <- db %>% SBCpip::build_prediction_table_db(config, start_date, end_date)
    
    # Number of units expiring in 1 day (4) + number expiring in 2 days (5)
    p1 <- coef_table %>% 
      tidyr::pivot_longer(cols = -date) %>% # pivot longer so we can plot in groups
      dplyr::filter(name %in% c(input$coefList, "l1_bound")) %>% # restrict to coefficients in the specified list
      ggplot2::ggplot() +
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = value, group = name, color = name))
    
    p1
  })
  output$cfPlot <- renderPlot({
    coefPlot()
  })
  
  
}

shinyApp(ui = ui, server = server)
