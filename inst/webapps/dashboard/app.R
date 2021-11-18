library(shiny)
library(shinydashboard)
library(shinyalert)
library(SBCpip)
library(tidyverse)
library(gridExtra)
library(bibtex)
library(DBI)
library(duckdb)
library(scales)

paper_citation <- bibtex::read.bib(system.file("extdata", "platelet.bib", 
                                               package = "SBCpip", mustWork = TRUE))
data_tables <-   data_tables <- c("cbc", "census", "surgery", "transfusion", "inventory")

## Initialization code

## Set config parameters for relevant column headers based on provided data mapping
SBCpip::set_org_col_params()

## Read column mappings from the sbc_data_mapping file.

## Set config params for cbc quantiles and abnormals
cbc_threshold_file <- system.file("extdata", "cbc_thresholds.csv",
                                  package = "SBCpip", mustWork = TRUE)
cbc_thresholds <- read.csv(cbc_threshold_file)
cbc_quantiles_tbl <- cbc_thresholds %>% 
  dplyr::filter(metric == "quantile")
cbc_abnormals_tbl <- cbc_thresholds %>% 
  dplyr::filter(metric == "abnormal")

## Helper to convert CBC quantiles from external table to the appropriate config functions
cbc_quantile_to_function <- function(component, table) {
  ## Assume we have a row with columns metric = "quantile", base_name, type, and value
  table_row <- table %>% dplyr::filter(base_name == component)
  if (nrow(table_row) != 1L) {
    warning(sprintf("Either the base_name %s is not included in cbc_thresholds.csv, or multiple rows were found. Ignoring this feature.",
                 component))
    return(NA)
  }
  
  return_val <- NA
  if (table_row$type == "literal")
    return_val <- as.character(table_row$value)
  else if (table_row$type == "quantile")
    return_val <- sprintf("quantile(x, probs = %f, na.rm = TRUE)", 
                          as.double(table_row$value))
  else stop("Quantile type must be 'quantile' or 'literal'. Please check cbc_thresholds.csv.")
  
  # evaluate the function 
  eval(parse(text = sprintf("function(x) {  %s }", return_val)))
}
  
## Helper to convert CBC abnormalities from external table to the appropriate config functions
cbc_abnormal_to_function <- function(component, table) {
  ## Assume we have a row with columns metric = "abnormal", base_name, type, and value
  table_row <- table %>% dplyr::filter(base_name == component)
  if (nrow(table_row) != 1L) {
    warning(sprintf("Either the base_name %s is not included in cbc_thresholds.csv, or multiple rows were found. Ignoring this feature.",
                    component))
    return(NA)
  }
  
  direction <- NA
  if (table_row$type == "greater") direction <- ">"
  else if (table_row$type == "less") direction <- "<"
  else stop("Abnormality type must be 'greater' or 'less'. Please check cbc_thresholds.csv.")
  
  # return the evaluated function 
  eval(parse(text = sprintf("function(x) {  x %s %f}", direction, as.double(table_row$value))))
}

## Dashboard UI Code
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

## Dashboard Sidebar Items. These are inputs that control the display in body
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
               , menuSubItem("Transfusion Summary", tabName = "transfusion")
               , conditionalPanel(
                 condition = "input.tabId == 'transfusion'"
                 , dateInput(inputId = "transfusionDateStart", label = "Start Date", value = as.character(Sys.Date()))
                 , dateInput(inputId = "transfusionDateEnd", label = "End Date", value = as.character(Sys.Date()))
                 , actionButton(inputId = "transfusionSummaryButton", "Summarize")
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
      , dateInput(inputId = "predictDate", label = "Date (i + 1)", value = as.character(Sys.Date()))
      , numericInput(inputId = "collect1", label = "Units Collected Today (i + 1)", value = 0)
      , numericInput(inputId = "collect2", label = "Units Collected Tomorrow (i + 2)", value = 0)
      , numericInput(inputId = "expire1", label = "Remaining Units Expiring Today", value = 0)
      , numericInput(inputId = "expire2", label = "Remaining Units Expiring Tomorrow", value = 0)
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

## Displays outputs of model validation and projections
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
  shinyalert::useShinyalert(),
  tabItems(
    tabItem(tabName = "dashboard"
            , h2("Platelet Inventory Prediction")
            , p(em(intro_line_1, intro_line_2))
            , h3("Abstract")
            , p(paper_citation$abstract)
    )
    , tabItem(tabName = "data_settings"
              , h2("Database Settings")
              , fluidRow(
                box(
                  h3("File Settings")
                  , fluidRow(align = "center"
                    , actionButton(inputId = "saveFileSettings", label = "Save File Settings")
                    , actionButton(inputId = "loadFileSettings", label = "Load File Settings")
                  )
                  , h3("Input and Output Locations")
                  , textInput(inputId = "database_path", label = "Database Path", 
                              value = SBCpip::get_SBC_config()$database_path)
                  , textInput(inputId = "data_folder", label = "Data Folder", 
                              value = SBCpip::get_SBC_config()$data_folder)
                  , textInput(inputId = "log_folder", label = "Log Folder", 
                              value = SBCpip::get_SBC_config()$log_folder)
                  
                  , h3("Filename Patterns")
                  , textInput(inputId = "cbc_filename_prefix", 
                              label = "CBC Files", 
                              value = SBCpip::get_SBC_config()$cbc_filename_prefix)
                  , textInput(inputId = "census_filename_prefix", 
                              label = "Census Files", 
                              value = SBCpip::get_SBC_config()$census_filename_prefix)
                  , textInput(inputId = "transfusion_filename_prefix", 
                              label = "Transfusion Files", 
                              value = SBCpip::get_SBC_config()$transfusion_filename_prefix)
                  , textInput(inputId = "surgery_filename_prefix", 
                              label = "Surgery Files", 
                              value = SBCpip::get_SBC_config()$surgery_filename_prefix)
                  , textInput(inputId = "inventory_filename_prefix", 
                              label = "Inventory Files", 
                              value = SBCpip::get_SBC_config()$inventory_filename_prefix)
                  , textInput(inputId = "log_filename_prefix", 
                              label = "Log Files", 
                              value = SBCpip::get_SBC_config()$log_filename_prefix)
                )
                , box(
                  h3("Feature Names")
                  , fluidRow(align = "center"
                    , actionButton(inputId = "refreshFeaturesButton", label = "Refresh Features")
                    , actionButton(inputId = "saveFeaturesButton", label = "Save Features")
                    , actionButton(inputId = "loadFeaturesButton", label = "Load Features")
                    , br()
                  )
                  , shinyWidgets::multiInput(inputId = "cbc_features", 
                                             label = "CBC Features List", 
                                             choices = SBCpip::get_SBC_config()$cbc_vars, 
                                             selected = SBCpip::get_SBC_config()$cbc_vars)
                  , shinyWidgets::multiInput(inputId = "census_features", 
                                             label = "Census Features List", 
                                             choices = SBCpip::get_SBC_config()$census_locations,
                                             selected = SBCpip::get_SBC_config()$census_locations)
                  , shinyWidgets::multiInput(inputId = "surgery_features", 
                                             label = "Surgery Features List", 
                                             choices = SBCpip::get_SBC_config()$surgery_services,
                                             selected = SBCpip::get_SBC_config()$surgery_services)
                )
              )
    )
    , tabItem(tabName = "model_settings"
              , fluidRow(
                h2("Configuration Settings")
                , HTML("<ul>
                    <li><strong>Minimum Remaining Fresh Units at EOD [<em>c0</em>]:</strong> Ensures that hospital inventory 
                      levels remain reasonably high at the end of the day. Raising this value will
                      bias the model's predictions upward.</li>
                    <li><strong>Prediction Error Bias [<em>b</em>]:</strong> This positive bias allows selection of
                      model hyperparameters that avoid negative prediction errors, which typically result in shortage. 
                      This value should be similar to c0 above.</li>
                    <li><strong>Shortage Penalty Factor [<em>pi</em>]:</strong> How much we prioritize minimizing shortage
                      over wastage. Impact tends to be lower than that of c0 and b, but it
                      should be kept high to avoid shortage where possible.</li>
                    <li><strong>History Window:</strong> The number of previous days we consider in retraining the model. We
                      ignore the first [Skip Initial] + 5 days in the window during training time. Larger history windows will tend
                      to produce more conservative / higher usage predictions and can lead to increased waste, and smaller
                      windows will tend to reduce waste but increase the risk of shortage.</li>
                    <li><strong>Lag Window:</strong> The number of previous days we average over in computing the lagged
                      platelet usage feature. This should be at least 7 days to incorporate information from the previous
                      week.</li> 
                    <li><strong>Skip Initial:</strong> The number of initial days we skip in training and evaluating the
                      model. The effective training window is the previous [History Window] - [Skip Initial] - 5 days.
                      Similarly, when we validate the model over n days, the number of days during which we record waste
                      and shortage is n - [Skip Initial] - 7 days.</li>
                    <li><strong>Model Update Frequency:</strong> How often we retrain the model when predicting over
                      a range of dates (recommended 7 days).</li>
                    <li><strong>Range of L1 Bounds on Model Coefs:</strong> This restricts the size of covariate coefficients
                      using L1-regularization, resulting in sparse models. The model uses cross-validation to select an
                      appropriate L1 Bound in the range during each retraining step. 0-60 is sufficient for most applications [The lower
                      bound should ideally be 0 in order to take full advantage of model sparsity].</li>
                    <li><strong>Bound on Lagged Usage:</strong> Occasionally it is helpful to also restrict the influence of the
                      previous usage to focus on other features that might cause a break in the pattern. The model uses
                      cross-validation to determine whether the magnitude of this coefficient should be clipped to the given value.</li>
                  </ul>")
                , box(
                  h3("Inventory Requirements for Model")
                  , sliderInput(inputId = "c0"
                                , label = "Minimum Remaining Fresh Units at EOD [c0] (units):"
                                , min = 0
                                , max = 50
                                , value = get_SBC_config()$c0)
                  , sliderInput(inputId = "prediction_bias"
                                , label = "Prediction Error Bias [b] (units):"
                                , min = 0
                                , max = 100
                                , value = get_SBC_config()$prediction_bias)
                  , sliderInput(inputId = "penalty_factor"
                                , label = "Shortage Penalty Factor [pi] (X units Wasted : 1 unit Short):"
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
                                , label = "Bound on Lagged Usage:"
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
    , tabItem(tabName = "transfusion"
              , h2("Transfusion Summary")
              , tableOutput("transfusionSummary"))
    , tabItem(tabName = "inventory"
              , h2("Inventory Summary")
              , tableOutput("inventorySummary")
    )
    , tabItem(tabName = "validation"
              , h2("Prediction Analysis")
              , h5("Click \"Validate Model\" to run a series of predictions across a given range of 
                   dates and analyze model performance.")
              , h5("Click \"Summary Table\" to display the analysis
                   from a previously computed series of predictions.")
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
logo_src <- system.file("webapps", "dashboard", "assets", "sbc.png", 
                        package = "SBCpip", mustWork = TRUE)
dbHeader$children[[2]]$children <-  tags$a(href='https://stanfordbloodcenter.org',
                                           tags$img(src = 'https://sbcdonor.org/client_assets/images/logos/logo_stanford.png',
                                                    alt = 'Stanford Blood Center Dashboard', height = '40'))

ui <- dashboardPage(
  title = "SBC Dashboard",
  header = dbHeader
  , sidebar = sidebar
  , body = body
  , skin = "green"
)

## Helper function to gather and summarize data from each table of the database
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
  
  tbl <- full_tbl %>% dplyr::ungroup() %>%
    dplyr::filter(date %in% all_dates) %>%
    tidyr::pivot_longer(-c(date), names_to = "var", values_to = "count") %>%
    dplyr::group_by(var) %>%
    dplyr::summarize(`Mean Value` = mean(count), `Standard Deviation` = sd(count)) %>%
    dplyr::select(var, `Mean Value`, `Standard Deviation`) %>%
    dplyr::mutate(`Standard Deviation` = replace_na(`Standard Deviation`, 0))
  tbl
}


server <- function(input, output, session) {
  
  ## Will remove once the multiInput is implemented
  parse_vars <- function(v) unlist(lapply(strsplit(as.character(v), split = ","), trimws))
  
  all_active_features <- reactive({
    fixed_features <- c("intercept", "lag", 
                        "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")
    all_active_features <- c(fixed_features, 
                             sapply(parse_vars(input$cbc_features), function(x) paste0(x, "_Nq")),
                             parse_vars(input$census_features),
                             parse_vars(input$surgery_features))
  })
  
  all_file_settings <- reactive({
    all_file_settings <- c("data_folder", "log_folder", "database_path",
                           "cbc_filename_prefix", "census_filename_prefix",
                           "surgery_filename_prefix", "transfusion_filename_prefix",
                           "inventory_filename_prefix", "log_filename_prefix")
  })
  
  # Exit the app
  observeEvent(input$exitApp, {
    stopApp(TRUE)
  })
  
  set_path_params <- reactive({
    
    sapply(all_file_settings(), function(x) {
      SBCpip::set_config_param(x, input[[x]])
    })
  })
  
  observeEvent(input$saveFileSettings, {
    
    set_path_params()

    file_settings <- lapply(all_file_settings(), function(x) input[[x]])
    names(file_settings) <- all_file_settings()
    
    package.path <- find.package("SBCpip",
                                 lib.loc = NULL, quiet = FALSE,
                                 verbose = getOption("verbose"))
    destination.path <- file.path(package.path,
                                  "user_settings", "file_settings.rds")
    
    result <- tryCatch(saveRDS(file_settings, 
                               file = destination.path),
                       warning = function(e) {
                         shinyalert::shinyalert("Oops!", "Saving failed. Make sure you have entered the correct data folder.")
                         simpleWarning(e)$message
                       })
    if (!is.character(result)) {
      shinyalert::shinyalert("Success!", "Saved New Default File Settings.")
    }
    
  })
  
  observeEvent(input$loadFileSettings, {
    
    package.path <- find.package("SBCpip",
                                 lib.loc = NULL, quiet = FALSE,
                                 verbose = getOption("verbose"))
    destination.path <- file.path(package.path,
                                  "user_settings", "file_settings.rds")
    
    file_settings <- tryCatch(readRDS(file = destination.path),
                              error = function(e) {
                                shinyalert::shinyalert("Oops!", "Loading failed. Make sure you have saved file settings first.")
                                return()
                              })
    
    sapply(all_file_settings(), function(x) {
      updateTextInput(session, inputId = x, value = file_settings[[x]])
    })
    
    set_path_params()
    
  })
  
  # Refresh possible features to choose from based on available data files
  observeEvent(input$refreshFeaturesButton, {
    
    set_path_params()

    # Grab CBC Features
    features <- tryCatch(SBCpip::set_features_from_file(),
                         error = function(e) {
                           shinyalert::shinyalert("Oops!", "Feature Refresh failed. 
                                                  Make sure you have supplied the correct data folder and updated sbc_data_mapping.csv.")
                           NULL
                         })
    
    if (!is.null(features)) {
      shinyWidgets::updateMultiInput(session, inputId = "cbc_features",
                                     selected = input$cbc_features,
                                     choices = union(features$cbc, input$cbc_features))
    
      shinyWidgets::updateMultiInput(session, inputId = "census_features", 
                                     selected = input$census_features,
                                     choices = union(features$census, input$census_features))
    
      shinyWidgets::updateMultiInput(session, inputId = "surgery_features", 
                                     selected = input$surgery_features,
                                     choices = union(features$surgery, input$surgery_features))
    }
    
  })
  
  observeEvent(input$saveFeaturesButton, {
    
    set_path_params()
    
    cbc <- input$cbc_features
    census <- input$census_features
    surgery <- input$surgery_features
    
    feat_list <- list(cbc = cbc, census = census, surgery = surgery)
    
    config <- SBCpip::get_SBC_config()
    
    package.path <- find.package("SBCpip",
                                 lib.loc = NULL, quiet = FALSE,
                                 verbose = getOption("verbose"))
    destination.path <- file.path(package.path,
                                  "user_settings", "features.rds")
    
    result <- tryCatch(saveRDS(feat_list, 
                               file = destination.path),
                       warning = function(e) {
                         shinyalert::shinyalert("Oops!", "Saving failed. Make sure you have entered the correct file path.")
                         simpleWarning(e)$message
                       })
                       
    if (!is.character(result)) shinyalert::shinyalert("Success!", "Saved New Default Features.")
  })
  
  observeEvent(input$loadFeaturesButton, {
    # Grab CBC Features
    config <- SBCpip::get_SBC_config()
    result <- NULL
    
    package.path <- find.package("SBCpip",
                                 lib.loc = NULL, quiet = FALSE,
                                 verbose = getOption("verbose"))
    destination.path <- file.path(package.path,
                                  "user_settings", "features.rds")
    
    features <- tryCatch(readRDS(file = destination.path),
                         error = function(e) {
                           shinyalert::shinyalert("Oops!", "Loading failed. Make sure you have supplied the correct folders and saved features.")
                           NULL
                         })
    
    if (!is.null(features)) {
      shinyWidgets::updateMultiInput(session, inputId = "cbc_features", 
                                     selected = features$cbc,
                                     choices = union(features$cbc, input$cbc_features))
    
      shinyWidgets::updateMultiInput(session, inputId = "census_features", 
                                     selected = features$census,
                                     choices = union(features$census, input$census_features))
    
      shinyWidgets::updateMultiInput(session, inputId = "surgery_features", 
                                     selected = features$surgery,
                                     choices = union(features$surgery, input$surgery_features))
    }
    
  })
  
  # Set the database build configurations
  observeEvent(input$setDBValues, {
    
    set_path_params()
    
    # Variables
    cbc_feature_vector <- input$cbc_features
    cbc_q <- lapply(cbc_feature_vector, cbc_quantile_to_function, cbc_quantiles_tbl)
    names(cbc_q) <- cbc_feature_vector
    cbc_a <- lapply(cbc_feature_vector, cbc_abnormal_to_function, cbc_abnormals_tbl)
    names(cbc_a) <- cbc_feature_vector
    cbc_q_clean <- cbc_q[!is.na(cbc_q)]
    cbc_a_clean <- cbc_a[!is.na(cbc_a)]
    cbc_feats_with_levels <- intersect(names(cbc_q_clean), names(cbc_a_clean))
    
    # Only include the given cbc variables for which we have a quantile and an abnormality level
    SBCpip::set_config_param("cbc_vars", cbc_feature_vector[cbc_feature_vector %in% cbc_feats_with_levels])
    SBCpip::set_config_param("cbc_quantiles", cbc_q_clean)
    SBCpip::set_config_param("cbc_abnormals", cbc_a_clean)
    
    SBCpip::set_config_param("census_locations", input$census_features)
    SBCpip::set_config_param("surgery_services", input$surgery_features)
    
    # Update the coefficients 
    shinyWidgets::updateMultiInput(session, 
                                   inputId = "coefList", 
                                   choices = all_active_features())
    
    shinyalert::shinyalert("Success", "The database settings have been saved successfully.")
  })
  
  # Save parameters from settings panel to config.
  observeEvent(input$setModelValues, {

    # Inventory parameters
    SBCpip::set_config_param("c0", input$c0) # for training and cross-validation
    SBCpip::set_config_param("prediction_bias", input$prediction_bias) # for loss function
    SBCpip::set_config_param("min_inventory", 0) # for prediction table building
    SBCpip::set_config_param("penalty_factor", input$penalty_factor)
    
    # Other model/prediction/validation parameters
    SBCpip::set_config_param("history_window", input$history_window)
    SBCpip::set_config_param("lag_window", input$lag_window)
    SBCpip::set_config_param("start", input$start)
    SBCpip::set_config_param("model_update_frequency", input$model_update_frequency)
    SBCpip::set_config_param("l1_bounds", seq(from = input$l1_bound_range[2], to = input$l1_bound_range[1], by = -2))
    SBCpip::set_config_param("lag_bounds", c(-1, input$lag_bound))
    
    loggit::loggit(log_lvl = "INFO", log_msg = "Settings saved.")

    shinyalert::shinyalert("Success", "The model settings have been saved successfully.")
    
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
    shinyalert::shinyalert("Success", "The cached predictions have been cleared.")
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
    date_range <- tryCatch(db %>% SBCpip::build_and_save_database(config, updateProgress), error = function(e) {
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
  
  # Summarize Transfusion data for a specific date. Assumes that db has table named transfusion
  transfusionSummary <- eventReactive(input$transfusionSummaryButton, {
    req(input$transfusionDateStart)
    req(input$transfusionDateEnd)
    
    config <- SBCpip::get_SBC_config()
    
    fetch_data_for_dates(input$transfusionDateStart, 
                         input$transfusionDateEnd, 
                         "transfusion",
                         config$database_path)
  })
  output$transfusionSummary <- renderTable({
    transfusionSummary()
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
  
  # Predict platelet usage for a single specific date.
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
      shinyalert::shinyalert("Note", "Model is retraining - this may take some time to complete.")
    }
    
    pr <- db %>% SBCpip::predict_for_date(config = config, date = input$predictDate) %>%
      dplyr::mutate_at("date", as.character)
    
    coefs <- db %>% dplyr::tbl("model") %>% 
      dplyr::collect() %>% 
      dplyr::filter(date == input$predictDate) %>%
      dplyr::select(-c(age, l1_bound, lag_bound)) %>%
      tidyr::pivot_longer(-c(date), names_to = "var") %>%
      dplyr::mutate(abs_coef = abs(value)) %>%
      dplyr::arrange(desc(abs_coef)) %>%
      dplyr::select(-c(date, abs_coef))  
    
    # We find the average platelet usage over the last [start] days in order to
    # correct for expected waste.
    next_collection <- db %>% 
      SBCpip::recommend_collection(config, input$predictDate, pr$t_pred, 
                                   2L, input$collect1, input$collect2, 
                                   input$expire1, input$expire2)
    
    DBI::dbDisconnect(db, shutdown = TRUE)
    
    # Return the predicted usage and recommended number of units to collect in 3 days.
    list(output_txt = sprintf("Prediction Date: %s\nPredicted Usage for Next 3 Days: %d Units\n Recommended Amount to Collect in 3 Days: %d Units\n Model retraining in: %d Days",
                              input$predictDate,
                              pr$t_pred, 
                              next_collection,
                              config$model_update_frequency - model_age$age),
         coef_tbl = head(coefs, 25L))
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
  
  validating <- reactiveVal(FALSE)
  
  observeEvent(input$predictionSummaryButton, {
    validating(TRUE)
  })
  
  # Generate prediction table for a range of dates.
  observe({
    btns <- c("predictionSummaryButton", "standaloneSummaryButton")
    lapply(btns, function(x) {   
      observeEvent(input[[x]], {
        
        if (!input$setDBValues | !input$setModelValues) 
          shinyalert::shinyalert("Oops!", "Make sure to apply the Database Settings and Model Settings first.")
        
        opts <- options(warn = 1) ## Report warnings as they appear
        req(input$setDBValues)
        req(input$setModelValues)
        req(input$startDate)
        req(input$endDate)
        
        start_date <- as.Date(startDate())
        end_date <- as.Date(endDate())
        num_days <- as.integer(end_date - start_date + 1)
        
        config <- SBCpip::get_SBC_config()
        
        # Eventually will want to add future_promise here for scalability. Not including this
        # for now as the resulting functionality is the same with a single session.
        db <- DBI::dbConnect(duckdb::duckdb(), config$database_path, read_only = FALSE)
        on.exit({
          if (DBI::dbIsValid(db)) DBI::dbDisconnect(db, shutdown = TRUE) 
          validating(FALSE)
        }, add = TRUE)
        
        if (sum(data_tables %in% DBI::dbListTables(db)) != length(data_tables)) {
          output$predAnalysis <- renderTable({
            "Data tables are not available in database. 
                   Please run 'Database Settings' >> 'Reset Database'."
            }, colnames = FALSE)
          return()
        }
        
        # Check to make sure all transfusion data is included in the range we are trying to predict
        # There's no particular reason tranfusion is used here as opposed to any other table
        transfusion_tbl <- db %>% dplyr::tbl("transfusion") %>% dplyr::collect()
        
        all_dates <- seq.Date(start_date, end_date, by = 1L)
        missing_dates <- all_dates[!(all_dates %in% transfusion_tbl$date)]
        
        if (length(missing_dates) > 0L) {
          output$predAnalysis <- renderTable({
            "Data for some or all of the selected dates are missing in the database."
          }, colnames = FALSE)
          return()
        }
    
        prediction_df <- NULL
    
        ## Only run model validation if the correct button is clicked
        if (validating()) {
          # Initialize shiny progress bar
          progress <- shiny::Progress$new()
          progress$set(message = "Model Validation Progress", value = 0)
          on.exit(progress$close(), add = TRUE)
    
          # Callback function to update progress
          updateProgress <- function(detail = NULL) {
            progress$inc(amount = 1/num_days, detail = detail)
          }
    
          # Predict range that needs to be predicted
          prediction_df <- tryCatch(db %>% SBCpip::predict_usage_over_range(start_date, num_days, config, 
                                                                   updateProgress = updateProgress),
                                    error = function(e) {
                                      output$predAnalysis <- renderTable({
                                        simpleError(e)$message
                                        }, colnames = FALSE)
                                      NULL
                                    })
          
          
        }
        
        if (!validating() | (validating() & !is.null(prediction_df))) {
          pred_analysis <- tryCatch(db %>% 
                                      SBCpip::build_prediction_table(config, 
                                                                        start_date, 
                                                                        end_date, 
                                                                        prediction_df) %>% 
                                      SBCpip::analyze_prediction_table(config),
                                    error = function(e) {
                                      output$predAnalysis <- renderTable({
                                        simpleError(e)$message
                                        }, colnames = FALSE)
                                      NULL
                                      })
        
          coef_analysis <- tryCatch(db %>% 
                                      SBCpip::build_coefficient_table(start_date, 
                                                                         num_days) %>%
                                      SBCpip::analyze_coef_table(),
                                    error = function(e) {
                                      output$predAnalysis <- renderTable({
                                        simpleError(e)$message
                                        }, colnames = FALSE)
                                      NULL
                                      })
      
          db %>% DBI::dbDisconnect(shutdown = TRUE)
    
          # Output both the prediction and coefficient analyses
          pred_analysis_items <- c("Prediction Started", "Prediction Ended", 
                                   "Number of Days", "Total Model Waste",
                                   "Total Model Short", "Total Actual Waste", 
                                   "Loss from Table", "Real Loss",
                                   "Overall RMSE", "Positive RMSE", "Negative RMSE", 
                                   "Sun RMSE", "Mon RMSE", "Tue RMSE", "Wed RMSE", 
                                   "Thu RMSE", "Fri RMSE", "Sat RMSE")
          
          if (!is.null(pred_analysis)) {
            output$predAnalysis <- renderTable({
              unlist(pred_analysis) %>% 
                as.data.frame() %>%
                `rownames<-`(pred_analysis_items) %>%
                `colnames<-`("Stats")
              }, rownames = TRUE)
          }
        
          if (!is.null(coef_analysis)) {
            output$coefAnalysis<- renderTable({
              coef_analysis %>%
                `$`(other) %>%
                dplyr::filter(.data$feat != "l1_bound") %>%
                rbind(coef_analysis$dow)
              }, rownames = TRUE)
          }
        }
      })
    })
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
    
    db %>% build_prediction_table(config, start_date = input$plotDateStart,
                                  end_date = input$plotDateEnd) %>%
      dplyr::filter(date >= input$plotDateStart + config$start + 5L) %>%
      dplyr::filter(date <= input$plotDateEnd - 3L) ->
      d
    
    # Actual vs. predicted usage for the next 3 days
    p1 <- ggplot2::ggplot(data = d) +
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `Three-day actual usage`, col = "Actual Usage")) +
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `Three-day prediction`, col = "Predicted Usage")) +
      ggplot2::ggtitle("Predicted vs. Actual 3-day Usage") +
      ggplot2::labs(x = "Date", y = "Units") +
      ggplot2::scale_x_date(breaks = scales::date_breaks("2 weeks")) +
      ggplot2::theme(legend.position = "bottom")
    
    # The recommended number of units to collect per the prediction
    p2 <- ggplot2::ggplot(data = d) +
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `No. to collect per prediction`, col = "Rec Collection")) +
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `Fresh Units Collected`, col = "True Collection ")) +
      ggplot2::ggtitle("Recommended vs. Actual Collection") + 
      ggplot2::labs(x = "Date", y = "Units") +
      ggplot2::scale_x_date(breaks = scales::date_breaks("2 weeks")) +
      ggplot2::theme(legend.position = "bottom")

    # Waste generated by model predictions vs. inventory expiring in 1 day
    p3 <- ggplot2::ggplot(data = d) +
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = Waste, col = "Waste")) + 
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `Adj. no. expiring in 1 day`, col = "Expiring in 1 Day")) + 
      ggplot2::ggtitle("Model Waste vs. Remaining Inventory Exp. Tomorrow") +
      ggplot2::labs(x = "Date", y = "Units") +
      ggplot2::scale_x_date(breaks = scales::date_breaks("2 weeks")) +
      ggplot2::theme(legend.position = "bottom")
    
    # Shortage generated by model prediction vs. inventory expiring in 2 days
    p4 <- ggplot2::ggplot(data = d) + 
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = Shortage, col = "Shortage")) + 
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `Adj. no. expiring in 2 days` + `Adj. no. expiring in 1 day`, 
                                                col = "Inventory Count")) + 
      ggplot2::ggtitle("Model Shortage vs. Total Remaining Inventory") +
      ggplot2::labs(x = "Date", y = "Units") +
      ggplot2::scale_x_date(breaks = scales::date_breaks("2 weeks")) +
      ggplot2::theme(legend.position = "bottom")
    
    # The actual waste generated per true protocol
    p5 <- ggplot2::ggplot(data = d) +
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `True Waste`, col = "True Waste")) + 
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `Inv. expiring in 1 day`, 
                                                col = "Inv. expiring in 1 day")) +
      ggplot2::ggtitle("True Waste vs. Remaining Inventory Exp. Tomorrow") + 
      ggplot2::labs(x = "Date", y = "Units") +
      ggplot2::scale_x_date(breaks = scales::date_breaks("2 weeks")) +
      ggplot2::theme(legend.position = "bottom")
    
    # The actual shortage generated per true protocol
    p6 <- ggplot2::ggplot(data = d) +
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `True Shortage`, col = "True Shortage")) + 
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = `Inv. count` - `Inv. expiring in 2+ days`, col = "True Inventory Count")) + 
      ggplot2::ggtitle("True Shortage vs. Total Remaining Inventory") +
      ggplot2::labs(x = "Date", y = "Units") +
      ggplot2::scale_x_date(breaks = scales::date_breaks("2 weeks")) +
      ggplot2::theme(legend.position = "bottom")

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
    
    coef_table <- db %>% SBCpip::build_coefficient_table(start_date, num_days)
    
    pred_table <- db %>% SBCpip::build_prediction_table(config, start_date, end_date)
    
    # Number of units expiring in 1 day (4) + number expiring in 2 days (5)
    p1 <- coef_table %>% 
      tidyr::pivot_longer(cols = -date) %>% # pivot longer so we can plot in groups
      dplyr::filter(name %in% c(input$coefList)) %>% # restrict to coefficients in the specified list
      ggplot2::ggplot() +
      ggplot2::geom_line(mapping = ggplot2::aes(x = date, y = value, group = name, color = name))
    p1 <- p1 + geom_line(mapping = ggplot2::aes(x = date, y = l1_bound, size = 2, color = "Coefficient L1 Bound"), 
                         data = coef_table) +
      ggplot2::scale_x_date(breaks = scales::date_breaks("2 weeks"))
    
    p1
  })
  output$cfPlot <- renderPlot({
    coefPlot()
  })
  
}

shinyApp(ui = ui, server = server)
