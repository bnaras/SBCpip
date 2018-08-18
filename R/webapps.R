#' Run the Stanford Blood Center dashboard app
#' @importFrom shiny runApp
#' @importFrom shinydashboard dashboardSidebar
#' @import tidyverse
#' @importFrom gridExtra grid.arrange
#' @importFrom grDevices hcl
#' @importFrom jsonlite read_json
#' @importFrom bibtex read.bib
#' @export
sbc_dashboard <- function(){
    shiny::runApp(system.file("webapps", "dashboard", package = "SBCpip"))
}

