#' connect to this db yo
#'
#' @export
connect <- function() {
  DBI::dbConnect(duckdb::duckdb(), Sys.getenv("DB_PATH_WNBA"))
}
