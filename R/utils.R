#' connect to this db yo
#'
#' @export
connect <- function() {
  DBI::dbConnect(duckdb::duckdb(), Sys.getenv("DB_PATH_WNBA"))
}

check_existing_connection <- function() {
  exists("con", envir = .GlobalEnv)
}

#' I just like to make it easier for myself
#'
#' @param schema Character
#' @param table Character
#'
#' @export
tblx <- function(schema, table) {
  if (!check_existing_connection()) {
    stop(
      "Database connection (`con`) not found in global environment.
          Please create a connection before using this function."
    )
  }

  schema <- rlang::as_string(rlang::ensym(schema))
  table <- rlang::as_string(rlang::ensym(table))

  schema <- stringr::str_to_upper(schema)
  table <- stringr::str_to_upper(table)
  dplyr::tbl(
    get("con", envir = .GlobalEnv),
    DBI::Id(schema = schema, table = table)
  )
}
