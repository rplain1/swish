#' Update the wehoop database
#'
#' This function is a wrapper that updates the `wehoop` database by
#' calling `wehoop::update_db()`.
#'
#' @param dbdir Character. The directory where the database is stored. Defaults to `~/.db`.
#' @param dbname Character. DB name, defaults to my dog `belle`
#' @param force_rebuild Logical. If `TRUE`, forces a rebuild of the database. Defaults to `FALSE`.
#' @param tblname name or DBI::Id(schema, table) of the table location
#' @param db_connection A `DBI::dbConnect()` object
#'
#' @return No return value. Updates the nflfastR database in place.
#' @export
#'
#' @examples
#' \dontrun{
#' update_w()  # Updates the database in the default location
#' update_nflfastR_db(dbdir = "data/nfl", force_rebuild = TRUE)  # Forces a rebuild
#' }
update_db <- function(
  dbdir = '~/.db',
  dbname = 'belle',
  force_rebuild = FALSE,
  tblname = DBI::Id(schema = "BASE", table = 'wehoop_wnba_pbp'),
  db_connection = DBI::dbConnect(duckdb::duckdb(), Sys.getenv("DB_PATH"))
) {
  wehoop::update_wnba_db(
    dbdir = dbdir,
    dbname = dbname,
    tblname = tblname,
    force_rebuild = force_rebuild,
    db_connection = db_connection
  )
}
