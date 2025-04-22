#' Update the wehoop database
#'
#' This function is a wrapper that updates the `wehoop` database by
#' calling `wehoop::update_db()`.
#'
#' @param dbdir Character. The directory where the database is stored. Defaults to `~/.db`.
#' @param dbname Character. DB name, defaults to my dog `belle`
#' @param force_rebuild Logical. If `TRUE`, forces a rebuild of the database. Defaults to `FALSE`.
#' @param tblname name or DBI::Id(schema, table) of the table location
#' @param dpath Path for custom db
#'
#' @return No return value. Updates the nflfastR database in place.
#' @export
#'
#' @examples
#' \dontrun{
#' update_db()  # Updates the database in the default location
#' }
update_db <- function(
  dbdir = '~/.db',
  dbname = 'belle',
  force_rebuild = FALSE,
  dbpath = Sys.getenv("DB_PATH_WNBA")
) {
  db_connection <- DBI::dbConnect(duckdb::duckdb(), dbpath)
  on.exit(DBI::dbDisconnect(db_connection), add = TRUE)
  wehoop::update_wnba_db(
    dbdir = dbdir,
    dbname = dbname,
    force_rebuild = force_rebuild,
    db_connection = db_connection
  )
}
