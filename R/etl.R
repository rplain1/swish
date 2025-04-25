#' @importFrom rlang .data
NULL

#' Update the wehoop database
#'
#' This function is a wrapper that updates the `wehoop` database by
#' calling `wehoop::update_wnba_db()`.
#'
#' @param dbdir Character. The directory where the database is stored. Defaults to `~/.db`.
#' @param dbname Character. DB name, defaults to my dog `belle`
#' @param force_rebuild Logical. If `TRUE`, forces a rebuild of the database. Defaults to `FALSE`.
#' @param dbpath Path for custom db
#'
#' @return No return value. Updates the nflfastR database in place.
#' @export
#'
#' @examples
#' \dontrun{
#' update_wnba_db()  # Updates the database in the default location
#' }
update_wnba_pbp <- function(
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

#' Update the wehoop database
#'
#' This function is a wrapper that updates the `wehoop` database by
#' calling `wehoop::update_wbb_db()`.
#'
#' @param dbdir Character. The directory where the database is stored. Defaults to `~/.db`.
#' @param dbname Character. DB name, defaults to my dog `belle`
#' @param force_rebuild Logical. If `TRUE`, forces a rebuild of the database. Defaults to `FALSE`.
#' @param dbpath Path for custom db
#'
#' @return No return value. Updates the nflfastR database in place.
#' @export
#'
#' @examples
#' \dontrun{
#' update_db()  # Updates the database in the default location
#' }
update_ncaa_pbp <- function(
  dbdir = '~/.db',
  dbname = 'belle',
  force_rebuild = FALSE,
  dbpath = Sys.getenv("DB_PATH_WNBA")
) {
  db_connection <- DBI::dbConnect(duckdb::duckdb(), dbpath)
  on.exit(DBI::dbDisconnect(db_connection), add = TRUE)
  wehoop::update_wbb_db(
    dbdir = dbdir,
    dbname = dbname,
    force_rebuild = force_rebuild,
    db_connection = db_connection
  )
}


#' Update the specified database table
#'
#' this function will load data into the db. It can handle creating new schemas
#' and tables, and will overwrite data if there is a specified season or set of seasons.
#'
#' @param df A data frame to load into the db
#' @param table_name name of table to use in db
#' @param schema_name name of schema to use, defaults to `"BASE"``
#' @param db_path location of db
#' @param seasons seasons to update when using existing database.
#'
#' @return No return value. Updates the specified db
#' @export
load_data <- function(
  df,
  table_name,
  schema_name = "BASE",
  db_path = Sys.getenv("DB_PATH_WNBA"),
  seasons = NULL
) {
  # initialize db connection
  con <- DBI::dbConnect(duckdb::duckdb(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # add metadata to database load time
  df <- df |>
    tibble::as_tibble() |>
    dplyr::mutate(
      updated_at = Sys.time(),
      local_updated_at = .data$updated_at - lubridate::hours(6)
    )

  # Create schema if it doesn't exist
  DBI::dbExecute(
    con,
    glue::glue_sql("CREATE SCHEMA IF NOT EXISTS {`schema_name`}", .con = con)
  )

  if (!is.numeric(seasons)) {
    # overwrite all the existing data
    DBI::dbWriteTable(
      con = con,
      name = DBI::Id(schema = schema_name, table = table_name),
      value = df,
      overwrite = TRUE
    )
  } else {
    # backfill or modify select values
    message(glue::glue(
      '{format(Sys.time(), "%H:%M:%S")} | `seasons` provided set to {seasons}, updating {nrow(df)} records'
    ))

    if (DBI::dbExistsTable(con, DBI::Id(schema_name, table_name))) {
      # check that table exists
      record_check <- DBI::dbGetQuery(
        con,
        glue::glue_sql(
          "SELECT COUNT(*) FROM {`schema_name`}.{`table_name`} WHERE SEASON IN ({vals*})",
          vals = seasons,
          .con = con
        )
      )
      if (record_check[1, 1] > 0) {
        # if data exists

        # TODO: look into why this outputs messages for multiple years funky
        # `seasons` provided set to 2017, updating 192340 records14:00:55 | `seasons` provided set to 2018, updating 192340 records14:00:55 | `seasons` provided set to 2019, updating 192340 records14:00:55 | `seasons` provided set to 2020, updating 192340 records
        message(glue::glue(
          '{format(Sys.time(), "%H:%M:%S")} | records exist for `seasons`: {seasons}, dropping existing records records'
        ))

        DBI::dbExecute(
          con,
          glue::glue_sql(
            "DELETE FROM {`schema_name`}.{`table_name`} WHERE SEASON IN ({vals*})",
            vals = seasons,
            .con = con
          )
        )
      }
    }

    DBI::dbWriteTable(
      con = con,
      name = DBI::Id(schema = schema_name, table = table_name),
      value = df,
      overwrite = FALSE,
      append = TRUE
    )
  }

  message(glue::glue(
    '{format(Sys.time(), "%H:%M:%S")} | {nrow(df)} records loaded into {schema_name}.{table_name}'
  ))
}

#' main function to update tables that are related to `{wehoop} WNBA data`.
#'
#' This function will update tables based on the ENV variable `DB_PATH` that is used in
#' `load_data()`.
#'
#' @param seasons numeric vector to indentify seasons to back filter for and backfill.
#' Note: this should typically only be used with `wehoop::most_recent_season()` or maintain
#' the default value of `NULL`, as datasets have different years that started collecting. It is
#' easier to do a full refresh than handle all the different conditions.
#'
#' @export
update_wnba_db <- function(seasons = NULL) {
  wehoop::load_wnba_schedule(seasons = seasons) |>
    load_data(table_name = "SCHEDULE", schema_name = "WNBA")

  wehoop::load_wnba_player_box(seasons = seasons) |>
    load_data(table_name = "PLAYER_BOX_STG", schema_name = "WNBA")

  wehoop::load_wnba_team_box(seasons = seasons[seasons >= 2006]) |>
    load_data(table_name = "TEAM_BOX_STG", schema_name = "WNBA")
}

#' main function to update tables that are related to `{wehoop}` WBB data.
#'
#' This function will update tables based on the ENV variable `DB_PATH` that is used in
#' `load_data()`.
#'
#' @param seasons numeric vector to indentify seasons to back filter for and backfill.
#' Note: this should typically only be used with `wehoop::most_recent_season()` or maintain
#' the default value of `NULL`, as datasets have different years that started collecting. It is
#' easier to do a full refresh than handle all the different conditions.
#'
#' @export
update_ncaa_db <- function(seasons = NULL) {
  if (min(seasons) < 2006) {
    seasons <- seasons[seasons >= 2006]
  }
  wehoop::load_wbb_schedule(seasons = seasons) |>
    load_data(table_name = "SCHEDULE", schema_name = "WBB")

  wehoop::load_wbb_player_box(seasons = seasons) |>
    load_data(table_name = "PLAYER_BOX_STG", schema_name = "WBB")

  wehoop::load_wbb_team_box(seasons = seasons) |>
    load_data(table_name = "TEAM_BOX_STG", schema_name = "WBB")
}

#' @export
update_static_data <- function() {
  purrr::map(
    2000:2025,
    .f = \(x)
      wehoop::wnba_drafthistory(season = x) |>
        purrr::pluck('DraftHistory')
  ) |>
    dplyr::bind_rows() |>
    load_data(table_name = 'DRAFT_HISTORY', schema_name = 'WNBA')
}

main <- function(
  force_rebuild = FALSE,
  seasons = wehoop::most_recent_wnba_season()
) {
  update_wnba_pbp(force_rebuild = force_rebuild)
  update_ncaa_pbp(force_rebuild = force_rebuild)
  update_wnba_db(seasons)
  update_ncaa_db(seasons)
}
