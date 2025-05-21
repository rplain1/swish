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

#' Add Metadata Timestamps to a Data Frame
#'
#' Adds two metadata columns to a data frame: the current system time (`updated_at`) and a local-adjusted version (`local_updated_at`) offset by 6 hours.
#'
#' @param df A data frame or tibble. The input data to which metadata will be added.
#'
#' @return A tibble with two additional columns:
add_metadata <- function(df) {
  # add metadata to database load time
  df |>
    tibble::as_tibble() |>
    dplyr::mutate(
      updated_at = Sys.time(),
      local_updated_at = .data$updated_at - lubridate::hours(6)
    )
}

#' Create a Database Schema if It Doesn't Exist
#'
#' Executes a SQL command to create a schema in the connected database, if it does not already exist.
#'
#' @param con A DBI database connection object. Typically created with `DBI::dbConnect()`.
#' @param schema_name A character string specifying the name of the schema to create.
#'
#' @return The number of rows affected (usually `0` or a backend-specific result), invisibly.
create_schema <- function(con, schema_name) {
  DBI::dbExecute(
    con,
    glue::glue_sql("CREATE SCHEMA IF NOT EXISTS {`schema_name`}", .con = con)
  )
}

#' Check If Records Exist in a Database Table
#'
#' Queries a table within a specified schema to count how many records match a given parameter and set of values.
#'
#' @param con A DBI database connection object.
#' @param schema_name A character string specifying the schema containing the table.
#' @param table_name A character string specifying the table to query.
#' @param param_vals A vector of values to filter on (e.g., seasons). Defaults to `NULL`.
#' @param param A character string specifying the column name to filter by. Defaults to `'SEASON'`.
#'
#' @return A data frame with a single row and column containing the count of matching records.
check_records_exist <- function(
  con,
  schema_name,
  table_name,
  param_vals = NULL,
  param = 'SEASON'
) {
  # check that table exists
  DBI::dbGetQuery(
    con,
    glue::glue_sql(
      "SELECT COUNT(*) FROM {`schema_name`}.{`table_name`} WHERE {`param`} IN ({vals*})",
      vals = param_vals,
      .con = con
    )
  )
}

#' Delete Records from a Database Table Based on Filter Criteria
#'
#' Deletes rows from a specified table in a schema where the values in a given column match a set of filter values.
#'
#' @param con A DBI database connection object.
#' @param schema_name A character string specifying the schema containing the table.
#' @param table_name A character string specifying the table to delete from.
#' @param param A character string specifying the column name to filter by. Defaults to `'SEASON'`.
#' @param param_vals A vector of values to filter on for deletion. Defaults to `seasons` (must exist in the calling environment).
#'
#' @return The number of rows deleted, invisibly.
#'
#' @export
delete_existing_records <- function(
  con,
  schema_name,
  table_name,
  param_vals,
  param = 'SEASON'
) {
  # TODO: add where clause
  #sql <- "DELETE FROM {`schema_name`}.{`table_name`}"

  DBI::dbExecute(
    con = con,
    glue::glue_sql(
      "DELETE FROM {`schema_name`}.{`table_name`} WHERE {`param`} IN ({vals*})",
      vals = param_vals,
      .con = con
    )
  )
}


check_table_columns <- function(con, df, table_name, schema_name) {
  db_struc <- DBI::dbGetQuery(
    con,
    "describe {`schema_name`}.{`table_name`}"
  )
  df_struc <- sapply(df, class)

  list()
}

#' Load Data into a Database Table with Optional Overwrite or Append Logic
#'
#' Loads a data frame into a specified database schema and table, with support for conditional overwrite or deduplicated appending. The function automatically creates the schema if it does not exist, adds metadata timestamps to the data, and checks for existing records using a filtering parameter (e.g., `SEASON`).
#'
#' @param con A DBI database connection object.
#' @param df A data frame containing the data to be loaded.
#' @param table_name A character string specifying the target table name.
#' @param schema_name A character string specifying the target schema name. Defaults to `'BASE'`.
#' @param param A character string specifying the column used to check for and delete existing records. Defaults to `'SEASON'`.
#' @param param_vals A vector of values for the `param` column to be checked and optionally deleted from the destination table. If `NULL`, values are inferred from the data frame.
#' @param overwrite A logical indicating whether to overwrite the existing table. If `TRUE`, the table is fully replaced. If `FALSE`, only matching records are removed and new data appended.
#' @param ... Additional arguments passed to `DBI::dbWriteTable()`.
#'
#' @return Invisibly returns the result of `DBI::dbWriteTable()`.
#'
#' @details
#' - If `overwrite = TRUE`, the table will be dropped and recreated with the new data.
#' - If `overwrite = FALSE`, the function checks for existing records using `param` and `param_vals`, deletes matching records, and appends new ones.
#' - Automatically adds `updated_at` and `local_updated_at` timestamp metadata to the input data.
#'
#' @export
load_data <- function(
  con,
  df,
  table_name,
  schema_name = 'BASE',
  param = 'SEASON',
  param_vals = NULL,
  overwrite = FALSE,
  ...
) {
  checkmate::check_logical(overwrite)

  if (is.null(param_vals)) {
    param_vals <- unique(df[, stringr::str_to_lower(param)]) |> dplyr::pull()
  }

  .df <- add_metadata(df = df)
  create_schema(con = con, schema_name = schema_name)

  if (overwrite) {
    message(glue::glue(
      '\n{format(Sys.time(), "%H:%M:%S")} | `overwrite` set to {overwrite}, deleting all records...'
    ))
    message(glue::glue(
      '\n{format(Sys.time(), "%H:%M:%S")} | {nrow(.df)} records loaded into {schema_name}.{table_name}'
    ))
    DBI::dbWriteTable(
      con = con,
      name = DBI::Id(schema = schema_name, table = table_name),
      value = .df,
      overwrite = TRUE
    )
  } else {
    if (DBI::dbExistsTable(con, DBI::Id(schema_name, table_name))) {
      # controls for modifying table
      records_check <- check_records_exist(
        con = con,
        schema_name = schema_name,
        table_name = table_name,
        param_vals = param_vals,
        param = param
      ) # TODO: add param
      if (records_check[1, 1] > 0) {
        message(glue::glue(
          '\n{format(Sys.time(), "%H:%M:%S")} | records exist for `param`: {list(param_vals)}, dropping existing records records {records_check[1, 1]}'
        ))
        delete_existing_records(
          con = con,
          table_name = table_name,
          schema_name = schema_name,
          param = param,
          param_vals = param_vals
        )
      }
    }

    DBI::dbWriteTable(
      con = con,
      name = DBI::Id(schema = schema_name, table = table_name),
      value = .df,
      overwrite = FALSE,
      append = TRUE
    )
  }
}

#' Update WNBA Database Tables from `wehoop` Data
#'
#' Loads and updates WNBA schedule, player box score, and team box score data for the specified seasons
#' into a connected database using the `load_data()` utility.
#'
#' @param con A DBI database connection object.
#' @param seasons A vector of numeric or character values indicating the seasons to load.
#' @param overwrite A logical flag. If `TRUE`, existing records will be overwritten. If `FALSE`, only matching records will be deleted and new data appended.
#'
#' @details
#' This function performs the following steps:
#' - Loads WNBA schedule data (removing the `venue_capacity` column if present).
#' - Loads WNBA player box score data for the given seasons.
#' - Loads WNBA team box score data for seasons >= 2006 (due to data availability).
#'
#' All data is written to the `WNBA` schema in the database:
#' - `SCHEDULE` table for schedule data
#' - `PLAYER_BOX_STG` for player box score data
#' - `TEAM_BOX_STG` for team box score data
#'
#' @return Invisibly returns `NULL`. Primarily called for its side effects on the database.
#'
#' @export
update_wnba_db <- function(
  con,
  seasons,
  overwrite = FALSE
) {
  df <- wehoop::load_wnba_schedule(seasons = seasons) |>
    dplyr::select(-dplyr::any_of('venue_capacity')) |>
    dplyr::mutate(
      status_type_alt_detail = as.character(.data$status_type_alt_detail)
    )

  load_data(
    con,
    df,
    table_name = "SCHEDULE",
    schema_name = "WNBA",
    overwrite = overwrite
  )

  df <- wehoop::load_wnba_player_box(seasons = seasons)
  load_data(
    con,
    df,
    table_name = "PLAYER_BOX_STG",
    schema_name = "WNBA",
    overwrite = overwrite
  )

  df <- wehoop::load_wnba_team_box(seasons = seasons[seasons >= 2006])
  load_data(
    con,
    df,
    table_name = "TEAM_BOX_STG",
    schema_name = "WNBA",
    overwrite = overwrite
  )
}

#' Update WBB Database Tables from `wehoop` Data
#'
#' Loads and updates WBB schedule, player box score, and team box score data for the specified seasons
#' into a connected database using the `load_data()` utility.
#'
#' @param con A DBI database connection object.
#' @param seasons A vector of numeric or character values indicating the seasons to load.
#' @param overwrite A logical flag. If `TRUE`, existing records will be overwritten. If `FALSE`, only matching records will be deleted and new data appended.
#'
#' @details
#' This function performs the following steps:
#' - Loads WBB schedule data (removing the `venue_capacity` column if present).
#' - Loads WBB player box score data for the given seasons.
#' - Loads WBB team box score data
#'
#' All data is written to the `WBB` schema in the database:
#' - `SCHEDULE` table for schedule data
#' - `PLAYER_BOX_STG` for player box score data
#' - `TEAM_BOX_STG` for team box score data
#'
#' @return Invisibly returns `NULL`. Primarily called for its side effects on the database.
#'
#' @export
update_ncaa_db <- function(con, seasons, overwrite = FALSE) {
  if (min(seasons) < 2006) {
    seasons <- seasons[seasons >= 2006]
  }
  df <- wehoop::load_wbb_schedule(seasons = seasons)
  load_data(
    con,
    df,
    table_name = "SCHEDULE",
    schema_name = "WBB",
    overwrite = overwrite
  )

  df <- wehoop::load_wbb_player_box(seasons = seasons)
  load_data(
    con,
    df,
    table_name = "PLAYER_BOX_STG",
    schema_name = "WBB",
    overwrite = overwrite
  )

  df <- wehoop::load_wbb_team_box(seasons = seasons)
  load_data(
    con,
    df,
    table_name = "TEAM_BOX_STG",
    schema_name = "WBB",
    overwrite = overwrite
  )
}

#' @export
update_static_data <- function(
  con,
  seasons = 2000:wehoop::most_recent_wnba_season()
) {
  df <- purrr::map(
    seasons,
    .f = \(x)
      wehoop::wnba_drafthistory(season = x) |>
        purrr::pluck('DraftHistory')
  ) |>
    dplyr::bind_rows()
  load_data(
    con,
    df,
    table_name = 'DRAFT_HISTORY',
    schema_name = 'WNBA',
    param = "SEASON",
    param_vals = seasons,
    overwrite = TRUE
  )
}

#' Main WNBA Data Pipeline
#'
#' Executes the primary data ingestion and processing steps for WNBA data, including play-by-play,
#' schedule, and box score updates.
#'
#' @param con A DBI database connection object used for writing data.
#' @param force_rebuild Logical. If `TRUE`, forces a full rebuild of the play-by-play data. Defaults to `FALSE`.
#' @param seasons A vector of seasons to update. Defaults to the most recent season available via
#'   `wehoop::most_recent_wnba_season()`.
#'
#' @details
#' This function runs the following:
#' - `update_wnba_pbp()` to update or rebuild the play-by-play data.
#' - `update_wnba_db()` to load the schedule, player box, and team box data into the database.
#'
#' It serves as the top-level orchestration function for refreshing WNBA data for analytical use.
#'
#' @return Invisibly returns `NULL`. Used for its side effects on the database and play-by-play data store.
#'
#' @export
main_wnba <- function(
  con,
  force_rebuild = FALSE,
  seasons = wehoop::most_recent_wnba_season()
) {
  update_wnba_pbp(force_rebuild = force_rebuild)
  update_wnba_db(con, seasons)
}

#' Main WBB Data Pipeline
#'
#' Executes the primary data ingestion and processing steps for WBB data, including play-by-play,
#' schedule, and box score updates.
#'
#' @param con A DBI database connection object used for writing data.
#' @param force_rebuild Logical. If `TRUE`, forces a full rebuild of the play-by-play data. Defaults to `FALSE`.
#' @param seasons A vector of seasons to update. Defaults to the most recent season available via
#'   `wehoop::most_recent_wbb_season()`.
#'
#' @details
#' This function runs the following:
#' - `update_wbb_pbp()` to update or rebuild the play-by-play data.
#' - `update_wbb_db()` to load the schedule, player box, and team box data into the database.
#'
#' It serves as the top-level orchestration function for refreshing WNBA data for analytical use.
#'
#' @return Invisibly returns `NULL`. Used for its side effects on the database and play-by-play data store.
#'
#' @export
main_wbb <- function(
  con,
  force_rebuild = FALSE,
  seasons = wehoop::most_recent_wbb_season()
) {
  update_ncaa_pbp(force_rebuild = force_rebuild)
  update_ncaa_db(con, seasons)
}
