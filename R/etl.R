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


add_metadata <- function(df) {
  # add metadata to database load time
  df |>
    tibble::as_tibble() |>
    dplyr::mutate(
      updated_at = Sys.time(),
      local_updated_at = .data$updated_at - lubridate::hours(6)
    )
}

create_schema <- function(con, schema_name) {
  DBI::dbExecute(
    con,
    glue::glue_sql("CREATE SCHEMA IF NOT EXISTS {`schema_name`}", .con = con)
  )
}

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
      "SELECT COUNT(*) FROM {`schema_name`}.{`table_name`} WHERE {`param`} IN ({param_vals*})",
      vals = seasons,
      .con = con
    )
  )
}

delete_existing_records <- function(
  con,
  schema_name,
  table_name,
  param = 'SEASON',
  param_vals = seasons
) {
  # TODO: add where clause
  #sql <- "DELETE FROM {`schema_name`}.{`table_name`}"

  DBI::dbExecute(
    con = con,
    glue::glue_sql(
      "DELETE FROM {`schema_name`}.{`table_name`} WHERE {`param`} IN ({param_vals*})",
      vals = seasons,
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

main_wnba <- function(
  con,
  force_rebuild = FALSE,
  seasons = wehoop::most_recent_wnba_season()
) {
  update_wnba_pbp(force_rebuild = force_rebuild)
  update_wnba_db(con, seasons)
}

main_wbb <- function(
  con,
  force_rebuild = FALSE,
  seasons = wehoop::most_recent_wbb_season()
) {
  update_ncaa_pbp(force_rebuild = force_rebuild)
  update_ncaa_db(con, seasons)
}
