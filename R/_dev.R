common_players <- wehoop::wnba_commonallplayers() |>
  purrr::pluck('CommonAllPlayers')

wnba_player_ids <- common_players |>
  janitor::clean_names() |>
  dplyr::mutate(
    join_name = gsub(
      "[[:space:]]",
      "_",
      stringr::str_to_lower(display_first_last)
    )
  ) |>
  dplyr::distinct(person_id, display_first_last, join_name)


espn_player_ids <- tblx(wnba, player_box_stg) |>
  dplyr::distinct(athlete_id, athlete_display_name, athlete_short_name) |>
  dplyr::collect() |>
  dplyr::mutate(
    join_name = gsub(
      "[[:space:]]+",
      "_",
      stringr::str_to_lower(athlete_display_name)
    )
  )


joined_ids <- espn_player_ids |>
  dplyr::inner_join(
    wnba_player_ids,
    by = c('join_name'),
    relationship = 'many-to-many'
  )

joined_ids

load_data(
  con,
  joined_ids,
  "PLAYER_IDS",
  "WNBA",
  param = 'athlete_id',
  overwrite = TRUE
)


draft_data <- tblx(wnba, player_box_stg) |> # 1 row
  dplyr::filter(season_type == '2', team_id < '90') |> # quick fix for teams
  dplyr::left_join(
    tblx(wnba, player_ids) |>
      dplyr::select(-athlete_display_name),
    by = c('athlete_id')
  ) |> # 1 row joined
  dplyr::left_join(
    tblx(wnba, draft_history) |>
      dplyr::filter(
        !is.na(TEAM_ID) # 2 rows with duplicate values
      ),
    by = c('person_id' = 'PERSON_ID')
  ) |>
  dplyr::filter(!is.na(OVERALL_PICK)) |>
  dplyr::group_by(athlete_display_name, athlete_id, season) |>
  dplyr::summarise(
    n = dplyr::n(),
    games = dplyr::n_distinct(ifelse(minutes > 0, game_id, NA)),
    minutes = sum(minutes),
    overall_pick = max(OVERALL_PICK),
    points = sum(points),
    .groups = 'drop'
  ) |>
  dplyr::collect() |>
  dplyr::group_by(athlete_id) |>
  dplyr::arrange(season, .by_group = TRUE) |>
  dplyr::mutate(
    experience = row_number(),
    ppg = points / games
  ) |>
  dplyr::ungroup()

draft_data |> filter(overall_pick == '1', experience == 1)
