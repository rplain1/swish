common_players <- wehoop::wnba_commonallplayers() |>
  pluck('CommonAllPlayers')

wnba_player_ids <- common_players |>
  janitor::clean_names() |>
  mutate(
    join_name = gsub("[[:space:]]", "_", str_to_lower(display_first_last))
  ) |>
  distinct(person_id, display_first_last, join_name)


espn_player_ids <- tblx(wnba, player_box_stg) |>
  distinct(athlete_id, athlete_display_name, athlete_short_name) |>
  collect() |>
  mutate(
    join_name = gsub("[[:space:]]+", "_", str_to_lower(athlete_display_name))
  )


joined_ids <- espn_player_ids |>
  inner_join(
    wnba_player_ids,
    by = c('join_name'),
    relationship = 'many-to-many'
  )

joined_ids

load_data(joined_ids, "player_ids", "wnba")


draft_data <- tblx(wnba, player_box) |> # 1 row
  filter(season_type == '2', team_id < '90') |> # quick fix for teams
  left_join(
    tblx(wnba, player_ids) |>
      select(-athlete_display_name),
    by = c('athlete_id')
  ) |> # 1 row joined
  left_join(
    tblx(wnba, draft_history) |>
      filter(
        !is.na(TEAM_ID) # 2 rows with duplicate values
      ),
    by = c('person_id' = 'PERSON_ID')
  ) |>
  filter(!is.na(OVERALL_PICK)) |>
  group_by(athlete_display_name, athlete_id, season) |>
  summarise(
    n = n(),
    games = n_distinct(ifelse(minutes > 0, game_id, NA)),
    minutes = sum(minutes),
    overall_pick = max(OVERALL_PICK),
    points = sum(points),
    .groups = 'drop'
  ) |>
  collect() |>
  group_by(athlete_id) |>
  arrange(season, .by_group = TRUE) |>
  mutate(
    experience = row_number(),
    ppg = points / games
  ) |>
  ungroup()

draft_data |> filter(overall_pick == '1', experience == 1)


con <- duckdb::dbConnect(duckdb::duckdb(), Sys.getenv("DB_PATH_WNBA"))

wehoop::load_wnba_player_box(
  c(2002:2004, 2006:wehoop::most_recent_wnba_season()),
  dbConnection = con,
  tablename = 'wnba_player_box'
)

duckdb::dbDisconnect(con)
