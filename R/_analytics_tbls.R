load_data(wehoop::wnba_teams(), "TEAMS", "WNBA")

df_player_box <- tblx(wnba, player_box_stg) |>
  #count(team_name, team_id, team_color) |>
  dplyr::collect() |>
  dplyr::select(!min:pf, -pts) |> # old stale coluns
  dplyr::mutate(
    plus_minus = as.numeric(plus_minus),
    season_type = ifelse(
      season_type == 2,
      'REG',
      ifelse(season_type == 3, 'POST', NA)
    )
  ) |>
  dplyr::filter(
    !stringr::str_starts(team_name, 'Team '),
    !team_name %in% c('East', 'West')
  )

load_data(
  df_player_box |> filter(season == 2024),
  "PLAYER_BOX",
  "WNBA",
  seasons = 2024
)


df_team_box <- tblx(wnba, team_box_stg) |>
  dplyr::mutate(
    season_type = ifelse(
      season_type == 2,
      'REG',
      ifelse(season_type == 3, 'POST', NA),
      two_point_field_goals_attempted = field_goals_attempted -
        three_point_field_goals_attempted,
      two_point_field_goals_made = field_goals_made -
        three_point_field_goals_made,
      is_home = as.numeric(team_home_away == 'home')
    )
  ) |>
  dplyr::collect()

load_data(df_team_box, "TEAM_BOX", "WNBA")

df_possessions <- tblx(main, wehoop_wnba_pbp) |>
  dplyr::group_by(
    season,
    game_id
  ) |>
  dplyr::mutate(pos_change = ifelse(team_id != lag(team_id), 1, 0)) |>
  dplyr::filter(!is.na(team_id)) |>
  dplyr::group_by(season, team_id, game_id) |>
  dplyr::summarise(
    possessions = sum(pos_change, na.rm = TRUE),
    .groups = 'drop'
  ) |>
  collect()

load_data(df_possessions, "TEAM_POSSESSIONS", "WNBA")

df_possessions

teams_tbl <- tblx(wnba, teams) |>
  distinct(team_id, espn_team_id)

df_possessions_box <- tblx(wnba, team_box_stg) |>
  group_by(season, game_id, team_id) |>
  summarise(
    poss = round(
      sum(field_goals_attempted) +
        sum(0.44 * free_throws_attempted) +
        sum(turnovers)
    ),
    .groups = 'drop'
  ) |>
  collect()

df_possessions |>
  inner_join(df_possessions_box) |>
  filter(season > 2010) |>
  mutate(delta = possessions - poss) |>
  ggplot(aes(delta)) +
  geom_histogram()
ggplot(aes(possessions, poss)) +
  geom_point()

# WNBA API
df_rotation <- wehoop::wnba_gamerotation(game_id = "1022200034")
df_adv_box <- wehoop::wnba_boxscoreadvancedv3(game_id = "1022400148")


# try and create my own adv stats

tblx(wnba, team_box) |>
  filter(season == 2024) |>
  left_join(
    teams_tbl |> rename(wnba_team_id = team_id),
    by = c('team_id' = 'espn_team_id')
  ) |>
  left_join(
    teams_tbl |> rename(wnba_team_id = team_id),
    by = c('opponent_team_id' = 'espn_team_id'),
    suffix = c('', '_opponent')
  ) |>
  collect() |>
  #group_by(season, game_id, team_id) |>
  mutate(
    poss = round(
      field_goals_attempted +
        (0.44 * free_throws_attempted) +
        turnovers
    ),
    to_perc = turnovers / poss,
    off_rating = 100 * (team_score / poss),
    def_rating = 100 * (opponent_team_score / poss),
    net_rating = off_rating - def_rating
  ) |>
  filter(
    wnba_team_id == 1611661325,
    wnba_team_id_opponent == 1611661321,
    game_date == '2024-07-17'
  ) |>
  select(game_id, game_date, team_name, poss, ends_with('rating'))


df_espn_schedule <- wehoop::load_wnba_schedule(2006:2024)
df_wnba_schedule <- wehoop::wnba_schedule(season = 2024)

s1 <- df_espn_schedule |>
  select(
    id,
    uid,
    game_date,
    espn_game_id = game_id,
    home_id,
    away_id,
    away_uid,
    home_team_name = home_name,
    away_team_name = away_name
  )

s2 <- df_wnba_schedule |>
  select(game_date, wnba_game_id = game_id, home_team_name, away_team_name)


df_adv_stats <- s1 |>
  inner_join(s2) |>
  mutate(
    adv_stats = map(
      .x = wnba_game_id,
      .f = ~ wehoop::wnba_boxscoreadvancedv3(game_id = .x)
    )
  )


get_adv_stats <- function(game_id) {
  wehoop::wnba_boxscoreadvancedv3()
}

df_adv_stats |>
  mutate(
    home_team_totals_advanced = map(
      .x = adv_stats,
      .f = ~ .x |> pluck('home_team_totals_advanced')
    ),
    away_team_totals_advanced = map(
      .x = adv_stats,
      .f = ~ .x |> pluck('away_team_totals_advanced')
    ),
    home_team_player_advanced = map(
      .x = adv_stats,
      .f = ~ .x |> pluck('home_team_player_advanced')
    ),
    away_team_player_advanced = map(
      .x = adv_stats,
      .f = ~ .x |> pluck('away_team_player_advanced')
    )
  )

# team unnest
df_adv_stats |>
  mutate(
    team_totals_advanced = map(
      .x = adv_stats,
      .f = ~ .x |> pluck('home_team_totals_advanced')
    )
  ) |>
  select(-adv_stats) |>
  unnest(team_totals_advanced) |>
  bind_rows(
    df_adv_stats |>
      mutate(
        team_totals_advanced = map(
          .x = adv_stats,
          .f = ~ .x |> pluck('away_team_totals_advanced')
        )
      ) |>
      select(-adv_stats) |>
      unnest(team_totals_advanced)
  ) |>
  group_by(team_id, team_name) |>
  summarise(
    across(
      c(offensive_rating, defensive_rating, net_rating, pace, possessions),
      mean
    )
  ) |>
  arrange(-net_rating)


# player unnest
df_adv_stats |>
  mutate(
    team_totals_advanced = map(
      .x = adv_stats,
      .f = ~ .x |> pluck('home_team_player_advanced')
    )
  ) |>
  select(-adv_stats) |>
  unnest(team_totals_advanced) |>
  bind_rows(
    df_adv_stats |>
      mutate(
        team_totals_advanced = map(
          .x = adv_stats,
          .f = ~ .x |> pluck('away_team_player_advanced')
        )
      ) |>
      select(-adv_stats) |>
      unnest(team_totals_advanced)
  ) |>
  group_by(person_id, first_name, family_name, team_tricode) |>
  summarise(
    across(
      c(
        estimated_offensive_rating,
        estimated_defensive_rating,
        estimated_net_rating,
        possessions
      ),
      mean
    ),
    .groups = 'drop'
  ) |>
  arrange(-estimated_offensive_rating) |>
  filter(possessions > 40) |>
  print(n = 22)
