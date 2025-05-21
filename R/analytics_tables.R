#' @export
create_wnba_teams <- function() {
  load_data(
    con,
    wehoop::wnba_teams(),
    "TEAMS",
    "WNBA",
    param = 'team_id',
    overwrite = TRUE
  )
}

#' @export
create_player_box <- function() {
  df_player_box <- tblx(wnba, player_box_stg) |>
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
    con,
    df_player_box,
    "PLAYER_BOX",
    "WNBA",
    overwrite = TRUE
  )
}

#' @export
create_team_box <- function() {
  df_team_box <- tblx(wnba, team_box_stg) |>
    dplyr::mutate(
      season_type = ifelse(
        season_type == 2,
        'REG',
        ifelse(season_type == 3, 'POST', NA)
      ),
      two_point_field_goals_attempted = field_goals_attempted -
        three_point_field_goals_attempted,
      two_point_field_goals_made = field_goals_made -
        three_point_field_goals_made,
      is_home = as.numeric(team_home_away == 'home')
    ) |>
    dplyr::collect()
  load_data(con, df_team_box, "TEAM_BOX", "WNBA", overwrite = TRUE)
}

#' @export
create_team_possessions <- function() {
  team_box_subset <- tblx(wnba, team_box) |>
    dplyr::select(
      game_id,
      season,
      team_id,
      opponent_team_id,
      fg_att = field_goals_attempted,
      fg_made = field_goals_made,
      ft_att = free_throws_attempted,
      dreb = defensive_rebounds,
      oreb = offensive_rebounds,
      team_turnovers
    )

  df_pos <- team_box_subset |>
    dplyr::left_join(
      team_box_subset,
      by = c('opponent_team_id' = 'team_id', 'season', 'game_id'),
      suffix = c('', '_opp')
    ) |>
    dplyr::mutate(
      possessions = round(
        0.5 *
          ((fg_att +
            0.4 * ft_att -
            (1.07 *
              (oreb / (oreb + dreb_opp)) *
              (fg_att - fg_made) +
              team_turnovers)) +
            (fg_att_opp +
              0.4 * ft_att_opp -
              1.07 *
                (oreb_opp / (oreb_opp + dreb)) *
                (fg_att_opp - fg_made_opp) +
              team_turnovers_opp))
      )
    ) |>
    dplyr::select(season, game_id, team_id, possessions) |>
    dplyr::collect()

  load_data(con, df_pos, "TEAM_POSSESSIONS", "WNBA", overwrite = TRUE)
}

#' @export
main_wnba_views <- function() {
  create_team_box()
  create_team_possessions()
  create_player_box()
}
