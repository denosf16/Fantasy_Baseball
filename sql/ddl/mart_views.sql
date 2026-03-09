USE fantasy_baseball;
GO

SET NOCOUNT ON;
GO

/* =========================================
   PITCHER GAME CHART INPUT
========================================= */
CREATE OR ALTER VIEW mart.pitcher_game_chart_input AS
SELECT
    p.pitch_event_id,
    p.game_pk,
    p.game_date,
    p.season,
    p.at_bat_number,
    p.pitch_number,
    p.inning,
    p.inning_half,

    p.pitcher_id,
    p.pitcher_name,
    p.batter_id,
    p.batter_name,

    p.pitcher_hand,
    p.batter_stand,
    p.balls,
    p.strikes,
    p.outs_when_up,

    p.pitch_type,
    p.pitch_name,
    p.pitch_group,

    p.release_speed,
    p.release_spin_rate,
    p.release_extension,
    p.spin_axis,

    p.release_pos_x,
    p.release_pos_y,
    p.release_pos_z,

    p.pfx_x,
    p.pfx_z,
    p.plate_x,
    p.plate_z,
    p.zone,

    p.vx0,
    p.vy0,
    p.vz0,
    p.ax,
    p.ay,
    p.az,

    p.sz_top,
    p.sz_bot,

    p.description,
    p.events,
    p.bb_type,

    p.launch_speed,
    p.launch_angle,
    p.launch_speed_angle,
    p.hit_distance_sc,
    p.hc_x,
    p.hc_y,

    p.estimated_ba_using_speedangle,
    p.estimated_slg_using_speedangle,
    p.estimated_woba_using_speedangle,

    p.is_swing,
    p.is_whiff,
    p.is_called_strike,
    p.is_ball,
    p.is_in_play,
    p.is_foul
FROM clean.pitches p;
GO


/* =========================================
   PITCHER SEASON CHART INPUT
========================================= */
CREATE OR ALTER VIEW mart.pitcher_season_chart_input AS
SELECT
    p.pitch_event_id,
    p.game_pk,
    p.game_date,
    p.season,
    p.at_bat_number,
    p.pitch_number,
    p.inning,
    p.inning_half,

    p.pitcher_id,
    p.pitcher_name,
    p.batter_id,
    p.batter_name,

    p.pitcher_hand,
    p.batter_stand,
    p.balls,
    p.strikes,
    p.outs_when_up,

    p.pitch_type,
    p.pitch_name,
    p.pitch_group,

    p.release_speed,
    p.release_spin_rate,
    p.release_extension,
    p.spin_axis,

    p.release_pos_x,
    p.release_pos_y,
    p.release_pos_z,

    p.pfx_x,
    p.pfx_z,
    p.plate_x,
    p.plate_z,
    p.zone,

    p.vx0,
    p.vy0,
    p.vz0,
    p.ax,
    p.ay,
    p.az,

    p.sz_top,
    p.sz_bot,

    p.description,
    p.events,
    p.bb_type,

    p.launch_speed,
    p.launch_angle,
    p.launch_speed_angle,
    p.hit_distance_sc,
    p.hc_x,
    p.hc_y,

    p.estimated_ba_using_speedangle,
    p.estimated_slg_using_speedangle,
    p.estimated_woba_using_speedangle,

    p.is_swing,
    p.is_whiff,
    p.is_called_strike,
    p.is_ball,
    p.is_in_play,
    p.is_foul
FROM clean.pitches p;
GO


/* =========================================
   HITTER GAME CHART INPUT
========================================= */
CREATE OR ALTER VIEW mart.hitter_game_chart_input AS
SELECT
    p.pitch_event_id,
    p.game_pk,
    p.game_date,
    p.season,
    p.at_bat_number,
    p.pitch_number,
    p.inning,
    p.inning_half,

    p.pitcher_id,
    p.pitcher_name,
    p.batter_id,
    p.batter_name,

    p.pitcher_hand,
    p.batter_stand,
    p.balls,
    p.strikes,
    p.outs_when_up,

    p.pitch_type,
    p.pitch_name,
    p.pitch_group,

    p.release_speed,
    p.release_spin_rate,
    p.release_extension,
    p.spin_axis,

    p.release_pos_x,
    p.release_pos_y,
    p.release_pos_z,

    p.pfx_x,
    p.pfx_z,
    p.plate_x,
    p.plate_z,
    p.zone,

    p.vx0,
    p.vy0,
    p.vz0,
    p.ax,
    p.ay,
    p.az,

    p.sz_top,
    p.sz_bot,

    p.description,
    p.events,
    p.bb_type,

    p.launch_speed,
    p.launch_angle,
    p.launch_speed_angle,
    p.hit_distance_sc,
    p.hc_x,
    p.hc_y,

    p.estimated_ba_using_speedangle,
    p.estimated_slg_using_speedangle,
    p.estimated_woba_using_speedangle,

    p.is_swing,
    p.is_whiff,
    p.is_called_strike,
    p.is_ball,
    p.is_in_play,
    p.is_foul
FROM clean.pitches p;
GO


/* =========================================
   HITTER SEASON CHART INPUT
========================================= */
CREATE OR ALTER VIEW mart.hitter_season_chart_input AS
SELECT
    p.pitch_event_id,
    p.game_pk,
    p.game_date,
    p.season,
    p.at_bat_number,
    p.pitch_number,
    p.inning,
    p.inning_half,

    p.pitcher_id,
    p.pitcher_name,
    p.batter_id,
    p.batter_name,

    p.pitcher_hand,
    p.batter_stand,
    p.balls,
    p.strikes,
    p.outs_when_up,

    p.pitch_type,
    p.pitch_name,
    p.pitch_group,

    p.release_speed,
    p.release_spin_rate,
    p.release_extension,
    p.spin_axis,

    p.release_pos_x,
    p.release_pos_y,
    p.release_pos_z,

    p.pfx_x,
    p.pfx_z,
    p.plate_x,
    p.plate_z,
    p.zone,

    p.vx0,
    p.vy0,
    p.vz0,
    p.ax,
    p.ay,
    p.az,

    p.sz_top,
    p.sz_bot,

    p.description,
    p.events,
    p.bb_type,

    p.launch_speed,
    p.launch_angle,
    p.launch_speed_angle,
    p.hit_distance_sc,
    p.hc_x,
    p.hc_y,

    p.estimated_ba_using_speedangle,
    p.estimated_slg_using_speedangle,
    p.estimated_woba_using_speedangle,

    p.is_swing,
    p.is_whiff,
    p.is_called_strike,
    p.is_ball,
    p.is_in_play,
    p.is_foul
FROM clean.pitches p;
GO

PRINT 'Views created: mart.pitcher_game_chart_input, mart.pitcher_season_chart_input, mart.hitter_game_chart_input, mart.hitter_season_chart_input';
GO