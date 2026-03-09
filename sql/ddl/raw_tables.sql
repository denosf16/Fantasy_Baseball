USE fantasy_baseball;
GO

SET NOCOUNT ON;
GO

IF OBJECT_ID('raw.statcast_pitches', 'U') IS NOT NULL
    DROP TABLE raw.statcast_pitches;
GO

CREATE TABLE raw.statcast_pitches (
    raw_pitch_id BIGINT IDENTITY(1,1) NOT NULL,
    source_file NVARCHAR(260) NULL,
    source_start_date DATE NULL,
    source_end_date DATE NULL,
    source_load_datetime DATETIME2(7) NOT NULL
        CONSTRAINT DF_raw_statcast_pitches_source_load_datetime DEFAULT SYSUTCDATETIME(),

    pitch_event_id NVARCHAR(200) NULL,
    game_pk INT NULL,
    game_date DATE NULL,
    game_type NVARCHAR(10) NULL,
    game_year INT NULL,

    at_bat_number INT NULL,
    pitch_number INT NULL,
    inning INT NULL,
    inning_topbot NVARCHAR(10) NULL,
    outs_when_up INT NULL,
    balls INT NULL,
    strikes INT NULL,

    pitcher INT NULL,
    player_name NVARCHAR(200) NULL,
    batter INT NULL,
    stand NVARCHAR(5) NULL,
    p_throws NVARCHAR(5) NULL,

    pitch_type NVARCHAR(20) NULL,
    pitch_name NVARCHAR(100) NULL,
    description NVARCHAR(100) NULL,
    events NVARCHAR(100) NULL,
    bb_type NVARCHAR(50) NULL,

    release_speed FLOAT NULL,
    release_spin_rate FLOAT NULL,
    release_extension FLOAT NULL,
    spin_axis FLOAT NULL,

    release_pos_x FLOAT NULL,
    release_pos_y FLOAT NULL,
    release_pos_z FLOAT NULL,

    pfx_x FLOAT NULL,
    pfx_z FLOAT NULL,
    plate_x FLOAT NULL,
    plate_z FLOAT NULL,
    zone INT NULL,

    launch_speed FLOAT NULL,
    launch_angle FLOAT NULL,
    launch_speed_angle FLOAT NULL,
    hit_distance_sc FLOAT NULL,
    hc_x FLOAT NULL,
    hc_y FLOAT NULL,

    estimated_ba_using_speedangle FLOAT NULL,
    estimated_slg_using_speedangle FLOAT NULL,
    estimated_woba_using_speedangle FLOAT NULL,

    vx0 FLOAT NULL,
    vy0 FLOAT NULL,
    vz0 FLOAT NULL,
    ax FLOAT NULL,
    ay FLOAT NULL,
    az FLOAT NULL,

    sz_top FLOAT NULL,
    sz_bot FLOAT NULL,

    on_1b BIGINT NULL,
    on_2b BIGINT NULL,
    on_3b BIGINT NULL,

    fielder_2 BIGINT NULL,
    fielder_3 BIGINT NULL,
    fielder_4 BIGINT NULL,
    fielder_5 BIGINT NULL,
    fielder_6 BIGINT NULL,
    fielder_7 BIGINT NULL,
    fielder_8 BIGINT NULL,
    fielder_9 BIGINT NULL,

    home_team NVARCHAR(10) NULL,
    away_team NVARCHAR(10) NULL,
    home_score INT NULL,
    away_score INT NULL,
    bat_score INT NULL,
    fld_score INT NULL,

    delta_run_exp FLOAT NULL,
    delta_home_win_exp FLOAT NULL,
    woba_value FLOAT NULL,
    woba_denom FLOAT NULL,

    CONSTRAINT PK_raw_statcast_pitches PRIMARY KEY CLUSTERED (raw_pitch_id)
);
GO

CREATE NONCLUSTERED INDEX IX_raw_statcast_pitches_game_pk
    ON raw.statcast_pitches (game_pk);
GO

CREATE NONCLUSTERED INDEX IX_raw_statcast_pitches_game_date
    ON raw.statcast_pitches (game_date);
GO

CREATE NONCLUSTERED INDEX IX_raw_statcast_pitches_pitcher
    ON raw.statcast_pitches (pitcher);
GO

CREATE NONCLUSTERED INDEX IX_raw_statcast_pitches_batter
    ON raw.statcast_pitches (batter);
GO

CREATE NONCLUSTERED INDEX IX_raw_statcast_pitches_pitch_event_id
    ON raw.statcast_pitches (pitch_event_id);
GO

PRINT 'Table created: raw.statcast_pitches';
GO