USE fantasy_baseball;
GO

SET NOCOUNT ON;
GO

IF OBJECT_ID('clean.pitches', 'U') IS NOT NULL
    DROP TABLE clean.pitches;
GO

CREATE TABLE clean.pitches (
    pitch_event_id NVARCHAR(200) NOT NULL,

    game_pk INT NOT NULL,
    game_date DATE NOT NULL,
    season INT NOT NULL,
    game_type NVARCHAR(10) NULL,

    at_bat_number INT NULL,
    pitch_number INT NULL,
    inning INT NULL,
    inning_half NVARCHAR(10) NULL,

    pitcher_id BIGINT NULL,
    pitcher_name NVARCHAR(200) NULL,
    batter_id BIGINT NULL,
    batter_name NVARCHAR(200) NULL,

    pitcher_hand NVARCHAR(5) NULL,
    batter_stand NVARCHAR(5) NULL,

    balls INT NULL,
    strikes INT NULL,
    outs_when_up INT NULL,

    pitch_type NVARCHAR(20) NULL,
    pitch_name NVARCHAR(100) NULL,
    pitch_group NVARCHAR(50) NULL,

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

    vx0 FLOAT NULL,
    vy0 FLOAT NULL,
    vz0 FLOAT NULL,
    ax FLOAT NULL,
    ay FLOAT NULL,
    az FLOAT NULL,

    sz_top FLOAT NULL,
    sz_bot FLOAT NULL,

    description NVARCHAR(100) NULL,
    events NVARCHAR(100) NULL,
    bb_type NVARCHAR(50) NULL,

    launch_speed FLOAT NULL,
    launch_angle FLOAT NULL,
    launch_speed_angle FLOAT NULL,
    hit_distance_sc FLOAT NULL,
    hc_x FLOAT NULL,
    hc_y FLOAT NULL,

    estimated_ba_using_speedangle FLOAT NULL,
    estimated_slg_using_speedangle FLOAT NULL,
    estimated_woba_using_speedangle FLOAT NULL,

    on_1b BIGINT NULL,
    on_2b BIGINT NULL,
    on_3b BIGINT NULL,

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

    is_swing BIT NOT NULL
        CONSTRAINT DF_clean_pitches_is_swing DEFAULT 0,
    is_whiff BIT NOT NULL
        CONSTRAINT DF_clean_pitches_is_whiff DEFAULT 0,
    is_called_strike BIT NOT NULL
        CONSTRAINT DF_clean_pitches_is_called_strike DEFAULT 0,
    is_ball BIT NOT NULL
        CONSTRAINT DF_clean_pitches_is_ball DEFAULT 0,
    is_in_play BIT NOT NULL
        CONSTRAINT DF_clean_pitches_is_in_play DEFAULT 0,
    is_foul BIT NOT NULL
        CONSTRAINT DF_clean_pitches_is_foul DEFAULT 0,

    source_system NVARCHAR(50) NULL,
    source_file NVARCHAR(260) NULL,
    source_load_datetime DATETIME2(7) NOT NULL
        CONSTRAINT DF_clean_pitches_source_load_datetime DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_clean_pitches
        PRIMARY KEY CLUSTERED (pitch_event_id)
);
GO

CREATE NONCLUSTERED INDEX IX_clean_pitches_game_pk
    ON clean.pitches (game_pk);
GO

CREATE NONCLUSTERED INDEX IX_clean_pitches_game_date
    ON clean.pitches (game_date);
GO

CREATE NONCLUSTERED INDEX IX_clean_pitches_season
    ON clean.pitches (season);
GO

CREATE NONCLUSTERED INDEX IX_clean_pitches_pitcher_season
    ON clean.pitches (pitcher_id, season);
GO

CREATE NONCLUSTERED INDEX IX_clean_pitches_batter_season
    ON clean.pitches (batter_id, season);
GO

CREATE NONCLUSTERED INDEX IX_clean_pitches_pitcher_game
    ON clean.pitches (pitcher_id, game_pk);
GO

CREATE NONCLUSTERED INDEX IX_clean_pitches_batter_game
    ON clean.pitches (batter_id, game_pk);
GO

CREATE NONCLUSTERED INDEX IX_clean_pitches_pitch_type
    ON clean.pitches (pitch_type);
GO

CREATE NONCLUSTERED INDEX IX_clean_pitches_events
    ON clean.pitches (events);
GO

CREATE NONCLUSTERED INDEX IX_clean_pitches_description
    ON clean.pitches (description);
GO

PRINT 'Table created: clean.pitches';
GO