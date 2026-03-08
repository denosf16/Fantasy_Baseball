USE fantasy_baseball;
GO

IF OBJECT_ID('clean.pitches', 'U') IS NOT NULL
    DROP TABLE clean.pitches;
GO

CREATE TABLE clean.pitches (
    pitch_event_id         NVARCHAR(100) NOT NULL PRIMARY KEY,
    game_pk                INT           NOT NULL,
    game_date              DATE          NULL,
    season                 INT           NULL,

    at_bat_number          INT           NULL,
    pitch_number           INT           NULL,
    inning                 INT           NULL,
    inning_half            NVARCHAR(10)  NULL,

    pitcher_id             INT           NULL,
    pitcher_name           NVARCHAR(150) NULL,
    batter_id              INT           NULL,
    batter_name            NVARCHAR(150) NULL,
    pitcher_hand           NVARCHAR(10)  NULL,
    batter_stand           NVARCHAR(10)  NULL,

    balls                  INT           NULL,
    strikes                INT           NULL,
    outs_when_up           INT           NULL,

    pitch_type             NVARCHAR(20)  NULL,
    pitch_name             NVARCHAR(50)  NULL,
    pitch_group            NVARCHAR(20)  NULL,

    release_speed          FLOAT         NULL,
    release_spin_rate      FLOAT         NULL,
    release_pos_x          FLOAT         NULL,
    release_pos_z          FLOAT         NULL,
    pfx_x                  FLOAT         NULL,
    pfx_z                  FLOAT         NULL,
    plate_x                FLOAT         NULL,
    plate_z                FLOAT         NULL,
    zone                   INT           NULL,

    description            NVARCHAR(100) NULL,
    events                 NVARCHAR(100) NULL,
    bb_type                NVARCHAR(50)  NULL,

    launch_speed           FLOAT         NULL,
    launch_angle           FLOAT         NULL,
    hit_distance_sc        FLOAT         NULL,

    is_swing               BIT           NULL,
    is_whiff               BIT           NULL,
    is_called_strike       BIT           NULL,
    is_ball                BIT           NULL,
    is_in_play             BIT           NULL,
    is_foul                BIT           NULL,

    source_system          NVARCHAR(50)  NULL,
    source_load_datetime   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE INDEX IX_pitches_game_pk
    ON clean.pitches (game_pk);
GO

CREATE INDEX IX_pitches_pitcher_id_game_pk
    ON clean.pitches (pitcher_id, game_pk);
GO

CREATE INDEX IX_pitches_batter_id_game_pk
    ON clean.pitches (batter_id, game_pk);
GO