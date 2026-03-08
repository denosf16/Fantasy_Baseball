USE fantasy_baseball;
GO

IF OBJECT_ID('clean.teams', 'U') IS NOT NULL
    DROP TABLE clean.teams;
GO

CREATE TABLE clean.teams (
    team_id              INT           NOT NULL PRIMARY KEY,
    team_name            NVARCHAR(100) NULL,
    team_code            NVARCHAR(20)  NULL,
    abbreviation         NVARCHAR(10)  NULL,
    league_name          NVARCHAR(50)  NULL,
    division_name        NVARCHAR(50)  NULL,
    active_flag          BIT           NULL,
    source_system        NVARCHAR(50)  NULL,
    source_load_datetime DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('clean.players', 'U') IS NOT NULL
    DROP TABLE clean.players;
GO

CREATE TABLE clean.players (
    player_id            INT            NOT NULL PRIMARY KEY,
    full_name            NVARCHAR(150)  NULL,
    first_name           NVARCHAR(100)  NULL,
    last_name            NVARCHAR(100)  NULL,
    primary_position     NVARCHAR(20)   NULL,
    bat_side             NVARCHAR(10)   NULL,
    pitch_hand           NVARCHAR(10)   NULL,
    current_team_id      INT            NULL,
    active_flag          BIT            NULL,
    source_system        NVARCHAR(50)   NULL,
    source_load_datetime DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('clean.games', 'U') IS NOT NULL
    DROP TABLE clean.games;
GO

CREATE TABLE clean.games (
    game_pk              INT            NOT NULL PRIMARY KEY,
    game_date            DATE           NULL,
    season               INT            NULL,
    game_type            NVARCHAR(10)   NULL,
    status_detailed      NVARCHAR(100)  NULL,
    home_team_id         INT            NULL,
    away_team_id         INT            NULL,
    home_score           INT            NULL,
    away_score           INT            NULL,
    venue_name           NVARCHAR(150)  NULL,
    source_system        NVARCHAR(50)   NULL,
    source_load_datetime DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
GO