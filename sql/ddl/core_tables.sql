USE fantasy_baseball;
GO

SET NOCOUNT ON;
GO

/* ===========================
   TEAMS
=========================== */

IF OBJECT_ID('core.teams', 'U') IS NOT NULL
    DROP TABLE core.teams;
GO

CREATE TABLE core.teams (

    team_id NVARCHAR(10) NOT NULL,
    team_name NVARCHAR(100) NULL,

    CONSTRAINT PK_core_teams
        PRIMARY KEY CLUSTERED (team_id)

);
GO


/* ===========================
   PLAYERS
=========================== */

IF OBJECT_ID('core.players', 'U') IS NOT NULL
    DROP TABLE core.players;
GO

CREATE TABLE core.players (

    player_id BIGINT NOT NULL,
    player_name NVARCHAR(200) NULL,

    bats NVARCHAR(5) NULL,
    throws NVARCHAR(5) NULL,

    CONSTRAINT PK_core_players
        PRIMARY KEY CLUSTERED (player_id)

);
GO


/* ===========================
   GAMES
=========================== */

IF OBJECT_ID('core.games', 'U') IS NOT NULL
    DROP TABLE core.games;
GO

CREATE TABLE core.games (

    game_pk INT NOT NULL,

    game_date DATE NULL,
    game_type NVARCHAR(10) NULL,
    season INT NULL,

    home_team NVARCHAR(10) NULL,
    away_team NVARCHAR(10) NULL,

    home_score INT NULL,
    away_score INT NULL,

    CONSTRAINT PK_core_games
        PRIMARY KEY CLUSTERED (game_pk)

);
GO


/* ===========================
   INDEXES
=========================== */

CREATE NONCLUSTERED INDEX IX_core_games_date
    ON core.games (game_date);

CREATE NONCLUSTERED INDEX IX_core_players_name
    ON core.players (player_name);

GO

PRINT 'Tables created: core.teams, core.players, core.games';
GO