import pandas as pd


def get_pitcher_game(engine, pitcher_id: int, game_pk: int) -> pd.DataFrame:
    query = f"""
        SELECT *
        FROM mart.pitcher_game_chart_input
        WHERE pitcher_id = {pitcher_id}
          AND game_pk = {game_pk}
    """
    return pd.read_sql(query, engine)


def get_pitcher_season(engine, pitcher_id: int, season: int) -> pd.DataFrame:
    query = f"""
        SELECT
            p.*
        FROM clean.pitches p
        WHERE p.pitcher_id = {pitcher_id}
          AND p.season = {season}
    """
    return pd.read_sql(query, engine)


def get_hitter_game(engine, batter_id: int, game_pk: int) -> pd.DataFrame:
    query = f"""
        SELECT
            p.*
        FROM clean.pitches p
        WHERE p.batter_id = {batter_id}
          AND p.game_pk = {game_pk}
    """
    return pd.read_sql(query, engine)


def get_hitter_season(engine, batter_id: int, season: int) -> pd.DataFrame:
    query = f"""
        SELECT
            p.*
        FROM clean.pitches p
        WHERE p.batter_id = {batter_id}
          AND p.season = {season}
    """
    return pd.read_sql(query, engine)


def get_game_pitchers(engine, game_pk: int) -> pd.DataFrame:
    query = f"""
        SELECT DISTINCT
            pitcher_id,
            pitcher_name
        FROM clean.pitches
        WHERE game_pk = {game_pk}
          AND pitcher_id IS NOT NULL
        ORDER BY pitcher_name, pitcher_id
    """
    return pd.read_sql(query, engine)


def get_game_hitters(engine, game_pk: int) -> pd.DataFrame:
    query = f"""
        SELECT DISTINCT
            batter_id,
            batter_name
        FROM clean.pitches
        WHERE game_pk = {game_pk}
          AND batter_id IS NOT NULL
        ORDER BY batter_name, batter_id
    """
    return pd.read_sql(query, engine)


def get_pitcher_games_in_season(engine, pitcher_id: int, season: int) -> pd.DataFrame:
    query = f"""
        SELECT DISTINCT
            game_pk,
            game_date
        FROM clean.pitches
        WHERE pitcher_id = {pitcher_id}
          AND season = {season}
        ORDER BY game_date, game_pk
    """
    return pd.read_sql(query, engine)


def get_hitter_games_in_season(engine, batter_id: int, season: int) -> pd.DataFrame:
    query = f"""
        SELECT DISTINCT
            game_pk,
            game_date
        FROM clean.pitches
        WHERE batter_id = {batter_id}
          AND season = {season}
        ORDER BY game_date, game_pk
    """
    return pd.read_sql(query, engine)