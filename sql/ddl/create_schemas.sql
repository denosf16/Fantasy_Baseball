USE fantasy_baseball;
GO

SET NOCOUNT ON;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'raw'
)
BEGIN
    EXEC ('CREATE SCHEMA raw');
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'clean'
)
BEGIN
    EXEC ('CREATE SCHEMA clean');
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'core'
)
BEGIN
    EXEC ('CREATE SCHEMA core');
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'mart'
)
BEGIN
    EXEC ('CREATE SCHEMA mart');
END;
GO

PRINT 'Schemas verified: raw, clean, core, mart';
GO