-- ALELT SQL Server CDC test schema (database alelttest_cdc only; dev / test).
-- Run as a db_owner of alelttest_cdc. The CDC enable calls are what a DBA runs on a real source
-- (see the prerequisite fix-it SQL); they are part of the TEST setup only.
-- GO-separated batches.
IF OBJECT_ID(N'dbo.ALELT_ORDERS') IS NULL
CREATE TABLE dbo.ALELT_ORDERS (
  ORDER_ID    int            NOT NULL PRIMARY KEY,
  CUSTOMER    nvarchar(40)   NULL,
  STATUS      char(1)        NOT NULL DEFAULT 'N',
  AMOUNT      decimal(12,2)  NULL,
  QTY         smallint       NULL,
  TINY        tinyint        NULL,
  BIG         bigint         NULL,
  RATE        float          NULL,
  RATE32      real           NULL,
  PRICE       money          NULL,
  ACTIVE      bit            NULL,
  ORDER_DATE  date           NULL,
  PICKUP_TIME time(3)        NULL,
  CREATED     datetime2(6)   NULL,
  LEGACY_DT   datetime       NULL,
  EVENT_TS    datetimeoffset(3) NULL,
  GUID        uniqueidentifier NULL,
  CODE        varchar(10)    NULL,
  RAW         varbinary(16)  NULL,
  NOTES       nvarchar(max)  NULL,
  DOC         varbinary(max) NULL,
  XDOC        xml            NULL
);
GO
IF OBJECT_ID(N'dbo.ALELT_ORDER_LINES') IS NULL
CREATE TABLE dbo.ALELT_ORDER_LINES (
  ORDER_ID int NOT NULL,
  LINE_NO  int NOT NULL,
  ITEM     varchar(30) NOT NULL,
  QTY      int NULL,
  CONSTRAINT PK_ALELT_ORDER_LINES PRIMARY KEY (ORDER_ID, LINE_NO)
);
GO
IF OBJECT_ID(N'dbo.ALELT_NOT_CAPTURED') IS NULL
CREATE TABLE dbo.ALELT_NOT_CAPTURED (ID int NOT NULL PRIMARY KEY, V varchar(10) NULL);
GO
IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0
  EXEC sys.sp_cdc_enable_db;
GO
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = N'dbo_ALELT_ORDERS')
  EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'ALELT_ORDERS', @role_name = NULL,
       @supports_net_changes = 0;
GO
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = N'dbo_ALELT_ORDER_LINES')
  EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'ALELT_ORDER_LINES', @role_name = NULL,
       @supports_net_changes = 0;
GO
