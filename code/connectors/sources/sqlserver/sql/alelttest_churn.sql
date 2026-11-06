-- ALELT SQL Server churn (alelttest_cdc). Expected outcome per block in the comments.
-- Uses ORDER_ID 1..99 range offset by @base so it can be run repeatedly (the harness passes @base).
-- GO-separated batches; the harness replaces {B} by the base id.

-- 1) committed multi-table insert, full type inventory, LOB values
BEGIN TRANSACTION;
INSERT INTO dbo.ALELT_ORDERS (ORDER_ID, CUSTOMER, STATUS, AMOUNT, QTY, TINY, BIG, RATE, RATE32, PRICE, ACTIVE, ORDER_DATE,
  PICKUP_TIME, CREATED, LEGACY_DT, EVENT_TS, GUID, CODE, RAW, NOTES, DOC, XDOC)
VALUES ({B}+1, N'Acmé Ltd ✓', 'N', 1234.50, -7, 255, 9007199254740993, 1.5, 0.25, 12.3456, 1, '2026-11-04',
  '07:30:15.123', '2026-11-04 08:00:00.123456', '2026-11-04 08:00:00.997', '2026-11-04 08:00:00.123 -05:00',
  '6F9619FF-8B86-D011-B42D-00C04FC964FF', 'ABC', 0x00FF10, REPLICATE(CONVERT(nvarchar(max), N'long text '), 1000),
  0xDEADBEEF, N'<a b="1">x</a>');
INSERT INTO dbo.ALELT_ORDER_LINES (ORDER_ID, LINE_NO, ITEM, QTY) VALUES ({B}+1, 1, 'TIRE', 4), ({B}+1, 2, 'OIL', 1);
INSERT INTO dbo.ALELT_ORDERS (ORDER_ID, STATUS) VALUES ({B}+2, 'N');                          -- NULLs everywhere
COMMIT;
GO
-- 2) rolled back transaction: must never land
BEGIN TRANSACTION;
INSERT INTO dbo.ALELT_ORDERS (ORDER_ID, STATUS, CUSTOMER) VALUES ({B}+3, 'R', N'rolled back');
UPDATE dbo.ALELT_ORDERS SET CUSTOMER = N'rolled back' WHERE ORDER_ID = {B}+1;
ROLLBACK;
GO
-- 3) savepoint: partial rollback; only line 3 lands
BEGIN TRANSACTION;
INSERT INTO dbo.ALELT_ORDER_LINES (ORDER_ID, LINE_NO, ITEM, QTY) VALUES ({B}+1, 3, 'WIPER', 2);
SAVE TRANSACTION sp1;
INSERT INTO dbo.ALELT_ORDER_LINES (ORDER_ID, LINE_NO, ITEM, QTY) VALUES ({B}+1, 4, 'SAVEPOINT_UNDONE', 9);
ROLLBACK TRANSACTION sp1;
COMMIT;
GO
-- 4) update of a non-LOB column (LOB columns unchanged -> not carried by CDC)
UPDATE dbo.ALELT_ORDERS SET STATUS = 'P', AMOUNT = 99.99 WHERE ORDER_ID = {B}+1;
GO
-- 5) LOB update
UPDATE dbo.ALELT_ORDERS SET NOTES = N'short now', DOC = NULL WHERE ORDER_ID = {B}+1;
GO
-- 6) primary key update
UPDATE dbo.ALELT_ORDER_LINES SET LINE_NO = 20 WHERE ORDER_ID = {B}+1 AND LINE_NO = 2;
GO
-- 7) delete (soft delete at the destination)
DELETE FROM dbo.ALELT_ORDERS WHERE ORDER_ID = {B}+2;
GO
-- 8) non-captured table: ignored
INSERT INTO dbo.ALELT_NOT_CAPTURED (ID, V) VALUES ({B}+1, 'x');
GO
