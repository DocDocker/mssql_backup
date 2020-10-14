USE [msdb];
GO


DROP  PROCEDURE IF EXISTS [dbo].[BackupDatabase];
/****** Object:  StoredProcedure [dbo].[BackupDatabase]    Script Date: 22.09.2020 10:30:45 ******/

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/*
@dbName - Имя базы
@dir - путь к файлу бэкапов
@backupType:
	'auto' - Автоматический режим (на первым в месяце делается полный бэкап, а потом разностный)
	'logs' - Бэкап журнала транзакций
	'full' - Полный бэкап (принудительно)
	'diff' - Разностный бэкап (принудительно)
@retainDays - количество дней хранения бэкапов, 0 - бессрочно

@weekIsBasePeriod - делать полные бэкапы каждую неделю, иначе ежемесячно
@newFileForDiff - складывать дифференциальные бэкапы в отдельный файл (по одному в день), если = 0, то складывается в один файл
@print - демо режим, сам бэкап не делается, но выводится команда SQL, которая должна выполниться в обычном режиме
*/



CREATE PROCEDURE [dbo].[BackupDatabase]
(@dbName              VARCHAR(256), 
 @dir                 VARCHAR(1000), 
 @backupType          VARCHAR(4)   ='auto', 
 @retainDays          INT      =31, 
 @weekIsBasePeriod TINYINT    =0, -- Делать полные бэкапы понедельно
 @newFileForDiff   TINYINT    =1, -- всегда новый файл для diff
 @print          TINYINT      =0
)
AS
	BEGIN
		DECLARE @backupTime DATETIME; --текущая дата
		DECLARE @backupDate DATE;
		DECLARE @backupBaseDate DATE; --Дата полного бэкапа месяц/неделя

		DECLARE @path VARCHAR(1000);				-- Польный путь для бэкапа
		DECLARE @file_name NVARCHAR(200);				-- Папка сохранения бэкапа
		DECLARE @comment NVARCHAR(100);				-- Комментарий к бэкапу
		DECLARE @folder NVARCHAR(100);				-- Папка сохранения бэкапа
		DECLARE @retainDaysSuffix NVARCHAR(100);	-- Время хранения бэкапа в днях
		DECLARE @cmd NVARCHAR(1000);				-- Команда бэкапа
		DECLARE @error NVARCHAR(1000);				-- Сообщение об ошибке
		DECLARE @compression VARCHAR(20);			-- Использовать сжатие
		DECLARE @copy_only VARCHAR(20);			-- Режим copy_only
		DECLARE @differential VARCHAR(20);			-- Использовать сжатие
		DECLARE @backup_target VARCHAR(20);			-- Что бэкапим базу или журнал

		SET @backupType=LOWER(@backupType);
		SET @copy_only=IIF(@backupType = 'copy', 'COPY_ONLY, ', '');

		SET @backupTime=GETDATE();					--получаем текущую дату
		SET @backupDate=CAST(@backupTime AS DATE);
		SET @backupBaseDate=IIF(@weekIsBasePeriod = 0, DATEADD(MONTH, DATEDIFF(MONTH, 0, @backupDate), 0), DATEADD(WEEK, DATEDIFF(WEEK, 0, @backupDate), 0));

		SET @compression=IIF(@@version LIKE '%Express Edition%', 'NO_COMPRESSION', 'COMPRESSION');
		SET @retainDaysSuffix=IIF(@retainDays > 0, N', RETAINDAYS = ' + CAST(@retainDays AS NVARCHAR), N'');-- определим параметр времени хранения бэкапа

		SELECT 
			   @folder=REPLACE(CONCAT(@dir, '\', [ttt].[database_name], '\'), '\\', '\')
			 , @comment=CASE
							WHEN @backupType = 'auto'
								 AND [ttt].[cnt_base_full_backup] = 0
								 OR @backupType = 'full'
								THEN CONCAT([ttt].[database_name], ' - Полный бэкап от ', @backupTime)
							WHEN @backupType = 'auto'
								 AND [ttt].[cnt_base_full_backup] != 0
								 AND [ttt].[cnt_day_diff_backup] = 0
								 OR @backupType = 'diff'
								THEN CONCAT([ttt].[database_name], ' - Разностный бэкап от ', @backupTime)
							WHEN @backupType = 'logs'
								THEN CONCAT([ttt].[database_name], ' - Бэкап журнала транзакций от ', @backupTime)
							WHEN @backupType = 'copy'
								THEN CONCAT([ttt].[database_name], ' - Полный бэкап режим copy_only от ', @backupTime)
							ELSE ''
						END
			 , @differential=CASE
								 WHEN @backupType = 'auto'
									  AND [ttt].[cnt_base_full_backup] != 0
									  AND [ttt].[cnt_day_diff_backup] = 0
									  OR @backupType = 'diff'
									 THEN ' DIFFERENTIAL, '
								 ELSE ''
							 END
			 , @backup_target=CASE
								  WHEN @backupType = 'logs'
									  THEN ' LOG '
								  ELSE ' DATABASE '
							  END
			 , @file_name=CASE
							  WHEN @backupType = 'auto'
								   AND [ttt].[cnt_base_full_backup] = 0
								   OR @backupType = 'full'
								  THEN CONCAT(REPLACE(REVERSE(LEFT(REVERSE([ttt].[full_name]), CHARINDEX('\', REVERSE([ttt].[full_name])) - 1)), '.bak', ''), '.bak')
							  WHEN @backupType = 'auto'
								   AND [ttt].[cnt_base_full_backup] != 0
								   AND [ttt].[cnt_day_diff_backup] = 0
								   OR @backupType = 'diff'
								  THEN CONCAT(REPLACE(REVERSE(LEFT(REVERSE([ttt].[diff_name]), CHARINDEX('\', REVERSE([ttt].[diff_name])) - 1)), '.bak', ''), '.bak')
							  WHEN @backupType = 'logs'
								   AND [ttt].[recovery_model] = 1
								  THEN CONCAT(REPLACE(REVERSE(LEFT(REVERSE([ttt].[logs_name]), CHARINDEX('\', REVERSE([ttt].[logs_name])) - 1)), '.bak', ''), '.trn')
							  WHEN @backupType = 'copy'
								  THEN CONCAT(REPLACE(REVERSE(LEFT(REVERSE([ttt].[copy_name]), CHARINDEX('\', REVERSE([ttt].[copy_name])) - 1)), '.bak', ''), '.bak')
							  ELSE ''
						  END
			 , @error=CASE
						  WHEN [ttt].[recovery_model] != 1
							   AND @backupType = 'logs'
							  THEN CONCAT('Error 1: База ', [ttt].[database_name], ' имеет модель восстановления ', [ttt].[recovery_model_desc], N'. Бэкап журнала  транзакций возможен только в режиме FULL')
						  WHEN @backupType = 'auto'
							   AND [ttt].[cnt_base_full_backup] != 0
							   AND [ttt].[cnt_day_diff_backup] != 0
							  THEN CONCAT('Разностный бэкап уже создавался в ', CONVERT(NVARCHAR(100), [ttt].[max_diff_backup_finish_date], 121))
						  ELSE '+++'
					  END
		FROM
			 (
			  SELECT 
					 [tt].*
				   , CONCAT('\', [tt].[database_name], '_', LEFT(REPLACE(CONVERT(CHAR(10), @backupDate, 120), '-', '_'), 10), '_', [cnt_day_copy_backup], '_copy') AS [copy_name]
				   , CONCAT('\', [tt].[database_name], '_', LEFT(REPLACE(CONVERT(CHAR(10), @backupDate, 120), '-', '_'), 10), '_', [cnt_day_full_backup], '_full.bak') AS [full_name]
				   , CONCAT(REPLACE(ISNULL([bmf_full].[physical_device_name], CONCAT('\', [tt].[database_name], '_', LEFT(REPLACE(CONVERT(CHAR(10), @backupDate, 120), '-', '_'), 10), '_', [cnt_day_full_backup], '_full')), '_full', ''), IIF(@newFileForDiff = 0, '', CONCAT(RIGHT(LEFT(REPLACE(CONVERT(CHAR(10), @backupDate, 120), '-', '_'), 10), 3), '_', [tt].[cnt_day_diff_backup])), '_diff') AS [diff_name]
				   , REPLACE(ISNULL([bmf_full].[physical_device_name], CONCAT('\', [tt].[database_name], '_', LEFT(REPLACE(CONVERT(CHAR(10), @backupDate, 120), '-', '_'), 10), '_', [cnt_day_full_backup], '_full')), '_full', '_logs') AS [logs_name]
			  FROM
				   (
					SELECT 
						   [t].[database_id]
						 , [t].[database_name]
						 , [t].[recovery_model]
						 , [t].[recovery_model_desc]
						 , [t].[full_backup_set_id]
						 , ISNULL(MAX([bs_diff].[backup_set_id]), 0) AS [diff_backup_set_id]
						 , MAX([bs_diff].[backup_finish_date]) AS [max_diff_backup_finish_date]
						 , [t].[cnt_day_copy_backup]
						 , [t].[cnt_base_full_backup]
						 , [t].[cnt_day_full_backup]
						 , COUNT(DISTINCT IIF(CAST([bs_diff].[backup_start_date] AS DATE) = @backupDate, [bs_diff].[backup_set_id], NULL)) AS [cnt_day_diff_backup]
					FROM
						 (
						  SELECT 
								 [db].[database_id]
							   , [db].[name] AS 'database_name'
							   , [db].[recovery_model]
							   , [db].[recovery_model_desc]
							   , ISNULL(MAX([bs_full].[backup_set_id]), 0) AS [full_backup_set_id]
							   , COUNT(DISTINCT [bs_copy].[backup_set_id]) AS [cnt_day_copy_backup]
							   , COUNT(DISTINCT IIF(CAST([bs_full].[backup_start_date] AS DATE) = @backupDate, [bs_full].[backup_set_id], NULL)) AS [cnt_day_full_backup]
							   , COUNT(DISTINCT IIF(IIF(@weekIsBasePeriod = 0, DATEADD(MONTH, DATEDIFF(MONTH, 0, CAST([bs_full].[backup_start_date] AS DATE)), 0), DATEADD(WEEK, DATEDIFF(WEEK, 0, CAST([bs_full].[backup_start_date] AS DATE)), 0)) = @backupBaseDate, [bs_full].[backup_set_id], NULL)) AS [cnt_base_full_backup]
						  FROM 
							   [sys].[databases] AS [db]
							   -- full backups
							   LEFT JOIN [msdb].[dbo].[backupset] AS [bs_full]
								   ON [bs_full].[database_name] = [db].[name]
									  AND [bs_full].[type] = 'D' -- FULL BACKUP 
									  AND [bs_full].[is_copy_only] = 0
									  AND [bs_full].[backup_start_date] >= @backupBaseDate
							   LEFT JOIN [msdb].[dbo].[backupset] AS [bs_copy]
								   ON [bs_copy].[database_name] = [db].[name]
									  AND [bs_copy].[type] = 'D' -- FULL BACKUP 
									  AND [bs_copy].[is_copy_only] = 1
									  AND CAST([bs_copy].[backup_start_date] AS DATE) = @backupDate
						  WHERE 1 = 1
								AND [db].[name] = @dbName
						  GROUP BY 
								   [db].[database_id]
								 , [db].[name]
								 , [db].[recovery_model]
								 , [db].[recovery_model_desc]
						 ) AS [t]
						 -- full backups
						 LEFT JOIN [msdb].[dbo].[backupset] AS [bs_full]
							 ON [bs_full].[backup_set_id] = [t].[full_backup_set_id]

						 -- diff backups
						 LEFT JOIN [msdb].[dbo].[backupset] AS [bs_diff]
							 ON [bs_diff].[differential_base_guid] = [bs_full].[backup_set_uuid]
								AND [bs_diff].[type] = 'I' --  DIFF BACKUP 
								AND [bs_diff].[is_copy_only] = 0
						 -- diff backups
						 LEFT JOIN [msdb].[dbo].[backupset] AS [bs_logs]
							 ON [bs_logs].[differential_base_guid] = [bs_full].[backup_set_uuid]
								AND [bs_logs].[type] = 'L' --  BACKUP LOG
								AND [bs_logs].[is_copy_only] = 0
					GROUP BY 
							 [t].[database_id]
						   , [t].[database_name]
						   , [t].[recovery_model]
						   , [t].[recovery_model_desc]
						   , [t].[full_backup_set_id]
						   , [t].[cnt_day_copy_backup]
						   , [t].[cnt_base_full_backup]
						   , [t].[cnt_day_full_backup]
				   ) AS [tt]
				   LEFT JOIN [msdb].[dbo].[backupset] AS [bs_full]
					   ON [bs_full].[backup_set_id] = [tt].[full_backup_set_id]
				   LEFT JOIN [msdb].[dbo].[backupmediafamily] AS [bmf_full]
					   ON [bs_full].[media_set_id] = [bmf_full].[media_set_id]
				   LEFT JOIN [msdb].[dbo].[backupset] AS [bs_diff]
					   ON [bs_diff].[backup_set_id] = [tt].[diff_backup_set_id]
				   LEFT JOIN [msdb].[dbo].[backupmediafamily] AS [bmf_diff]
					   ON [bs_diff].[media_set_id] = [bmf_diff].[media_set_id]
			 ) AS [ttt];
		IF @file_name != ''
			BEGIN

				-- Создадим папку, если такой еще нет
				EXECUTE [master].[dbo].[xp_create_subdir] 
						@folder;
				SET @path=CONCAT(@folder, @file_name);
				SET @cmd=N'BACKUP ' + @backup_target + ' [' + @dbName + '] TO  DISK = N''' + @path + ''' WITH ' + @copy_only + +@differential + 'NOFORMAT, NOINIT ' + @retainDaysSuffix + ', NAME = N''' + @comment + ''' , SKIP, NOREWIND, NOUNLOAD, ' + @compression + ',  STATS = 10, CHECKSUM';
			END;
			ELSE
			BEGIN
				SELECT 
					   @error as error;
				RETURN;
			END;
		BEGIN TRY
			IF @print = 0
				BEGIN
					EXEC [sp_executesql] 
						 @cmd;
				END;
				ELSE
				BEGIN
					SELECT 
						   @cmd AS [Будет выполена команда:];
				END;
		END TRY
		BEGIN CATCH
			BEGIN
				SELECT 
					   @cmd AS [cmd]
					 , ERROR_NUMBER() AS [ErrorNumber]
					 , ERROR_MESSAGE() AS [ErrorMessage];
			END;
		END CATCH;

	END;
GO