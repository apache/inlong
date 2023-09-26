if exists(select 1 from sys.databases where name='master' and is_cdc_enabled=0)
begin
exec sys.sp_cdc_enable_db
end