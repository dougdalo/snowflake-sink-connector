USE test;
GO

insert into dbo.events (type) values (CONVERT(varchar(255), NEWID()) );
GO