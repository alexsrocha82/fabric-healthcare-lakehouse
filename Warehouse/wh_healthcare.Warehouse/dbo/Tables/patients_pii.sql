CREATE TABLE [dbo].[patients_pii] (

	[patient_hash_id] varchar(8000) NULL, 
	[patient_id] varchar(8000) NULL, 
	[full_name] varchar(8000) NULL, 
	[ssn] varchar(8000) NULL, 
	[email] varchar(8000) NULL, 
	[phone] varchar(8000) NULL, 
	[address_street] varchar(8000) NULL, 
	[_silver_loaded_at] datetime2(6) NULL
);