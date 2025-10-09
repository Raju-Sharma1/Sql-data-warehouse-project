-- First Moving to Master Database to Create a Database in the system

Use master;
Go

-- Drop and Recreate the 'Datawarehouse' database if already exits in the system

If Exists (Select 1 From sys.databases Where name = 'Datawarehouse')
Begin
	Alter Database Datawarehouse Set Single_user with Rollback immediate;
	Drop Database Datawarehouse;
End;
Go

-- Creating Database 'Datawarehouse'

Create Database Datawarehouse;
Go

-- Using Created Database 'Datawarehouse'

Use Datawarehouse;
Go

-- Creating Schemas

Create Schema bronze;
Go

Create Schema silver;
Go

Create Schema gold;
Go





