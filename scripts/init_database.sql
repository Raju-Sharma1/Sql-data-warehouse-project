/*
Creating Database and Schemas
===============================

Script Purpose:
	This script creates a new Database called 'Datawarehouse' after checking If a database named Datawarehouse exists, kick everyone out of it,
	roll back their transactions, and then delete that database completely. Subsequently, setting up 3 Schemas in the database: bronze, silver and gold.

WARNING:
	 Running this script will permanently DROP the database 'Datawarehouse'
	 if it exists. ALL data inside the database will be LOST.
	
	 Proceed with EXTREME caution.
	
	 ✅ Make sure you have a recent BACKUP before executing this script.
*/


-- First Moving to Master Database to Create a Database in the system

Use master;
Go

-- Drop and Recreate the 'Datawarehouse' database if already exits in the system

/*--If a database named Datawarehouse exists, kick everyone out of it,
roll back their transactions, and then delete that database completely.*/

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
