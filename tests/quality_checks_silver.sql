/*
=====================================================================================================================================
Quality Checks
=====================================================================================================================================
Script Purpose:
The script performs various quality checks for data consistency,accuracy,
and standardization across the 'silver' schema.It includes checks for;
-Null or duplicate primary keys.
-Unwanted spaces in string files.
-Data standardization and consistency.
-Invid date ranges and orders.
-Data consistency between related fields.

Usage Notes;
    - Run these checks after data loading silver layer,
    - Investigate and resolve any discrepancies found during the checks.
====================================================================================================================================
*/

--=================================================================================================================
--Checking 'silver.crm_cust_info'
--=================================================================================================================
--Checks for Nulls or Duplicates in primary keys
--Expectation: No Results
SELECT
