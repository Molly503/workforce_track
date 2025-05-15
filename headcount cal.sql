SELECT * FROM employee_db.employees;

SELECT COUNT(*) AS current_employees_count
FROM employee_db.employees
WHERE (hire_date <= CURDATE())
  AND (termination_date IS NULL OR termination_date > CURDATE());
  
  SELECT employee_id, hire_date, termination_date
   FROM employee_db.employees;
   
   CREATE VIEW yearly_headcount AS
WITH years AS (
    SELECT 2003 AS year 
     UNION SELECT 2004 
      UNION SELECT 2005
     UNION SELECT 2006 
    UNION SELECT 2007 
    UNION SELECT 2008 
    UNION SELECT 2009 UNION SELECT 2010
    UNION SELECT 2011 UNION SELECT 2012 UNION SELECT 2013 UNION SELECT 2014
    UNION SELECT 2015 UNION SELECT 2016 UNION SELECT 2017 UNION SELECT 2018
    UNION SELECT 2019 UNION SELECT 2020 UNION SELECT 2021 UNION SELECT 2022
    UNION SELECT 2023 UNION SELECT 2024 UNION SELECT 2025
),
yearly_stats AS (
    SELECT 
        y.year,
        SUM(CASE 
            WHEN e.hire_date <= CONCAT(y.year, '-12-31')
            AND (e.termination_date IS NULL OR e.termination_date > CONCAT(y.year, '-12-31'))
            THEN 1 ELSE 0 
        END) AS headcount,
        
        SUM(CASE 
            WHEN YEAR(e.hire_date) = y.year 
            THEN 1 ELSE 0 
        END) AS new_hires,
        
        SUM(CASE 
            WHEN YEAR(e.termination_date) = y.year 
            THEN 1 ELSE 0 
        END) AS terminations
    FROM 
        years y
    CROSS JOIN 
        employees e
    GROUP BY 
        y.year
)
SELECT * FROM yearly_stats
ORDER BY year;

-- Script to modify the employees table and create a view for automatic transformation
-- Database: employee_db
-- Date: 2025-05-15

-- Step 1: Use the correct database
USE employee_db;

-- Step 2: Rename the columns in the employees table
ALTER TABLE employees
    
   
        CHANGE COLUMN `satisfaction_level` `satisfaction` FLOAT
;

-- Step 3: Create a view to reflect the updated table structure
-- This view will automatically update whenever the employees table is modified
CREATE OR REPLACE VIEW employees_view AS
SELECT
    employee_id,
    name,
    department,
    salary_level,
    actual_salary,
    turnover,  -- Already 0 for in-job, 1 for terminated
    satisfaction,  -- Corrected from 'satisfaction' to 'satisfaction_level'
    evaluation,  -- Renamed from last_evaluation
    number_project,
    average_monthly_hours,
    years_at_company,  -- Renamed from time_spend_company
    Work_accident,
    promotion_last_5years,
    hire_date,
    termination_date,
    turnover_probability,
    last_updated
FROM employees;

-- Step 4: Verify the changes (optional, for debugging)
-- SELECT * FROM employees LIMIT 5;
-- SELECT * FROM employees_view LIMIT 5;