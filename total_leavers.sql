SELECT * FROM employee_db.employees;

SELECT YEAR(termination_date) AS year, COUNT(*) AS leavers
FROM employees
WHERE turnover = 1
  AND termination_date >= '2013-01-01'
  AND termination_date <= '2025-12-31'
GROUP BY YEAR(termination_date)
ORDER BY year;

USE employee_db;
SELECT COUNT(*) AS total_leavers
FROM employees
WHERE turnover = 1
  AND termination_date >= '2013-01-01'
  AND termination_date <= '2025-12-31';