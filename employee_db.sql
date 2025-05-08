USE employee_db;
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    department VARCHAR(50),
    salary_level VARCHAR(20),
    actual_salary INT,
    turnover BOOLEAN,
    satisfaction FLOAT,
    evaluation FLOAT,
    project_count INT,
    average_monthly_hours INT,
    years_at_company INT,
    hire_date DATE,
    termination_date DATE,
    work_accident BOOLEAN,
    promotion BOOLEAN,
    last_updated DATETIME
);