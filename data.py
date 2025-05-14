import mysql.connector
import logging
from datetime import datetime, timedelta
import pandas as pd
import os
import random
import numpy as np
from faker import Faker
import calendar

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('update.log'),
        logging.StreamHandler()
    ]
)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Taylor@1989',
    'database': 'employee_db',
    'port': 3306
}

# Constants for employee data generation - SaaS company growth model
TOTAL_CURRENT_EMPLOYEES = 2000
DEPARTMENTS = {
    'Sales': 0.35,
    'Marketing': 0.15,
    'Engineering': 0.30,
    'Customer Success': 0.10,
    'HR': 0.03,
    'Finance': 0.03,
    'Operations': 0.04
}
SALARY_LEVELS = {
    'low': 0.40,
    'medium': 0.45,
    'high': 0.15
}
SALARY_RANGES = {
    'Sales': {'min': 180, 'max': 550},
    'Marketing': {'min': 150, 'max': 500},
    'Engineering': {'min': 220, 'max': 650},
    'Customer Success': {'min': 130, 'max': 450},
    'HR': {'min': 140, 'max': 480},
    'Finance': {'min': 170, 'max': 520},
    'Operations': {'min': 120, 'max': 420}
}

# Yearly headcount targets
YEARLY_HEADCOUNT_TARGETS = {
    '2002': 25, '2003': 40, '2004': 65, '2005': 100, '2006': 150, '2007': 220, '2008': 310,
    '2009': 450, '2010': 610, '2011': 780, '2012': 980, '2013': 1180, '2014': 1350, '2015': 1520,
    '2016': 1670, '2017': 1780, '2018': 1850, '2019': 1900, '2020': 1930, '2021': 1960, '2022': 1980,
    '2023': 1990, '2024': 2000, '2025': 2005
}

# Yearly turnover rates
YEARLY_TURNOVER_RATES = {
    '2002': 0.08, '2003': 0.10, '2004': 0.10, '2005': 0.12, '2006': 0.12, '2007': 0.15, '2008': 0.15,
    '2009': 0.18, '2010': 0.20, '2011': 0.22, '2012': 0.23, '2013': 0.24, '2014': 0.25, '2015': 0.24,
    '2016': 0.23, '2017': 0.22, '2018': 0.21, '2019': 0.20, '2020': 0.18, '2021': 0.20, '2022': 0.21,
    '2023': 0.19, '2024': 0.18, '2025': 0.17
}

# Initialize Faker
fake = Faker('zh_CN')
fake_en = Faker()

# Set random seeds
random.seed(42)
np.random.seed(42)

def generate_employee_ids(count, existing_ids=None):
    """Generate unique employee IDs"""
    max_id = 999999
    min_id = 100000
    available_ids = set(range(min_id, max_id + 1)) - (set(existing_ids) if existing_ids else set())
    if count > len(available_ids):
        raise ValueError(f"Cannot generate {count} unique IDs, insufficient range")
    return random.sample(list(available_ids), count)

def calculate_turnover_probability(satisfaction_score, years_at_company):
    """Calculate turnover probability based on satisfaction and tenure"""
    base_prob = 0.5 - 0.4 * satisfaction_score
    if years_at_company < 1:
        return min(0.95, base_prob * 3.0)
    if years_at_company < 2:
        return min(0.85, base_prob * 2.0)
    if years_at_company < 3:
        return min(0.75, base_prob * 1.5)
    if years_at_company > 5:
        return base_prob * 0.6
    return base_prob

def distribute_by_month(yearly_count):
    """Distribute yearly count across months"""
    base_monthly = max(1, yearly_count // 24)
    remaining = yearly_count - (base_monthly * 12)
    weights = [0.07, 0.06, 0.08, 0.09, 0.10, 0.12, 0.10, 0.08, 0.07, 0.07, 0.06, 0.10]
    extra_distribution = [int(remaining * w) for w in weights]
    while sum(extra_distribution) < remaining:
        idx = random.randint(0, 11)
        extra_distribution[idx] += 1
    monthly_distribution = [base_monthly + extra for extra in extra_distribution]
    return [max(1, count) for count in monthly_distribution]

def generate_termination_date(hire_date, years_at_company, is_leaving=True):
    """Generate termination date based on hire date and tenure"""
    if not is_leaving:
        return None
    hire_date_obj = datetime.strptime(hire_date, '%Y-%m-%d')
    if years_at_company == 0:
        months_worked = random.randint(1, 11)
        days_worked = random.randint(0, 30)
        term_date = hire_date_obj + timedelta(days=months_worked*30 + days_worked)
    else:
        term_date = hire_date_obj + timedelta(days=int(years_at_company * 365) + random.randint(-180, 180))
    current_date = datetime.now()
    term_date = min(term_date, current_date)
    return term_date.strftime('%Y-%m-%d')

def generate_hire_date(year, month=None):
    """Generate hire date for a given year"""
    if month is None:
        month = random.randint(1, 12)
    _, days_in_month = calendar.monthrange(year, month)
    day = random.randint(1, days_in_month)
    hire_date = datetime(year, month, day)
    current_date = datetime.now()
    hire_date = min(hire_date, current_date)
    return hire_date.strftime('%Y-%m-%d')

def generate_saas_company_data():
    """Generate employee data for a SaaS company (2002-2025)"""
    all_employees = []
    active_pool = []
    all_employee_ids = set()
    
    logging.info("Starting generation of SaaS company employee data (2002-2025)...")
    
    prev_year_headcount = 0
    
    for year in range(2002, 2026):
        year_str = str(year)
        target_year_end_headcount = YEARLY_HEADCOUNT_TARGETS.get(year_str, 0)
        year_turnover_rate = YEARLY_TURNOVER_RATES.get(year_str, 0.20)
        year_start_headcount = prev_year_headcount
        expected_leavers = int(year_start_headcount * year_turnover_rate)
        needed_hires = target_year_end_headcount - year_start_headcount + expected_leavers
        
        if needed_hires < 0:
            needed_hires = max(5, int(year_start_headcount * 0.05))
        
        monthly_hires = distribute_by_month(needed_hires)
        monthly_leavers = distribute_by_month(expected_leavers)
        
        logging.info(f"{year} Plan: Start {year_start_headcount}, End Target {target_year_end_headcount}, "
                     f"Turnover Rate {year_turnover_rate:.2f}, Expected Leavers {expected_leavers}, Planned Hires {needed_hires}")
        
        # Generate new hires
        year_employees = []
        new_employee_ids = generate_employee_ids(needed_hires, all_employee_ids)
        all_employee_ids.update(new_employee_ids)
        
        id_index = 0
        for month, hire_count in enumerate(monthly_hires, 1):
            for _ in range(hire_count):
                if id_index >= len(new_employee_ids):
                    break
                department = random.choices(list(DEPARTMENTS.keys()), weights=list(DEPARTMENTS.values()), k=1)[0]
                hire_date = generate_hire_date(year, month)
                if year < 2009:
                    first_year_leaver_prob = 0.20
                elif year < 2019:
                    first_year_leaver_prob = 0.33
                else:
                    first_year_leaver_prob = 0.25
                first_year_leaver = random.random() < first_year_leaver_prob
                years_at_company = 0 if first_year_leaver else max(0, min(10, int(np.random.gamma(1.5, 1.5))))
                satisfaction = round(random.uniform(0.1, 0.5), 2) if first_year_leaver else round(random.uniform(0.5, 0.9), 2)
                
                employee = {
                    'employee_id': new_employee_ids[id_index],
                    'name': fake_en.name(),
                    'department': department,
                    'salary_level': random.choices(list(SALARY_LEVELS.keys()), weights=list(SALARY_LEVELS.values()), k=1)[0],
                    'actual_salary': random.randint(SALARY_RANGES[department]['min'], SALARY_RANGES[department]['max']) * 1000,
                    'turnover': 0,
                    'satisfaction': satisfaction,
                    'evaluation': round(random.uniform(0.4, 1.0), 2),
                    'project_count': max(0, min(7, int(np.random.normal(3.8, 1.5)))),
                    'average_monthly_hours': random.randint(140, 280),
                    'years_at_company': years_at_company,
                    'hire_date': hire_date,
                    'termination_date': None,
                    'work_accident': 1 if random.random() < 0.08 else 0,
                    'promotion': 1 if random.random() < 0.05 else 0,
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'first_year_leaver': first_year_leaver,
                    'hire_year': year,
                    'hire_month': month
                }
                year_employees.append(employee)
                active_pool.append(employee)
                id_index += 1
        
        # Process leavers
        actual_leavers = 0
        for month in range(1, 13):
            target_month_leavers = monthly_leavers[month-1] if month <= len(monthly_leavers) else 0
            max_allowed_leavers = max(0, len(active_pool) - 5)
            target_month_leavers = min(target_month_leavers, max_allowed_leavers)
            
            if target_month_leavers <= 0 or not active_pool:
                continue
            
            for emp in active_pool:
                hire_date_obj = datetime.strptime(emp['hire_date'], '%Y-%m-%d')
                current_date = datetime(year, month, 15)
                months_employed = (current_date - hire_date_obj).days / 30.0
                emp['months_employed'] = months_employed
                if emp['first_year_leaver'] and months_employed < 12:
                    if months_employed <= 3:
                        emp['leave_prob'] = 0.9 - (months_employed * 0.05)
                    else:
                        emp['leave_prob'] = 0.75 - ((months_employed - 3) * 0.05)
                elif months_employed < 12:
                    emp['leave_prob'] = 0.1
                else:
                    years_employed = months_employed / 12.0
                    emp['leave_prob'] = calculate_turnover_probability(emp['satisfaction'], years_employed)
            
            active_pool.sort(key=lambda x: x['leave_prob'], reverse=True)
            leavers = active_pool[:target_month_leavers]
            
            for leaver in leavers:
                leaver['turnover'] = 1
                term_date = datetime(year, month, random.randint(1, 28))
                leaver['termination_date'] = term_date.strftime('%Y-%m-%d')
                hire_date_obj = datetime.strptime(leaver['hire_date'], '%Y-%m-%d')
                months_worked = (term_date - hire_date_obj).days / 30.0
                if months_worked < 12:
                    leaver['years_at_company'] = 0
                else:
                    leaver['years_at_company'] = int(months_worked / 12.0)
                if leaver['years_at_company'] < 1:
                    leaver['project_count'] = random.randint(0, 2)
                    leaver['evaluation'] = round(random.uniform(0.3, 0.7), 2)
                    leaver['promotion'] = 0
                actual_leavers += 1
            
            active_pool = [emp for emp in active_pool if emp['turnover'] == 0]
            if leavers:
                logging.info(f"{year} Month {month}: {len(leavers)} leavers")
        
        all_employees.extend(year_employees)
        prev_year_headcount = len(active_pool)
        logging.info(f"{year} End: Hired {len(year_employees)}, Left {actual_leavers}, "
                     f"Year-End Headcount {prev_year_headcount}")
    
    # Clean up temporary fields
    for emp in all_employees:
        for field in ['first_year_leaver', 'leave_prob', 'hire_year', 'hire_month', 'months_employed']:
            if field in emp:
                del emp[field]
    
    # Log final stats
    active_employees = [emp for emp in all_employees if emp['turnover'] == 0]
    terminated_employees = [emp for emp in all_employees if emp['turnover'] == 1]
    zero_tenure_leavers = [emp for emp in terminated_employees if emp['years_at_company'] == 0]
    
    logging.info(f"Total Employees Generated: {len(all_employees)}")
    logging.info(f"Active Employees: {len(active_employees)}")
    logging.info(f"Terminated Employees: {len(terminated_employees)}")
    logging.info(f"1-Year Leavers: {len(zero_tenure_leavers)} "
                 f"({len(zero_tenure_leavers)/max(1,len(terminated_employees))*100:.1f}%)")
    
    for year in range(2002, 2026):
        year_leavers = [emp for emp in terminated_employees 
                        if emp['termination_date'] and datetime.strptime(emp['termination_date'], '%Y-%m-%d').year == year]
        year_hires = [emp for emp in all_employees 
                      if emp['hire_date'] and datetime.strptime(emp['hire_date'], '%Y-%m-%d').year == year]
        year_end_headcount = len([emp for emp in all_employees 
                                  if datetime.strptime(emp['hire_date'], '%Y-%m-%d') <= datetime(year, 12, 31) and 
                                  (emp['termination_date'] is None or 
                                   datetime.strptime(emp['termination_date'], '%Y-%m-%d') > datetime(year, 12, 31))])
        logging.info(f"{year} Stats: Hires={len(year_hires)}, Leavers={len(year_leavers)}, "
                     f"Year-End Headcount={year_end_headcount}")
    
    return all_employees

def check_and_fix_database():
    """Check and fix database structure"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            employee_id INT PRIMARY KEY,
            name VARCHAR(100),
            department VARCHAR(50),
            salary_level VARCHAR(20),
            actual_salary INT,
            turnover TINYINT,
            satisfaction FLOAT,
            evaluation FLOAT,
            project_count INT,
            average_monthly_hours INT,
            years_at_company INT,
            hire_date DATE,
            termination_date DATE,
            work_accident TINYINT,
            promotion TINYINT,
            last_updated DATETIME
        )
        """)
        cursor.execute("DESCRIBE employees")
        columns = [column[0] for column in cursor.fetchall()]
        logging.info(f"Current table columns: {columns}")
        cursor.close()
        conn.close()
        return True, columns
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        return False, []

def generate_updated_data(columns):
    """Generate or update employee data"""
    logging.info("Starting data generation/update...")
    logging.info("Generating SaaS company employee data...")
    employees_data = generate_saas_company_data()
    df = pd.DataFrame(employees_data)
    logging.info(f"Generated {len(df)} new records")
    
    for col in columns:
        if col not in df.columns:
            logging.warning(f"CSV missing column: {col}")
            df[col] = None
    
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['last_updated'] = current_time
    
    active_employees = df[df['turnover'] == 0]
    terminated_employees = df[df['turnover'] == 1]
    zero_tenure_leavers = terminated_employees[terminated_employees['years_at_company'] == 0]
    
    logging.info(f"Total Employees: {len(df)}")
    logging.info(f"Active Employees: {len(active_employees)}")
    logging.info(f"Terminated Employees: {len(terminated_employees)}")
    logging.info(f"1-Year Leavers: {len(zero_tenure_leavers)} "
                 f"({len(zero_tenure_leavers)/max(1, len(terminated_employees))*100:.1f}%)")
    
    yearly_stats = {}
    for year in range(2002, 2026):
        year_hires = df[pd.to_datetime(df['hire_date']).dt.year == year]
        year_leavers = df[(df['turnover'] == 1) & (pd.to_datetime(df['termination_date']).dt.year == year)]
        year_headcount = df[(pd.to_datetime(df['hire_date']) <= f"{year}-12-31") & 
                            ((df['termination_date'].isna()) | (pd.to_datetime(df['termination_date']) > f"{year}-12-31"))]
        yearly_stats[year] = {
            'hires': len(year_hires),
            'terminations': len(year_leavers),
            'headcount': len(year_headcount)
        }
        logging.info(f"{year} Stats: Hires={yearly_stats[year]['hires']}, "
                     f"Leavers={yearly_stats[year]['terminations']}, "
                     f"Year-End Headcount={yearly_stats[year]['headcount']}")
    
    current_date = datetime.now().strftime('%Y%m%d%H%M%S')
    new_filename = f'saas_company_data_{current_date}.csv'
    df.to_csv(new_filename, index=False, encoding='utf-8-sig')
    logging.info(f"Generated new data file: {new_filename}, {len(df)} records")
    return new_filename, df

def import_to_mysql(filename, df, columns):
    """Import data to MySQL"""
    logging.info("Starting data import to MySQL...")
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        columns_str = ', '.join([f"`{col}`" for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        updates = ', '.join([f"`{col}` = VALUES(`{col}`)" for col in columns])
        insert_query = f"INSERT INTO employees ({columns_str}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
        
        values = []
        for _, row in df.iterrows():
            row_data = []
            for col in columns:
                val = row.get(col)
                if isinstance(val, float) and np.isnan(val):
                    val = None
                row_data.append(val)
            values.append(tuple(row_data))
        
        batch_size = 1000
        for i in range(0, len(values), batch_size):
            cursor.executemany(insert_query, values[i:i + batch_size])
            conn.commit()
            logging.info(f"Imported {min(i + batch_size, len(values))}/{len(values)} records")
        
        logging.info(f"Successfully imported {len(values)} records to MySQL")
        cursor.close()
        conn.close()
        return True
    except mysql.connector.Error as err:
        logging.error(f"Error importing to MySQL: {err}")
        return False

if __name__ == "__main__":
    logging.info("Starting SaaS company employee data generation and import...")
    db_ok, columns = check_and_fix_database()
    if not db_ok:
        logging.error("Database connection failed, exiting...")
        exit(1)
    
    result = generate_updated_data(columns)
    if not result:
        logging.error("Data generation failed, exiting...")
        exit(1)
    
    new_filename, df = result
    if import_to_mysql(new_filename, df, columns):
        logging.info("SaaS company employee data updated and imported to MySQL - Refresh database view in VSCode")
        print("\n\n==== Import Complete ====\nPlease refresh database view in VSCode!\n==================\n\n")
    else:
        logging.error("MySQL import failed")
        exit(1)