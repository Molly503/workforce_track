#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
员工数据生成器与MySQL导入脚本 (Modified)

该脚本使用Faker库生成类似HubSpot公司的员工数据，并自动导入MySQL数据库。
特点:
- 生成约500名员工的数据（含历史离职员工）
- 包含各个部门(市场、销售、法务、研发、人力、行政)
- 月离职率3%-5%，每月离职和入职15-25人
- 离职员工的termination_date覆盖2014-01-01至2025-03-31
- 每天更新数据
- 自动导入MySQL数据库
"""

import pandas as pd
import numpy as np
import mysql.connector
from faker import Faker
from datetime import datetime, timedelta
import random
import os
import time

# 设置随机种子以确保可重复性
random.seed(42)
np.random.seed(42)

# 设置中文和英文随机数据生成器
fake = Faker('zh_CN')
fake_en = Faker()

# 数据库连接配置 - 请修改为您的实际配置
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'Taylor@1989',  # 修改为实际MySQL密码
    'database': 'employee_db',
    'port': 3306
}

# 常量设置
TOTAL_EMPLOYEES = 500
HISTORICAL_LEAVERS = 100  # 历史离职员工数量 (新增)
MONTHLY_TURNOVER_RATE = (0.03, 0.05)  # 月离职率 3%-5%
MIN_MONTHLY_LEAVERS = 15  # 每月最少离职人数
MAX_MONTHLY_LEAVERS = 25  # 每月最多离职人数
MIN_MONTHLY_HIRING = 15   # 每月最少入职人数
MAX_MONTHLY_HIRING = 25   # 每月最多入职人数
HIRING_START_DAYS = (0, 7)  # 入职开始时间（立即开始）

# 部门设置
DEPARTMENTS = {
    'Sales': 0.30,
    'Marketing': 0.20,
    'Engineering': 0.25,
    'HR': 0.10,
    'Legal': 0.05,
    'Operations': 0.10
}

# 薪资水平设置
SALARY_LEVELS = {
    'low': 0.50,
    'medium': 0.40,
    'high': 0.10
}

# 薪资范围（单位：千元/年）
SALARY_RANGES = {
    'Sales': {'min': 150, 'max': 500},
    'Marketing': {'min': 120, 'max': 450},
    'Engineering': {'min': 180, 'max': 600},
    'HR': {'min': 100, 'max': 400},
    'Legal': {'min': 150, 'max': 500},
    'Operations': {'min': 80, 'max': 350}
}

def generate_employee_ids(count):
    """生成唯一的员工ID"""
    return random.sample(range(1000, 100000), count)

def generate_satisfaction_score(is_leaving):
    """生成满意度分数，离职员工分数较低"""
    if is_leaving:
        return round(max(0, min(1, np.random.beta(2, 3) * 0.8)), 2)
    return round(max(0, min(1, np.random.beta(4, 2) * 0.9 + 0.1)), 2)

def generate_evaluation_score():
    """生成绩效评估分数，双峰分布"""
    return round(random.uniform(0.4, 0.6) if random.random() < 0.5 else random.uniform(0.8, 1.0), 2)

def generate_project_count(is_leaving):
    """生成项目数量"""
    if is_leaving and random.random() < 0.4:
        return random.randint(6, 7)
    return max(0, min(7, int(np.random.normal(3.8, 1.5))))

def generate_monthly_hours(is_leaving):
    """生成月均工作小时"""
    if is_leaving and random.random() < 0.6:
        return random.randint(250, 310)
    return random.randint(96, 150) if random.random() < 0.5 else random.randint(250, 280)

def generate_years_at_company(is_leaving, termination_date=None):
    """生成在职年限，考虑离职日期"""
    if is_leaving and termination_date:
        term_date = datetime.strptime(termination_date, '%Y-%m-%d')
        max_years = min(10, (datetime.now() - term_date).days // 365 + 1)
        years = random.randint(1, max_years) if max_years > 1 else 1
    else:
        years = random.randint(3, 5) if is_leaving and random.random() < 0.6 else max(0, min(10, int(np.random.gamma(2, 1.8))))
    return years

def generate_hire_date(years_at_company, termination_date=None):
    """生成入职日期，考虑离职日期"""
    if termination_date:
        term_date = datetime.strptime(termination_date, '%Y-%m-%d')
        hire_date = term_date - timedelta(days=years_at_company * 365 + random.randint(-180, 180))
    else:
        current_date = datetime.now()
        hire_date = current_date - timedelta(days=years_at_company * 365 + random.randint(-180, 180))
    return hire_date.strftime('%Y-%m-%d')

def generate_termination_date(is_leaving):
    """生成离职日期，历史数据覆盖2014-01-01至2025-03-31"""
    if is_leaving:
        start_date = datetime(2014, 1, 1)
        end_date = datetime(2025, 3, 31)
        time_diff = (end_date - start_date).days
        random_days = random.randint(0, time_diff)
        termination_date = start_date + timedelta(days=random_days)
        return termination_date.strftime('%Y-%m-%d')
    return None

def generate_work_accident(is_leaving):
    """生成工伤记录"""
    return 1 if random.random() < (0.05 if is_leaving else 0.18) else 0

def generate_promotion(is_leaving):
    """生成晋升记录"""
    return 1 if random.random() < (0.005 if is_leaving else 0.03) else 0

def generate_department():
    """生成部门"""
    return random.choices(list(DEPARTMENTS.keys()), weights=list(DEPARTMENTS.values()), k=1)[0]

def generate_salary_level():
    """生成薪资水平"""
    return random.choices(list(SALARY_LEVELS.keys()), weights=list(SALARY_LEVELS.values()), k=1)[0]

def generate_actual_salary(department):
    """生成实际薪资"""
    salary_range = SALARY_RANGES[department]
    return random.randint(salary_range['min'], salary_range['max']) * 1000

def generate_employees_data(count=TOTAL_EMPLOYEES + HISTORICAL_LEAVERS):
    """生成员工数据，包含历史离职员工"""
    # 包含历史离职员工
    historical_leavers = HISTORICAL_LEAVERS
    current_leavers = random.randint(MIN_MONTHLY_LEAVERS, MAX_MONTHLY_LEAVERS)
    total_leavers = historical_leavers + current_leavers
    turnover_rate = total_leavers / count
    print(f"生成数据: 月离职率 {turnover_rate:.2%}，当前月离职 {current_leavers} 人，历史离职 {historical_leavers} 人")

    employee_ids = generate_employee_ids(count)
    leaving_indices = random.sample(range(count), total_leavers)
    historical_leaving_indices = leaving_indices[:historical_leavers]
    current_leaving_indices = leaving_indices[historical_leavers:]
    employees_data = []

    for i in range(count):
        is_leaving = i in leaving_indices
        is_historical_leaver = i in historical_leaving_indices
        department = generate_department()
        termination_date = generate_termination_date(is_leaving)
        years = generate_years_at_company(is_leaving, termination_date)
        employee = {
            'employee_id': employee_ids[i],
            'name': fake_en.name(),
            'department': department,
            'salary_level': generate_salary_level(),
            'actual_salary': generate_actual_salary(department),
            'turnover': 1 if is_leaving else 0,
            'satisfaction': generate_satisfaction_score(is_leaving),
            'evaluation': generate_evaluation_score(),
            'project_count': generate_project_count(is_leaving),
            'average_monthly_hours': generate_monthly_hours(is_leaving),
            'years_at_company': years,
            'hire_date': generate_hire_date(years, termination_date),
            'termination_date': termination_date,
            'work_accident': generate_work_accident(is_leaving),
            'promotion': generate_promotion(is_leaving),
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        employees_data.append(employee)

    actual_leavers = sum(1 for e in employees_data if e['turnover'] == 1)
    print(f"实际离职人数: {actual_leavers} 人，离职率: {actual_leavers/count:.2%}")
    return employees_data

def create_database():
    """创建数据库和表"""
    try:
        print("\n尝试创建数据库...")
        conn = mysql.connector.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        cursor.execute(f"USE {DB_CONFIG['database']}")
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
        conn.commit()
        cursor.close()
        conn.close()
        print("数据库和表创建成功!")
        return True
    except mysql.connector.Error as e:
        print(f"创建数据库失败: {e}")
        print("\n请检查以下可能的问题:")
        print("1. MySQL服务是否正在运行")
        print("2. 用户名和密码是否正确")
        print("3. 用户是否有创建数据库的权限")
        return False

def import_to_mysql(employees_data):
    """将数据导入MySQL数据库"""
    try:
        print("\n尝试连接MySQL数据库...")
        backup_file = "employee_data_backup.csv"
        print(f"保存数据备份到 {backup_file}...")
        save_to_csv(employees_data, backup_file)

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
        insert_sql = """
        INSERT INTO employees (
            employee_id, name, department, salary_level, actual_salary, 
            turnover, satisfaction, evaluation, project_count, 
            average_monthly_hours, years_at_company, hire_date, 
            termination_date, work_accident, promotion, last_updated
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            department = VALUES(department),
            salary_level = VALUES(salary_level),
            actual_salary = VALUES(actual_salary),
            turnover = VALUES(turnover),
            satisfaction = VALUES(satisfaction),
            evaluation = VALUES(evaluation),
            project_count = VALUES(project_count),
            average_monthly_hours = VALUES(average_monthly_hours),
            years_at_company = VALUES(years_at_company),
            hire_date = VALUES(hire_date),
            termination_date = VALUES(termination_date),
            work_accident = VALUES(work_accident),
            promotion = VALUES(promotion),
            last_updated = VALUES(last_updated)
        """
        data_to_insert = [(
            emp['employee_id'], emp['name'], emp['department'], emp['salary_level'], emp['actual_salary'],
            emp['turnover'], emp['satisfaction'], emp['evaluation'], emp['project_count'],
            emp['average_monthly_hours'], emp['years_at_company'], emp['hire_date'],
            emp['termination_date'], emp['work_accident'], emp['promotion'], emp['last_updated']
        ) for emp in employees_data]

        batch_size = 50
        for i in range(0, len(data_to_insert), batch_size):
            cursor.executemany(insert_sql, data_to_insert[i:i+batch_size])
            conn.commit()
            print(f"已导入 {min(i+batch_size, len(data_to_insert))}/{len(data_to_insert)} 条记录")

        cursor.close()
        conn.close()
        print(f"已成功导入 {len(employees_data)} 条员工数据到MySQL")
        return True
    except mysql.connector.Error as e:
        print(f"MySQL操作失败: {e}")
        print("\n可能的解决方案:")
        print("1. 确认MySQL服务正在运行")
        print("2. 检查DB_CONFIG中的信息是否正确")
        print("3. 确认用户拥有CREATE TABLE和INSERT权限")
        return False

def save_to_csv(employees_data, filename='employee_data.csv'):
    """保存员工数据为CSV"""
    try:
        df = pd.DataFrame(employees_data)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"数据已保存到 {filename}")
        return True
    except Exception as e:
        print(f"保存CSV失败: {e}")
        return False

def display_sample_data(employees_data, sample_size=10):
    """显示样例数据"""
    if not employees_data:
        print("没有数据可以显示")
        return

    sample = random.sample(employees_data, min(sample_size, len(employees_data)))
    df = pd.DataFrame(sample)
    display_columns = [
        'employee_id', 'name', 'department', 'salary_level', 'actual_salary',
        'turnover', 'satisfaction', 'evaluation', 'project_count',
        'average_monthly_hours', 'years_at_company', 'hire_date', 'termination_date'
    ]
    print("\n===== 数据样例 (随机选择的10条记录) =====")
    print(df[display_columns].to_string())

    print("\n===== 数据统计信息 =====")
    print(f"总记录数: {len(employees_data)}")
    turnover_count = sum(1 for emp in employees_data if emp['turnover'] == 1)
    turnover_rate = turnover_count / len(employees_data)
    print(f"当前离职人数: {turnover_count}, 离职率: {turnover_rate:.2%}")

    if not (0.03 <= turnover_rate <= 0.05):
        print(f"警告: 当前离职率 {turnover_rate:.2%} 不在目标范围 3%-5% 内")

    dept_counts = pd.Series([emp['department'] for emp in employees_data]).value_counts()
    print("\n部门分布:")
    for dept, count in dept_counts.items():
        print(f"  {dept}: {count}人 ({count/len(employees_data):.2%})")

    salary_counts = pd.Series([emp['salary_level'] for emp in employees_data]).value_counts()
    print("\n薪资水平分布:")
    for level, count in salary_counts.items():
        print(f"  {level}: {count}人 ({count/len(employees_data):.2%})")

    avg_satisfaction = sum(emp['satisfaction'] for emp in employees_data) / len(employees_data)
    print(f"\n平均满意度: {avg_satisfaction:.2f}")

    # 新增：显示 termination_date 分布
    term_dates = [emp['termination_date'] for emp in employees_data if emp['termination_date']]
    if term_dates:
        term_df = pd.DataFrame({'termination_date': term_dates})
        term_df['year'] = pd.to_datetime(term_df['termination_date']).dt.year
        term_counts = term_df['year'].value_counts().sort_index()
        print("\n离职日期分布 (按年份):")
        for year, count in term_counts.items():
            print(f"  {year}: {count}人")

def generate_initial_csv_with_turnover(count=TOTAL_EMPLOYEES + HISTORICAL_LEAVERS):
    """生成初始CSV文件，包含历史离职数据"""
    historical_leavers = HISTORICAL_LEAVERS
    current_leavers = random.randint(MIN_MONTHLY_LEAVERS, MAX_MONTHLY_LEAVERS)
    total_leavers = historical_leavers + current_leavers
    turnover_rate = total_leavers / count
    print(f"生成初始数据: 月离职率 {turnover_rate:.2%}, 当前月离职 {current_leavers} 人, 历史离职 {historical_leavers} 人")

    employee_ids = generate_employee_ids(count)
    leaving_indices = random.sample(range(count), total_leavers)
    historical_leaving_indices = leaving_indices[:historical_leavers]
    current_leaving_indices = leaving_indices[historical_leavers:]
    employees_data = []

    for i in range(count):
        is_leaving = i in leaving_indices
        is_historical_leaver = i in historical_leaving_indices
        department = generate_department()
        termination_date = generate_termination_date(is_leaving)
        years = generate_years_at_company(is_leaving, termination_date)
        employee = {
            'employee_id': employee_ids[i],
            'name': fake_en.name(),
            'department': department,
            'salary_level': generate_salary_level(),
            'actual_salary': generate_actual_salary(department),
            'turnover': 1 if is_leaving else 0,
            'satisfaction': generate_satisfaction_score(is_leaving),
            'evaluation': generate_evaluation_score(),
            'project_count': random.randint(5, 7) if is_leaving and random.random() < 0.7 else random.randint(2, 4),
            'average_monthly_hours': random.randint(260, 310) if is_leaving and random.random() < 0.6 else random.randint(120, 150),
            'years_at_company': years,
            'hire_date': generate_hire_date(years, termination_date),
            'termination_date': termination_date,
            'work_accident': generate_work_accident(is_leaving),
            'promotion': generate_promotion(is_leaving),
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        employees_data.append(employee)

    actual_leavers = sum(1 for e in employees_data if e['turnover'] == 1)
    print(f"实际离职人数: {actual_leavers} 人，离职率: {actual_leavers/count:.2%}")

    df = pd.DataFrame(employees_data)
    csv_filename = "employee_data_initial.csv"
    df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
    print(f"初始数据已保存到 {csv_filename}")
    return employees_data

def daily_update(start_date=None):
    """每日更新函数 - 处理离职和新员工入职"""
    current_date = datetime.now()
    current_date_str = current_date.strftime('%Y-%m-%d %H:%M:%S')
    print(f"开始每日更新: {current_date_str}")

    days_since_start = 0
    if start_date:
        days_since_start = (current_date - start_date).days

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT COUNT(*) as total FROM employees WHERE turnover = 0")
        current_count = cursor.fetchone()['total']
        
        if current_count == 0:
            print("数据库为空，生成初始数据...")
            employees_data = generate_employees_data()
            import_to_mysql(employees_data)
            cursor.execute("SELECT COUNT(*) as total FROM employees WHERE turnover = 0")
            current_count = cursor.fetchone()['total']
            
            if current_count == 0:
                print("无法生成初始数据，请尝试选项 1 手动生成初始数据")
                cursor.close()
                conn.close()
                return False
        
        print(f"当前在职员工数: {current_count}")

        monthly_leavers = random.randint(MIN_MONTHLY_LEAVERS, MAX_MONTHLY_LEAVERS)
        monthly_hiring = monthly_leavers
        daily_leavers = max(1, round(monthly_leavers / 30))
        daily_hiring = daily_leavers
        turnover_rate = monthly_leavers / current_count if current_count > 0 else 0

        print(f"本次更新: {daily_leavers}人离职, {daily_hiring}人新入职")
        print(f"预计月离职率: {turnover_rate:.2%}")

        daily_leavers = min(daily_leavers, current_count)
        if daily_leavers > 0:
            cursor.execute("SELECT employee_id FROM employees WHERE turnover = 0 ORDER BY RAND() LIMIT %s", (daily_leavers,))
            leaving_employees = cursor.fetchall()

            for emp in leaving_employees:
                cursor.execute(
                    "UPDATE employees SET turnover = 1, satisfaction = %s, termination_date = %s, last_updated = %s WHERE employee_id = %s",
                    (round(random.uniform(0.1, 0.5), 2), current_date.strftime('%Y-%m-%d'), current_date_str, emp['employee_id'])
                )
        else:
            print("没有员工离职")
            leaving_employees = []

        cursor.execute("SELECT MAX(employee_id) as max_id FROM employees")
        max_id_result = cursor.fetchone()
        max_id = max_id_result['max_id'] if max_id_result['max_id'] is not None else 0
        new_employees = []

        for i in range(daily_hiring):
            department = generate_department()
            new_id = max_id + i + 1
            new_employees.append({
                'employee_id': new_id,
                'name': fake_en.name(),
                'department': department,
                'salary_level': generate_salary_level(),
                'actual_salary': generate_actual_salary(department),
                'turnover': 0,
                'satisfaction': round(random.uniform(0.6, 0.95), 2),
                'evaluation': round(random.uniform(0.7, 0.9), 2),
                'project_count': random.randint(1, 3),
                'average_monthly_hours': random.randint(160, 200),
                'years_at_company': 0,
                'hire_date': current_date.strftime('%Y-%m-%d'),
                'termination_date': None,
                'work_accident': 0,
                'promotion': 0,
                'last_updated': current_date_str
            })

        for emp in new_employees:
            cursor.execute("""
            INSERT INTO employees (
                employee_id, name, department, salary_level, actual_salary, 
                turnover, satisfaction, evaluation, project_count, 
                average_monthly_hours, years_at_company, hire_date, 
                termination_date, work_accident, promotion, last_updated
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                emp['employee_id'], emp['name'], emp['department'], emp['salary_level'], emp['actual_salary'],
                emp['turnover'], emp['satisfaction'], emp['evaluation'], emp['project_count'],
                emp['average_monthly_hours'], emp['years_at_company'], emp['hire_date'],
                emp['termination_date'], emp['work_accident'], emp['promotion'], emp['last_updated']
            ))

        conn.commit()

        cursor.execute("SELECT COUNT(*) as total FROM employees WHERE turnover = 0")
        updated_count = cursor.fetchone()['total']
        cursor.execute("SELECT COUNT(*) as total FROM employees WHERE turnover = 1")
        leaving_count = cursor.fetchone()['total']
        total_count = updated_count + leaving_count
        turnover_rate = leaving_count / total_count if total_count > 0 else 0
        
        print(f"更新完成: 在职员工 {updated_count} 人，历史离职员工 {leaving_count} 人")
        print(f"当前总离职率: {turnover_rate:.2%}")
        
        thirty_days_ago = (current_date - timedelta(days=30)).strftime('%Y-%m-%d')
        cursor.execute("""
        SELECT COUNT(*) as count 
        FROM employees 
        WHERE termination_date >= %s AND termination_date <= %s
        """, (thirty_days_ago, current_date.strftime('%Y-%m-%d')))
        monthly_departures = cursor.fetchone()['count']
        monthly_rate = monthly_departures / total_count if total_count > 0 else 0
        print(f"过去30天离职率: {monthly_rate:.2%}")

        cursor.execute("SELECT * FROM employees")
        all_employees = cursor.fetchall()
        
        if all_employees:
            df = pd.DataFrame(all_employees)
            csv_filename = f"employee_data_{current_date.strftime('%Y%m%d')}.csv"
            df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            print(f"数据已保存到 {csv_filename}")
        else:
            print("警告: 没有数据可以保存到CSV")

        cursor.close()
        conn.close()
        
        print(f"每日更新完成: {current_date_str}")
        return True
        
    except mysql.connector.Error as e:
        print(f"数据库更新失败: {e}")
        print("请确认MySQL连接信息是否正确")
        return False
    except Exception as e:
        print(f"更新过程发生错误: {e}")
        import traceback
        traceback.print_exc()
        return False

def setup_database_config():
    """设置数据库连接配置"""
    global DB_CONFIG
    print("\n========= 数据库配置 =========")
    print("请输入MySQL数据库连接信息 (直接按回车可使用默认值):")
    DB_CONFIG['host'] = input(f"主机地址 (默认: {DB_CONFIG['host']}): ") or DB_CONFIG['host']
    port_input = input(f"端口 (默认: {DB_CONFIG['port']}): ") or str(DB_CONFIG['port'])
    DB_CONFIG['port'] = int(port_input)
    DB_CONFIG['user'] = input(f"用户名 (默认: {DB_CONFIG['user']}): ") or DB_CONFIG['user']
    DB_CONFIG['password'] = input(f"密码 (默认: {DB_CONFIG['password']}): ") or DB_CONFIG['password']
    DB_CONFIG['database'] = input(f"数据库名 (默认: {DB_CONFIG['database']}): ") or DB_CONFIG['database']
    
    print("\n当前数据库配置:")
    for key, value in DB_CONFIG.items():
        if key == 'password':
            print(f"  {key}: {'*' * len(str(value))}")
        else:
            print(f"  {key}: {value}")
    
    if input("\n确认使用上述配置? (y/n, 默认y): ").lower() != 'n':
        return True
    return setup_database_config()

def generate_and_import_initial_data():
    """生成初始数据并导入MySQL"""
    print("\n========= 生成初始员工数据 =========")
    employees_data = generate_employees_data()
    display_sample_data(employees_data)
    save_to_csv(employees_data)
    
    if input("\n是否导入到MySQL数据库? (y/n, 默认y): ").lower() != 'n':
        if setup_database_config():
            if create_database() and import_to_mysql(employees_data):
                print("\n初始数据导入完成!")
                return True
            else:
                print("\n数据导入失败，请检查数据库配置或连接")
                return False
    return True
    
def refresh_data_and_import():
    """刷新数据并自动导入MySQL"""
    print("\n========= 刷新数据并导入MySQL =========")
    if setup_database_config():
        if not create_database():
            print("\n数据库连接失败，无法进行数据刷新")
            return False
        
        start_date = datetime.now()
        if daily_update(start_date):
            print("\n数据刷新完成并已导入MySQL!")
            return True
        else:
            print("\n数据刷新失败")
            return False
    return False

def setup_scheduled_job():
    """设置每日定时任务"""
    print("已设置每日自动更新任务")
    start_date = datetime.now()

    def get_next_midnight():
        tomorrow = datetime.now() + timedelta(days=1)
        return datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 0, 0)

    while True:
        now = datetime.now()
        next_run = get_next_midnight()
        seconds_until_next_run = (next_run - now).total_seconds()
        print(f"下一次更新将在 {next_run.strftime('%Y-%m-%d %H:%M:%S')} 进行，还有 {int(seconds_until_next_run)} 秒")
        time.sleep(min(seconds_until_next_run, 3600))
        if datetime.now() >= next_run:
            daily_update(start_date)

def main():
    """主函数"""
    print("员工数据生成与MySQL导入程序启动 (修改版)")
    print("新版本特点: 月离职率3%-5%，每月离职和入职15-25人，历史离职数据覆盖2014-2025年3月")
    print("\n请选择操作模式:")
    print("1. 重新生成初始员工数据")
    print("2. 执行一次每日更新 (离职和入职)")
    print("3. 模拟一个月的数据变化")
    print("4. 生成带有正确离职率的CSV文件")
    choice = input("请输入选项 (1/2/3/4): ")

    start_date = datetime.now()

    if choice == '1':
        employees_data = generate_employees_data()
        display_sample_data(employees_data)
        save_to_csv(employees_data)
        mysql_choice = input("\n是否导入MySQL? (y/n, 默认y): ") or 'y'
        if mysql_choice.lower() == 'y':
            setup_database_config()
            if create_database() and import_to_mysql(employees_data):
                if input("\n是否设置每日自动更新? (y/n): ").lower() == 'y':
                    setup_scheduled_job()
            else:
                print("无法导入MySQL，数据已保存为CSV")
        else:
            print("跳过MySQL导入，数据已保存为CSV")
    
    elif choice == '2':
        setup_database_config()
        daily_update(start_date)
    
    elif choice == '3':
        setup_database_config()
        month_choice = input("请输入要模拟的天数 (默认30): ") or '30'
        days = int(month_choice)
        for i in range(days):
            print(f"\n=== 第 {i+1} 天 ===")
            daily_update(start_date)
            time.sleep(1)
    
    elif choice == '4':
        employees_data = generate_initial_csv_with_turnover()
        display_sample_data(employees_data)
    
    else:
        print("无效选项")

if __name__ == "__main__":
    main()