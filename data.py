#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
员工数据生成器与MySQL导入脚本（满意度-离职相关版）

此脚本生成类似HubSpot公司的员工数据，并自动导入MySQL数据库。
特点：
- 生成约6000名在职员工和7200名历史离职员工（共约13200条记录）
- 包含各部门（市场、销售、法务、研发、人力、行政）
- 月离职率5%，每月离职约270-330人
- 满意度与离职率负相关（满意度低的员工更容易离职）
- 离职日期覆盖2014-01-01至2025-03-31
- 直接导入MySQL，无需用户交互
"""

import pandas as pd
import numpy as np
import mysql.connector
from faker import Faker
from datetime import datetime, timedelta
import random
import os

# 设置随机种子以确保可重复性
random.seed(42)
np.random.seed(42)

# 设置中文和英文随机数据生成器
fake = Faker('zh_CN')
fake_en = Faker()

# 数据库连接配置 - 请修改为您的实际配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Taylor@1989',  # 修改为您的MySQL密码
    'database': 'employee_db',
    'port': 3306
}

# 常量设置
TOTAL_EMPLOYEES = 6000  # 当前在职员工数量
HISTORICAL_LEAVERS = 7200  # 历史离职员工数量
MONTHLY_TURNOVER_RATE = (0.045, 0.055)  # 月离职率 4.5%-5.5%
MIN_MONTHLY_LEAVERS = 270  # 每月最少离职人数
MAX_MONTHLY_LEAVERS = 330  # 每月最多离职人数
MIN_MONTHLY_HIRING = 270   # 每月最少入职人数
MAX_MONTHLY_HIRING = 330   # 每月最多入职人数

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

def calculate_turnover_probability(satisfaction_score):
    """根据满意度计算离职概率（负相关）"""
    # Adjusted to make the relationship steeper for a stronger correlation
    return 0.6 - 0.6 * satisfaction_score  # Maps satisfaction (0.1 to 0.9) to turnover probability (0.54 to 0.06)

def generate_employee_data_with_correlation(target_turnover_rate, total_employees, is_historical=False):
    """生成具有满意度-离职相关性的员工数据"""
    employees_data = []
    
    # 满意度分布：增加当前员工和历史离职员工之间的满意度差异
    if is_historical:
        satisfaction_scores = np.random.beta(2, 6, total_employees) * 0.8 + 0.1  # More skewed toward lower satisfaction
    else:
        satisfaction_scores = np.random.beta(4, 2, total_employees) * 0.8 + 0.1  # More skewed toward higher satisfaction
    satisfaction_scores = np.clip(satisfaction_scores, 0.1, 0.9)
    
    # 计算离职概率
    turnover_probabilities = [calculate_turnover_probability(s) for s in satisfaction_scores]
    
    # 按离职概率排序，选择概率最高的员工作为离职者
    sorted_indices = np.argsort(turnover_probabilities)[::-1]
    target_leavers = int(total_employees * target_turnover_rate) if not is_historical else total_employees
    
    leaving_indices = set(sorted_indices[:target_leavers])
    
    employee_ids = generate_employee_ids(total_employees)
    
    for i in range(total_employees):
        is_leaving = i in leaving_indices or is_historical
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
            'satisfaction': round(satisfaction_scores[i], 2),
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
    
    return employees_data

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
    """生成员工数据，使满意度与离职率呈负相关"""
    print(f"生成数据：目标月离职率 5%，总员工 {count} 人")
    
    historical_leavers = HISTORICAL_LEAVERS
    current_employees = count - historical_leavers
    
    print(f"目标在职员工：{current_employees} 人，历史离职员工：{historical_leavers} 人")
    
    current_turnover_rate = 0.05
    current_data = generate_employee_data_with_correlation(current_turnover_rate, current_employees, is_historical=False)
    
    historical_data = generate_employee_data_with_correlation(1.0, historical_leavers, is_historical=True)
    
    employees_data = current_data + historical_data
    
    # Calculate active and leaver counts
    actual_leavers = sum(1 for e in employees_data if e['turnover'] == 1)
    actual_active = count - actual_leavers
    
    print(f"实际生成数据 - 在职员工：{actual_active} 人，离职员工：{actual_leavers} 人")
    
    all_satisfaction = [emp['satisfaction'] for emp in employees_data]
    active_satisfaction = [emp['satisfaction'] for emp in employees_data if emp['turnover'] == 0]
    leaving_satisfaction = [emp['satisfaction'] for emp in employees_data if emp['turnover'] == 1]
    
    print(f"总体平均满意度：{np.mean(all_satisfaction):.3f}")
    print(f"在职员工平均满意度：{np.mean(active_satisfaction):.3f}")
    print(f"离职员工平均满意度：{np.mean(leaving_satisfaction):.3f}")
    
    turnover_values = [emp['turnover'] for emp in employees_data]
    satisfactions = [emp['satisfaction'] for emp in employees_data]
    correlation = np.corrcoef(satisfactions, turnover_values)[0, 1]
    print(f"满意度与离职的相关系数：{correlation:.3f} (应为负数)")
    
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
        print("\n可能的问题：")
        print("1. MySQL服务是否正在运行")
        print("2. 用户名和密码是否正确")
        print("3. 用户是否有创建数据库的权限")
        return False

def import_to_mysql(employees_data):
    """将数据导入MySQL数据库"""
    try:
        print("\n尝试连接MySQL数据库...")
        backup_file = "employee_data_initial.csv"
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

        batch_size = 100
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
    active_count = len(employees_data) - turnover_count
    turnover_rate = turnover_count / len(employees_data)
    print(f"在职员工: {active_count}, 历史离职员工: {turnover_count}")
    print(f"总离职率: {turnover_rate:.2%} (包含历史数据)")
    
    expected_monthly_leavers = active_count * 0.05
    print(f"预期每月离职人数: {expected_monthly_leavers:.0f} 人 (5% 月离职率)")

    dept_counts = pd.Series([emp['department'] for emp in employees_data]).value_counts()
    print("\n部门分布:")
    for dept, count in dept_counts.items():
        print(f"  {dept}: {count}人 ({count/len(employees_data):.2%})")

    salary_counts = pd.Series([emp['salary_level'] for emp in employees_data]).value_counts()
    print("\n薪资水平分布:")
    for level, count in salary_counts.items():
        print(f"  {level}: {count}人 ({count/len(employees_data):.2%})")

    all_satisfaction = [emp['satisfaction'] for emp in employees_data]
    active_satisfaction = [emp['satisfaction'] for emp in employees_data if emp['turnover'] == 0]
    leaving_satisfaction = [emp['satisfaction'] for emp in employees_data if emp['turnover'] == 1]
    
    print(f"\n满意度统计:")
    print(f"  总体平均满意度: {np.mean(all_satisfaction):.3f}")
    print(f"  在职员工平均满意度: {np.mean(active_satisfaction):.3f}")
    print(f"  离职员工平均满意度: {np.mean(leaving_satisfaction):.3f}")
    
    turnover_values = [emp['turnover'] for emp in employees_data]
    satisfactions = [emp['satisfaction'] for emp in employees_data]
    correlation = np.corrcoef(satisfactions, turnover_values)[0, 1]
    print(f"  满意度与离职的相关系数: {correlation:.3f} (负相关)")

    satisfaction_bins = [(0, 0.3, "低满意度"), (0.3, 0.6, "中等满意度"), (0.6, 1.0, "高满意度")]
    print("\n按满意度分层的离职率:")
    for low, high, label in satisfaction_bins:
        in_range = [emp for emp in employees_data if low <= emp['satisfaction'] < high]
        if in_range:
            turnover_in_range = sum(1 for emp in in_range if emp['turnover'] == 1)
            rate = turnover_in_range / len(in_range) * 100
            print(f"  {label} ({low}-{high}): {rate:.1f}% 离职率")

    term_dates = [emp['termination_date'] for emp in employees_data if emp['termination_date']]
    if term_dates:
        term_df = pd.DataFrame({'termination_date': term_dates})
        term_df['year'] = pd.to_datetime(term_df['termination_date']).dt.year
        term_counts = term_df['year'].value_counts().sort_index()
        print("\n离职日期分布 (按年份):")
        for year, count in term_counts.items():
            print(f"  {year}: {count}人")

# 主执行逻辑
if __name__ == "__main__":
    print("员工数据生成程序启动（满意度-离职相关版）")
    print("正在生成约6000名在职员工和7200名历史离职员工...")
    print(f"月离职率目标：5.0% (每月离职 {MIN_MONTHLY_LEAVERS}-{MAX_MONTHLY_LEAVERS} 人)")
    print("满意度与离职率：负相关")
    print("历史离职数据覆盖2014-2025年3月")
    
    # 生成数据
    employees_data = generate_employees_data()
    
    # 显示样本数据
    display_sample_data(employees_data)
    
    # 保存到 CSV
    save_to_csv(employees_data, 'employee_data_initial.csv')
    
    # 导入 MySQL
    if create_database():
        import_to_mysql(employees_data)
    else:
        print("无法导入MySQL，数据已保存为CSV")