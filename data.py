#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
员工数据生成器与MySQL导入脚本（HR离职分析版）

此脚本基于真实HR离职数据集特征生成员工数据，并自动导入MySQL数据库。
特点：
- 生成约11500名在职员工和3500名历史离职员工（共约15000条记录）
- 包含10个部门（sales, technical, support, IT等）
- 离职率约为23.8%，符合原始数据集特征
- 满意度与离职率负相关（满意度低的员工更容易离职）
- 工作项目数与离职的非线性关系（过多或过少项目的员工更易离职）
- 直接导入MySQL，无需用户交互
- 生成的数据分布与真实数据集一致
- 控制new_hires和terminations的年度变化不超过20%，并应用平滑机制
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
TOTAL_EMPLOYEES = 15000  # 总员工数量
TARGET_TURNOVER_RATE = 0.238  # 目标离职率 23.8%
HISTORICAL_LEAVERS = int(TOTAL_EMPLOYEES * TARGET_TURNOVER_RATE)  # 历史离职员工数量
CURRENT_EMPLOYEES = TOTAL_EMPLOYEES - HISTORICAL_LEAVERS  # 当前在职员工数量

# 新增：年度范围和控制参数
START_YEAR = 2005
END_YEAR = 2025
MAX_ANNUAL_CHANGE = 0.20  # 年度变化最大20%
SMOOTHING_FACTOR = 0.5  # 平滑因子，用于移动平均

# 部门设置 - 基于原始数据集中的分布
DEPARTMENTS = {
    'sales': 0.276,         # 27.6%
    'technical': 0.181,     # 18.1%
    'support': 0.149,       # 14.9%
    'IT': 0.082,            # 8.2%
    'product_mng': 0.060,   # 6.0%
    'marketing': 0.057,     # 5.7%
    'RandD': 0.052,         # 5.2%
    'accounting': 0.051,    # 5.1%
    'hr': 0.049,            # 4.9%
    'management': 0.042     # 4.2%
}

# 薪资水平设置 - 基于原始数据集
SALARY_LEVELS = {
    'low': 0.488,     # 48.8%
    'medium': 0.430,  # 43.0%
    'high': 0.082     # 8.2%
}

# 薪资范围（单位：千元/年）
SALARY_RANGES = {
    'low': {'min': 80, 'max': 150},
    'medium': {'min': 150, 'max': 250},
    'high': {'min': 250, 'max': 500}
}

# 项目数量分布 - 基于原始数据集
PROJECT_DISTRIBUTION = {
    2: 0.159,  # 15.9%
    3: 0.270,  # 27.0%
    4: 0.291,  # 29.1%
    5: 0.184,  # 18.4%
    6: 0.078,  # 7.8%
    7: 0.017   # 1.7%
}

# 工作年限分布 - 基于原始数据集
YEARS_DISTRIBUTION = {
    2: 0.216,  # 21.6%
    3: 0.430,  # 43.0%
    4: 0.170,  # 17.0%
    5: 0.098,  # 9.8%
    6: 0.048,  # 4.8%
    7: 0.013,  # 1.3%
    8: 0.011,  # 1.1%
    10: 0.014  # 1.4%
}

def generate_employee_ids(count):
    """生成唯一的员工ID"""
    return random.sample(range(1000, 100000), count)

def calculate_turnover_probability(satisfaction_score, evaluation_score, project_count, monthly_hours, years, accident, promotion):
    """根据多个因素计算离职概率，基于原始数据集的特征"""
    prob = 0.238  # 基础离职率
    
    if satisfaction_score < 0.2:
        prob += 0.5
    elif satisfaction_score < 0.4:
        prob += 0.3
    elif satisfaction_score > 0.7:
        prob -= 0.2
    
    if project_count <= 2:
        prob += 0.1
    elif project_count >= 6:
        prob += 0.4
    
    if monthly_hours < 150:
        prob -= 0.05
    elif monthly_hours > 250:
        prob += 0.2
    
    if years > 5:
        prob += 0.1
    
    if evaluation_score < 0.5:
        prob += 0.1
    elif 0.6 < evaluation_score < 0.8:
        prob -= 0.05
    elif evaluation_score > 0.8 and monthly_hours > 220:
        prob += 0.2
    
    if accident == 1:
        prob -= 0.15
    
    if promotion == 1:
        prob -= 0.3
    
    return max(0.0, min(1.0, prob))

def generate_satisfaction_level(is_leaver):
    """生成员工满意度"""
    if is_leaver:
        return np.clip(np.random.beta(2, 3) * 0.9 + 0.1, 0.09, 1.0)
    else:
        return np.clip(np.random.beta(5, 2) * 0.85 + 0.15, 0.1, 1.0)

def generate_evaluation_score(is_leaver):
    """生成评估分数"""
    return np.clip(np.random.beta(7, 3) * 0.7 + 0.35, 0.36, 1.0)

def generate_project_count(is_leaver):
    """生成项目数量，2-7个，基于原始数据分布"""
    projects = random.choices(
        list(PROJECT_DISTRIBUTION.keys()),
        weights=list(PROJECT_DISTRIBUTION.values()),
        k=1
    )[0]
    
    if is_leaver and random.random() < 0.4:
        if random.random() < 0.5:
            return min(7, projects + 2)
        else:
            return max(2, projects - 1)
    
    return projects

def generate_monthly_hours(is_leaver, project_count):
    """生成月均工作小时"""
    base_hours = int(np.random.normal(201, 30))
    hours_adjustment = (project_count - 3.8) * 10
    if is_leaver:
        hours_adjustment += 8
    hours = base_hours + hours_adjustment
    return max(96, min(310, hours))

def generate_years_at_company():
    """生成在职年限，根据原始数据分布"""
    return random.choices(
        list(YEARS_DISTRIBUTION.keys()),
        weights=list(YEARS_DISTRIBUTION.values()),
        k=1
    )[0]

def generate_work_accident(is_leaver):
    """生成工作事故记录，离职员工事故率更低(4.73% vs 17.50%)"""
    if is_leaver:
        return 1 if random.random() < 0.0473 else 0
    else:
        return 1 if random.random() < 0.1750 else 0

def generate_promotion(is_leaver):
    """生成晋升记录，离职员工晋升率更低(0.53% vs 2.63%)"""
    if is_leaver:
        return 1 if random.random() < 0.0053 else 0
    else:
        return 1 if random.random() < 0.0263 else 0

def generate_department():
    """生成部门，基于原始数据分布"""
    return random.choices(
        list(DEPARTMENTS.keys()),
        weights=list(DEPARTMENTS.values()),
        k=1
    )[0]

def generate_salary_level():
    """生成薪资水平，基于原始数据分布"""
    return random.choices(
        list(SALARY_LEVELS.keys()),
        weights=list(SALARY_LEVELS.values()),
        k=1
    )[0]

def generate_actual_salary(salary_level):
    """生成实际薪资"""
    salary_range = SALARY_RANGES[salary_level]
    return random.randint(salary_range['min'], salary_range['max']) * 1000

def smooth_values(values, smoothing_factor):
    """应用平滑机制（移动平均）"""
    smoothed = []
    for i in range(len(values)):
        if i == 0:
            smoothed.append(values[i])
        else:
            smoothed.append(int(smoothed[-1] * (1 - smoothing_factor) + values[i] * smoothing_factor))
    return smoothed

def control_annual_change(values, max_change):
    """限制年度变化幅度"""
    controlled = [values[0]]
    for i in range(1, len(values)):
        prev = controlled[-1]
        current = values[i]
        max_allowed = prev * (1 + max_change)
        min_allowed = prev * (1 - max_change)
        controlled.append(int(max(min_allowed, min(max_allowed, current))))
    return controlled

def generate_hire_date(years_at_company, termination_date=None):
    """生成入职日期，考虑离职日期"""
    if termination_date:
        term_date = datetime.strptime(termination_date, '%Y-%m-%d')
        hire_date = term_date - timedelta(days=years_at_company * 365 + random.randint(-180, 180))
    else:
        current_date = datetime.now()
        hire_date = current_date - timedelta(days=years_at_company * 365 + random.randint(-180, 180))
    return hire_date.strftime('%Y-%m-%d')

def generate_termination_date(is_leaver):
    """生成离职日期"""
    if is_leaver:
        start_date = datetime(2020, 1, 1)
        end_date = datetime.now() - timedelta(days=30)
        time_diff = (end_date - start_date).days
        random_days = random.randint(0, time_diff)
        termination_date = start_date + timedelta(days=random_days)
        return termination_date.strftime('%Y-%m-%d')
    return None

def generate_employee_data():
    """生成所有员工数据，并控制new_hires和terminations"""
    print(f"生成数据：目标离职率 {TARGET_TURNOVER_RATE:.1%}，总员工 {TOTAL_EMPLOYEES} 人")
    print(f"目标在职员工：{CURRENT_EMPLOYEES} 人，历史离职员工：{HISTORICAL_LEAVERS} 人")
    
    employee_ids = generate_employee_ids(TOTAL_EMPLOYEES)
    leaver_ids = set(employee_ids[:HISTORICAL_LEAVERS])
    
    employees_data = []
    
    # 计算每年的new_hires和terminations目标
    years_range = range(START_YEAR, END_YEAR + 1)
    num_years = len(years_range)
    
    # 初始值（参考你的表格2005年的数据）
    base_new_hires = 988  # 2005年的new_hires
    base_terminations = 589  # 2005年的terminations
    
    # 生成每年的new_hires和terminations
    raw_new_hires = []
    raw_terminations = []
    
    for year in years_range:
        if year == START_YEAR:
            raw_new_hires.append(base_new_hires)
            raw_terminations.append(base_terminations)
        else:
            # 模拟增长趋势，同时添加随机波动
            prev_new_hires = raw_new_hires[-1]
            prev_terminations = raw_terminations[-1]
            
            # 假设一个基础增长率（每年增长2-5%），加上小的随机波动
            growth_rate_new_hires = np.random.uniform(0.02, 0.05)
            growth_rate_terminations = np.random.uniform(0.02, 0.05)
            
            new_hires = int(prev_new_hires * (1 + growth_rate_new_hires + np.random.uniform(-0.05, 0.05)))
            terminations = int(prev_terminations * (1 + growth_rate_terminations + np.random.uniform(-0.05, 0.05)))
            
            raw_new_hires.append(new_hires)
            raw_terminations.append(terminations)
    
    # 应用控制和平滑
    controlled_new_hires = control_annual_change(raw_new_hires, MAX_ANNUAL_CHANGE)
    controlled_terminations = control_annual_change(raw_terminations, MAX_ANNUAL_CHANGE)
    
    smoothed_new_hires = smooth_values(controlled_new_hires, SMOOTHING_FACTOR)
    smoothed_terminations = smooth_values(controlled_terminations, SMOOTHING_FACTOR)
    
    # 计算每年实际的员工分配
    total_new_hires = sum(smoothed_new_hires)
    total_terminations = sum(smoothed_terminations)
    
    # 按比例分配员工
    # 修改：分配所有员工（包括leavers）到hire_counts，因为所有员工都需要一个hire_date
    hire_counts = [int((nh / total_new_hires) * TOTAL_EMPLOYEES) for nh in smoothed_new_hires]
    termination_counts = [int((t / total_terminations) * HISTORICAL_LEAVERS) for t in smoothed_terminations]
    
    # 调整以确保总数精确
    hire_counts[-1] += TOTAL_EMPLOYEES - sum(hire_counts)
    termination_counts[-1] += HISTORICAL_LEAVERS - sum(termination_counts)
    
    # 分配员工到各年
    hire_indices = []
    termination_indices = []
    current_index = 0
    
    for i, year in enumerate(years_range):
        # 分配new_hires
        for _ in range(hire_counts[i]):
            hire_indices.append((current_index, year))
            current_index += 1
    
    current_index = 0
    for i, year in enumerate(years_range):
        # 分配terminations（只针对leavers）
        for _ in range(termination_counts[i]):
            termination_indices.append((current_index, year))
            current_index += 1
    
    # 打乱索引以随机分配
    random.shuffle(hire_indices)
    random.shuffle(termination_indices)
    
    hire_map = {idx: year for idx, year in hire_indices}
    termination_map = {idx: year for idx, year in termination_indices}
    
    # 生成员工数据
    for i, emp_id in enumerate(employee_ids):
        is_leaver = emp_id in leaver_ids
        
        satisfaction = round(generate_satisfaction_level(is_leaver), 2)
        evaluation = round(generate_evaluation_score(is_leaver), 2)
        projects = generate_project_count(is_leaver)
        years = generate_years_at_company()
        accident = generate_work_accident(is_leaver)
        promotion = generate_promotion(is_leaver)
        department = generate_department()
        salary_level = generate_salary_level()
        
        monthly_hours = generate_monthly_hours(is_leaver, projects)
        
        # 根据分配的年份生成日期
        hire_year = hire_map[i]  # 现在hire_map包含所有15000个员工的索引
        hire_date = f"{hire_year}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        
        if is_leaver:
            # 只有leavers使用termination_map
            term_index = list(leaver_ids).index(emp_id)  # 获取该leaver在leaver_ids中的索引
            term_year = termination_map[term_index]
            termination_date = f"{term_year}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
            
            # 确保termination_date晚于hire_date
            hire_dt = datetime.strptime(hire_date, '%Y-%m-%d')
            term_dt = datetime.strptime(termination_date, '%Y-%m-%d')
            if term_dt <= hire_dt:
                term_dt = hire_dt + timedelta(days=years * 365 + random.randint(1, 180))
                termination_date = term_dt.strftime('%Y-%m-%d')
        else:
            termination_date = None
        
        turnover_prob = calculate_turnover_probability(
            satisfaction, evaluation, projects, monthly_hours, 
            years, accident, promotion
        )
        
        left_value = 1 if is_leaver else 0
        
        employee = {
            'employee_id': emp_id,
            'name': fake_en.name(),
            'department': department,
            'salary_level': salary_level,
            'actual_salary': generate_actual_salary(salary_level),
            'left': left_value,
            'satisfaction_level': satisfaction,
            'last_evaluation': evaluation,
            'number_project': projects,
            'average_monthly_hours': monthly_hours,
            'time_spend_company': years,
            'Work_accident': accident,
            'promotion_last_5years': promotion,
            'hire_date': hire_date,
            'termination_date': termination_date,
            'turnover_probability': round(turnover_prob, 3),
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        employees_data.append(employee)
    
    # 计算实际离职率
    actual_leavers = sum(1 for e in employees_data if e['left'] == 1)
    actual_turnover_rate = actual_leavers / len(employees_data)
    print(f"实际离职率: {actual_turnover_rate:.2%} ({actual_leavers}/{len(employees_data)})")
    
    # 输出年度统计
    annual_stats = {year: {'new_hires': 0, 'terminations': 0} for year in years_range}
    for emp in employees_data:
        hire_year = int(emp['hire_date'].split('-')[0])
        annual_stats[hire_year]['new_hires'] += 1
        if emp['termination_date']:
            term_year = int(emp['termination_date'].split('-')[0])
            annual_stats[term_year]['terminations'] += 1
    
    # 计算headcount
    headcount = 0
    annual_headcount = {}
    for year in years_range:
        headcount = headcount + annual_stats[year]['new_hires'] - annual_stats[year]['terminations']
        annual_headcount[year] = headcount
    
    print("\n===== 年度统计 =====")
    print("year, headcount, new_hires, terminations")
    for year in years_range:
        print(f"{year}, {annual_headcount[year]}, {annual_stats[year]['new_hires']}, {annual_stats[year]['terminations']}")
    
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
            `left` TINYINT,
            satisfaction_level FLOAT,
            last_evaluation FLOAT,
            number_project INT,
            average_monthly_hours INT,
            time_spend_company INT,
            Work_accident TINYINT,
            promotion_last_5years TINYINT,
            hire_date DATE,
            termination_date DATE,
            turnover_probability FLOAT,
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
        backup_file = "employee_data_turnover.csv"
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
            `left` TINYINT,
            satisfaction_level FLOAT,
            last_evaluation FLOAT,
            number_project INT,
            average_monthly_hours INT,
            time_spend_company INT,
            Work_accident TINYINT,
            promotion_last_5years TINYINT,
            hire_date DATE,
            termination_date DATE,
            turnover_probability FLOAT,
            last_updated DATETIME
        )
        """)
        
        insert_sql = """
        INSERT INTO employees (
            employee_id, name, department, salary_level, actual_salary, 
            `left`, satisfaction_level, last_evaluation, number_project, 
            average_monthly_hours, time_spend_company, Work_accident, 
            promotion_last_5years, hire_date, termination_date, 
            turnover_probability, last_updated
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            department = VALUES(department),
            salary_level = VALUES(salary_level),
            actual_salary = VALUES(actual_salary),
            `left` = VALUES(`left`),
            satisfaction_level = VALUES(satisfaction_level),
            last_evaluation = VALUES(last_evaluation),
            number_project = VALUES(number_project),
            average_monthly_hours = VALUES(average_monthly_hours),
            time_spend_company = VALUES(time_spend_company),
            Work_accident = VALUES(Work_accident),
            promotion_last_5years = VALUES(promotion_last_5years),
            hire_date = VALUES(hire_date),
            termination_date = VALUES(termination_date),
            turnover_probability = VALUES(turnover_probability),
            last_updated = VALUES(last_updated)
        """
        
        data_to_insert = [(
            emp['employee_id'], emp['name'], emp['department'], emp['salary_level'], emp['actual_salary'],
            emp['left'], emp['satisfaction_level'], emp['last_evaluation'], emp['number_project'],
            emp['average_monthly_hours'], emp['time_spend_company'], emp['Work_accident'],
            emp['promotion_last_5years'], emp['hire_date'], emp['termination_date'],
            emp['turnover_probability'], emp['last_updated']
        ) for emp in employees_data]

        batch_size = 1000
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

def save_to_csv(employees_data, filename='employee_data_turnover.csv'):
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
        'employee_id', 'name', 'department', 'salary_level', 
        'left', 'satisfaction_level', 'last_evaluation', 'number_project',
        'average_monthly_hours', 'time_spend_company', 'Work_accident',
        'promotion_last_5years'
    ]
    print("\n===== 数据样例 (随机选择的10条记录) =====")
    print(df[display_columns].to_string())

    print("\n===== 数据统计信息 =====")
    print(f"总记录数: {len(employees_data)}")
    turnover_count = sum(1 for emp in employees_data if emp['left'] == 1)
    active_count = len(employees_data) - turnover_count
    turnover_rate = turnover_count / len(employees_data)
    print(f"在职员工: {active_count}, 历史离职员工: {turnover_count}")
    print(f"总离职率: {turnover_rate:.2%}")

    dept_counts = pd.Series([emp['department'] for emp in employees_data]).value_counts()
    print("\n部门分布:")
    for dept, count in dept_counts.items():
        print(f"  {dept}: {count}人 ({count/len(employees_data):.2%})")

    salary_counts = pd.Series([emp['salary_level'] for emp in employees_data]).value_counts()
    print("\n薪资水平分布:")
    for level, count in salary_counts.items():
        print(f"  {level}: {count}人 ({count/len(employees_data):.2%})")

    all_satisfaction = [emp['satisfaction_level'] for emp in employees_data]
    active_satisfaction = [emp['satisfaction_level'] for emp in employees_data if emp['left'] == 0]
    leaving_satisfaction = [emp['satisfaction_level'] for emp in employees_data if emp['left'] == 1]
    
    print(f"\n满意度统计:")
    print(f"  总体平均满意度: {np.mean(all_satisfaction):.3f}")
    print(f"  在职员工平均满意度: {np.mean(active_satisfaction):.3f}")
    print(f"  离职员工平均满意度: {np.mean(leaving_satisfaction):.3f}")

    project_counts = pd.Series([emp['number_project'] for emp in employees_data]).value_counts().sort_index()
    print("\n项目数量分布:")
    for projects, count in project_counts.items():
        print(f"  {projects}个项目: {count}人 ({count/len(employees_data):.2%})")
    
    years_counts = pd.Series([emp['time_spend_company'] for emp in employees_data]).value_counts().sort_index()
    print("\n工作年限分布:")
    for years, count in years_counts.items():
        print(f"  {years}年: {count}人 ({count/len(employees_data):.2%})")

    print("\n按项目数量的离职率:")
    for project_count in sorted(set([emp['number_project'] for emp in employees_data])):
        project_employees = [emp for emp in employees_data if emp['number_project'] == project_count]
        if project_employees:
            left_count = sum(1 for emp in project_employees if emp['left'] == 1)
            project_turnover = left_count / len(project_employees)
            print(f"  {project_count}个项目: {project_turnover:.2%} 离职率 ({left_count}/{len(project_employees)})")

    hour_bins = [(0, 150, "低工时"), (150, 220, "正常工时"), (220, 350, "高工时")]
    print("\n按工作时长的离职率:")
    for low, high, label in hour_bins:
        in_range = [emp for emp in employees_data if low <= emp['average_monthly_hours'] < high]
        if in_range:
            left_in_range = sum(1 for emp in in_range if emp['left'] == 1)
            rate = left_in_range / len(in_range) * 100
            print(f"  {label} ({low}-{high}小时): {rate:.1f}% 离职率 ({left_in_range}/{len(in_range)})")

    satisfaction_bins = [(0, 0.3, "低满意度"), (0.3, 0.6, "中等满意度"), (0.6, 1.0, "高满意度")]
    print("\n按满意度分层的离职率:")
    for low, high, label in satisfaction_bins:
        in_range = [emp for emp in employees_data if low <= emp['satisfaction_level'] < high]
        if in_range:
            left_in_range = sum(1 for emp in in_range if emp['left'] == 1)
            rate = left_in_range / len(in_range) * 100
            print(f"  {label} ({low}-{high}): {rate:.1f}% 离职率 ({left_in_range}/{len(in_range)})")

    accident_employees = [emp for emp in employees_data if emp['Work_accident'] == 1]
    no_accident_employees = [emp for emp in employees_data if emp['Work_accident'] == 0]
    
    accident_turnover = sum(1 for emp in accident_employees if emp['left'] == 1) / len(accident_employees) if accident_employees else 0
    no_accident_turnover = sum(1 for emp in no_accident_employees if emp['left'] == 1) / len(no_accident_employees) if no_accident_employees else 0
    
    print("\n工作事故与离职率关系:")
    print(f"  有工作事故: {accident_turnover:.2%} 离职率")
    print(f"  无工作事故: {no_accident_turnover:.2%} 离职率")

    promoted_employees = [emp for emp in employees_data if emp['promotion_last_5years'] == 1]
    not_promoted_employees = [emp for emp in employees_data if emp['promotion_last_5years'] == 0]
    
    promoted_turnover = sum(1 for emp in promoted_employees if emp['left'] == 1) / len(promoted_employees) if promoted_employees else 0
    not_promoted_turnover = sum(1 for emp in not_promoted_employees if emp['left'] == 1) / len(not_promoted_employees) if not_promoted_employees else 0
    
    print("\n晋升与离职率关系:")
    print(f"  有晋升: {promoted_turnover:.2%} 离职率")
    print(f"  无晋升: {not_promoted_turnover:.2%} 离职率")

def drop_table_if_exists():
    """删除表（如果存在）"""
    try:
        print("\n尝试删除现有表...")
        conn = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            port=DB_CONFIG['port']
        )
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS employees")
        conn.commit()
        cursor.close()
        conn.close()
        print("表已删除（如果存在）")
        return True
    except mysql.connector.Error as e:
        print(f"删除表失败: {e}")
        return False

# 主执行逻辑
if __name__ == "__main__":
    print("HR离职预测数据生成程序启动")
    print(f"目标：生成 {TOTAL_EMPLOYEES} 条员工记录，离职率 {TARGET_TURNOVER_RATE:.1%}")
    print("数据特征：基于真实HR离职数据集，包含满意度、评估、项目数等关键预测因子")
    
    employees_data = generate_employee_data()
    
    display_sample_data(employees_data)
    
    save_to_csv(employees_data)
    
    try:
        import_choice = input("\n是否要将数据导入到MySQL? (y/n): ").strip().lower()
        if import_choice == 'y':
            drop_table_if_exists()
            if create_database():
                import_to_mysql(employees_data)
        else:
            print("跳过MySQL导入，数据已保存为CSV文件")
    except Exception as e:
        print(f"发生错误: {e}")
        print("数据已保存为CSV文件")