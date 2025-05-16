#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
员工数据每日更新脚本 (HR离职分析版)

此脚本用于每日更新员工数据库，实现：
- 根据公司规模生成合理的每日入职和离职记录
- 维护历史数据完整性
- 记录数据库刷新日期
- 支持手动设置更新日期（用于补充历史数据）
- 提供数据更新日志
"""

import pandas as pd
import numpy as np
import mysql.connector
from faker import Faker
from datetime import datetime, timedelta
import random
import os
import argparse
import logging

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('employee_updates.log'),
        logging.StreamHandler()
    ]
)

# 设置随机种子以确保可重复性
random.seed(datetime.now().timestamp())
np.random.seed(int(datetime.now().timestamp()))

# 设置中文和英文随机数据生成器
fake = Faker('zh_CN')
fake_en = Faker()

# 数据库连接配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Taylor@1989',  # 修改为您的MySQL密码
    'database': 'employee_db',
    'port': 3306
}

# 部门设置
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

# 薪资水平设置
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

# 星期与入职/离职率的关系（周一至周日）
WEEKDAY_HIRE_FACTOR = [1.2, 1.0, 1.0, 0.9, 0.8, 0.2, 0.1]  # 周一入职最多，周末很少
WEEKDAY_TERM_FACTOR = [1.0, 0.9, 0.9, 1.1, 1.3, 0.4, 0.1]  # 周五离职较多，周末很少

# 月份与入职/离职率的关系（1月至12月）
MONTH_HIRE_FACTOR = [1.2, 1.0, 1.0, 1.1, 1.0, 1.2, 0.8, 0.9, 1.3, 1.0, 1.0, 0.7]  # 9月、1月、6月入职高峰
MONTH_TERM_FACTOR = [0.8, 0.9, 1.0, 1.0, 1.1, 1.2, 1.0, 0.9, 0.8, 0.9, 1.0, 1.5]  # 12月、6月离职高峰

def get_db_connection():
    """连接到MySQL数据库"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        logging.error(f"数据库连接失败: {e}")
        return None

def get_current_employee_count():
    """获取当前在职员工数量"""
    conn = get_db_connection()
    if not conn:
        return 0
    
    cursor = conn.cursor()
    try:
        # 获取在职员工数量
        cursor.execute("SELECT COUNT(*) FROM employees WHERE `left` = 0")
        result = cursor.fetchone()
        employee_count = result[0] if result else 0
        
        # 获取总历史员工数量
        cursor.execute("SELECT COUNT(*) FROM employees")
        result = cursor.fetchone()
        total_count = result[0] if result else 0
        
        # 获取最新的员工ID
        cursor.execute("SELECT MAX(employee_id) FROM employees")
        result = cursor.fetchone()
        max_id = result[0] if result else 1000
        
        cursor.close()
        conn.close()
        return employee_count, total_count, max_id
    except mysql.connector.Error as e:
        logging.error(f"获取员工数量失败: {e}")
        cursor.close()
        conn.close()
        return 0, 0, 1000

def get_last_update_date():
    """获取最后更新日期"""
    conn = get_db_connection()
    if not conn:
        return None
    
    cursor = conn.cursor()
    try:
        # 先检查是否存在last_update表
        cursor.execute("SHOW TABLES LIKE 'last_update'")
        if not cursor.fetchone():
            # 如果表不存在，创建它
            cursor.execute("""
            CREATE TABLE last_update (
                id INT PRIMARY KEY AUTO_INCREMENT,
                update_date DATE NOT NULL,
                updated_at DATETIME NOT NULL
            )
            """)
            conn.commit()
            cursor.close()
            conn.close()
            return None
        
        # 获取最后更新日期
        cursor.execute("SELECT update_date FROM last_update ORDER BY id DESC LIMIT 1")
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result:
            return result[0]
        return None
    except mysql.connector.Error as e:
        logging.error(f"获取最后更新日期失败: {e}")
        cursor.close()
        conn.close()
        return None

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

def generate_satisfaction_level():
    """生成员工满意度"""
    return np.clip(np.random.beta(5, 2) * 0.85 + 0.15, 0.1, 1.0)

def generate_evaluation_score():
    """生成评估分数"""
    return np.clip(np.random.beta(7, 3) * 0.7 + 0.35, 0.36, 1.0)

def generate_project_count():
    """生成项目数量，通常为2-4个对于新员工"""
    return random.choice([2, 3, 3, 3, 4])

def generate_monthly_hours(project_count):
    """生成月均工作小时"""
    base_hours = int(np.random.normal(201, 25))
    hours_adjustment = (project_count - 3) * 8
    hours = base_hours + hours_adjustment
    return max(150, min(250, hours))

def generate_department():
    """生成部门，基于预设分布"""
    return random.choices(
        list(DEPARTMENTS.keys()),
        weights=list(DEPARTMENTS.values()),
        k=1
    )[0]

def generate_salary_level():
    """生成薪资水平，基于预设分布"""
    return random.choices(
        list(SALARY_LEVELS.keys()),
        weights=list(SALARY_LEVELS.values()),
        k=1
    )[0]

def generate_actual_salary(salary_level):
    """生成实际薪资"""
    salary_range = SALARY_RANGES[salary_level]
    return random.randint(salary_range['min'], salary_range['max']) * 1000

def calculate_daily_changes(employee_count, update_date):
    """计算每日的入职和离职人数"""
    # 计算基础比率：每1000名员工每日的入职和离职人数
    base_hire_rate = 1.8   # 每天每1000名员工1.8人入职
    base_term_rate = 1.5   # 每天每1000名员工1.5人离职
    
    # 根据公司规模调整比率（大公司流动性降低）
    company_size_factor = 1.0
    if employee_count > 5000:
        company_size_factor = 0.9
    elif employee_count > 10000:
        company_size_factor = 0.8
    elif employee_count < 1000:
        company_size_factor = 1.2
    
    # 根据星期几调整比率
    weekday = update_date.weekday()  # 0=周一, 6=周日
    weekday_hire = WEEKDAY_HIRE_FACTOR[weekday]
    weekday_term = WEEKDAY_TERM_FACTOR[weekday]
    
    # 根据月份调整比率
    month = update_date.month - 1  # 0=1月, 11=12月
    month_hire = MONTH_HIRE_FACTOR[month]
    month_term = MONTH_TERM_FACTOR[month]
    
    # 加入随机波动（±20%）
    random_factor_hire = random.uniform(0.8, 1.2)
    random_factor_term = random.uniform(0.8, 1.2)
    
    # 计算最终每日变化
    daily_hires = int(employee_count * base_hire_rate / 1000 * company_size_factor * 
                      weekday_hire * month_hire * random_factor_hire)
    
    daily_terminations = int(employee_count * base_term_rate / 1000 * company_size_factor * 
                            weekday_term * month_term * random_factor_term)
    
    # 确保至少有一人变动（如果公司不太小）
    if employee_count > 500:
        daily_hires = max(1, daily_hires)
        daily_terminations = max(1, daily_terminations)
    
    # 特殊情况：节假日、年底
    # 简单处理：如果是12月底，离职率提高
    if update_date.month == 12 and update_date.day > 25:
        daily_terminations = int(daily_terminations * 1.5)
    
    # 如果是元旦附近，入职率提高
    if update_date.month == 1 and update_date.day < 5:
        daily_hires = int(daily_hires * 1.3)
    
    return daily_hires, daily_terminations

def select_employees_for_termination(count, update_date):
    """从数据库中选择可能离职的员工"""
    conn = get_db_connection()
    if not conn:
        return []
    
    cursor = conn.cursor(dictionary=True)
    try:
        # 查询在职员工，加权离职概率较高的员工
        query = """
        SELECT employee_id, name, department, satisfaction_level, last_evaluation, 
               number_project, average_monthly_hours, time_spend_company, 
               Work_accident, promotion_last_5years, hire_date
        FROM employees 
        WHERE `left` = 0 
        ORDER BY (
            CASE 
                WHEN satisfaction_level < 0.3 THEN 5
                WHEN satisfaction_level < 0.5 THEN 3
                WHEN satisfaction_level > 0.8 THEN 0.5
                ELSE 1
            END + 
            CASE
                WHEN number_project > 5 THEN 3
                WHEN number_project < 3 THEN 1.5
                ELSE 1
            END +
            CASE
                WHEN average_monthly_hours > 250 THEN 2
                WHEN average_monthly_hours < 150 THEN 1.5
                ELSE 1
            END +
            CASE
                WHEN time_spend_company > 5 THEN 1.5
                WHEN time_spend_company < 1 THEN 0.8
                ELSE 1
            END +
            CASE
                WHEN promotion_last_5years = 1 THEN 0.5
                ELSE 1
            END +
            CASE
                WHEN Work_accident = 1 THEN 0.7
                ELSE 1
            END
        ) * RAND() DESC
        LIMIT %s
        """
        
        cursor.execute(query, (count * 2,))  # 获取两倍数量的候选人
        candidates = cursor.fetchall()
        
        # 从候选人中随机选择需要的数量，但权重较高的更可能被选中
        selected = []
        if candidates:
            # 使用简单的加权随机选择
            weights = []
            for i, emp in enumerate(candidates):
                # 计算权重因子：满意度低、工作量大、工作年限长的员工更可能离职
                weight = 1.0
                if emp['satisfaction_level'] < 0.3:
                    weight *= 2.0
                if emp['average_monthly_hours'] > 240:
                    weight *= 1.5
                if emp['number_project'] > 5:
                    weight *= 1.8
                weights.append(weight)
            
            # 归一化权重
            total_weight = sum(weights)
            normalized_weights = [w / total_weight for w in weights]
            
            # 选择员工
            indices = np.random.choice(
                range(len(candidates)), 
                size=min(count, len(candidates)), 
                replace=False, 
                p=normalized_weights
            )
            selected = [candidates[i] for i in indices]
        
        cursor.close()
        conn.close()
        return selected
    except mysql.connector.Error as e:
        logging.error(f"选择离职员工失败: {e}")
        cursor.close()
        conn.close()
        return []

def generate_new_hire(emp_id, hire_date):
    """生成新员工数据"""
    satisfaction = round(generate_satisfaction_level(), 2)
    evaluation = round(generate_evaluation_score(), 2)
    projects = generate_project_count()
    monthly_hours = generate_monthly_hours(projects)
    accident = 0  # 新员工无事故记录
    promotion = 0  # 新员工无晋升记录
    years = 0  # 新员工工作年限为0
    department = generate_department()
    salary_level = generate_salary_level()
    
    turnover_prob = calculate_turnover_probability(
        satisfaction, evaluation, projects, monthly_hours, 
        years, accident, promotion
    )
    
    employee = {
        'employee_id': emp_id,
        'name': fake_en.name(),
        'department': department,
        'salary_level': salary_level,
        'actual_salary': generate_actual_salary(salary_level),
        'left': 0,
        'satisfaction_level': satisfaction,
        'last_evaluation': evaluation,
        'number_project': projects,
        'average_monthly_hours': monthly_hours,
        'time_spend_company': years,
        'Work_accident': accident,
        'promotion_last_5years': promotion,
        'hire_date': hire_date.strftime('%Y-%m-%d'),
        'termination_date': None,
        'turnover_probability': round(turnover_prob, 3),
        'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    return employee

def update_employee_database(update_date=None):
    """更新员工数据库"""
    # 如果未指定更新日期，使用当前日期
    if update_date is None:
        update_date = datetime.now().date()
    
    # 获取最后更新日期
    last_update = get_last_update_date()
    
    # 如果最后更新日期与当前更新日期相同，则不处理
    if last_update and last_update == update_date:
        logging.info(f"数据库已经在 {update_date} 更新过，不再重复更新")
        return False
    
    # 获取当前员工数量
    employee_count, total_count, max_id = get_current_employee_count()
    
    if employee_count == 0:
        logging.error("无法获取员工数量或数据库为空")
        return False
    
    # 计算当日变动
    daily_hires, daily_terminations = calculate_daily_changes(employee_count, update_date)
    
    logging.info(f"更新日期: {update_date}")
    logging.info(f"当前在职员工: {employee_count}, 总历史员工: {total_count}")
    logging.info(f"生成 {daily_hires} 名新员工, {daily_terminations} 名员工离职")
    
    # 选择离职的员工
    terminating_employees = select_employees_for_termination(daily_terminations, update_date)
    
    if len(terminating_employees) < daily_terminations:
        logging.warning(f"只找到 {len(terminating_employees)} 名员工离职，少于计划的 {daily_terminations} 名")
    
    # 生成新员工
    new_employees = []
    for i in range(daily_hires):
        new_id = max_id + i + 1
        new_emp = generate_new_hire(new_id, update_date)
        new_employees.append(new_emp)
    
    # 更新数据库
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    try:
        # 1. 更新离职员工
        for emp in terminating_employees:
            update_query = """
            UPDATE employees 
            SET `left` = 1, 
                termination_date = %s,
                last_updated = %s,
                turnover_probability = 1.0
            WHERE employee_id = %s
            """
            cursor.execute(update_query, (
                update_date.strftime('%Y-%m-%d'),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                emp['employee_id']
            ))
        
        # 2. 插入新员工
        insert_query = """
        INSERT INTO employees (
            employee_id, name, department, salary_level, actual_salary, 
            `left`, satisfaction_level, last_evaluation, number_project, 
            average_monthly_hours, time_spend_company, Work_accident, 
            promotion_last_5years, hire_date, termination_date, 
            turnover_probability, last_updated
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for emp in new_employees:
            cursor.execute(insert_query, (
                emp['employee_id'], emp['name'], emp['department'], emp['salary_level'], emp['actual_salary'],
                emp['left'], emp['satisfaction_level'], emp['last_evaluation'], emp['number_project'],
                emp['average_monthly_hours'], emp['time_spend_company'], emp['Work_accident'],
                emp['promotion_last_5years'], emp['hire_date'], emp['termination_date'],
                emp['turnover_probability'], emp['last_updated']
            ))
        
        # 3. 更新所有在职员工的工作年限和其他参数
        update_all_query = """
        UPDATE employees
        SET time_spend_company = TIMESTAMPDIFF(YEAR, hire_date, %s),
            last_updated = %s
        WHERE `left` = 0
        """
        cursor.execute(update_all_query, (
            update_date.strftime('%Y-%m-%d'),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))
        
        # 4. 更新last_update表
        update_date_query = """
        INSERT INTO last_update (update_date, updated_at)
        VALUES (%s, %s)
        """
        cursor.execute(update_date_query, (
            update_date.strftime('%Y-%m-%d'),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"数据库更新成功: {len(new_employees)} 名新员工, {len(terminating_employees)} 名员工离职")
        return True
    except mysql.connector.Error as e:
        logging.error(f"数据库更新失败: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return False

def generate_date_range(start_date_str, end_date_str=None):
    """生成日期范围"""
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    
    if end_date_str:
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    else:
        end_date = datetime.now().date()
    
    date_list = []
    current_date = start_date
    
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += timedelta(days=1)
    
    return date_list

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='员工数据每日更新')
    parser.add_argument('--date', type=str, help='指定更新日期 (YYYY-MM-DD 格式)')
    parser.add_argument('--start-date', type=str, help='批量更新起始日期 (YYYY-MM-DD 格式)')
    parser.add_argument('--end-date', type=str, help='批量更新结束日期 (YYYY-MM-DD 格式)')
    
    args = parser.parse_args()
    
    if args.start_date:
        # 批量更新模式
        if not args.end_date:
            end_date = datetime.now().date().strftime('%Y-%m-%d')
        else:
            end_date = args.end_date
        
        date_range = generate_date_range(args.start_date, end_date)
        logging.info(f"批量更新模式: 从 {args.start_date} 到 {end_date}, 共 {len(date_range)} 天")
        
        for single_date in date_range:
            logging.info(f"正在更新: {single_date}")
            update_employee_database(single_date)
    elif args.date:
        # 单日更新模式
        date_obj = datetime.strptime(args.date, '%Y-%m-%d').date()
        logging.info(f"单日更新模式: {date_obj}")
        update_employee_database(date_obj)
    else:
        # 当前日期更新模式
        logging.info("更新模式: 当前日期")
        update_employee_database()

if __name__ == "__main__":
    main()