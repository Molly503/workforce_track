#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
员工数据生成器与MySQL导入脚本 - 修改版

该脚本使用Faker库生成类似HubSpot公司的员工数据，并自动导入MySQL数据库。
特点:
- 生成约500名员工的数据
- 包含各个部门(市场、销售、法务、研发、人力、行政)
- 历史数据从2014年11月至2025年3月
- 每月约15人离职
- 每月10-15人入职补充
- 包含入职日和离职日列
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
TOTAL_INITIAL_EMPLOYEES = 500  # 初始员工数
MONTHLY_LEAVERS = 15  # 每月约15人离职
MONTHLY_HIRING = (10, 15)  # 每月10-15人入职
START_DATE = datetime(2014, 11, 1)  # 数据开始日期
END_DATE = datetime(2025, 3, 31)    # 数据结束日期

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
    # 确保有足够的ID可供抽样
    id_pool_size = max(count * 2, 200000)
    return random.sample(range(1000, id_pool_size), count)

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

def calculate_years_at_company(hire_date, term_date=None):
    """计算在职年限"""
    # 确保日期是datetime对象
    if isinstance(hire_date, str):
        try:
            hire_date = datetime.strptime(hire_date, '%Y-%m-%d')
        except ValueError:
            print(f"警告: 无法解析入职日期 '{hire_date}'，使用当前日期替代")
            hire_date = datetime.now()
    
    if term_date is None:
        end_date = END_DATE
    else:
        # 确保离职日期是datetime对象
        if isinstance(term_date, str):
            try:
                term_date = datetime.strptime(term_date, '%Y-%m-%d')
            except ValueError:
                print(f"警告: 无法解析离职日期 '{term_date}'，使用当前日期替代")
                term_date = datetime.now()
        end_date = term_date
    
    # 计算天数差并转换为年
    days_employed = (end_date - hire_date).days
    return max(0, int(days_employed / 365))

def generate_specific_date(from_date, to_date=None):
    """在指定日期范围内生成随机日期"""
    if to_date is None:
        to_date = END_DATE
    
    days_range = (to_date - from_date).days
    if days_range <= 0:
        return from_date
    
    random_days = random.randint(0, days_range)
    return from_date + timedelta(days=random_days)

def generate_historical_data():
    """生成包含从2014年11月到2025年3月的历史数据"""
    print(f"生成从 {START_DATE.strftime('%Y-%m-%d')} 到 {END_DATE.strftime('%Y-%m-%d')} 的历史数据")
    
    # 计算月份数量
    months_count = (END_DATE.year - START_DATE.year) * 12 + END_DATE.month - START_DATE.month + 1
    print(f"总计 {months_count} 个月的数据")
    
    # 预计需要的员工数量
    # 初始员工 + 每月新入职 * 月数 (用最大值计算，确保足够)
    estimated_employees = TOTAL_INITIAL_EMPLOYEES + (months_count * MONTHLY_HIRING[1])
    print(f"预计总员工数量: 约 {estimated_employees} 人")
    
    # 初始员工数据 (假设在起始日期前已有的员工)
    active_employees = []
    employee_ids = generate_employee_ids(estimated_employees + 500)  # 额外多生成一些ID以保证足够
    id_counter = 0
    
    # 创建初始员工队伍 (假设所有人都在START_DATE之前入职)
    for i in range(TOTAL_INITIAL_EMPLOYEES):
        # 生成START_DATE之前1-5年的入职日期
        years_before = random.randint(1, 5)
        hire_date = START_DATE - timedelta(days=years_before*365 + random.randint(0, 180))
        
        department = random.choices(list(DEPARTMENTS.keys()), weights=list(DEPARTMENTS.values()), k=1)[0]
        
        employee = {
            'employee_id': employee_ids[id_counter],
            'name': fake_en.name(),
            'department': department,
            'salary_level': random.choices(list(SALARY_LEVELS.keys()), weights=list(SALARY_LEVELS.values()), k=1)[0],
            'actual_salary': random.randint(SALARY_RANGES[department]['min'], SALARY_RANGES[department]['max']) * 1000,
            'turnover': 0,  # 初始都是在职的
            'satisfaction': round(random.uniform(0.6, 0.95), 2),
            'evaluation': generate_evaluation_score(),
            'project_count': random.randint(2, 5),
            'average_monthly_hours': random.randint(150, 250),
            'hire_date': hire_date,
            'termination_date': None,
            'work_accident': 1 if random.random() < 0.1 else 0,
            'promotion': 1 if random.random() < 0.2 else 0,
            'last_updated': START_DATE.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        active_employees.append(employee)
        id_counter += 1
    
    print(f"初始员工数: {len(active_employees)}")
    all_employees = active_employees.copy()
    
    # 按月份生成历史数据
    current_date = START_DATE
    while current_date <= END_DATE:
        month_end = datetime(current_date.year, current_date.month, 
                            28 if current_date.month == 2 else 30)  # 简化月尾日期
        
        # 本月离职的员工
        if len(active_employees) > MONTHLY_LEAVERS:
            # 确定本月实际离职人数 (加入随机波动)
            month_leavers = max(1, int(MONTHLY_LEAVERS * random.uniform(0.8, 1.2)))
            month_leavers = min(month_leavers, len(active_employees) - 300)  # 保证至少有300人在职
            
            leaving_indices = random.sample(range(len(active_employees)), month_leavers)
            
            # 更新离职员工状态
            for idx in sorted(leaving_indices, reverse=True):
                leaver = active_employees[idx]
                term_date = generate_specific_date(current_date, month_end)
                
                # 更新离职信息
                leaver['turnover'] = 1
                leaver['termination_date'] = term_date
                leaver['satisfaction'] = generate_satisfaction_score(True)
                leaver['last_updated'] = term_date.strftime('%Y-%m-%d %H:%M:%S')
                
                # 从活跃员工中移除
                active_employees.pop(idx)
        
        # 本月新入职的员工
        hiring_count = random.randint(MONTHLY_HIRING[0], MONTHLY_HIRING[1])
        
        for _ in range(hiring_count):
            hire_date = generate_specific_date(current_date, month_end)
            department = random.choices(list(DEPARTMENTS.keys()), weights=list(DEPARTMENTS.values()), k=1)[0]
            
            new_employee = {
                'employee_id': employee_ids[id_counter],
                'name': fake_en.name(),
                'department': department,
                'salary_level': random.choices(list(SALARY_LEVELS.keys()), weights=list(SALARY_LEVELS.values()), k=1)[0],
                'actual_salary': random.randint(SALARY_RANGES[department]['min'], SALARY_RANGES[department]['max']) * 1000,
                'turnover': 0,
                'satisfaction': round(random.uniform(0.6, 0.95), 2),
                'evaluation': generate_evaluation_score(),
                'project_count': random.randint(1, 3),
                'average_monthly_hours': random.randint(150, 220),
                'hire_date': hire_date,
                'termination_date': None,
                'work_accident': 0,
                'promotion': 0,
                'last_updated': hire_date.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            active_employees.append(new_employee)
            all_employees.append(new_employee)
            id_counter += 1
        
        # 更新在职员工年份数据
        for emp in active_employees:
            emp['years_at_company'] = calculate_years_at_company(emp['hire_date'], current_date)
        
        # 移至下个月
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)
    
    # 计算所有员工的最终在职年限
    for emp in all_employees:
        end_date = emp['termination_date'] if emp['termination_date'] else END_DATE
        emp['years_at_company'] = calculate_years_at_company(emp['hire_date'], end_date)
    
    print(f"生成结束。总员工数: {len(all_employees)}, 在职员工: {len(active_employees)}, 离职员工: {len(all_employees) - len(active_employees)}")
    print(f"离职率: {(len(all_employees) - len(active_employees)) / len(all_employees):.2%}")
    
    return all_employees

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
        # 清空历史数据
        cursor.execute("DELETE FROM employees")

        insert_sql = """
        INSERT INTO employees (
            employee_id, name, department, salary_level, actual_salary, 
            turnover, satisfaction, evaluation, project_count, 
            average_monthly_hours, years_at_company, hire_date, 
            termination_date, work_accident, promotion, last_updated
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    print("\n===== 数据样例 (随机选择的记录) =====")
    print(df[display_columns].to_string())

    print("\n===== 数据统计信息 =====")
    print(f"总记录数: {len(employees_data)}")
    turnover_count = sum(1 for emp in employees_data if emp['turnover'] == 1)
    active_count = sum(1 for emp in employees_data if emp['turnover'] == 0)
    turnover_rate = turnover_count / len(employees_data)
    print(f"在职员工: {active_count}, 历史离职员工: {turnover_count}, 离职率: {turnover_rate:.2%}")

    # 按年份统计离职情况
    term_years = {}
    for emp in employees_data:
        if emp['turnover'] == 1 and emp['termination_date']:
            year = emp['termination_date'].year if isinstance(emp['termination_date'], datetime) else int(emp['termination_date'][:4])
            term_years[year] = term_years.get(year, 0) + 1
    
    print("\n离职年份分布:")
    for year in sorted(term_years.keys()):
        print(f"  {year}年: {term_years[year]}人")

    # 部门分布
    dept_counts = {}
    for emp in employees_data:
        dept = emp['department']
        dept_counts[dept] = dept_counts.get(dept, 0) + 1
    
    print("\n部门分布:")
    for dept, count in dept_counts.items():
        print(f"  {dept}: {count}人 ({count/len(employees_data):.2%})")

def analyze_monthly_turnover(employees_data):
    """分析每月离职情况"""
    if not employees_data:
        print("没有数据可以分析")
        return
    
    monthly_stats = {}
    
    for emp in employees_data:
        if emp['turnover'] == 1 and emp['termination_date']:
            try:
                term_date = emp['termination_date']
                # 处理各种可能的日期格式
                if isinstance(term_date, str):
                    # 尝试多种日期格式
                    for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d-%m-%Y']:
                        try:
                            term_date = datetime.strptime(term_date, fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        # 如果所有格式都失败，跳过这条记录
                        print(f"警告: 无法解析日期格式 '{emp['termination_date']}', 跳过此记录")
                        continue
                
                # 确保term_date是datetime对象
                if not isinstance(term_date, datetime):
                    print(f"警告: 日期类型异常 {type(term_date)}, 跳过此记录")
                    continue
                
                year_month = f"{term_date.year}-{term_date.month:02d}"
                monthly_stats[year_month] = monthly_stats.get(year_month, 0) + 1
            except Exception as e:
                print(f"处理离职日期时出错: {e}, 跳过此记录")
    
    print("\n===== 每月离职人数统计 =====")
    for ym in sorted(monthly_stats.keys()):
        print(f"{ym}: {monthly_stats[ym]}人")
    
    # 计算平均每月离职人数
    avg_monthly = sum(monthly_stats.values()) / len(monthly_stats) if monthly_stats else 0
    print(f"\n平均每月离职人数: {avg_monthly:.2f}")

def main():
    """主函数"""
    try:
        print("修改后的员工数据生成程序启动")
        print(f"将生成从 {START_DATE.strftime('%Y-%m-%d')} 到 {END_DATE.strftime('%Y-%m-%d')} 的历史员工数据")
        print("每月约15人离职，10-15人新入职")
        
        choice = input("\n请选择操作: \n1. 生成历史数据并保存为CSV \n2. 生成历史数据并导入MySQL \n3. 仅分析数据离职分布 \n选择: ")
        
        
        if choice in ['1', '2']:
            try:
                print("\n开始生成历史数据...")
                employees_data = generate_historical_data()
                
                # 显示样例数据
                display_sample_data(employees_data)
                
                # 分析每月离职情况
                analyze_monthly_turnover(employees_data)
                
                # 保存CSV文件
                csv_filename = f"employee_data_history_{datetime.now().strftime('%Y%m%d')}.csv"
                save_to_csv(employees_data, csv_filename)
                
                if choice == '2':
                    # 询问数据库连接信息
                    DB_CONFIG['host'] = input(f"MySQL主机地址 (默认: {DB_CONFIG['host']}): ") or DB_CONFIG['host']
                    DB_CONFIG['port'] = int(input(f"端口 (默认: {DB_CONFIG['port']}): ") or DB_CONFIG['port'])
                    DB_CONFIG['user'] = input(f"用户名 (默认: {DB_CONFIG['user']}): ") or DB_CONFIG['user']
                    DB_CONFIG['password'] = input(f"密码 (默认: {DB_CONFIG['password']}): ") or DB_CONFIG['password']
                    DB_CONFIG['database'] = input(f"数据库名 (默认: {DB_CONFIG['database']}): ") or DB_CONFIG['database']
                    
                    # 创建数据库并导入数据
                    if create_database():
                        import_to_mysql(employees_data)
            except Exception as e:
                print(f"生成历史数据时出错: {e}")
                print("错误详情:")
                import traceback
                traceback.print_exc()
        
        elif choice == '3':
            csv_path = input("请输入要分析的CSV文件路径: ")
            try:
                df = pd.read_csv(csv_path)
                employees = df.to_dict('records')
                
                # 转换日期字段
                date_conversion_errors = 0
                for emp in employees:
                    try:
                        if not pd.isna(emp['hire_date']):
                            emp['hire_date'] = datetime.strptime(str(emp['hire_date']), '%Y-%m-%d')
                        if not pd.isna(emp['termination_date']):
                            emp['termination_date'] = datetime.strptime(str(emp['termination_date']), '%Y-%m-%d')
                    except Exception:
                        date_conversion_errors += 1
                
                if date_conversion_errors > 0:
                    print(f"警告: {date_conversion_errors} 条记录的日期格式转换失败，但分析将继续")
                
                print(f"成功加载 {len(employees)} 条记录")
                display_sample_data(employees)
                analyze_monthly_turnover(employees)
            except Exception as e:
                print(f"分析CSV文件失败: {e}")
                print("错误详情:")
                import traceback
                traceback.print_exc()
        
        else:
            print("无效选择，程序退出")
    
    except Exception as e:
        print(f"程序运行时发生错误: {e}")
        print("错误详情:")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()