import mysql.connector
import logging
from datetime import datetime, timedelta
import pandas as pd
import os
import random
import numpy as np
from faker import Faker

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('update.log'),
        logging.StreamHandler()
    ]
)

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Taylor@1989',  # 修改为您的MySQL密码
    'database': 'employee_db',  # 确保与 data.py 一致
    'port': 3306
}

# 员工数据生成的常量
TOTAL_EMPLOYEES = 6000  # 目标在职员工
HISTORICAL_LEAVERS = 7200  # 历史离职员工
DEPARTMENTS = {
    'Sales': 0.30,
    'Marketing': 0.20,
    'Engineering': 0.25,
    'HR': 0.10,
    'Legal': 0.05,
    'Operations': 0.10
}
SALARY_LEVELS = {
    'low': 0.50,
    'medium': 0.40,
    'high': 0.10
}
SALARY_RANGES = {
    'Sales': {'min': 150, 'max': 500},
    'Marketing': {'min': 120, 'max': 450},
    'Engineering': {'min': 180, 'max': 600},
    'HR': {'min': 100, 'max': 400},
    'Legal': {'min': 150, 'max': 500},
    'Operations': {'min': 80, 'max': 350}
}

# 新增：控制参数
MAX_CHANGE_RATE = 0.20  # 最大变化率，限制new_hires和terminations的变化
SMOOTHING_FACTOR = 0.5  # 平滑因子

# 初始化Faker用于数据生成
fake = Faker('zh_CN')
fake_en = Faker()

# 设置随机种子以确保可重复性
random.seed(42)
np.random.seed(42)

def generate_employee_ids(count, existing_ids=None):
    """生成唯一的员工ID，确保不重复"""
    max_id = 999999
    min_id = 100000
    available_ids = set(range(min_id, max_id + 1)) - (set(existing_ids) if existing_ids else set())
    if count > len(available_ids):
        raise ValueError(f"无法生成 {count} 个唯一ID，剩余范围不足")
    return random.sample(list(available_ids), count)

def calculate_turnover_probability(satisfaction_score, evaluation_score, project_count, monthly_hours, years, accident, promotion):
    """根据多个因素计算离职概率，与data.py一致"""
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

def generate_employee_data_with_correlation(target_turnover_rate, total_employees, is_historical=False, existing_ids=None, prev_stats=None):
    """生成具有满意度-离职相关性的员工数据，并控制new_hires和terminations的变化"""
    employees_data = []
    
    # 满意度分布：活跃员工倾向于更高满意度，历史离职员工倾向于更低满意度
    if is_historical:
        satisfaction_scores = np.random.beta(2, 5, total_employees) * 0.8 + 0.1
    else:
        satisfaction_scores = np.random.beta(3, 2, total_employees) * 0.8 + 0.1
    satisfaction_scores = np.clip(satisfaction_scores, 0.1, 0.9)
    
    employee_ids = generate_employee_ids(total_employees, existing_ids)
    
    # 计算每年的new_hires和terminations目标
    START_YEAR = 2014
    END_YEAR = 2025
    years_range = range(START_YEAR, END_YEAR + 1)
    
    # 如果有历史数据，获取上一年的new_hires和terminations
    if prev_stats:
        base_new_hires = prev_stats[max(prev_stats.keys())]['new_hires'] if prev_stats else 500
        base_terminations = prev_stats[max(prev_stats.keys())]['terminations'] if prev_stats else 300
    else:
        base_new_hires = 500  # 初始值
        base_terminations = 300
    
    # 生成每年的new_hires和terminations
    raw_new_hires = []
    raw_terminations = []
    
    for year in years_range:
        if year == START_YEAR:
            raw_new_hires.append(base_new_hires)
            raw_terminations.append(base_terminations)
        else:
            prev_new_hires = raw_new_hires[-1]
            prev_terminations = raw_terminations[-1]
            
            growth_rate_new_hires = np.random.uniform(0.01, 0.03)
            growth_rate_terminations = np.random.uniform(0.01, 0.03)
            
            new_hires = int(prev_new_hires * (1 + growth_rate_new_hires + np.random.uniform(-0.03, 0.03)))
            terminations = int(prev_terminations * (1 + growth_rate_terminations + np.random.uniform(-0.03, 0.03)))
            
            raw_new_hires.append(new_hires)
            raw_terminations.append(terminations)
    
    # 应用控制和平滑
    controlled_new_hires = control_annual_change(raw_new_hires, MAX_CHANGE_RATE)
    smoothed_new_hires = smooth_values(controlled_new_hires, SMOOTHING_FACTOR)
    controlled_terminations = control_annual_change(raw_terminations, MAX_CHANGE_RATE)
    smoothed_terminations = smooth_values(controlled_terminations, SMOOTHING_FACTOR)
    
    # 计算每年实际的员工分配
    total_new_hires = sum(smoothed_new_hires)
    total_terminations = sum(smoothed_terminations)
    
    # 按比例分配员工
    hire_counts = [int((nh / total_new_hires) * total_employees) for nh in smoothed_new_hires]
    termination_counts = [int((t / total_terminations) * (total_employees if is_historical else int(total_employees * target_turnover_rate))) for t in smoothed_terminations]
    
    # 调整以确保总数精确
    hire_counts[-1] += total_employees - sum(hire_counts)
    if is_historical:
        termination_counts[-1] += total_employees - sum(termination_counts)
    else:
        target_leavers = int(total_employees * target_turnover_rate)
        termination_counts[-1] += target_leavers - sum(termination_counts)
    
    # 分配员工到各年
    hire_indices = []
    termination_indices = []
    current_index = 0
    
    for i, year in enumerate(years_range):
        for _ in range(hire_counts[i]):
            hire_indices.append((current_index, year))
            current_index += 1
    
    current_index = 0
    for i, year in enumerate(years_range):
        for _ in range(termination_counts[i]):
            termination_indices.append((current_index, year))
            current_index += 1
    
    random.shuffle(hire_indices)
    random.shuffle(termination_indices)
    
    hire_map = {idx: year for idx, year in hire_indices}
    termination_map = {idx: year for idx, year in termination_indices}
    
    for i in range(total_employees):
        is_leaving = (is_historical or (i < int(total_employees * target_turnover_rate)))
        department = random.choices(list(DEPARTMENTS.keys()), weights=list(DEPARTMENTS.values()), k=1)[0]
        
        # 生成日期
        hire_year = hire_map[i]
        hire_date = f"{hire_year}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        
        termination_date = None
        if is_leaving:
            term_index = i if is_historical else i
            term_year = termination_map[term_index]
            termination_date = f"{term_year}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        
        years = generate_years_at_company(is_leaving, termination_date)
        
        # 确保hire_date和termination_date一致
        if termination_date:
            hire_dt = datetime.strptime(hire_date, '%Y-%m-%d')
            term_dt = datetime.strptime(termination_date, '%Y-%m-%d')
            if term_dt <= hire_dt:
                term_dt = hire_dt + timedelta(days=years * 365 + random.randint(1, 180))
                termination_date = term_dt.strftime('%Y-%m-%d')
        
        # 生成其他字段
        satisfaction = round(satisfaction_scores[i], 2)
        evaluation = round(random.uniform(0.4, 0.6) if random.random() < 0.5 else random.uniform(0.8, 1.0), 2)
        project_count = max(0, min(7, int(np.random.normal(3.8, 1.5)))) if not (is_leaving and random.random() < 0.4) else random.randint(6, 7)
        monthly_hours = random.randint(96, 150) if random.random() < 0.5 else random.randint(250, 280) if not (is_leaving and random.random() < 0.6) else random.randint(250, 310)
        accident = 1 if random.random() < (0.05 if is_leaving else 0.18) else 0
        promotion = 1 if random.random() < (0.005 if is_leaving else 0.03) else 0
        
        # 计算离职概率
        turnover_prob = calculate_turnover_probability(
            satisfaction, evaluation, project_count, monthly_hours, 
            years, accident, promotion
        )
        
        employee = {
            'employee_id': employee_ids[i],
            'name': fake_en.name(),
            'department': department,
            'salary_level': random.choices(list(SALARY_LEVELS.keys()), weights=list(SALARY_LEVELS.values()), k=1)[0],
            'actual_salary': random.randint(SALARY_RANGES[department]['min'], SALARY_RANGES[department]['max']) * 1000,
            'left': 1 if is_leaving else 0,
            'satisfaction_level': satisfaction,  # 改为satisfaction_level，与data.py一致
            'last_evaluation': evaluation,  # 改为last_evaluation
            'number_project': project_count,  # 改为number_project
            'average_monthly_hours': monthly_hours,
            'time_spend_company': years,
            'Work_accident': accident,  # 改为Work_accident
            'promotion_last_5years': promotion,  # 改为promotion_last_5years
            'hire_date': hire_date,
            'termination_date': termination_date,
            'turnover_probability': round(turnover_prob, 3),
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        employees_data.append(employee)
    
    return employees_data

def generate_termination_date(is_leaving):
    """生成离职日期（2014-01-01至2025-03-31）"""
    if is_leaving:
        start_date = datetime(2014, 1, 1)
        end_date = datetime(2025, 3, 31)
        time_diff = (end_date - start_date).days
        random_days = random.randint(0, time_diff)
        termination_date = start_date + timedelta(days=random_days)
        return termination_date.strftime('%Y-%m-%d')
    return None

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

def check_and_fix_database():
    """检查并修复数据库结构，与data.py一致"""
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
            `left` TINYINT,
            satisfaction_level FLOAT,  -- 改为satisfaction_level，与data.py一致
            last_evaluation FLOAT,  -- 改为last_evaluation
            number_project INT,  -- 改为number_project
            average_monthly_hours INT,
            time_spend_company INT,
            Work_accident TINYINT,  -- 改为Work_accident
            promotion_last_5years TINYINT,  -- 改为promotion_last_5years
            hire_date DATE,
            termination_date DATE,
            turnover_probability FLOAT,
            last_updated DATETIME
        )
        """)
        
        cursor.execute("DESCRIBE employees")
        columns = [column[0] for column in cursor.fetchall()]
        
        logging.info(f"当前表列: {columns}")
        
        cursor.close()
        conn.close()
        return True, columns
    except mysql.connector.Error as err:
        logging.error(f"数据库错误: {err}")
        return False, []

def generate_updated_data(columns):
    """生成或更新员工数据"""
    logging.info("开始数据生成/更新...")
    
    # 尝试读取最新的CSV文件
    csv_files = [f for f in os.listdir() if f.startswith('employee_data_') and f.endswith('.csv')]
    csv_files.sort(reverse=True)
    
    if not csv_files:
        csv_files = ['employee_data_turnover.csv'] if os.path.exists('employee_data_turnover.csv') else []
    
    df = None
    existing_ids = set()
    prev_stats = None
    if csv_files:
        latest_csv = csv_files[0]
        logging.info(f"使用现有文件作为基础: {latest_csv}")
        try:
            df = pd.read_csv(latest_csv)
            # Validate employee_id
            if df['employee_id'].isnull().any() or df['employee_id'].duplicated().any():
                logging.warning(f"CSV 文件 {latest_csv} 包含无效或重复的 employee_id")
                df['employee_id'] = generate_employee_ids(len(df))
            existing_ids = set(df['employee_id'])
            logging.info(f"从 {latest_csv} 读取 {len(df)} 条记录")
            
            # 计算历史年度统计
            START_YEAR = 2014
            END_YEAR = 2025
            years_range = range(START_YEAR, END_YEAR + 1)
            annual_stats = {year: {'new_hires': 0, 'terminations': 0} for year in years_range}
            for _, row in df.iterrows():
                hire_year = int(row['hire_date'].split('-')[0])
                if hire_year in annual_stats:
                    annual_stats[hire_year]['new_hires'] += 1
                if pd.notna(row['termination_date']):
                    term_year = int(row['termination_date'].split('-')[0])
                    if term_year in annual_stats:
                        annual_stats[term_year]['terminations'] += 1
            prev_stats = annual_stats
        except Exception as e:
            logging.error(f"读取CSV文件 {latest_csv} 失败: {e}")
            df = None
    
    # 如果CSV缺失或记录数不足，生成新数据
    expected_records = TOTAL_EMPLOYEES + HISTORICAL_LEAVERS
    if df is None or len(df) < expected_records * 0.9:
        logging.info(f"CSV缺失或记录不足（{len(df) if df is not None else 0} 条记录，预期约 {expected_records}）。正在生成新数据...")
        employees_data = generate_employees_data(existing_ids=existing_ids, prev_stats=prev_stats)
        df = pd.DataFrame(employees_data)
        logging.info(f"生成了 {len(df)} 条新记录")
    
    # 确保所有必需列都存在
    for col in columns:
        if col not in df.columns:
            logging.warning(f"CSV缺少列: {col}")
            if col == 'turnover_probability':
                # 为现有记录计算turnover_probability
                df['turnover_probability'] = df.apply(
                    lambda row: calculate_turnover_probability(
                        row['satisfaction_level'],  # 改为satisfaction_level
                        row['last_evaluation'],  # 改为last_evaluation
                        row['number_project'],  # 改为number_project
                        row['average_monthly_hours'],
                        row['time_spend_company'],
                        row['Work_accident'],  # 改为Work_accident
                        row['promotion_last_5years']  # 改为promotion_last_5years
                    ), axis=1
                )
            elif col == 'left':
                # 如果有turnover字段，转换到left
                if 'turnover' in df.columns:
                    df['left'] = df['turnover']
                else:
                    df['left'] = df['termination_date'].apply(lambda x: 1 if pd.notna(x) else 0)
            elif col == 'time_spend_company':
                # 如果有years_at_company字段，转换到time_spend_company
                if 'years_at_company' in df.columns:
                    df['time_spend_company'] = df['years_at_company']
                else:
                    df['time_spend_company'] = 0  # 默认值
            elif col == 'satisfaction_level':
                if 'satisfaction' in df.columns:
                    df['satisfaction_level'] = df['satisfaction']
                else:
                    df['satisfaction_level'] = 0.5  # 默认值
            elif col == 'last_evaluation':
                if 'evaluation' in df.columns:
                    df['last_evaluation'] = df['evaluation']
                else:
                    df['last_evaluation'] = 0.5  # 默认值
            elif col == 'number_project':
                if 'project_count' in df.columns:
                    df['number_project'] = df['project_count']
                else:
                    df['number_project'] = 3  # 默认值
            elif col == 'Work_accident':
                if 'work_accident' in df.columns:
                    df['Work_accident'] = df['work_accident']
                else:
                    df['Work_accident'] = 0  # 默认值
            elif col == 'promotion_last_5years':
                if 'promotion' in df.columns:
                    df['promotion_last_5years'] = df['promotion']
                else:
                    df['promotion_last_5years'] = 0  # 默认值
            else:
                df[col] = None
    
    # 重命名旧字段到新字段（如果存在）
    column_mappings = {
        'years_at_company': 'time_spend_company',
        'satisfaction': 'satisfaction_level',
        'evaluation': 'last_evaluation',
        'project_count': 'number_project',
        'work_accident': 'Work_accident',
        'promotion': 'promotion_last_5years',
        'turnover': 'left'
    }
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns:
            df[new_col] = df[old_col]
            df = df.drop(columns=[old_col])
    
    # 更新数据（30%的记录更新关键字段）
    update_mask = np.random.rand(len(df)) < 0.3
    if update_mask.any():
        df.loc[update_mask, 'satisfaction_level'] = np.round(np.random.uniform(0, 1, update_mask.sum()), 2)
        df.loc[update_mask, 'last_evaluation'] = np.round(np.random.uniform(0, 1, update_mask.sum()), 2)
        df.loc[update_mask, 'number_project'] = np.random.randint(2, 8, update_mask.sum())
        df.loc[update_mask, 'average_monthly_hours'] = np.random.randint(140, 311, update_mask.sum())
        
        # 重新计算受影响记录的turnover_probability
        df.loc[update_mask, 'turnover_probability'] = df[update_mask].apply(
            lambda row: calculate_turnover_probability(
                row['satisfaction_level'],
                row['last_evaluation'],
                row['number_project'],
                row['average_monthly_hours'],
                row['time_spend_company'],
                row['Work_accident'],
                row['promotion_last_5years']
            ), axis=1
        )
    
    # 更新 last_updated 字段
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['last_updated'] = current_time
    
    # 保存到新的CSV文件
    current_date = datetime.now().strftime('%Y%m%d%H%M%S')
    new_filename = f'employee_data_{current_date}.csv'
    df.to_csv(new_filename, index=False, encoding='utf-8-sig')
    
    logging.info(f"生成新数据文件: {new_filename}，包含 {len(df)} 条记录")
    return new_filename, df

def import_to_mysql(filename, df, columns):
    """将数据导入MySQL"""
    logging.info("开始将数据导入MySQL...")
    
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 构建插入语句
        columns_str = ', '.join([f"`{col}`" for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        updates = ', '.join([f"`{col}` = VALUES(`{col}`)" for col in columns])
        insert_query = f"INSERT INTO employees ({columns_str}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
        
        # 准备数据
        values = []
        for _, row in df.iterrows():
            row_data = []
            for col in columns:
                val = row.get(col)
                if isinstance(val, float) and np.isnan(val):
                    val = None
                row_data.append(val)
            values.append(tuple(row_data))
        
        # 批量插入
        batch_size = 1000
        for i in range(0, len(values), batch_size):
            cursor.executemany(insert_query, values[i:i + batch_size])
            conn.commit()
            logging.info(f"已导入 {min(i + batch_size, len(values))}/{len(values)} 条记录")
        
        logging.info(f"成功导入 {len(values)} 条记录到MySQL")
        
        cursor.close()
        conn.close()
        return True
    except mysql.connector.Error as err:
        logging.error(f"导入MySQL时出错: {err}")
        return False

def generate_employees_data(count=TOTAL_EMPLOYEES + HISTORICAL_LEAVERS, existing_ids=None, prev_stats=None):
    """生成员工数据，包含满意度-离职相关性"""
    logging.info(f"生成数据：目标月离职率 5%，总员工 {count} 人")
    
    historical_leavers = HISTORICAL_LEAVERS
    current_employees = count - historical_leavers
    
    logging.info(f"目标在职员工：{current_employees}，历史离职员工：{historical_leavers}")
    
    current_turnover_rate = 0.05
    current_data = generate_employee_data_with_correlation(
        current_turnover_rate, current_employees, is_historical=False, 
        existing_ids=existing_ids, prev_stats=prev_stats
    )
    
    historical_data = generate_employee_data_with_correlation(
        1.0, historical_leavers, is_historical=True, 
        existing_ids=set([d['employee_id'] for d in current_data]) | (existing_ids or set()),
        prev_stats=prev_stats
    )
    
    employees_data = current_data + historical_data
    
    actual_leavers = sum(1 for e in employees_data if e['left'] == 1)
    actual_active = count - actual_leavers
    
    logging.info(f"生成数据 - 在职员工：{actual_active}，离职员工：{actual_leavers}")
    
    all_satisfaction = [emp['satisfaction_level'] for emp in employees_data]
    active_satisfaction = [emp['satisfaction_level'] for emp in employees_data if emp['left'] == 0]
    leaving_satisfaction = [emp['satisfaction_level'] for emp in employees_data if emp['left'] == 1]
    
    logging.info(f"总体平均满意度：{np.mean(all_satisfaction):.3f}")
    logging.info(f"在职员工平均满意度：{np.mean(active_satisfaction):.3f}")
    logging.info(f"离职员工平均满意度：{np.mean(leaving_satisfaction):.3f}")
    
    turnover_values = [emp['left'] for emp in employees_data]
    satisfactions = [emp['satisfaction_level'] for emp in employees_data]
    correlation = np.corrcoef(satisfactions, turnover_values)[0, 1]
    logging.info(f"满意度与离职的相关系数：{correlation:.3f} (应为负数)")
    
    return employees_data

# 主执行逻辑
if __name__ == "__main__":
    logging.info("开始数据更新和导入...")
    
    # 检查数据库
    db_ok, columns = check_and_fix_database()
    if not db_ok:
        logging.error("数据库连接失败，退出程序...")
        exit(1)
    
    # 生成/更新数据
    result = generate_updated_data(columns)
    if not result:
        logging.error("数据生成失败，退出程序...")
        exit(1)
    
    new_filename, df = result
    
    # 导入MySQL
    if import_to_mysql(new_filename, df, columns):
        logging.info("数据成功更新并导入MySQL - 请在VSCode中刷新数据库视图")
        print("\n\n==== 导入完成 ====\n请在VSCode中刷新数据库视图!\n==================\n\n")
    else:
        logging.error("导入MySQL失败")
        exit(1)