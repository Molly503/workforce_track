import mysql.connector
import logging
from datetime import datetime
import pandas as pd
import os
import random
import numpy as np

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
    'password': 'Taylor@1989',  # 您的MySQL密码
    'database': 'employee_db'
}

def check_and_fix_database():
    """检查并修复数据库结构"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 检查表结构
        cursor.execute("DESCRIBE employees")
        columns = [column[0] for column in cursor.fetchall()]
        
        logging.info(f"当前表包含的列: {columns}")
        
        cursor.close()
        conn.close()
        return True, columns
    except mysql.connector.Error as err:
        logging.error(f"数据库错误: {err}")
        return False, []

def generate_updated_data(columns):
    """生成更新的数据"""
    logging.info("开始生成更新的数据...")
    
    # 尝试读取最新的CSV文件作为基础
    csv_files = [f for f in os.listdir() if f.startswith('employee_data_') and f.endswith('.csv')]
    csv_files.sort(reverse=True)  # 按文件名排序，最新的应该在前面
    
    if not csv_files:
        # 如果没有发现employee_data_*.csv文件，尝试使用employee_data_initial.csv
        if os.path.exists('employee_data_initial.csv'):
            csv_files = ['employee_data_initial.csv']
        else:
            logging.error("找不到现有的CSV文件")
            return None
    
    latest_csv = csv_files[0]
    logging.info(f"使用现有文件作为基础: {latest_csv}")
    
    try:
        df = pd.read_csv(latest_csv)
    except Exception as e:
        logging.error(f"读取CSV文件失败: {e}")
        return None
    
    # 检查列名是否与数据库匹配
    missing_columns = [col for col in columns if col not in df.columns]
    if missing_columns:
        logging.warning(f"CSV文件缺少以下列: {missing_columns}")
        # 为缺失的列添加空值
        for col in missing_columns:
            df[col] = None
    
    # 更新数据：这里只是示例，您可以根据需要进行修改
    # 例如，可以随机更新一些员工的满意度、评估等
    for index, row in df.iterrows():
        # 随机更新一些数据点
        if random.random() < 0.3:  # 30%的概率更新
            df.at[index, 'satisfaction'] = round(random.uniform(0, 1), 2)
            df.at[index, 'evaluation'] = round(random.uniform(0, 1), 2)
            df.at[index, 'project_count'] = random.randint(2, 7)
            df.at[index, 'average_monthly_hours'] = random.randint(140, 310)
    
    # 更新last_updated字段
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['last_updated'] = current_time
    
    # 生成新的CSV文件
    current_date = datetime.now().strftime('%Y%m%d%H%M%S')
    new_filename = f'employee_data_{current_date}.csv'
    df.to_csv(new_filename, index=False)
    
    logging.info(f"已生成新的数据文件: {new_filename}")
    return new_filename, df

def import_to_mysql(filename, df, columns):
    """将数据导入MySQL"""
    logging.info(f"开始将数据导入MySQL...")
    
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 先清空表
        cursor.execute("TRUNCATE TABLE employees")
        
        # 构建插入语句
        columns_str = ', '.join([f"`{col}`" for col in columns])  # 使用反引号包围列名
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO employees ({columns_str}) VALUES ({placeholders})"
        
        # 准备数据
        values = []
        for _, row in df.iterrows():
            # 确保数据顺序与列顺序一致，并将NaN转换为None
            row_data = []
            for col in columns:
                val = row.get(col)
                # 检查是否为NaN并转换为None
                if isinstance(val, float) and np.isnan(val):
                    val = None
                row_data.append(val)
            values.append(tuple(row_data))
        
        # 批量插入
        cursor.executemany(insert_query, values)
        conn.commit()
        
        
        logging.info(f"成功导入 {cursor.rowcount} 条记录到MySQL")
        
        cursor.close()
        conn.close()
        return True
    except mysql.connector.Error as err:
        logging.error(f"导入MySQL时出错: {err}")
        return False

def main():
    """主函数"""
    logging.info("开始执行数据更新和导入...")
    
    # 检查数据库
    db_ok, columns = check_and_fix_database()
    if not db_ok:
        logging.error("数据库连接失败，退出程序")
        return
    
    # 生成更新的数据
    result = generate_updated_data(columns)
    if not result:
        logging.error("生成数据失败，退出程序")
        return
    
    new_filename, df = result
    
    # 导入到MySQL
    if import_to_mysql(new_filename, df, columns):
        logging.info("数据已成功更新并导入MySQL - 请在VSCode中手动刷新数据库视图以查看更改")
        print("\n\n==== 导入完成 ====\n请在VSCode中刷新数据库视图!\n==================\n\n")
    else:
        logging.error("导入MySQL失败")

if __name__ == "__main__":
    main()