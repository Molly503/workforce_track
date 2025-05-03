import mysql.connector
import logging
from datetime import datetime
import pandas as pd

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
        return True
    except mysql.connector.Error as err:
        logging.error(f"数据库错误: {err}")
        return False

def main():
    """主函数"""
    if not check_and_fix_database():
        logging.error("数据库连接失败，退出程序")
        return
    
    # 您的其他代码...

if __name__ == "__main__":
    main()