# test_mysql_connection.py
import mysql.connector

try:
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Taylor@1989',  # 填写您的密码
        database='employee_db'
    )
    print("连接成功！")
    
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    print("数据库中的表：")
    for table in tables:
        print(f"- {table[0]}")
    
    cursor.close()
    conn.close()
except mysql.connector.Error as err:
    print(f"连接失败：{err}")