#!/bin/bash

echo "=== InLong Manager MySQL Setup Script ==="

# 检查MySQL服务
echo "1. Checking MySQL service..."
if ! systemctl is-active --quiet mysql && ! systemctl is-active --quiet mysqld; then
    echo "Starting MySQL service..."
    sudo systemctl start mysql 2>/dev/null || sudo systemctl start mysqld 2>/dev/null
    sleep 5
fi

# 测试MySQL连接
echo "2. Testing MySQL connection..."
mysql -u root -pinlong -h 127.0.0.1 -P 3306 -e "SELECT 1;" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "MySQL connection failed. Please configure MySQL first:"
    echo "sudo mysql"
    echo "ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'inlong';"
    echo "FLUSH PRIVILEGES;"
    echo "EXIT;"
    exit 1
fi

# 创建数据库
echo "3. Creating database..."
mysql -u root -pinlong -h 127.0.0.1 -P 3306 << EOF
CREATE DATABASE IF NOT EXISTS apache_inlong_manager CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
GRANT ALL PRIVILEGES ON apache_inlong_manager.* TO 'root'@'localhost';
FLUSH PRIVILEGES;
EOF

# 检查数据库表
echo "4. Checking database tables..."
TABLE_COUNT=$(mysql -u root -pinlong -h 127.0.0.1 -P 3306 -N -e "USE apache_inlong_manager; SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='apache_inlong_manager';" 2>/dev/null)

if [ "$TABLE_COUNT" -eq 0 ]; then
    echo "Database is empty. Please import the SQL schema:"
    echo "mysql -u root -pinlong -h 127.0.0.1 -P 3306 apache_inlong_manager < /home/shuideyimei/inlong/inlong-manager/manager-web/sql/apache_inlong_manager.sql"
else
    echo "Database contains $TABLE_COUNT tables."
fi

echo "5. MySQL setup complete!"
echo ""
echo "To start InLong Manager:"
echo "cd /home/shuideyimei/inlong/inlong-manager/manager-web"
echo "mvn spring-boot:run -Dspring-boot.run.profiles=dev-simple"