#!/bin/bash

set -x
set -e

echo "Starting database initialization..."

# Wait for MySQL to be ready
echo "Waiting for MySQL to be ready..."
while ! mysqladmin ping -h"localhost" -u"root" -p"root" --silent; do
    sleep 1
done

echo "MySQL is ready. Creating databases..."

# Create both databases
mysql -u root -proot < /docker-entrypoint-initdb.d/00-create-db.sql

# Grant permissions to test_user for both databases
mysql -u root -proot -e "GRANT ALL PRIVILEGES ON acme_corp.* TO 'test_user'@'%';"
mysql -u root -proot -e "GRANT ALL PRIVILEGES ON acme_corp_verify.* TO 'test_user'@'%';"
mysql -u root -proot -e "FLUSH PRIVILEGES;"

echo "Databases created. Populating acme_corp..."

# Create schema and populate the first database (for cleanup)
mysql -u root -proot acme_corp -e "source /docker-entrypoint-initdb.d/schema.sql.seed"
mysql -u root -proot acme_corp -e "source /docker-entrypoint-initdb.d/data.sql.seed"

echo "Populating acme_corp_verify..."

# Copy entire database from acme_corp to acme_corp_verify
mysqldump -u root -proot acme_corp | mysql -u root -proot acme_corp_verify

echo "Both databases populated successfully!"
echo "Available databases:"
mysql -u root -proot -e "SHOW DATABASES;"
