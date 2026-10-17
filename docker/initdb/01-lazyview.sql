-- Executed once by the image entrypoint (as root) on first start.
-- Mirrors scripts/test-servers.sh: user lv/lv with ALL PRIVILEGES and database lvtest.
CREATE USER IF NOT EXISTS 'lv'@'%' IDENTIFIED BY 'lv';
GRANT ALL PRIVILEGES ON *.* TO 'lv'@'%' WITH GRANT OPTION;
CREATE DATABASE IF NOT EXISTS lvtest;
FLUSH PRIVILEGES;
