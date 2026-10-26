-- Executed once by the image entrypoint (as root) on first start.
-- Mirrors scripts/test-servers.sh: user lv/lv with ALL PRIVILEGES and database lvtest.
CREATE USER IF NOT EXISTS 'lv'@'%' IDENTIFIED BY 'lv';
GRANT ALL PRIVILEGES ON *.* TO 'lv'@'%' WITH GRANT OPTION;
CREATE DATABASE IF NOT EXISTS lvtest;
FLUSH PRIVILEGES;

-- The account of docs/server-setup.md "User and grants" and nothing more (the Amazon RDS / Aurora
-- privilege model: no SUPER, no admin rights). The integration suites run every engine as this
-- account (LAZYVIEW_TEST_ENGINE_USER=lvdoc:doc); the schemas the tests create are lvt_*.
CREATE USER IF NOT EXISTS 'lvdoc'@'%' IDENTIFIED BY 'doc';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'lvdoc'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, CREATE VIEW, SHOW VIEW ON `lvt\_%`.* TO 'lvdoc'@'%';
FLUSH PRIVILEGES;
