set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

-- running a sql query to initialize the authorization - not needed in real HS2 mode
use default;

set hive.metastore.server.min.threads=101;
set hive.metastore.server.min.threads;
reset;
set hive.metastore.server.min.threads;
