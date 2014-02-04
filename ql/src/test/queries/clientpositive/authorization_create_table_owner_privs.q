set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=user1;

create table create_table_owner_priv_test(i int);

-- all privileges should have been set for user

show grant user user1 on table create_table_owner_priv_test;
