set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
-- This test will fail because hive_test_user is not in admin role
show role principals role1; 
