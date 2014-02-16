set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/a_uri_add_part;
dfs -touchz ${system:test.tmp.dir}/a_uri_add_part/1.txt;
dfs -chmod 555 ${system:test.tmp.dir}/a_uri_add_part/1.txt;

create table t1(i int);
load data inpath 'pfile:${system:test.tmp.dir}/a_uri_add_part/' overwrite into table t1;

