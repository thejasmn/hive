set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/authz_addpart_1;

set user.name=user1;
-- check add partition without insert privilege
create table tpart(i int, j int) partitioned by (k string);         
set user.name=${system:user.name};
alter table tpart add partition (k = 'abc') location 'file:${system:test.tmp.dir}/authz_addpart_1' ;
