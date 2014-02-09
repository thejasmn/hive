set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;

-- an error should be thrown if 'set role ' is done for role that does not exist

create role rset_role_neg;
grant role rset_role_neg to user user2;

set user.name=user2;
set role rset_role_neg;
set role public;
set role nosuchroleexists;;

