set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;

create table src_autho_test (key STRING, value STRING) ;

set hive.security.authorization.enabled=true;

--table grant to user

grant select on table src_autho_test to user hive_test_user;

show grant user hive_test_user on table src_autho_test;
show grant user hive_test_user on table src_autho_test(key);

-- select key from src_autho_test order by key limit 20;

revoke select on table src_autho_test from user hive_test_user;
show grant user hive_test_user on table src_autho_test;
show grant user hive_test_user on table src_autho_test(key);

--column grant to user

grant select(key) on table src_autho_test to user hive_test_user;

show grant user hive_test_user on table src_autho_test;
show grant user hive_test_user on table src_autho_test(key);

-- select key from src_autho_test order by key limit 20;

revoke select(key) on table src_autho_test from user hive_test_user;
show grant user hive_test_user on table src_autho_test;
show grant user hive_test_user on table src_autho_test(key); 

-- select key from src_autho_test order by key limit 20;


--role
create role src_role;
grant role src_role to user hive_test_user;
show role grant user hive_test_user;

--column grant to role

grant select(key) on table src_autho_test to role src_role;

show grant role src_role on table src_autho_test;
show grant role src_role on table src_autho_test(key);

-- select key from src_autho_test order by key limit 20;

revoke select(key) on table src_autho_test from role src_role;

--table grant to role

grant select on table src_autho_test to role src_role;

-- select key from src_autho_test order by key limit 20;

show grant role src_role on table src_autho_test;
show grant role src_role on table src_autho_test(key);
revoke select on table src_autho_test from role src_role;

-- drop role
drop role src_role;

set hive.security.authorization.enabled=false;
drop table src_autho_test;