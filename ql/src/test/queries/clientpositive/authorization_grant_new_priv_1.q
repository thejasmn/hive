
create table src_autho_new_priv as select * from src;

set hive.security.authorization.enabled=true;

--table grant to user

grant insert on table src_autho_new_priv to user user_new_priv;
grant delete on table src_autho_new_priv to user user_new_priv;
show grant user user_new_priv on table src_autho_new_priv;

revoke insert on table src_autho_new_priv from user user_new_priv;
show grant user user_new_priv on table src_autho_new_priv;

revoke delete on table src_autho_new_priv from user user_new_priv;
show grant user user_new_priv on table src_autho_new_priv;
