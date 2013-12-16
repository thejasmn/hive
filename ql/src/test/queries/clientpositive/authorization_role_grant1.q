-- role granting without role keyword
create role src_role2;
grant  src_role2 to user user2 ;
show role grant user user2;

-- revoke role without role keyword
revoke src_role2 from user user2;
show role grant user user2;

