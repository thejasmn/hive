package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import java.util.List;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationValidator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;

public class SQLStdHiveAuthorizationValidator implements HiveAuthorizationValidator {

  @Override
  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputHObjs,
      List<HivePrivilegeObject> outputHObjs) throws HiveAuthorizationPluginException {
    // TODO Auto-generated method stub
    
  }

}
