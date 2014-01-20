package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessController;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal.HivePrincipalType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;


/**
 * Implements functionality of access control statements for sql standard based authorization
 */
@Private
public class SQLStdHiveAccessController implements HiveAccessController {
  
  private HiveMetastoreClientFactory metastoreClientFactory;


  SQLStdHiveAccessController(HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf, String hiveCurrentUser){
    this.metastoreClientFactory = metastoreClientFactory;
  }
      

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthorizationPluginException {

    PrivilegeBag privBag =
        getThriftPrivilegesBag(hivePrincipals, hivePrivileges, hivePrivObject, grantorPrincipal,
            grantOption);
    try {
      metastoreClientFactory.getHiveMetastoreClient().grant_privileges(privBag);
    } catch (Exception e) {
      throw new HiveAuthorizationPluginException("Error granting privileges", e); 
    }
  }

  /**
   * Create thrift privileges bag
   * @param hivePrincipals
   * @param hivePrivileges
   * @param hivePrivObject
   * @param grantorPrincipal
   * @param grantOption
   * @return
   * @throws HiveAuthorizationPluginException
   */
  private PrivilegeBag getThriftPrivilegesBag(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthorizationPluginException {
    HiveObjectRef privObj = getThriftHiveObjectRef(hivePrivObject);
    PrivilegeBag privBag = new PrivilegeBag();
    for(HivePrivilege privilege : hivePrivileges){
      PrivilegeGrantInfo grantInfo = getThriftPrivilegeGrantInfo(privilege, grantorPrincipal, grantOption);
      for(HivePrincipal principal : hivePrincipals){
        HiveObjectPrivilege objPriv = new HiveObjectPrivilege(privObj, principal.getName(),
            getThriftPrincipalType(principal.getType()), grantInfo);
        privBag.addToPrivileges(objPriv);
      }
    }
    return privBag;
  }


  private PrivilegeGrantInfo getThriftPrivilegeGrantInfo(HivePrivilege privilege,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthorizationPluginException {
    return new PrivilegeGrantInfo(privilege.getName(), 0 /* time gets added by server */,
        grantorPrincipal.getName(), getThriftPrincipalType(grantorPrincipal.getType()), grantOption);
  }

  private PrincipalType getThriftPrincipalType(HivePrincipalType type)
      throws HiveAuthorizationPluginException {
    switch(type){
    case USER:
      return PrincipalType.USER;
    case ROLE:
      return PrincipalType.ROLE;
    default:
      throw new HiveAuthorizationPluginException("Invalid principal type");
    }
  }

  /**
   * Create a thrift privilege object from the plugin interface privilege object
   * @param privObj 
   * @return
   * @throws HiveAuthorizationPluginException 
   */
  private HiveObjectRef getThriftHiveObjectRef(HivePrivilegeObject privObj)
      throws HiveAuthorizationPluginException {
    HiveObjectType objType = getThriftHiveObjType(privObj.getType());
    return new HiveObjectRef(objType, privObj.getDbname(), privObj.getTableviewname(), null, null);
  }

  private HiveObjectType getThriftHiveObjType(HivePrivilegeObjectType type)
      throws HiveAuthorizationPluginException {
    switch(type){
    case DATABASE:
      return HiveObjectType.DATABASE;
    case TABLE:
      return HiveObjectType.TABLE;
    case PARTITION:
      return HiveObjectType.PARTITION;
    default:
      throw new HiveAuthorizationPluginException("Unsupported type");
    }
  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthorizationPluginException {

    PrivilegeBag privBag =
        getThriftPrivilegesBag(hivePrincipals, hivePrivileges, hivePrivObject, grantorPrincipal,
            grantOption);
    try {
      metastoreClientFactory.getHiveMetastoreClient().revoke_privileges(privBag);
    } catch (Exception e) {
      throw new HiveAuthorizationPluginException("Error revoking privileges", e); 
    }
  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor)
      throws HiveAuthorizationPluginException {
    try {
      metastoreClientFactory.getHiveMetastoreClient()
        .create_role(new Role(roleName, 0, adminGrantor.getName()));
    } catch (Exception e) {
      throw new HiveAuthorizationPluginException("Error create role", e);
    } 
  }

  @Override
  public void dropRole(String roleName) throws HiveAuthorizationPluginException {
    try {
      metastoreClientFactory.getHiveMetastoreClient().drop_role(roleName);
    } catch (Exception e) {
      throw new HiveAuthorizationPluginException("Error dropping role", e);
    } 
  }

  @Override
  public List<String> getRoles(HivePrincipal hivePrincipal) throws HiveAuthorizationPluginException {
    try {
      List<Role> roles = metastoreClientFactory.getHiveMetastoreClient().list_roles(
          hivePrincipal.getName(), getThriftPrincipalType(hivePrincipal.getType()));
      List<String> roleNames = new ArrayList<String>(roles.size());
      for(Role role : roles){
        roleNames.add(role.getRoleName());
      }
      return roleNames;
    } catch (Exception e) {
      throw new HiveAuthorizationPluginException(
          "Error listing roles for user" + hivePrincipal.getName(), e);
    }
  }

  @Override
  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roleNames,
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthorizationPluginException {
    for(HivePrincipal hivePrincipal : hivePrincipals){
      for(String roleName : roleNames){
        try {
          IMetaStoreClient mClient = metastoreClientFactory.getHiveMetastoreClient();
          mClient.grant_role(roleName,
              hivePrincipal.getName(),
              getThriftPrincipalType(hivePrincipal.getType()),
              grantorPrinc.getName(),
              getThriftPrincipalType(grantorPrinc.getType()), 
              grantOption
              );
        }  catch (Exception e) {
          String msg = "Error granting roles for " + hivePrincipal.getName() +  " to role " + roleName 
              + hivePrincipal.getName();
          throw new HiveAuthorizationPluginException(msg, e);
        }
      }
    }
  }

  @Override
  public void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roleNames,
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthorizationPluginException {
    for(HivePrincipal hivePrincipal : hivePrincipals){
      for(String roleName : roleNames){
        try {
          IMetaStoreClient mClient = metastoreClientFactory.getHiveMetastoreClient();
          mClient.grant_role(roleName,
              hivePrincipal.getName(),
              getThriftPrincipalType(hivePrincipal.getType()),
              grantorPrinc.getName(),
              getThriftPrincipalType(grantorPrinc.getType()), 
              grantOption
              );
        }  catch (Exception e) {
          String msg = "Error granting roles for " + hivePrincipal.getName() +  " to role " + roleName 
              + hivePrincipal.getName();
          throw new HiveAuthorizationPluginException(msg, e);
        }
      }
    }
  }

  @Override
  public List<String> getAllRoles() throws HiveAuthorizationPluginException {
    try {
      return metastoreClientFactory.getHiveMetastoreClient().listRoleNames();
    } catch (Exception e) {
      throw new HiveAuthorizationPluginException("Error listing all roles", e);
    }
  }


  @Override
  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj)
      throws HiveAuthorizationPluginException {
    try {
      
      List<HivePrivilegeInfo> resPrivInfos = new ArrayList<HivePrivilegeInfo>();
      IMetaStoreClient mClient = metastoreClientFactory.getHiveMetastoreClient();
      
      //get metastore/thrift privilege object using metastore api
      List<HiveObjectPrivilege> msObjPrivs
        = mClient.list_privileges(principal.getName(), getThriftPrincipalType(principal.getType()),
            getThriftHiveObjectRef(privObj));
      
      //convert the metastore thrift objects to result objects
      for(HiveObjectPrivilege msObjPriv : msObjPrivs){
        //result principal
        HivePrincipal resPrincipal = 
            new HivePrincipal(msObjPriv.getPrincipalName(), 
                AuthorizationUtils.getHivePrincipalType(msObjPriv.getPrincipalType()));

        //result privilege 
        PrivilegeGrantInfo msGrantInfo = msObjPriv.getGrantInfo();
        HivePrivilege resPrivilege = new HivePrivilege(msGrantInfo.getPrivilege(), null);
        
        //result object
        HiveObjectRef msObjRef = msObjPriv.getHiveObject();
        HivePrivilegeObject resPrivObj = new HivePrivilegeObject(
            getPluginObjType(msObjRef.getObjectType()), 
            msObjRef.getDbName(), 
            msObjRef.getObjectName()
            );
        
        //result grantor principal
        HivePrincipal grantorPrincipal = 
            new HivePrincipal(msGrantInfo.getGrantor(), 
                AuthorizationUtils.getHivePrincipalType(msGrantInfo.getGrantorType()));

        
        HivePrivilegeInfo resPrivInfo = new HivePrivilegeInfo(resPrincipal, resPrivilege,
            resPrivObj, grantorPrincipal, msGrantInfo.isGrantOption());
        resPrivInfos.add(resPrivInfo);
      }
      return resPrivInfos;
      
    }
    catch (Exception e) {
      throw new HiveAuthorizationPluginException("Error showing privileges", e);
    }

  }


  private HivePrivilegeObjectType getPluginObjType(HiveObjectType objectType)
      throws HiveAuthorizationPluginException {
    switch(objectType){
    case DATABASE:
      return HivePrivilegeObjectType.DATABASE;
    case TABLE:
      return HivePrivilegeObjectType.TABLE;
    default:
      throw new HiveAuthorizationPluginException("Unsupported object type " + objectType);
    }
  }

}
