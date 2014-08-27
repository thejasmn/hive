/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.security.authorization;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * StorageBasedAuthorizationProvider is an implementation of
 * HiveMetastoreAuthorizationProvider that tries to look at the hdfs
 * permissions of files and directories associated with objects like
 * databases, tables and partitions to determine whether or not an
 * operation is allowed. The rule of thumb for which location to check
 * in hdfs is as follows:
 *
 * CREATE : on location specified, or on location determined from metadata
 * READS : not checked (the preeventlistener does not have an event to fire)
 * UPDATES : on location in metadata
 * DELETES : on location in metadata
 *
 * If the location does not yet exist, as the case is with creates, it steps
 * out to the parent directory recursively to determine its permissions till
 * it finds a parent that does exist.
 */
public class StorageBasedAuthorizationProvider extends HiveAuthorizationProviderBase
    implements HiveMetastoreAuthorizationProvider {

  private Warehouse wh;
  private boolean isRunFromMetaStore = false;

  private static Log LOG = LogFactory.getLog(StorageBasedAuthorizationProvider.class);

  /**
   * Make sure that the warehouse variable is set up properly.
   * @throws MetaException if unable to instantiate
   */
  private void initWh() throws MetaException, HiveException {
    if (wh == null){
      if(!isRunFromMetaStore){
        // Note, although HiveProxy has a method that allows us to check if we're being
        // called from the metastore or from the client, we don't have an initialized HiveProxy
        // till we explicitly initialize it as being from the client side. So, we have a
        // chicken-and-egg problem. So, we now track whether or not we're running from client-side
        // in the SBAP itself.
        hive_db = new HiveProxy(Hive.get(getConf(), StorageBasedAuthorizationProvider.class));
        this.wh = new Warehouse(getConf());
        if (this.wh == null){
          // If wh is still null after just having initialized it, bail out - something's very wrong.
          throw new IllegalStateException("Unable to initialize Warehouse from clientside.");
        }
      }else{
        // not good if we reach here, this was initialized at setMetaStoreHandler() time.
        // this means handler.getWh() is returning null. Error out.
        throw new IllegalStateException("Uninitialized Warehouse from MetastoreHandler");
      }
    }
  }

  @Override
  public void init(Configuration conf) throws HiveException {
    hive_db = new HiveProxy();
  }

  @Override
  public void authorize(Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    // Currently not used in hive code-base, but intended to authorize actions
    // that are directly user-level. As there's no storage based aspect to this,
    // we can follow one of two routes:
    // a) We can allow by default - that way, this call stays out of the way
    // b) We can deny by default - that way, no privileges are authorized that
    // is not understood and explicitly allowed.
    // Both approaches have merit, but given that things like grants and revokes
    // that are user-level do not make sense from the context of storage-permission
    // based auth, denying seems to be more canonical here.

    // Update to previous comment: there does seem to be one place that uses this
    // and that is to authorize "show databases" in hcat commandline, which is used
    // by webhcat. And user-level auth seems to be a reasonable default in this case.
    // The now deprecated HdfsAuthorizationProvider in hcatalog approached this in
    // another way, and that was to see if the user had said above appropriate requested
    // privileges for the hive root warehouse directory. That seems to be the best
    // mapping for user level privileges to storage. Using that strategy here.

    Path root = null;
    try {
      initWh();
      root = wh.getWhRoot();
      authorize(root, readRequiredPriv, writeRequiredPriv);
    } catch (MetaException ex) {
      throw hiveException(ex);
    }
  }

  @Override
  public void authorize(Database db, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    Path path = getDbLocation(db);

    // extract drop privileges
    DropPrivilegeExtractor privExtractor = new DropPrivilegeExtractor(readRequiredPriv,
        writeRequiredPriv);
    readRequiredPriv = privExtractor.getReadReqPriv();
    writeRequiredPriv = privExtractor.getWriteReqPriv();

    // authorize drops if there was a drop privilege requirement
    if(privExtractor.hasDropPrivilege()) {
      authorizeDropPrivilege(path);
    }

    authorize(path, readRequiredPriv, writeRequiredPriv);
  }

  @Override
  public void authorize(Table table, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    try {
      initWh();
    } catch (MetaException ex) {
      throw hiveException(ex);
    }
    // if CREATE priv requirement is there, the owner should have WRITE permission on
    // the database directory
    if(requireCreatePrivilege(readRequiredPriv) || requireCreatePrivilege(writeRequiredPriv)) {
      authorize(hive_db.getDatabase(table.getDbName()), new Privilege[] {},
          new Privilege[] { Privilege.ALTER_DATA });
    }

    // extract any drop privileges out of required privileges
    DropPrivilegeExtractor privExtractor = new DropPrivilegeExtractor(readRequiredPriv,
        writeRequiredPriv);
    readRequiredPriv = privExtractor.getReadReqPriv();
    writeRequiredPriv = privExtractor.getWriteReqPriv();

    Path path = table.getDataLocation();
    // authorize drops if there was a drop privilege requirement
    if(privExtractor.hasDropPrivilege()) {
      authorizeDropPrivilege(path);
    }

    // If the user has specified a location - external or not, check if the user
    // has the permissions on the table dir
    if (path != null) {
      authorize(path, readRequiredPriv, writeRequiredPriv);
    }
  }


  /**
   *
   * @param privs
   * @return true, if set of given privileges privs contain CREATE privilege
   */
  private boolean requireCreatePrivilege(Privilege[] privs) {
    for (Privilege priv : privs) {
      if (priv.equals(Privilege.CREATE)) {
        return true;
      }
    }
    return false;
  }


  private void authorizeDropPrivilege(Path path) throws HiveException {
    // This requires ability to delete the given path.
    // The following 2 conditions should be satisfied for this-
    // 1. Write permissions on parent dir
    // 2. If sticky bit is set on parent dir then one of following should be
    // true
    //   a. User is owner of the current dir/file
    //   b. User is owner of the parent dir
    //   Super users are also allowed to drop the file, but there is no good way of checking
    //   if a user is a super user. Also super users running hive queries is not a common
    //   use case. super users can also do a chown to be able to drop the file

    try {
      final FileSystem fs = path.getFileSystem(getConf());
      Path parPath = path.getParent();
      // check user has write permissions on the parent dir
      checkPermissions(getConf(), parPath, EnumSet.of(FsAction.WRITE));

      // check if sticky bit is set on the parent dir
      FileStatus parStatus = fs.getFileStatus(parPath);
      if (!parStatus.getPermission().getStickyBit()) {
        // no sticky bit, so write permission on parent dir is sufficient
        // no further checks needed
        return;
      }

      // check if user is owner of parent dir
      String user = authenticator.getUserName();
      if (parStatus.getOwner().equals(user)) {
        return;
      }

      // check if user is owner of current dir/file
      FileStatus childStatus = fs.getFileStatus(path);
      if (childStatus.getOwner().equals(user)) {
        return;
      }
      String msg = String.format("Permission Denied: User %s can't delete %s because sticky bit is"
          + " set on the parent dir and user does not own this file or its parent", user, path);
      throw new HiveException(msg);
    }
    catch (HiveException e) {
      throw e;
    }
    catch (Exception e) {
      throw new HiveException(e);
    }

  }

  @Override
  public void authorize(Partition part, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    authorize(part.getTable(), part, readRequiredPriv, writeRequiredPriv);
  }

  private void authorize(Table table, Partition part, Privilege[] readRequiredPriv,
      Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {

    // extract drop privileges
    DropPrivilegeExtractor privExtractor = new DropPrivilegeExtractor(readRequiredPriv,
        writeRequiredPriv);
    readRequiredPriv = privExtractor.getReadReqPriv();
    writeRequiredPriv = privExtractor.getWriteReqPriv();

    // authorize drops if there was a drop privilege requirement
    if(privExtractor.hasDropPrivilege()) {
      authorizeDropPrivilege(part.getDataLocation());
    }

    // Partition path can be null in the case of a new create partition - in this case,
    // we try to default to checking the permissions of the parent table.
    // Partition itself can also be null, in cases where this gets called as a generic
    // catch-all call in cases like those with CTAS onto an unpartitioned table (see HIVE-1887)
    if ((part == null) || (part.getLocation() == null)) {
      // this should be the case only if this is a create partition.
      // The privilege needed on the table should be ALTER_DATA, and not CREATE
      authorize(table, new Privilege[]{}, new Privilege[]{Privilege.ALTER_DATA});
    } else {
      authorize(part.getDataLocation(), readRequiredPriv, writeRequiredPriv);
    }
  }

  @Override
  public void authorize(Table table, Partition part, List<String> columns,
      Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException,
      AuthorizationException {
    // In a simple storage-based auth, we have no information about columns
    // living in different files, so we do simple partition-auth and ignore
    // the columns parameter.
    authorize(table, part, readRequiredPriv, writeRequiredPriv);
  }

  @Override
  public void setMetaStoreHandler(HMSHandler handler) {
    hive_db.setHandler(handler);
    this.wh = handler.getWh();
    this.isRunFromMetaStore = true;
  }

  /**
   * Given a privilege, return what FsActions are required
   */
  protected FsAction getFsAction(Privilege priv) {

    switch (priv.getPriv()) {
    case ALL:
      return FsAction.READ_WRITE;
    case ALTER_DATA:
      return FsAction.WRITE;
    case ALTER_METADATA:
      return FsAction.WRITE;
    case CREATE:
      return FsAction.WRITE;
    case DROP:
      return FsAction.WRITE;
    case INDEX:
      throw new AuthorizationException(
          "StorageBasedAuthorizationProvider cannot handle INDEX privilege");
    case LOCK:
      throw new AuthorizationException(
          "StorageBasedAuthorizationProvider cannot handle LOCK privilege");
    case SELECT:
      return FsAction.READ;
    case SHOW_DATABASE:
      return FsAction.READ;
    case UNKNOWN:
    default:
      throw new AuthorizationException("Unknown privilege");
    }
  }

  /**
   * Given a Privilege[], find out what all FsActions are required
   */
  protected EnumSet<FsAction> getFsActions(Privilege[] privs) {
    EnumSet<FsAction> actions = EnumSet.noneOf(FsAction.class);

    if (privs == null) {
      return actions;
    }

    for (Privilege priv : privs) {
      actions.add(getFsAction(priv));
    }

    return actions;
  }

  /**
   * Authorization privileges against a path.
   *
   * @param path
   *          a filesystem path
   * @param readRequiredPriv
   *          a list of privileges needed for inputs.
   * @param writeRequiredPriv
   *          a list of privileges needed for outputs.
   */
  public void authorize(Path path, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    try {
      EnumSet<FsAction> actions = getFsActions(readRequiredPriv);
      actions.addAll(getFsActions(writeRequiredPriv));
      if (actions.isEmpty()) {
        return;
      }

      checkPermissions(getConf(), path, actions);

    } catch (AccessControlException ex) {
      throw authorizationException(ex);
    } catch (LoginException ex) {
      throw authorizationException(ex);
    } catch (IOException ex) {
      throw hiveException(ex);
    }
  }


  /**
   * Checks the permissions for the given path and current user on Hadoop FS.
   * If the given path does not exists, it checks for its parent folder.
   */
  protected void checkPermissions(final Configuration conf, final Path path,
      final EnumSet<FsAction> actions) throws IOException, LoginException, HiveException {

    if (path == null) {
      throw new IllegalArgumentException("path is null");
    }

    final FileSystem fs = path.getFileSystem(conf);

    if (fs.exists(path)) {
      checkPermissions(fs, path, actions, authenticator.getUserName());
    } else if (path.getParent() != null) {
      // find the ancestor which exists to check its permissions
      Path par = path.getParent();
      while (par != null) {
        if (fs.exists(par)) {
          break;
        }
        par = par.getParent();
      }

      checkPermissions(fs, par, actions, authenticator.getUserName());
    }
  }

  /**
   * Checks the permissions for the given path and current user on Hadoop FS. If the given path
   * does not exists, it returns.
   */
  @SuppressWarnings("deprecation")
  protected static void checkPermissions(final FileSystem fs, final Path path,
      final EnumSet<FsAction> actions, String user) throws IOException,
      AccessControlException, HiveException {

    try {
      FileStatus stat = fs.getFileStatus(path);
      for (FsAction action : actions) {
        FileUtils.checkFileAccessWithImpersonation(fs, stat, action, user);
      }
    } catch (FileNotFoundException fnfe) {
      // File named by path doesn't exist; nothing to validate.
      return;
    } catch (org.apache.hadoop.fs.permission.AccessControlException ace) {
      // Older hadoop version will throw this @deprecated Exception.
      throw accessControlException(ace);
    } catch (Exception err) {
      throw new HiveException(err);
    }
  }

  protected Path getDbLocation(Database db) throws HiveException {
    try {
      initWh();
      String location = db.getLocationUri();
      if (location == null) {
        return wh.getDefaultDatabasePath(db.getName());
      } else {
        return wh.getDnsPath(wh.getDatabasePath(db));
      }
    } catch (MetaException ex) {
      throw hiveException(ex);
    }
  }

  private HiveException hiveException(Exception e) {
    return new HiveException(e);
  }

  private AuthorizationException authorizationException(Exception e) {
    return new AuthorizationException(e);
  }

  private static AccessControlException accessControlException(
      org.apache.hadoop.fs.permission.AccessControlException e) {
    AccessControlException ace = new AccessControlException(e.getMessage());
    ace.initCause(e);
    return ace;
  }

  @Override
  public void authorizeAuthorizationApiInvocation() throws HiveException, AuthorizationException {
    // no-op - SBA does not attempt to authorize auth api call. Allow it
  }

  public class DropPrivilegeExtractor {

    private boolean hasDropPrivilege = false;
    private final Privilege[] readReqPriv;
    private final Privilege[] writeReqPriv;
    public DropPrivilegeExtractor(Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) {
      this.readReqPriv = extractDropPriv(readRequiredPriv);
      this.writeReqPriv = extractDropPriv(writeRequiredPriv);
    }
    private Privilege[] extractDropPriv(Privilege[] requiredPrivs) {
      List<Privilege> privList = new ArrayList<Privilege>();
      for(Privilege priv : requiredPrivs) {
        if(priv.equals(Privilege.DROP)) {
          hasDropPrivilege = true;
        } else {
          privList.add(priv);
        }
      }
      return privList.toArray(new Privilege[0]);
    }

    public boolean hasDropPrivilege() {
      return hasDropPrivilege;
    }
    public void setHasDropPrivilege(boolean hasDropPrivilege) {
      this.hasDropPrivilege = hasDropPrivilege;
    }
    public Privilege[] getReadReqPriv() {
      return readReqPriv;
    }
    public Privilege[] getWriteReqPriv() {
      return writeReqPriv;
    }

  }

}
