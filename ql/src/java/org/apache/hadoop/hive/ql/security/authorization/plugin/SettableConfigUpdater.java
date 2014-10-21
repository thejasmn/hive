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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Unstable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import com.google.common.base.Joiner;

/**
 * Helper class that can be used by authorization implementations to set a
 * default list of 'safe' HiveConf parameters that can be edited by user. It
 * uses HiveConf white list parameters to enforce this. This can be called from
 * HiveAuthorizer.applyAuthorizationConfigPolicy
 *
 * The set of config parameters that can be set is restricted to parameters that
 * don't allow for any code injection, and config parameters that are not
 * considered an 'admin config' option.
 *
 */
@LimitedPrivate(value = { "Apache Argus (incubating)" })
@Evolving
@Unstable
public class SettableConfigUpdater {

  public static void setHiveConfWhiteList(HiveConf hiveConf) {

    hiveConf.setIsModWhiteListEnabled(true);

    String whiteListParamsStr = hiveConf
        .getVar(ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST);
    if (whiteListParamsStr != null && !whiteListParamsStr.trim().equals("")) {
      //for backward compatibility reasons we need to support comma separated list of regexes
      // convert "," into "|"
      whiteListParamsStr = whiteListParamsStr.trim().replaceAll(",", "|");
    } else {
      // set the default configs in whitelist
      whiteListParamsStr = getDefaultWhiteListPattern();
    }
    // set it in hiveconf so that current value can be seen for debugging purposes
    hiveConf
    .setVar(ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST, whiteListParamsStr);

    // append regexes that user wanted to add
    String whiteListAppend = hiveConf
        .getVar(ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST_APPEND);
    if (whiteListAppend != null && !whiteListAppend.trim().equals("")) {
      whiteListAppend = whiteListAppend.trim().replaceAll(",", "|");
      whiteListParamsStr = whiteListParamsStr + "|" + whiteListAppend;
    }

    hiveConf.setModifiableWhiteListRegex(whiteListParamsStr);

  }

  private static String getDefaultWhiteListPattern() {
    String confVarPatternStr = Joiner.on("|").join(convertDotsToSlashDot(defaultConfVars));
    String regexPatternStr = Joiner.on("|").join(
        convertStarToDotStar(convertDotsToSlashDot(defaultPatterns)));
    return regexPatternStr + "|" + confVarPatternStr;
  }

  /**
   * @param paramList  list of parameter strings
   * @return list of parameter strings with "." replaced by "\."
   */
  private static String[] convertDotsToSlashDot(String[] paramList) {
    String[] regexes = new String[paramList.length];
    for(int i=0; i<paramList.length; i++) {
      regexes[i] = paramList[i].replace(".", "\\." );
    }
    return regexes;
  }

  /**
   * @param paramList  list of parameter strings
   * @return list of parameter strings with "*" replaced by ".*"
   */
  private static String[] convertStarToDotStar(String[] paramList) {
    String[] regexes = new String[paramList.length];
    for(int i=0; i<paramList.length; i++) {
      regexes[i] = paramList[i].replace("*", ".*" );
    }
    return regexes;
  }


  /**
   * Default list of modifiable config parameters for sql standard authorization
   * For internal use only. This is package private only for testing purposes
   */
  static final String [] defaultConfVars = new String [] {
    ConfVars.BYTESPERREDUCER.varname,
    ConfVars.CLIENT_STATS_COUNTERS.varname,
    ConfVars.DEFAULTPARTITIONNAME.varname,
    ConfVars.DROPIGNORESNONEXISTENT.varname,
    ConfVars.HIVECOUNTERGROUP.varname,
    ConfVars.HIVEENFORCEBUCKETING.varname,
    ConfVars.HIVEENFORCEBUCKETMAPJOIN.varname,
    ConfVars.HIVEENFORCESORTING.varname,
    ConfVars.HIVEENFORCESORTMERGEBUCKETMAPJOIN.varname,
    ConfVars.HIVEEXPREVALUATIONCACHE.varname,
    ConfVars.HIVEGROUPBYSKEW.varname,
    ConfVars.HIVEHASHTABLELOADFACTOR.varname,
    ConfVars.HIVEHASHTABLETHRESHOLD.varname,
    ConfVars.HIVEIGNOREMAPJOINHINT.varname,
    ConfVars.HIVELIMITMAXROWSIZE.varname,
    ConfVars.HIVEMAPREDMODE.varname,
    ConfVars.HIVEMAPSIDEAGGREGATE.varname,
    ConfVars.HIVEOPTIMIZEMETADATAQUERIES.varname,
    ConfVars.HIVEROWOFFSET.varname,
    ConfVars.HIVEVARIABLESUBSTITUTE.varname,
    ConfVars.HIVEVARIABLESUBSTITUTEDEPTH.varname,
    ConfVars.HIVE_AUTOGEN_COLUMNALIAS_PREFIX_INCLUDEFUNCNAME.varname,
    ConfVars.HIVE_AUTOGEN_COLUMNALIAS_PREFIX_LABEL.varname,
    ConfVars.HIVE_CHECK_CROSS_PRODUCT.varname,
    ConfVars.HIVE_COMPAT.varname,
    ConfVars.HIVE_CONCATENATE_CHECK_INDEX.varname,
    ConfVars.HIVE_DISPLAY_PARTITION_COLUMNS_SEPARATELY.varname,
    ConfVars.HIVE_ERROR_ON_EMPTY_PARTITION.varname,
    ConfVars.HIVE_EXECUTION_ENGINE.varname,
    ConfVars.HIVE_EXIM_URI_SCHEME_WL.varname,
    ConfVars.HIVE_FILE_MAX_FOOTER.varname,
    ConfVars.HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES.varname,
    ConfVars.HIVE_INSERT_INTO_MULTILEVEL_DIRS.varname,
    ConfVars.HIVE_LOCALIZE_RESOURCE_NUM_WAIT_ATTEMPTS.varname,
    ConfVars.HIVE_MULTI_INSERT_MOVE_TASKS_SHARE_DEPENDENCIES.varname,
    ConfVars.HIVE_QUOTEDID_SUPPORT.varname,
    ConfVars.HIVE_RESULTSET_USE_UNIQUE_COLUMN_NAMES.varname,
    ConfVars.HIVE_STATS_COLLECT_PART_LEVEL_STATS.varname,
    ConfVars.JOB_DEBUG_CAPTURE_STACKTRACES.varname,
    ConfVars.JOB_DEBUG_TIMEOUT.varname,
    ConfVars.MAXCREATEDFILES.varname,
    ConfVars.MAXREDUCERS.varname,
    ConfVars.OUTPUT_FILE_EXTENSION.varname,
    ConfVars.SHOW_JOB_FAIL_DEBUG_INFO.varname,
    ConfVars.TASKLOG_DEBUG_TIMEOUT.varname,

    "mapred.reduce.tasks",
    "mapred.output.compression.codec",
    "mapred.map.output.compression.codec",
    "mapreduce.job.reduce.slowstart.completedmaps",
    "mapreduce.job.queuename",
    "mapreduce.input.fileinputformat.split.minsize",
  };

  /**
   * To use the following patterns as java regexes, the "." is converted to "\\."
   * and then "*" is converted to ".*" .
   * Storing them with "\\." is an eye sore
   * For internal use only. This is package private only for testing purposes
   */
  static final String [] defaultPatterns = new String [] {
    "hive.auto.*",
    "hive.cbo.*",
    "hive.convert.*",
    "hive.exec.*.dynamic.partitions.*",
    "hive.exec.compress.*",
    "hive.exec.infer.*",
    "hive.exec.mode.local.*",
    "hive.exec.orc.*",
    "hive.fetch.task.*",
    "hive.hbase.*",
    "hive.index.*",
    "hive.index.*",
    "hive.intermediate.*",
    "hive.join.*",
    "hive.limit.*",
    "hive.mapjoin.*",
    "hive.merge.*",
    "hive.optimize.*",
    "hive.orc.*",
    "hive.outerjoin.*",
    "hive.ppd.*",
    "hive.prewarm.*",
    "hive.skewjoin.*",
    "hive.smbjoin.*",
    "hive.stats.*",
    "hive.tez.*",
    "hive.vectorized.*",
    "mapred.map.*",
    "mapred.reduce.*",
    "mapreduce.map.*",
    "mapreduce.reduce.*",
    "tez.am.*",
    "tez.task.*",
    "tez.runtime.*"
  };

}
