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
package org.apache.hadoop.hive.ql.parse.authorization;

import java.io.Serializable;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.session.SessionState;

public class AuthorizationTestUtil {

  public static DDLWork analyze(ASTNode ast, HiveConf conf, Hive db) throws Exception {
    DDLSemanticAnalyzer analyzer = new DDLSemanticAnalyzer(conf, db);
    SessionState.start(conf);
    analyzer.analyze(ast, new Context(conf));
    List<Task<? extends Serializable>> rootTasks = analyzer.getRootTasks();
    return (DDLWork) inList(rootTasks).ofSize(1).get(0).getWork();
  }

  public static DDLWork analyze(String command, HiveConf conf, Hive db) throws Exception {
    System.err.println("XXXXXXXXXXX 3 " + conf.getVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER));
    return analyze(parse(command), conf, db);
  }

  private static ASTNode parse(String command) throws Exception {
    return ParseUtils.findRootNonNullToken((new ParseDriver()).parse(command));
  }

  public static class ListSizeMatcher<E> {
    private final List<E> list;
    private ListSizeMatcher(List<E> list) {
      this.list = list;
    }
    private List<E> ofSize(int size) {
      Assert.assertEquals(list.toString(),  size, list.size());
      return list;
    }
  }

  public static <E> ListSizeMatcher<E> inList(List<E> list) {
    return new ListSizeMatcher<E>(list);
  }

}

