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

package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.JobCloseFeedBack;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezSession;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;

/**
 *
 * TezTask handles the execution of TezWork. Currently it executes a graph of map and reduce work
 * using the Tez APIs directly.
 *
 */
@SuppressWarnings({"serial", "deprecation"})
public class TezTask extends Task<TezWork> {

  public TezTask() {
    super();
  }

  @Override
  public int execute(DriverContext driverContext) {
    int rc = 1;
    boolean cleanContext = false;
    Context ctx = null;
    DAGClient client = null;
    TezSessionState session = null;

    try {
      // Get or create Context object. If we create it we have to clean
      // it later as well.
      ctx = driverContext.getCtx();
      if (ctx == null) {
        ctx = new Context(conf);
        cleanContext = true;
      }

      // Need to remove this static hack. But this is the way currently to
      // get a session.
      SessionState ss = SessionState.get();
      session = ss.getTezSession();
      if (!session.isOpen()) {
        // can happen if the user sets the tez flag after the session was
        // established
        session.open(ss.getSessionId(), conf);
      }

      // we will localize all the files (jars, plans, hashtables) to the
      // scratch dir. let's create this first.
      Path scratchDir = new Path(ctx.getMRScratchDir());

      // create the tez tmp dir
      DagUtils.createTezDir(scratchDir, conf);

      // jobConf will hold all the configuration for hadoop, tez, and hive
      JobConf jobConf = DagUtils.createConfiguration(conf);

      // unless already installed on all the cluster nodes, we'll have to
      // localize hive-exec.jar as well.
      LocalResource appJarLr = session.getAppJarLr();

      // next we translate the TezWork to a Tez DAG
      DAG dag = build(jobConf, work, scratchDir, appJarLr, ctx);

      // submit will send the job to the cluster and start executing
      client = submit(jobConf, dag, scratchDir, appJarLr, session.getSession());

      // finally monitor will print progress until the job is done
      TezJobMonitor monitor = new TezJobMonitor();
      rc = monitor.monitorExecution(client);

    } catch (Exception e) {
      LOG.error("Failed to execute tez graph.", e);
      // rc will be 1 at this point indicating failure.
    } finally {
      Utilities.clearWork(conf);
      if (cleanContext) {
        try {
          ctx.clear();
        } catch (Exception e) {
          /*best effort*/
          LOG.warn("Failed to clean up after tez job");
        }
      }
      // need to either move tmp files or remove them
      if (client != null) {
        // rc will only be overwritten if close errors out
        rc = close(work, rc);
      }
    }
    return rc;
  }

  private DAG build(JobConf conf, TezWork work, Path scratchDir,
      LocalResource appJarLr, Context ctx)
      throws Exception {

    Map<BaseWork, Vertex> workToVertex = new HashMap<BaseWork, Vertex>();
    Map<BaseWork, JobConf> workToConf = new HashMap<BaseWork, JobConf>();

    // we need to get the user specified local resources for this dag
    List<LocalResource> additionalLr = DagUtils.localizeTempFiles(conf);

    // getAllWork returns a topologically sorted list, which we use to make
    // sure that vertices are created before they are used in edges.
    List<BaseWork> ws = work.getAllWork();
    Collections.reverse(ws);

    Path tezDir = DagUtils.getTezDir(scratchDir);
    FileSystem fs = tezDir.getFileSystem(conf);

    // the name of the dag is what is displayed in the AM/Job UI
    DAG dag = new DAG(
        Utilities.abbreviate(HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYSTRING),
        HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVEJOBNAMELENGTH)));

    int i = ws.size();
    for (BaseWork w: ws) {

      boolean isFinal = work.getLeaves().contains(w);

      // translate work to vertex
      JobConf wxConf = DagUtils.initializeVertexConf(conf, w);
      Vertex wx = DagUtils.createVertex(wxConf, w, tezDir,
          i--, appJarLr, additionalLr, fs, ctx, !isFinal);
      dag.addVertex(wx);
      workToVertex.put(w, wx);
      workToConf.put(w, wxConf);

      // add all dependencies (i.e.: edges) to the graph
      for (BaseWork v: work.getChildren(w)) {
        assert workToVertex.containsKey(v);
        Edge e = DagUtils.createEdge(wxConf, wx, workToConf.get(v), workToVertex.get(v));
        dag.addEdge(e);
      }
    }

    return dag;
  }

  private DAGClient submit(JobConf conf, DAG dag, Path scratchDir,
      LocalResource appJarLr, TezSession session)
      throws IOException, TezException, InterruptedException {

    // ready to start execution on the cluster
    DAGClient dagClient = session.submitDAG(dag);

    return dagClient;
  }

  /*
   * close will move the temp files into the right place for the fetch
   * task. If the job has failed it will clean up the files.
   */
  private int close(TezWork work, int rc) {
    try {
      JobCloseFeedBack feedBack = new JobCloseFeedBack();
      List<BaseWork> ws = work.getAllWork();
      for (BaseWork w: ws) {
        List<Operator<?>> ops = w.getAllOperators();
        for (Operator<?> op: ops) {
          op.jobClose(conf, rc == 0, feedBack);
        }
      }
    } catch (Exception e) {
      // jobClose needs to execute successfully otherwise fail task
      if (rc == 0) {
        rc = 3;
        String mesg = "Job Commit failed with exception '"
          + Utilities.getNameMessage(e) + "'";
        console.printError(mesg, "\n" + StringUtils.stringifyException(e));
      }
    }
    return rc;
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public StageType getType() {
    return StageType.MAPRED;
  }

  @Override
  public String getName() {
    return "TEZ";
  }
}
