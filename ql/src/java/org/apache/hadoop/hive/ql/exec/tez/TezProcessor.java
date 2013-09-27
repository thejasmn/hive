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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalIOProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.library.api.KVWriter;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;

/**
 * Hive processor for Tez that forms the vertices in Tez and processes the data.
 * Does what ExecMapper and ExecReducer does for hive in MR framework.
 */
public class TezProcessor implements LogicalIOProcessor {
  private static final Log LOG = LogFactory.getLog(TezProcessor.class);

  boolean isMap;
  RecordProcessor rproc = null;

  private JobConf jobConf;

  private TezProcessorContext processorContext;

  public TezProcessor() {
    this.isMap = true;
  }

  @Override
  public void close() throws IOException {
    if(rproc != null){
      rproc.close();
    }
  }

  @Override
  public void handleEvents(List<Event> arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void initialize(TezProcessorContext processorContext)
      throws IOException {
    this.processorContext = processorContext;
    //get the jobconf
    byte[] userPayload = processorContext.getUserPayload();
    Configuration conf = TezUtils.createConfFromUserPayload(userPayload);
    this.jobConf = new JobConf(conf);
  }


  @Override
  public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs)
      throws IOException, InterruptedException {
    // in case of broadcast-join read the broadcast edge inputs
    // (possibly asynchronously)

    LOG.info("Running map: " + processorContext.getUniqueIdentifier());

    //TODO - change this to support shuffle joins, broadcast joins .
    if (inputs.size() != 1
        || outputs.size() != 1) {
      throw new IOException("Cannot handle multiple inputs or outputs"
          + ", inputCount=" + inputs.size()
          + ", outputCount=" + outputs.size());
    }
    LogicalInput in = inputs.values().iterator().next();
    LogicalOutput out = outputs.values().iterator().next();

    // Sanity check
    if (!(in instanceof MRInput)) {
      throw new IOException(new TezException(
          "Only Simple Input supported. Input: " + in.getClass()));
    }
    MRInput input = (MRInputLegacy)in;

    //update config
    Configuration updatedConf = input.getConfigUpdates();
    if (updatedConf != null) {
      for (Entry<String, String> entry : updatedConf) {
        this.jobConf.set(entry.getKey(), entry.getValue());
      }
    }


    KVWriter kvWriter = null;
    //TODO: this instanceof probably can be cleaned up
    if (!(out instanceof OnFileSortedOutput)) {
      kvWriter = ((MROutput)out).getWriter();
    } else {
      kvWriter = ((OnFileSortedOutput)out).getWriter();
    }

    OutputCollector collector = new KVOutputCollector(kvWriter);

    if(isMap){
      rproc = new MapRecordProcessor();
    }
    else{
      //TODO: implement reduce side
      throw new UnsupportedOperationException("Reduce is yet to be implemented");
    }
    //printJobConf(jobConf);
    MRTaskReporter mrReporter = new MRTaskReporter(processorContext);
    rproc.init(jobConf, mrReporter, inputs.values(), collector);
    rproc.run();

    done(out);
  }

  private void done(LogicalOutput output) throws IOException {
    MROutput sOut = (MROutput)output;
    if (sOut.isCommitRequired()) {
      //wait for commit approval and commit
      // TODO EVENTUALLY - Commit is not required for map tasks.
      // skip a couple of RPCs before exiting.
      commit(sOut);
    }
  }

  private void commit(MROutput output) throws IOException {
    int retries = 3;
    while (true) {
      // This will loop till the AM asks for the task to be killed. As
      // against, the AM sending a signal to the task to kill itself
      // gracefully.
      try {
        if (processorContext.canCommit()) {
          break;
        }
        Thread.sleep(1000);
      } catch(InterruptedException ie) {
        //ignore
      } catch (IOException ie) {
        LOG.warn("Failure sending canCommit: "
            + StringUtils.stringifyException(ie));
        if (--retries == 0) {
          throw ie;
        }
      }
    }

    // task can Commit now
    try {
      output.commit();
      return;
    } catch (IOException iee) {
      LOG.warn("Failure committing: " +
          StringUtils.stringifyException(iee));
      //if it couldn't commit a successfully then delete the output
      discardOutput(output);
      throw iee;
    }
  }

  private void discardOutput(MROutput output) {
    try {
      output.abort();
    } catch (IOException ioe)  {
      LOG.warn("Failure cleaning up: " +
               StringUtils.stringifyException(ioe));
    }
  }

  private void printJobConf(JobConf jobConf) {
    Iterator<Entry<String, String>> it = jobConf.iterator();
    while(it.hasNext()){
      Entry<String, String> entry = it.next();
      System.err.println("JobConf entry " + entry.getKey() + " = " + entry.getValue());
    }
  }

  /**
   * KVOutputCollector. OutputCollector that writes using KVWriter
   *
   */
  static class KVOutputCollector implements OutputCollector {
    private final KVWriter output;

    KVOutputCollector(KVWriter output) {
      this.output = output;
    }

    public void collect(Object key, Object value) throws IOException {
        output.write(key, value);
    }
  }

}
