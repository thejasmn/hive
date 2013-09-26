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
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.MRTask;
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
public class TezProcessor extends MRTask implements LogicalIOProcessor {
  private static final Log LOG = LogFactory.getLog(TezProcessor.class);

  //TODO: make isMap in MRTask protected
  boolean isMap;
  RecordProcessor rproc = null;

  public TezProcessor() {
    super(true);
    this.isMap = true;
  }

  @Override
  public void close() throws IOException {
    rproc.close();
  }

  @Override
  public void handleEvents(List<Event> arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void initialize(TezProcessorContext processorContext)
      throws IOException {
    try {
      super.initialize(processorContext);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }


  @Override
  public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs)
      throws IOException, InterruptedException {
    // in case of broadcast-join read the broadcast edge inputs
    // (possibly asynchronously)
    //call RecordProcessor.run

    LOG.info("Running map: " + processorContext.getUniqueIdentifier());

    initTask();

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
    if (!(in instanceof MRInputLegacy)) {
      throw new IOException(new TezException(
          "Only Simple Input supported. Input: " + in.getClass()));
    }
    MRInputLegacy input = (MRInputLegacy)in;

    KVWriter kvWriter = null;
    if (!(out instanceof OnFileSortedOutput)) {
      kvWriter = ((MROutput)out).getWriter();
    } else {
      kvWriter = ((OnFileSortedOutput)out).getWriter();
    }

    OutputCollector collector = new KVOutputCollector(kvWriter);

    RecordProcessor rproc = null;
    if(isMap){
      rproc = new MapRecordProcessor();
    }
    else{
      //TODO: implement reduce side
      throw new UnsupportedOperationException("Reduce is yet to be implemented");
    }
    rproc.init(jobConf, mrReporter, inputs.values(), collector);
    rproc.run();

    done(out);
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

  @Override
  public TezCounter getOutputRecordsCounter() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TezCounter getInputRecordsCounter() {
    // TODO Auto-generated method stub
    return null;
  }


}
