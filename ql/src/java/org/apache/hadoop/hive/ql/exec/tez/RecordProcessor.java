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
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.LogicalInput;

/**
 * Process input from tez LogicalInput and write output
 */
public abstract class RecordProcessor  {

  protected JobConf jconf;
  protected Collection<LogicalInput> inputs;
  protected OutputCollector out;

  public static final Log l4j = LogFactory.getLog(RecordProcessor.class);


  // used to log memory usage periodically
  public static MemoryMXBean memoryMXBean;
  protected boolean isLogInfoEnabled = false;
  protected MRTaskReporter reporter;

  private long numRows = 0;
  private long nextCntr = 1;


  void init(JobConf jconf, MRTaskReporter mrReporter, Collection<LogicalInput> inputs,
      OutputCollector out){
    this.jconf = jconf;
    this.reporter = mrReporter;
    this.inputs = inputs;
    this.out = out;

    // Allocate the bean at the beginning -
    memoryMXBean = ManagementFactory.getMemoryMXBean();
    l4j.info("maximum memory = " + memoryMXBean.getHeapMemoryUsage().getMax());

    isLogInfoEnabled = l4j.isInfoEnabled();

    try {
      l4j.info("conf classpath = "
          + Arrays.asList(((URLClassLoader) jconf.getClassLoader()).getURLs()));
      l4j.info("thread classpath = "
          + Arrays.asList(((URLClassLoader) Thread.currentThread()
          .getContextClassLoader()).getURLs()));
    } catch (Exception e) {
      l4j.info("cannot get classpath: " + e.getMessage());
    }

  }


  abstract void close();

  abstract void run() throws IOException;

  protected void logCloseInfo() {
    long used_memory = memoryMXBean.getHeapMemoryUsage().getUsed();
    l4j.info("ExecMapper: processed " + numRows + " rows: used memory = "
        + used_memory);
  }

  protected void logProgress() {
    numRows++;
    if (numRows == nextCntr) {
      long used_memory = memoryMXBean.getHeapMemoryUsage().getUsed();
      l4j.info("ExecMapper: processing " + numRows
          + " rows: used memory = " + used_memory);
      nextCntr = getNextCntr(numRows);
    }
  }

  private long getNextCntr(long cntr) {
    // A very simple counter to keep track of number of rows processed by the
    // reducer. It dumps
    // every 1 million times, and quickly before that
    if (cntr >= 1000000) {
      return cntr + 1000000;
    }

    return 10 * cntr;
  }

  static class LogicalInputRecordReader implements RecordReader {
    private final MRInputLegacy input;

    LogicalInputRecordReader(LogicalInput in) throws IOException {
      // Sanity check
      if (!(in instanceof MRInputLegacy)) {
        throw new IOException(
            "Only Simple Input supported. Input: " + in.getClass());
      }
      input = (MRInputLegacy)in;
    }

    @Override
    public boolean next(Object key, Object value) throws IOException {
      // TODO broken
//      simpleInput.setKey(key);
//      simpleInput.setValue(value);
//      try {
//        return simpleInput.hasNext();
//      } catch (InterruptedException ie) {
//        throw new IOException(ie);
//      }
      return input.getOldRecordReader().next(key, value);
    }

    @Override
    public Object createKey() {
      return input.getOldRecordReader().createKey();
    }

    @Override
    public Object createValue() {
      return input.getOldRecordReader().createValue();
    }

    @Override
    public long getPos() throws IOException {
      return input.getOldRecordReader().getPos();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
      try {
        return input.getProgress();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
  }

}
