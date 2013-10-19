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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.ql.io.orc.Reader.FileMetaInfo;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
/**
 * A MapReduce/Hive input format for ORC files.
 */
public class OrcInputFormat  implements InputFormat<NullWritable, OrcStruct>,
  InputFormatChecker, VectorizedInputFormatInterface {

  VectorizedOrcInputFormat voif = new VectorizedOrcInputFormat();

  private static final Log LOG = LogFactory.getLog(OrcInputFormat.class);
  static final String MIN_SPLIT_SIZE = "mapred.min.split.size";
  static final String MAX_SPLIT_SIZE = "mapred.max.split.size";
  private static final long DEFAULT_MIN_SPLIT_SIZE = 16 * 1024 * 1024;
  private static final long DEFAULT_MAX_SPLIT_SIZE = 256 * 1024 * 1024;

  private static final PerfLogger perfLogger = PerfLogger.getPerfLogger();
  private static final String CLASS_NAME = ReaderImpl.class.getName();

  /**
   * When picking the hosts for a split that crosses block boundaries,
   * any drop any host that has fewer than MIN_INCLUDED_LOCATION of the
   * number of bytes available on the host with the most.
   * If host1 has 10MB of the split, host2 has 20MB, and host3 has 18MB the
   * split will contain host2 (100% of host2) and host3 (90% of host2). Host1
   * with 50% will be dropped.
   */
  private static final double MIN_INCLUDED_LOCATION = 0.80;

  private static class OrcRecordReader
      implements RecordReader<NullWritable, OrcStruct> {
    private final org.apache.hadoop.hive.ql.io.orc.RecordReader reader;
    private final long offset;
    private final long length;
    private final int numColumns;
    private float progress = 0.0f;


    OrcRecordReader(Reader file, Configuration conf,
                    long offset, long length) throws IOException {
      List<OrcProto.Type> types = file.getTypes();
      numColumns = (types.size() == 0) ? 0 : types.get(0).getSubtypesCount();
      boolean[] includedColumns = findIncludedColumns(types, conf);
      String[] columnNames = getIncludedColumnNames(types, includedColumns, conf);
      SearchArgument sarg = createSarg(types, conf);
      this.reader = file.rows(offset, length, includedColumns, sarg, columnNames);
      this.offset = offset;
      this.length = length;
    }

    @Override
    public boolean next(NullWritable key, OrcStruct value) throws IOException {
      if (reader.hasNext()) {
        reader.next(value);
        progress = reader.getProgress();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public OrcStruct createValue() {
      return new OrcStruct(numColumns);
    }

    @Override
    public long getPos() throws IOException {
      return offset + (long) (progress * length);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public float getProgress() throws IOException {
      return progress;
    }
  }

  private static final PathFilter hiddenFileFilter = new PathFilter(){
    public boolean accept(Path p){
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  /**
   * Recurse down into a type subtree turning on all of the sub-columns.
   * @param types the types of the file
   * @param result the global view of columns that should be included
   * @param typeId the root of tree to enable
   */
  static void includeColumnRecursive(List<OrcProto.Type> types,
                                             boolean[] result,
                                             int typeId) {
    result[typeId] = true;
    OrcProto.Type type = types.get(typeId);
    int children = type.getSubtypesCount();
    for(int i=0; i < children; ++i) {
      includeColumnRecursive(types, result, type.getSubtypes(i));
    }
  }

  public static SearchArgument createSarg(List<OrcProto.Type> types, Configuration conf) {
    String serializedPushdown = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (serializedPushdown == null
        || conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR) == null) {
      LOG.info("No ORC pushdown predicate");
      return null;
    }
    SearchArgument sarg = SearchArgument.FACTORY.create
        (Utilities.deserializeExpression(serializedPushdown, conf));
    LOG.info("ORC pushdown predicate: " + sarg);
    return sarg;
  }

  public static String[] getIncludedColumnNames(
      List<OrcProto.Type> types, boolean[] includedColumns, Configuration conf) {
    String columnNamesString = conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
    LOG.info("included columns names = " + columnNamesString);
    if (columnNamesString == null || conf.get(TableScanDesc.FILTER_EXPR_CONF_STR) == null) {
      return null;
    }
    String[] neededColumnNames = columnNamesString.split(",");
    int i = 0;
    String[] columnNames = new String[types.size()];
    for(int columnId: types.get(0).getSubtypesList()) {
      if (includedColumns == null || includedColumns[columnId]) {
        columnNames[columnId] = neededColumnNames[i++];
      }
    }
    return columnNames;
  }

  /**
   * Take the configuration and figure out which columns we need to include.
   * @param types the types of the file
   * @param conf the configuration
   * @return true for each column that should be included
   */
  public static boolean[] findIncludedColumns(List<OrcProto.Type> types, Configuration conf) {
    LOG.info("included column ids = " + conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
    if (ColumnProjectionUtils.isReadAllColumns(conf)) {
      return null;
    } else {
      int numColumns = types.size();
      boolean[] result = new boolean[numColumns];
      result[0] = true;
      OrcProto.Type root = types.get(0);
      List<Integer> included = ColumnProjectionUtils.getReadColumnIDs(conf);
      for(int i=0; i < root.getSubtypesCount(); ++i) {
        if (included.contains(i)) {
          includeColumnRecursive(types, result, root.getSubtypes(i));
        }
      }
      // if we are filtering at least one column, return the boolean array
      for(boolean include: result) {
        if (!include) {
          return result;
        }
      }
      return null;
    }
  }

  @Override
  public RecordReader<NullWritable, OrcStruct>
      getRecordReader(InputSplit inputSplit, JobConf conf,
                      Reporter reporter) throws IOException {
    if (isVectorMode(conf)) {
      RecordReader<NullWritable, VectorizedRowBatch> vorr = voif.getRecordReader(inputSplit, conf,
          reporter);
      return (RecordReader) vorr;
    }

    OrcSplit orcSplit = (OrcSplit) inputSplit;
    Path path = orcSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
    reporter.setStatus(orcSplit.toString());
    return new OrcRecordReader(OrcFile.createReader(fs, path, orcSplit.getFileMetaInfo()), conf,
                               orcSplit.getStart(), orcSplit.getLength());
  }

  @Override
  public boolean validateInput(FileSystem fs, HiveConf conf,
                               ArrayList<FileStatus> files
                              ) throws IOException {

    if (isVectorMode(conf)) {
      return voif.validateInput(fs, conf, files);
    }

    if (files.size() <= 0) {
      return false;
    }
    for (FileStatus file : files) {
      try {
        OrcFile.createReader(fs, file.getPath());
      } catch (IOException e) {
        return false;
      }
    }
    return true;
  }

  private boolean isVectorMode(Configuration conf) {
    if (Utilities.getPlanPath(conf) != null && Utilities
        .getMapRedWork(conf).getMapWork().getVectorMode()) {
      return true;
    }
    return false;
  }

  /**
   * Get the list of input {@link Path}s for the map-reduce job.
   *
   * @param conf The configuration of the job
   * @return the list of input {@link Path}s for the map-reduce job.
   */
  static Path[] getInputPaths(JobConf conf) throws IOException {
    String dirs = conf.get("mapred.input.dir");
    if (dirs == null) {
      throw new IOException("Configuration mapred.input.dir is not defined.");
    }
    String [] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }

  /**
   * The global information about the split generation that we pass around to
   * the different worker threads.
   */
  static class Context {
    private final ExecutorService threadPool = Executors.newFixedThreadPool(10);
    private final List<OrcSplit> splits = new ArrayList<OrcSplit>(10000);
    private final List<Throwable> errors = new ArrayList<Throwable>();
    private final HadoopShims shims = ShimLoader.getHadoopShims();
    private final Configuration conf;
    private final long maxSize;
    private final long minSize;

    /**
     * A count of the number of threads that may create more work for the
     * thread pool.
     */
    private int schedulers = 0;

    Context(Configuration conf) {
      this.conf = conf;
      minSize = conf.getLong(MIN_SPLIT_SIZE, DEFAULT_MIN_SPLIT_SIZE);
      maxSize = conf.getLong(MAX_SPLIT_SIZE, DEFAULT_MAX_SPLIT_SIZE);
    }

    int getSchedulers() {
      return schedulers;
    }

    /**
     * Get the Nth split.
     * @param index if index >= 0, count from the front, otherwise count from
     *     the back.
     * @result the Nth file split
     */
    OrcSplit getResult(int index) {
      if (index >= 0) {
        return splits.get(index);
      } else {
        return splits.get(splits.size() + index);
      }
    }

    List<Throwable> getErrors() {
      return errors;
    }

    /**
     * Add a unit of work.
     * @param runnable the object to run
     */
    synchronized void schedule(Runnable runnable) {
      if (runnable instanceof FileGenerator) {
        schedulers += 1;
      }
      threadPool.execute(runnable);
    }

    /**
     * Mark a worker that may generate more work as done.
     */
    synchronized void decrementSchedulers() {
      schedulers -= 1;
      if (schedulers == 0) {
        notify();
      }
    }

    /**
     * Wait until all of the tasks are done. It waits until all of the
     * threads that may create more work are done and then shuts down the
     * thread pool and waits for the final threads to finish.
     */
    synchronized void waitForTasks() {
      try {
        while (schedulers != 0) {
          wait();
        }
        threadPool.shutdown();
        threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      } catch (InterruptedException ie) {
        throw new IllegalStateException("interrupted", ie);
      }
    }
  }

  /**
   * Given a directory, get the list of files and blocks in those files.
   * A thread is used for each directory.
   */
  static final class FileGenerator implements Runnable {
    private final Context context;
    private final FileSystem fs;
    private final Path dir;

    FileGenerator(Context context, FileSystem fs, Path dir) {
      this.context = context;
      this.fs = fs;
      this.dir = dir;
    }

    /**
     * For each path, get the list of files and blocks that they consist of.
     */
    @Override
    public void run() {
      try {
        Iterator<FileStatus> itr = context.shims.listLocatedStatus(fs, dir,
            hiddenFileFilter);
        while (itr.hasNext()) {
          FileStatus file = itr.next();
          if (!file.isDir()) {
            context.schedule(new SplitGenerator(context, fs, file));
          }
        }
        // mark the fact that we are done
        context.decrementSchedulers();
      } catch (Throwable th) {
        context.decrementSchedulers();
        synchronized (context.errors) {
          context.errors.add(th);
        }
      }
    }
  }

  /**
   * Split the stripes of a given file into input splits.
   * A thread is used for each file.
   */
  static final class SplitGenerator implements Runnable {
    private final Context context;
    private final FileSystem fs;
    private final FileStatus file;
    private final long blockSize;
    private final BlockLocation[] locations;

    SplitGenerator(Context context, FileSystem fs,
                   FileStatus file) throws IOException {
      this.context = context;
      this.fs = fs;
      this.file = file;
      this.blockSize = file.getBlockSize();
      locations = context.shims.getLocations(fs, file);
    }

    Path getPath() {
      return file.getPath();
    }

    @Override
    public String toString() {
      return "splitter(" + file.getPath() + ")";
    }

    /**
     * Compute the number of bytes that overlap between the two ranges.
     * @param offset1 start of range1
     * @param length1 length of range1
     * @param offset2 start of range2
     * @param length2 length of range2
     * @return the number of bytes in the overlap range
     */
    static long getOverlap(long offset1, long length1,
                           long offset2, long length2) {
      long end1 = offset1 + length1;
      long end2 = offset2 + length2;
      if (end2 <= offset1 || end1 <= offset2) {
        return 0;
      } else {
        return Math.min(end1, end2) - Math.max(offset1, offset2);
      }
    }

    /**
     * Create an input split over the given range of bytes. The location of the
     * split is based on where the majority of the byte are coming from. ORC
     * files are unlikely to have splits that cross between blocks because they
     * are written with large block sizes.
     * @param offset the start of the split
     * @param length the length of the split
     * @param fileMetaInfo file metadata from footer and postscript
     * @throws IOException
     */
    void createSplit(long offset, long length, FileMetaInfo fileMetaInfo) throws IOException {
      String[] hosts;
      if ((offset % blockSize) + length <= blockSize) {
        // handle the single block case
        hosts = locations[(int) (offset / blockSize)].getHosts();
      } else {
        // Calculate the number of bytes in the split that are local to each
        // host.
        Map<String, LongWritable> sizes = new HashMap<String, LongWritable>();
        long maxSize = 0;
        for(BlockLocation block: locations) {
          long overlap = getOverlap(offset, length, block.getOffset(),
              block.getLength());
          if (overlap > 0) {
            for(String host: block.getHosts()) {
              LongWritable val = sizes.get(host);
              if (val == null) {
                val = new LongWritable();
                sizes.put(host, val);
              }
              val.set(val.get() + overlap);
              maxSize = Math.max(maxSize, val.get());
            }
          }
        }
        // filter the list of locations to those that have at least 80% of the
        // max
        long threshold = (long) (maxSize * MIN_INCLUDED_LOCATION);
        List<String> hostList = new ArrayList<String>();
        // build the locations in a predictable order to simplify testing
        for(BlockLocation block: locations) {
          for(String host: block.getHosts()) {
            if (sizes.containsKey(host)) {
              if (sizes.get(host).get() >= threshold) {
                hostList.add(host);
              }
              sizes.remove(host);
            }
          }
        }
        hosts = new String[hostList.size()];
        hostList.toArray(hosts);
      }
      synchronized (context.splits) {
        context.splits.add(new OrcSplit(file.getPath(), offset, length,
            hosts, fileMetaInfo));
      }
    }

    /**
     * Divide the adjacent stripes in the file into input splits based on the
     * block size and the configured minimum and maximum sizes.
     */
    @Override
    public void run() {
      perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.CREATE_ORC_SPLITS);
      try {
        Reader orcReader = OrcFile.createReader(fs, file.getPath());
        long currentOffset = -1;
        long currentLength = 0;
        for(StripeInformation stripe: orcReader.getStripes()) {
          // if we are working on a stripe, over the min stripe size, and
          // crossed a block boundary, cut the input split here.
          if (currentOffset != -1 && currentLength > context.minSize &&
              (currentOffset / blockSize != stripe.getOffset() / blockSize)) {
            createSplit(currentOffset, currentLength, orcReader.getFileMetaInfo());
            currentOffset = -1;
          }
          // if we aren't building a split, start a new one.
          if (currentOffset == -1) {
            currentOffset = stripe.getOffset();
            currentLength = stripe.getLength();
          } else {
            currentLength += stripe.getLength();
          }
          if (currentLength >= context.maxSize) {
            createSplit(currentOffset, currentLength, orcReader.getFileMetaInfo());
            currentOffset = -1;
          }
        }
        if (currentOffset != -1) {
          createSplit(currentOffset, currentLength, orcReader.getFileMetaInfo());
        }
      } catch (Throwable th) {
        synchronized (context.errors) {
          context.errors.add(th);
        }
      }
      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.CREATE_ORC_SPLITS);
    }
  }

  @Override
  public InputSplit[] getSplits(JobConf job,
                                int numSplits) throws IOException {
    // use threads to resolve directories into splits
    Context context = new Context(job);
    for(Path dir: getInputPaths(job)) {
      FileSystem fs = dir.getFileSystem(job);
      context.schedule(new FileGenerator(context, fs, dir));
    }
    context.waitForTasks();
    // deal with exceptions
    if (!context.errors.isEmpty()) {
      List<IOException> errors =
          new ArrayList<IOException>(context.errors.size());
      for(Throwable th: context.errors) {
        if (th instanceof IOException) {
          errors.add((IOException) th);
        } else {
          throw new IOException("serious problem", th);
        }
      }
      throw new InvalidInputException(errors);
    }
    InputSplit[] result = new InputSplit[context.splits.size()];
    context.splits.toArray(result);
    return result;
  }
}
