package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by gates on 12/17/14.
 */
public class WritableMicroBench {

  private static void encodeStrings(StringBuilder bldr, String... strings) {
    for (String s : strings) {
      bldr.append(s.length());
      if (s.length() > 0) bldr.append(s);
    }
  }

  public static void main(String[] argv) throws Exception {

    Set<byte[]> s = new HashSet<byte[]>();
    byte[] b1 = "hello world".getBytes();
    byte[] b2 = "hello world".getBytes();
    s.add(b1);
    s.add(b2);
    System.out.println("b1 == b2 " + (b1.equals(b2)));
    System.out.println("b1 hash " + b1.hashCode());
    System.out.println("b2 hash " + b2.hashCode());
    System.out.println("Array.equals " + Arrays.equals(b1, b2));
    System.out.println("b1 Arrays hash " + Arrays.hashCode(b1));
    System.out.println("b2 Arrays hash " + Arrays.hashCode(b2));
    /*
    int numIters = 1000000;

    StringBuilder bldr = new StringBuilder();

    String[] vals = {"name", "varchar32", "", "age", "int", "", "gpa", "decimal(3,2)", "",
        "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"};
    encodeStrings(bldr, vals);
    bldr.append(0);
    bldr.append(0);
    encodeStrings(bldr, "org.apache.hadoop.hive.ql.io.orc.OrcSerde", "dontknowwhatthisis");
    bldr.append(0);
    bldr.append(0);
    bldr.append(0);

    byte[] bytes = bldr.toString().getBytes(StandardCharsets.UTF_8);
    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] digest = null;

    long begin = System.currentTimeMillis();
    for (int i = 0; i < numIters; i++) {
      md.update(bytes);
      digest = md.digest();
    }
    long end = System.currentTimeMillis();
    System.out.println("md5 length is " + digest.length);
    System.out.println("md5 time is " + (end - begin));

    md = MessageDigest.getInstance("SHA-1");
    begin = System.currentTimeMillis();
    for (int i = 0; i < numIters; i++) {
      md.update(bytes);
      digest = md.digest();
    }
    end = System.currentTimeMillis();
    System.out.println("sha1 length is " + digest.length);
    System.out.println("sha1 time is " + (end - begin));

    md = MessageDigest.getInstance("SHA-256");
    begin = System.currentTimeMillis();
    for (int i = 0; i < numIters; i++) {
      md.update(bytes);
      digest = md.digest();
    }
    end = System.currentTimeMillis();
    System.out.println("sha256 length is " + digest.length);
    System.out.println("sha256 time is " + (end - begin));







    if (argv.length > 0) numIters = Integer.valueOf(argv[0]);

    Partition part = new Partition();

    List<String> values = new ArrayList<String>(2);
    values.add("2014-12-17");
    values.add("NorthAmerica");
    part.setValues(values);
    part.setDbName("mydb");
    part.setTableName("mytable");
    part.setCreateTime(93);
    part.setLastAccessTime(3242423);
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = new ArrayList<FieldSchema>(10);
    for (int i = 0; i < 10; i++) {
      FieldSchema fs = new FieldSchema("col_" + Integer.toString(i), "no comment", "varchar(32)");
      cols.add(fs);
    }
    sd.setCols(cols);
    sd.setLocation("/hive/warehouse/somewhere");
    sd.setInputFormat("org.apache.hive.io.unicorn.UnicornInputFormat");
    sd.setOutputFormat("org.apache.hive.io.unicorn.UnicornOutputFormat");
    sd.setCompressed(false);
    sd.setNumBuckets(0);
    SerDeInfo serde = new SerDeInfo("org.apache.hive.io.unicorn.UnicornSerde",
        "dontknowwhatthisis", new HashMap<String, String>());
    sd.setSerdeInfo(serde);
    sd.setBucketCols(new ArrayList<String>());
    sd.setSortCols(new ArrayList<Order>());
    sd.setParameters(new HashMap<String, String>());
    part.setSd(sd);
    Map<String,String> parameters = new HashMap<String, String>(2);
    parameters.put("transactional", "true");
    parameters.put("someotherparam", "whatever");
    part.setParameters(parameters);

    try {
      long beginSerialization = System.currentTimeMillis();
      ByteArrayOutputStream baos = null;
      for (int i = 0; i < numIters; i++) {
        baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(part);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        Partition newPart = (Partition)ois.readObject();
        assert part.getTableName() == newPart.getTableName();
      }
      long endSerialization = System.currentTimeMillis();
      System.out.println("serializable size is " + baos.toByteArray().length);

      long beginWritable = System.currentTimeMillis();
      for (int i = 0; i < numIters; i++) {
        baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        PartitionWritable pw = new PartitionWritable(part);
        pw.write(dos);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        PartitionWritable newPart = new PartitionWritable();
        newPart.readFields(dis);
        assert part.getTableName() == newPart.p.getTableName();
      }
      long endWritable = System.currentTimeMillis();
      System.out.println("writable size is " + baos.toByteArray().length);

      System.out.println("Serialization time is " + (endSerialization - beginSerialization) + " ms");
      System.out.println("Writable time is " + (endWritable - beginWritable) + " ms");

    } catch (Exception e) {
      System.err.println("Received exception " + e.getClass().getName() + ", " + e.getMessage());
      e.printStackTrace();
    }
    */

  }

  static class WritableWrapper {

    protected DataOutput out;
    protected DataInput in;

    protected void writeStr(String str) throws IOException {
      out.writeInt(str.length());
      out.write(str.getBytes(), 0, str.length());
    }

    protected String readStr() throws IOException {
      int len = in.readInt();
      byte[] b = new byte[len];
      in.readFully(b, 0, len);
      return new String(b);
    }

    protected void writeList(List<String> list) throws IOException {
      out.writeInt(list.size());
      for (String val : list) {
        writeStr(val);
      }
    }

    protected List<String> readList() throws IOException {
      int sz = in.readInt();
      List<String> list = new ArrayList<String>(sz);
      for (int i = 0; i < sz; i++) {
        list.add(readStr());
      }
      return list;
    }

    protected void writeMap(Map<String, String> m) throws IOException {
      out.writeInt(m.size());
      for (Map.Entry<String, String> e : m.entrySet()) {
        writeStr(e.getKey());
        writeStr(e.getValue());
      }
    }

    protected Map<String, String> readMap() throws IOException {
      int sz = in.readInt();
      Map<String, String> m = new HashMap<String, String>(sz);
      for (int i = 0; i < sz; i++) {
        m.put(readStr(), readStr());
      }
      return m;
    }
  }

  static class PartitionWritable extends WritableWrapper implements Writable {
    public final Partition p;

    public PartitionWritable() {
      p = new Partition();
    }

    public PartitionWritable(Partition partition) {
      p = partition;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      this.out = out;
      writeList(p.getValues());
      writeStr(p.getDbName());
      writeStr(p.getTableName());
      out.writeInt(p.getCreateTime());
      out.writeInt(p.getLastAccessTime());
      StorageDescriptorWritable sd = new StorageDescriptorWritable(p.getSd());
      sd.write(out);
      writeMap(p.getParameters());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.in = in;
      p.setValues(readList());
      p.setDbName(readStr());
      p.setTableName(readStr());
      p.setCreateTime(in.readInt());
      p.setLastAccessTime(in.readInt());
      StorageDescriptorWritable sd = new StorageDescriptorWritable();
      sd.readFields(in);
      p.setSd(sd.sd);
      p.setParameters(readMap());
    }
  }

  static class StorageDescriptorWritable extends WritableWrapper implements Writable {
    public final StorageDescriptor sd;

    public StorageDescriptorWritable() {
      sd = new StorageDescriptor();
    }

    public StorageDescriptorWritable(StorageDescriptor sd) {
      this.sd = sd;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      this.out = out;
      out.writeInt(sd.getColsSize());
      for (FieldSchema fs : sd.getCols()) {
        writeStr(fs.getName());
        writeStr(fs.getComment());
        writeStr(fs.getType());
      }
      writeStr(sd.getLocation());
      writeStr(sd.getInputFormat());
      writeStr(sd.getOutputFormat());
      out.writeBoolean(sd.isCompressed());
      out.writeInt(sd.getNumBuckets());
      writeStr(sd.getSerdeInfo().getName());
      writeStr(sd.getSerdeInfo().getSerializationLib());
      writeMap(sd.getSerdeInfo().getParameters());
      writeList(sd.getBucketCols());
      out.writeInt(sd.getSortColsSize());
      for (Order o : sd.getSortCols()) {
        writeStr(o.getCol());
        out.writeInt(o.getOrder());
      }
      writeMap(sd.getParameters());
      // skipping SkewedInfo
      out.writeBoolean(sd.isStoredAsSubDirectories());


    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.in = in;
      int numCols = in.readInt();
      for (int i = 0; i < numCols; i++) {
        FieldSchema fs = new FieldSchema();
        fs.setName(readStr());
        fs.setComment(readStr());
        fs.setType(readStr());
        sd.addToCols(fs);
      }
      sd.setLocation(readStr());
      sd.setInputFormat(readStr());
      sd.setOutputFormat(readStr());
      sd.setCompressed(in.readBoolean());
      sd.setNumBuckets(in.readInt());
      SerDeInfo serde = new SerDeInfo();
      serde.setName(readStr());
      serde.setSerializationLib(readStr());
      serde.setParameters(readMap());
      sd.setSerdeInfo(serde);
      sd.setBucketCols(readList());
      int numOrder = in.readInt();
      for (int i = 0; i < numOrder; i++) {
        Order o = new Order();
        o.setCol(readStr());
        o.setOrder(in.readInt());
        sd.addToSortCols(o);
      }
      sd.setParameters(readMap());
      // skipping SkewedInfo
      sd.setStoredAsSubDirectories(in.readBoolean());
    }
  }


}
