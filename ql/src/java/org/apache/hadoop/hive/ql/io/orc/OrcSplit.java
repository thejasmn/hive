package org.apache.hadoop.hive.ql.io.orc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.Reader.FileMetaInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileSplit;



/**
 * OrcFileSplit. Holds file meta info
 *
 */
public class OrcSplit extends FileSplit {
  private Reader.FileMetaInfo fileMetaInfo;

  public OrcSplit(){
    super();
  }

  public OrcSplit(Path path, long offset, long length, String[] hosts,
      FileMetaInfo fileMetaInfo) {
    super(path, offset, length, hosts);
    this.fileMetaInfo = fileMetaInfo;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    //serialize path, offset, length using FileSplit
    super.write(out);

    //serialize FileMetaInfo fields
    Text.writeString(out, fileMetaInfo.compressionType);
    WritableUtils.writeVInt(out, fileMetaInfo.bufferSize);

    //serialize FileMetaInfo field footer
    ByteBuffer footerBuff = fileMetaInfo.footerBuffer;
    footerBuff.reset();
    //write length of buffer
    WritableUtils.writeVInt(out, footerBuff.limit() - footerBuff.position());
    out.write(footerBuff.array(), footerBuff.position(), footerBuff.limit() - footerBuff.position());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    //deserialize path, offset, length using FileSplit
    super.readFields(in);

    //deserialize FileMetaInfo fields
    String compressionType = Text.readString(in);
    int bufferSize = WritableUtils.readVInt(in);

    //deserialize FileMetaInfo field footer
    int footerBuffSize = WritableUtils.readVInt(in);
    ByteBuffer footerBuff = ByteBuffer.allocate(footerBuffSize);
    in.readFully(footerBuff.array(), 0, footerBuffSize);

    fileMetaInfo = new FileMetaInfo(compressionType, bufferSize, footerBuff);
  }

  public FileMetaInfo getFileMetaInfo(){
    return fileMetaInfo;
  }

}
