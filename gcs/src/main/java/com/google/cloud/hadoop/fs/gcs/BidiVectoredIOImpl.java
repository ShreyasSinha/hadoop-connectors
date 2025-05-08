package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.storage.Storage;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;
import javax.annotation.Nonnull;
import org.apache.hadoop.fs.FileRange;

public class BidiVectoredIOImpl implements VectoredIO{

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  public BidiVectoredIOImpl() {
  }

  @Override
  public void readVectored(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate,
      GoogleCloudStorageFileSystem gcsFs, FileInfo fileInfo, @Nonnull URI gcsPath)
      throws IOException {

  }

  @Override
  public void close() throws IOException {

  }
}
