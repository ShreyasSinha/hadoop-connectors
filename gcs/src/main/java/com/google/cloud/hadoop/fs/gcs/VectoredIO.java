package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import javax.annotation.Nonnull;
import org.apache.hadoop.fs.FileRange;

public interface VectoredIO extends Closeable {

  /**
   * Reads data from Storage using vectored I/O operations.
   *
   * @param ranges List of file ranges to read.
   * @param allocate Function to allocate ByteBuffer for reading.
   * @param gcsFs GCFS implementation to use while creating channel and reading content for ranges.
   * @param fileInfo FileInfo of the gcs object agaisnt which range request are fired, this can be
   *     null for some code path fall back to URI path provided.
   * @param gcsPath URI of the gcs object for which the range requests are fired.
   * @throws IOException If invalid range is requested, offset<0.
   */
  void readVectored(
      List<? extends FileRange> ranges,
      IntFunction<ByteBuffer> allocate,
      GoogleCloudStorageFileSystem gcsFs,
      FileInfo fileInfo,
      @Nonnull URI gcsPath)
      throws IOException, ExecutionException, InterruptedException, TimeoutException;
}
