package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.VectoredIORange;
import com.google.cloud.hadoop.gcsio.VectoredIOResult;
import com.google.cloud.storage.BlobId;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;

public class BidiVectoredIOImpl implements VectoredIO {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private final GhfsGlobalStorageStatistics storageStatistics;
  private final FileSystem.Statistics statistics;

  public BidiVectoredIOImpl(
      GhfsGlobalStorageStatistics globalStorageStatistics, Statistics statistics) {
    this.storageStatistics = globalStorageStatistics;
    this.statistics = statistics;
  }

  @Override
  public void readVectored(
      List<? extends FileRange> ranges,
      IntFunction<ByteBuffer> allocate,
      GoogleCloudStorageFileSystem gcsFs,
      FileInfo fileInfo,
      @Nonnull URI gcsPath)
      throws IOException {
    StorageResourceId resourceId =
        StorageResourceId.fromUriPath(gcsPath, /* allowEmptyObjectName= */ false);
    BlobId blobId =
        BlobId.of(
            resourceId.getBucketName(), resourceId.getObjectName(), resourceId.getGenerationId());
    ranges.stream().forEach(range -> range.setData(new CompletableFuture<>()));
    try {
      VectoredIOResult result =
          gcsFs
              .getGcs()
              .readVectored(
                  ranges.stream()
                      .map(
                          range ->
                              VectoredIORange.builder()
                                  .setLength(range.getLength())
                                  .setOffset(range.getOffset())
                                  .setData(range.getData())
                                  .build())
                      .collect(Collectors.toList()),
                  allocate,
                  blobId);
      updateBytesRead(result.getReadBytes());
      storageStatistics.updateStats(
          GhfsStatistic.STREAM_READ_VECTORED_TOTAL_READ_RANGE_DURATION,
          result.getReadDuration(),
          gcsPath);
      storageStatistics.updateStats(
          GhfsStatistic.STREAM_READ_VECTORED_CLIENT_INITIALIZATION_DURATION,
          result.getClientInitializationDuration(),
          gcsPath);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new IOException(e);
    }
  }

  private void updateBytesRead(int readBytes) {
    statistics.incrementBytesRead(readBytes);
    storageStatistics.streamReadBytes(readBytes);
  }

  @Override
  public void close() throws IOException {}
}
