/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.gcsio.StatisticTypeEnum.TYPE_COUNTER;
import static com.google.cloud.hadoop.gcsio.StatisticTypeEnum.TYPE_DURATION;
import static com.google.cloud.hadoop.gcsio.StatisticTypeEnum.TYPE_DURATION_TOTAL;

import com.google.cloud.hadoop.gcsio.StatisticTypeEnum;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import java.util.EnumSet;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;

/**
 * Statistics which are collected in GCS. Counter and duration statistics are published in {@link
 * GoogleHadoopFileSystem} and as metrics in {@link GhfsInstrumentation}.
 *
 * <p>Where possible, stream names come from {@link StreamStatisticNames} and {@link
 * StoreStatisticNames}
 */
@InterfaceStability.Unstable
public enum GhfsStatistic {
  /** HTTP request counters */
  ACTION_HTTP_GET_REQUEST(
      StoreStatisticNames.ACTION_HTTP_GET_REQUEST, "GET request.", TYPE_COUNTER),
  ACTION_HTTP_PUT_REQUEST(
      StoreStatisticNames.ACTION_HTTP_PUT_REQUEST, "PUT request.", TYPE_DURATION_TOTAL),
  ACTION_HTTP_PATCH_REQUEST(
      StoreStatisticNames.ACTION_HTTP_PATCH_REQUEST, "PATCH request.", TYPE_DURATION),
  ACTION_HTTP_DELETE_REQUEST(
      StoreStatisticNames.ACTION_HTTP_DELETE_REQUEST, "DELETE request.", TYPE_DURATION_TOTAL),

  ACTION_HTTP_POST_REQUEST(
      StoreStatisticNames.ACTION_HTTP_POST_REQUEST, "POST request.", TYPE_DURATION_TOTAL),

  /** FileSystem-level statistics */
  DIRECTORIES_CREATED(
      "directories_created",
      "Total number of directories created through the object store.",
      TYPE_COUNTER),

  FILES_CREATED(
      "files_created", "Total number of files created through the object store.", TYPE_COUNTER),
  FILES_DELETED(
      "files_deleted", "Total number of files deleted from the object store.", TYPE_COUNTER),
  FILES_DELETE_REJECTED(
      "files_delete_rejected",
      "Total number of files whose delete request was rejected",
      TYPE_COUNTER),
  INVOCATION_CREATE(StoreStatisticNames.OP_CREATE, "Calls of create()", TYPE_DURATION_TOTAL, true),
  INVOCATION_DELETE(StoreStatisticNames.OP_DELETE, "Calls of delete()", TYPE_DURATION_TOTAL, true),
  INVOCATION_EXISTS(StoreStatisticNames.OP_EXISTS, "Calls of exists()", TYPE_COUNTER, true),
  INVOCATION_GET_FILE_STATUS(
      StoreStatisticNames.OP_GET_FILE_STATUS,
      "Calls of getFileStatus()",
      TYPE_DURATION_TOTAL,
      true),
  INVOCATION_GET_FILE_CHECKSUM(
      StoreStatisticNames.OP_GET_FILE_CHECKSUM, "Calls of getFileChecksum()", TYPE_COUNTER),

  INVOCATION_LIST_STATUS_RESULT_SIZE(
      "op_get_list_status_result_size", "Number of files returned from list call", TYPE_COUNTER),
  INVOCATION_GLOB_STATUS(
      StoreStatisticNames.OP_GLOB_STATUS, "Calls of globStatus()", TYPE_DURATION_TOTAL, true),
  INVOCATION_HFLUSH(StoreStatisticNames.OP_HFLUSH, "Calls of hflush()", TYPE_DURATION_TOTAL, true),
  INVOCATION_HSYNC(StoreStatisticNames.OP_HSYNC, "Calls of hsync()", TYPE_DURATION_TOTAL, true),
  INVOCATION_LIST_STATUS(
      StoreStatisticNames.OP_LIST_STATUS, "Calls of listStatus()", TYPE_DURATION_TOTAL, true),
  INVOCATION_MKDIRS(StoreStatisticNames.OP_MKDIRS, "Calls of mkdirs()", TYPE_DURATION_TOTAL, true),
  INVOCATION_OPEN(StoreStatisticNames.OP_OPEN, "Calls of open()", TYPE_DURATION_TOTAL, true),
  INVOCATION_RENAME(StoreStatisticNames.OP_RENAME, "Calls of rename()", TYPE_DURATION_TOTAL, true),
  INVOCATION_COPY_FROM_LOCAL_FILE(
      StoreStatisticNames.OP_COPY_FROM_LOCAL_FILE,
      "Calls of copyFromLocalFile()",
      TYPE_COUNTER,
      true),
  INVOCATION_CREATE_NON_RECURSIVE(
      StoreStatisticNames.OP_CREATE_NON_RECURSIVE,
      "Calls of createNonRecursive()",
      TYPE_DURATION,
      true),
  INVOCATION_GET_DELEGATION_TOKEN(
      StoreStatisticNames.OP_GET_DELEGATION_TOKEN,
      "Calls of getDelegationToken()",
      TYPE_COUNTER,
      true),
  INVOCATION_LIST_LOCATED_STATUS(
      StoreStatisticNames.OP_LIST_LOCATED_STATUS,
      "Calls of listLocatedStatus()",
      TYPE_COUNTER,
      true),

  /** Stream reads */
  STREAM_READ_BYTES(
      StreamStatisticNames.STREAM_READ_BYTES,
      "Bytes read from an input stream in read() calls",
      TYPE_COUNTER),
  STREAM_READ_CLOSE_OPERATIONS(
      StreamStatisticNames.STREAM_READ_CLOSE_OPERATIONS,
      "Calls of read stream close()",
      TYPE_DURATION),

  STREAM_READ_VECTORED_OPERATIONS(
      StreamStatisticNames.STREAM_READ_VECTORED_OPERATIONS,
      "Calls of readVectored()",
      TYPE_DURATION),

  STREAM_READ_VECTORED_EXTRA_READ_BYTES(
      StreamStatisticNames.STREAM_READ_VECTORED_READ_BYTES_DISCARDED,
      "Discarded read bytes during readVectored operation",
      TYPE_COUNTER),

  STREAM_READ_VECTORED_READ_RANGE_DURATION(
      "stream_readVectored_range_duration", "Latency of individual FileRange", TYPE_DURATION),

  STREAM_READ_VECTORED_TOTAL_READ_RANGE_DURATION(
      "stream_readVectored_total_range_duration", "Latency of all the FileRange", TYPE_DURATION),

  STREAM_READ_VECTORED_CLIENT_INITIALIZATION_DURATION(
      "stream_readVectored_client_initialization_duration",
      "Latency of client initialization",
      TYPE_DURATION),

  STREAM_READ_VECTORED_READ_INCOMING_RANGES(
      StreamStatisticNames.STREAM_READ_VECTORED_INCOMING_RANGES,
      "size of fileRanges requested in readVectoredRequest",
      TYPE_COUNTER),
  STREAM_READ_OPERATIONS("stream_read_operations", "Calls of read()", TYPE_DURATION_TOTAL),

  STREAM_READ_VECTORED_READ_COMBINED_RANGES(
      StreamStatisticNames.STREAM_READ_VECTORED_COMBINED_RANGES,
      "size of combined fileRange requested per readVectoredRequest",
      TYPE_COUNTER),

  STREAM_READ_OPERATIONS_INCOMPLETE(
      StreamStatisticNames.STREAM_READ_OPERATIONS_INCOMPLETE,
      "Count of incomplete read() operations in an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_BACKWARD_OPERATIONS(
      StreamStatisticNames.STREAM_READ_SEEK_BACKWARD_OPERATIONS,
      "Count of executed seek operations which went backwards in a stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_BYTES_BACKWARDS(
      StreamStatisticNames.STREAM_READ_SEEK_BYTES_BACKWARDS,
      "Count of bytes moved backwards during seek operations" + " in an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_BYTES_SKIPPED(
      StreamStatisticNames.STREAM_READ_SEEK_BYTES_SKIPPED,
      "Count of bytes skipped during forward seek operations" + " an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_FORWARD_OPERATIONS(
      StreamStatisticNames.STREAM_READ_SEEK_FORWARD_OPERATIONS,
      "Count of executed seek operations which went forward in" + " an input stream",
      TYPE_COUNTER),
  STREAM_READ_SEEK_OPERATIONS(
      StreamStatisticNames.STREAM_READ_SEEK_OPERATIONS, "Calls of seek()", TYPE_DURATION),

  STREAM_WRITE_BYTES(
      StreamStatisticNames.STREAM_WRITE_BYTES,
      "Count of bytes written to output stream" + " (including all not yet uploaded)",
      TYPE_COUNTER),
  STREAM_WRITE_CLOSE_OPERATIONS(
      "stream_write_close_operations", "Calls of write stream close()", TYPE_DURATION),
  STREAM_WRITE_OPERATIONS("stream_write_operations", "Calls of write()", TYPE_DURATION_TOTAL),

  /** The XAttr API statistics */
  INVOCATION_XATTR_GET_MAP(
      StoreStatisticNames.OP_XATTR_GET_MAP, "Calls of getXAttrs(Path path)", TYPE_DURATION),
  INVOCATION_XATTR_GET_NAMED(
      StoreStatisticNames.OP_XATTR_GET_NAMED, "Calls of getXAttr(Path, String)", TYPE_DURATION),
  INVOCATION_XATTR_GET_NAMED_MAP(
      StoreStatisticNames.OP_XATTR_GET_NAMED_MAP, "Calls of xattr()", TYPE_DURATION),
  INVOCATION_OP_XATTR_LIST(
      StoreStatisticNames.OP_XATTR_LIST,
      "Calls of getXAttrs(Path path, List<String> names)",
      TYPE_DURATION),

  /** Delegation token operations */
  DELEGATION_TOKENS_ISSUED(
      StoreStatisticNames.DELEGATION_TOKENS_ISSUED,
      "Count of delegation tokens issued",
      TYPE_DURATION),
  GCS_CONNECTOR_TIME(
      "gcs_connector_time",
      "Time in milliseconds spend on HDFS API calls. Note that sometimes this might be less than GCS_API_TIME since some of the operations are done concurrently",
      TYPE_COUNTER);

  public static final ImmutableSet<GhfsStatistic> VALUES =
      ImmutableSet.copyOf(EnumSet.allOf(GhfsStatistic.class));

  /** A map used to support the {@link #fromSymbol(String)} call. */
  private static final ImmutableMap<String, GhfsStatistic> SYMBOL_MAP =
      Maps.uniqueIndex(Iterators.forArray(values()), GhfsStatistic::getSymbol);

  private final boolean isHadoopApi;

  /**
   * Statistic definition.
   *
   * @param symbol name
   * @param description description.
   * @param type type
   */
  GhfsStatistic(String symbol, String description, StatisticTypeEnum type) {
    this(symbol, description, type, false);
  }

  GhfsStatistic(String symbol, String description, StatisticTypeEnum type, boolean isHadoopApi) {
    this.symbol = symbol;
    this.description = description;
    this.type = type;
    this.isHadoopApi = isHadoopApi;
  }

  /** Statistic name. */
  private final String symbol;

  /** Statistic description. */
  private final String description;

  /** Statistic type. */
  private final StatisticTypeEnum type;

  /** the name of the statistic */
  public String getSymbol() {
    return symbol;
  }

  /**
   * Get a statistic from a symbol.
   *
   * @param symbol statistic to look up
   * @return the value or null.
   */
  public static GhfsStatistic fromSymbol(String symbol) {
    return SYMBOL_MAP.get(symbol);
  }

  /** The description of the Statistic */
  public String getDescription() {
    return description;
  }

  /**
   * The string value is simply the symbol. This makes this operation very low cost.
   *
   * @return the symbol of this statistic.
   */
  @Override
  public String toString() {
    return symbol;
  }

  /**
   * What type is this statistic?
   *
   * @return the type.
   */
  public StatisticTypeEnum getType() {
    return type;
  }

  boolean getIsHadoopApi() {
    return this.isHadoopApi;
  }
}
