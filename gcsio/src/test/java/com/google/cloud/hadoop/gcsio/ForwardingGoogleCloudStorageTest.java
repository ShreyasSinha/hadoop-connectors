/*
 * Copyright 2013 Google Inc.
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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.cloud.storage.BlobId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class ForwardingGoogleCloudStorageTest {

  /** Sample string. */
  private static final String TEST_STRING = "test";

  /** Sample long. */
  private static final long TEST_LONG = 1L;

  /** Sample int. */
  private static final int TEST_INT = 1;

  /** Sample list of strings. */
  private static final List<String> TEST_STRINGS = Lists.newArrayList(TEST_STRING);

  /** Sample storage resource id. */
  private static final StorageResourceId TEST_STORAGE_RESOURCE_ID =
      new StorageResourceId("test-bucket-name", "test/object/name");

  /** Sample list of storage resource ids. */
  private static final List<StorageResourceId> TEST_STORAGE_RESOURCE_IDS =
      Lists.newArrayList(TEST_STORAGE_RESOURCE_ID);

  /** Sample updatable item info. */
  private static final UpdatableItemInfo TEST_ITEM_INFO =
      new UpdatableItemInfo(TEST_STORAGE_RESOURCE_ID, null);

  private static final GoogleCloudStorageItemInfo TEST_GCS_ITEM_INFO =
      GoogleCloudStorageItemInfo.createObject(
          TEST_STORAGE_RESOURCE_ID,
          /* creationTime= */ 100,
          /* modificationTime= */ 1000,
          /* size= */ 324,
          /* contentType= */ "text",
          /* contentEncoding */ "gzip",
          /* metadata */ ImmutableMap.of("mkey", new byte[] {1, 6}),
          /* contentGeneration= */ 122,
          /* metaGeneration= */ 3,
          new VerificationAttributes(/* md5hash= */ null, /* crc32c= */ null));

  /** Sample list of updatable item infos. */
  private static final List<UpdatableItemInfo> TEST_ITEM_INFOS = Lists.newArrayList(TEST_ITEM_INFO);

  /** Sample create object options. */
  private static final CreateObjectOptions TEST_OBJECT_OPTIONS =
      CreateObjectOptions.DEFAULT_NO_OVERWRITE;

  /** Sample create bucket options. */
  private static final CreateBucketOptions TEST_BUCKET_OPTIONS =
      CreateBucketOptions.builder().setLocation(TEST_STRING).setStorageClass(TEST_STRING).build();

  /** Sample create object options. */
  private static final GoogleCloudStorageReadOptions TEST_READ_OPTIONS =
      GoogleCloudStorageReadOptions.DEFAULT;

  /** Instance of the wrapper to test. */
  private ForwardingGoogleCloudStorage gcs;

  @Mock private GoogleCloudStorage mockGcsDelegate;

  @Before
  public void setUp() {
    // Setup mocks.
    MockitoAnnotations.initMocks(this);

    // Create the wrapper.
    gcs = new ForwardingGoogleCloudStorage(mockGcsDelegate);
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockGcsDelegate);
  }

  @Test
  public void testGetOptions() {
    gcs.getOptions();

    verify(mockGcsDelegate).getOptions();
  }

  @Test
  public void testCreateWithResource() throws IOException {
    gcs.create(TEST_STORAGE_RESOURCE_ID);

    verify(mockGcsDelegate)
        .create(eq(TEST_STORAGE_RESOURCE_ID), eq(CreateObjectOptions.DEFAULT_OVERWRITE));
  }

  @Test
  public void testCreateWithResourceAndOptions() throws IOException {
    gcs.create(TEST_STORAGE_RESOURCE_ID, TEST_OBJECT_OPTIONS);

    verify(mockGcsDelegate).create(eq(TEST_STORAGE_RESOURCE_ID), eq(TEST_OBJECT_OPTIONS));
  }

  @Test
  public void testCreateEmptyObject() throws IOException {
    gcs.createEmptyObject(TEST_STORAGE_RESOURCE_ID);

    verify(mockGcsDelegate).createEmptyObject(eq(TEST_STORAGE_RESOURCE_ID));
  }

  @Test
  public void testCreateEmptyObjectWithOptions() throws IOException {
    gcs.createEmptyObject(TEST_STORAGE_RESOURCE_ID, TEST_OBJECT_OPTIONS);

    verify(mockGcsDelegate)
        .createEmptyObject(eq(TEST_STORAGE_RESOURCE_ID), eq(TEST_OBJECT_OPTIONS));
  }

  @Test
  public void testCreateEmptyObjects() throws IOException {
    gcs.createEmptyObjects(TEST_STORAGE_RESOURCE_IDS);

    verify(mockGcsDelegate).createEmptyObjects(eq(TEST_STORAGE_RESOURCE_IDS));
  }

  @Test
  public void testCreateEmptyObjectsWithOptions() throws IOException {
    gcs.createEmptyObjects(TEST_STORAGE_RESOURCE_IDS, TEST_OBJECT_OPTIONS);

    verify(mockGcsDelegate)
        .createEmptyObjects(eq(TEST_STORAGE_RESOURCE_IDS), eq(TEST_OBJECT_OPTIONS));
  }

  @Test
  public void testOpen() throws IOException {
    gcs.open(TEST_STORAGE_RESOURCE_ID);

    verify(mockGcsDelegate)
        .open(eq(TEST_STORAGE_RESOURCE_ID), eq(GoogleCloudStorageReadOptions.DEFAULT));
  }

  @Test
  public void testOpenWithReadOptions() throws IOException {
    gcs.open(TEST_STORAGE_RESOURCE_ID, TEST_READ_OPTIONS);

    verify(mockGcsDelegate).open(eq(TEST_STORAGE_RESOURCE_ID), eq(TEST_READ_OPTIONS));
  }

  @Test
  public void testOpenWithItemInfo() throws IllegalArgumentException, IOException {
    gcs.open(TEST_GCS_ITEM_INFO);

    verify(mockGcsDelegate).open(eq(TEST_GCS_ITEM_INFO), eq(GoogleCloudStorageReadOptions.DEFAULT));
  }

  @Test
  public void testOpenWithItemInfoWithOptions() throws IllegalArgumentException, IOException {
    gcs.open(TEST_GCS_ITEM_INFO, TEST_READ_OPTIONS);

    verify(mockGcsDelegate).open(eq(TEST_GCS_ITEM_INFO), eq(TEST_READ_OPTIONS));
  }

  @Test
  public void testCreateWithBucket() throws IOException {
    gcs.createBucket(TEST_STRING);

    verify(mockGcsDelegate).createBucket(eq(TEST_STRING), eq(CreateBucketOptions.DEFAULT));
  }

  @Test
  public void testCreateWithBucketAndOptions() throws IOException {
    gcs.createBucket(TEST_STRING, TEST_BUCKET_OPTIONS);

    verify(mockGcsDelegate).createBucket(eq(TEST_STRING), eq(TEST_BUCKET_OPTIONS));
  }

  @Test
  public void testDeleteBuckets() throws IOException {
    gcs.deleteBuckets(TEST_STRINGS);

    verify(mockGcsDelegate).deleteBuckets(eq(TEST_STRINGS));
  }

  @Test
  public void testDeleteObjects() throws IOException {
    gcs.deleteObjects(TEST_STORAGE_RESOURCE_IDS);

    verify(mockGcsDelegate).deleteObjects(eq(TEST_STORAGE_RESOURCE_IDS));
  }

  @Test
  public void testCopy() throws IOException {
    gcs.copy(TEST_STRING, TEST_STRINGS, TEST_STRING, TEST_STRINGS);

    verify(mockGcsDelegate)
        .copy(eq(TEST_STRING), eq(TEST_STRINGS), eq(TEST_STRING), eq(TEST_STRINGS));
  }

  @Test
  public void testMove() throws IOException {
    Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap = new HashMap<>();
    sourceToDestinationObjectsMap.put(
        new StorageResourceId(TEST_STRING, TEST_STRING),
        new StorageResourceId(TEST_STRING, TEST_STRING));
    gcs.move(sourceToDestinationObjectsMap);

    verify(mockGcsDelegate).move(eq(sourceToDestinationObjectsMap));
  }

  @Test
  public void testListBucketNames() throws IOException {
    gcs.listBucketNames();

    verify(mockGcsDelegate).listBucketNames();
  }

  @Test
  public void testListBucketInfo() throws IOException {
    gcs.listBucketInfo();

    verify(mockGcsDelegate).listBucketInfo();
  }

  @Test
  public void testListObjectInfo() throws IOException {
    gcs.listObjectInfo(TEST_STRING, TEST_STRING);

    verify(mockGcsDelegate)
        .listObjectInfo(eq(TEST_STRING), eq(TEST_STRING), eq(ListObjectOptions.DEFAULT));
  }

  @Test
  public void testListObjectInfoWithMax() throws IOException {
    gcs.listObjectInfo(TEST_STRING, TEST_STRING, ListObjectOptions.DEFAULT);

    verify(mockGcsDelegate)
        .listObjectInfo(eq(TEST_STRING), eq(TEST_STRING), eq(ListObjectOptions.DEFAULT));
  }

  @Test
  public void testListObjectInfoPage() throws IOException {
    gcs.listObjectInfoPage(TEST_STRING, TEST_STRING, TEST_STRING);

    verify(mockGcsDelegate)
        .listObjectInfoPage(
            eq(TEST_STRING), eq(TEST_STRING), eq(ListObjectOptions.DEFAULT), eq(TEST_STRING));
  }

  @Test
  public void testListObjectInfoPageWithMax() throws IOException {
    gcs.listObjectInfoPage(TEST_STRING, TEST_STRING, ListObjectOptions.DEFAULT, TEST_STRING);

    verify(mockGcsDelegate)
        .listObjectInfoPage(
            eq(TEST_STRING), eq(TEST_STRING), eq(ListObjectOptions.DEFAULT), eq(TEST_STRING));
  }

  @Test
  public void testGetItemInfo() throws IOException {
    gcs.getItemInfo(TEST_STORAGE_RESOURCE_ID);

    verify(mockGcsDelegate).getItemInfo(eq(TEST_STORAGE_RESOURCE_ID));
  }

  @Test
  public void testGetItemInfos() throws IOException {
    gcs.getItemInfos(TEST_STORAGE_RESOURCE_IDS);

    verify(mockGcsDelegate).getItemInfos(eq(TEST_STORAGE_RESOURCE_IDS));
  }

  @Test
  public void testUpdateItems() throws IOException {
    gcs.updateItems(TEST_ITEM_INFOS);

    verify(mockGcsDelegate).updateItems(eq(TEST_ITEM_INFOS));
  }

  @Test
  public void testClose() {
    gcs.close();

    verify(mockGcsDelegate).close();
  }

  @Test
  public void testCompose() throws IOException {
    gcs.compose(TEST_STRING, TEST_STRINGS, TEST_STRING, TEST_STRING);

    verify(mockGcsDelegate)
        .compose(eq(TEST_STRING), eq(TEST_STRINGS), eq(TEST_STRING), eq(TEST_STRING));
  }

  @Test
  public void testComposeObjects() throws IOException {
    gcs.composeObjects(TEST_STORAGE_RESOURCE_IDS, TEST_STORAGE_RESOURCE_ID, TEST_OBJECT_OPTIONS);

    verify(mockGcsDelegate)
        .composeObjects(
            eq(TEST_STORAGE_RESOURCE_IDS), eq(TEST_STORAGE_RESOURCE_ID), eq(TEST_OBJECT_OPTIONS));
  }

  @Test
  public void testGetDelegate() {
    GoogleCloudStorage delegate = gcs.getDelegate();

    assertThat(delegate).isEqualTo(mockGcsDelegate);
  }

  @Test
  public void delegate_throwsExceptionWhenNull() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> new ForwardingGoogleCloudStorage(null));
    assertThat(exception).hasMessageThat().startsWith("delegate must not be null");
  }

  @Test
  public void readVectored_callsBlobReadSession()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    gcs.readVectored(
        List.of(getRange(), getRange()),
        getAllocator(),
        BlobId.of(TEST_STRING, TEST_STRING, TEST_LONG));

    verify(mockGcsDelegate)
        .readVectored(
            eq(List.of(getRange(), getRange())),
            eq(getAllocator()),
            eq(BlobId.of(TEST_STRING, TEST_STRING, TEST_LONG)));
  }

  private IntFunction<ByteBuffer> getAllocator() {
    return length -> ByteBuffer.allocateDirect(length);
  }

  private VectoredIORange getRange() {
    return VectoredIORange.builder().setOffset(TEST_LONG).setLength(TEST_INT).build();
  }
}
