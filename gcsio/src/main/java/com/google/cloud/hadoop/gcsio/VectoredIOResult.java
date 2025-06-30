package com.google.cloud.hadoop.gcsio;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

@AutoValue
public abstract class VectoredIOResult {

  public static Builder builder() {
    return new AutoValue_VectoredIOResult.Builder().setReadBytes(null);
  }

  public abstract VectoredIOResult.Builder toBuilder();

  @Nullable
  public abstract Integer getReadBytes();

  @Nullable
  public abstract Long getClientInitializationDuration();

  @Nullable
  public abstract Long getReadDuration();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract VectoredIOResult.Builder setReadBytes(Integer readBytes);

    public abstract VectoredIOResult.Builder setClientInitializationDuration(
        Long clientInitializationDuration);

    public abstract VectoredIOResult.Builder setReadDuration(Long readDuration);

    abstract VectoredIOResult autoBuild();

    public VectoredIOResult build() {
      VectoredIOResult result = autoBuild();
      return result;
    }
  }
}
