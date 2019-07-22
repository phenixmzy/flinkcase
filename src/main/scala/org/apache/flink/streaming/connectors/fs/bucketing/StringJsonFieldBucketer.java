package org.apache.flink.streaming.connectors.fs.bucketing;

import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.hadoop.fs.Path;

public class StringJsonFieldBucketer<T> implements Bucketer<T>  {

  @Override
  public Path getBucketPath(Clock clock, Path basePath, T element) {
    return new Path(basePath + "/");
  }
}
