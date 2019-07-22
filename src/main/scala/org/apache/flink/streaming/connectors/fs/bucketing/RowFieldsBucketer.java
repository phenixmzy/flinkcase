package org.apache.flink.streaming.connectors.fs.bucketing;

import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;

public class RowFieldsBucketer implements Bucketer<Row>  {
  private int rowIdx;
  public RowFieldsBucketer(int rowIdx) {
    this.rowIdx = rowIdx;
  }

  @Override
  public Path getBucketPath(Clock clock, Path basePath, Row row) {
    return new Path(basePath + "/"+ row.getField(rowIdx).toString());
  }

  public String toString() {
    return "RowFieldsBucketer{rowIdx='" + this.rowIdx + '\'' + '}';
  }
}
