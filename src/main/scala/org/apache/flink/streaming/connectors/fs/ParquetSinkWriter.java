package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class ParquetSinkWriter<T extends GenericRecord> implements Writer<T> {

  private static final long serialVersionUID = -975302556515811398L;

  private final CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
  private final int pageSize = 64 * 1024;
  private final int blockSize = 256 * 1024 * 1024;
  private final String schemaRepresentation;

  private transient Schema schema;
  private transient ParquetWriter<GenericRecord> writer;
  private transient Path path;

  private int position;

  public ParquetSinkWriter(String schemaRepresentation) {
    this.schemaRepresentation = Preconditions.checkNotNull(schemaRepresentation);
  }

  @Override
  public void open(FileSystem fs, Path path) throws IOException {
    this.position = 0;
    this.path = path;

    if (writer != null) {
      writer.close();
    }
    writer = createWriter();
  }

  @Override
  public long flush() throws IOException {
    Preconditions.checkNotNull(writer);
    position += writer.getDataSize();
    writer.close();
    writer = createWriter();
    return position;
  }

  @Override
  public long getPos() throws IOException {
    Preconditions.checkNotNull(writer);
    return position + writer.getDataSize();
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }

  @Override
  public void write(T element) throws IOException {
    Preconditions.checkNotNull(writer);
    writer.write(element);
  }

  @Override
  public Writer<T> duplicate() {
    return new ParquetSinkWriter<>(schemaRepresentation);
  }

  private ParquetWriter<GenericRecord> createWriter() throws IOException {
    if (schema == null) {
      schema = new Schema.Parser().parse(schemaRepresentation);
    }

    return new AvroParquetWriter<GenericRecord>(this.path, schema, compressionCodecName, blockSize, pageSize,true);
   /*return AvroParquetWriter.<GenericRecord>builder(path)
            .withSchema(schema)
            .withDataModel(GenericData.get())
            .withCompressionCodec(compressionCodecName)
            .withPageSize(pageSize)
            .build();*/
  }
}

