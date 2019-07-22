package org.apache.flink.formats.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Created by KT on 2018/8/5.
 */
public class AvroGenericRowSerializationSchema implements SerializationSchema<Row> {
    /**
     * Avro serialization schema.
     */
    private transient Schema schema;

    /**
     * Writer to serialize Avro record into a byte array.
     */
    private transient DatumWriter<GenericRecord> datumWriter;

    /**
     * Output stream to serialize records into byte array.
     */
    private transient ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();

    /**
     * Low-level class for serialization of Avro values.
     */
    private transient Encoder encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);

    public AvroGenericRowSerializationSchema(Schema schema) {
        this.schema = schema;
        this.datumWriter = new GenericDatumWriter(this.schema);
    }

    @Override
    public byte[] serialize(Row row) {
        final Object record = convertToRecord(schema, row);
        // write
        try {
            arrayOutputStream.reset();
            datumWriter.write((GenericRecord) record, encoder);
            encoder.flush();
            return arrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Row.", e);
        }
    }


    /**
     * Converts a (nested) Flink Row into Avro's {@link GenericRecord}.
     * Strings are converted into Avro's {@link Utf8} fields.
     */
    private static Object convertToRecord(Schema schema, Object rowObj) {
        if (rowObj instanceof Row) {
            // records can be wrapped in a union
            if (schema.getType() == Schema.Type.UNION) {
                final List<Schema> types = schema.getTypes();
                if (types.size() == 2 && types.get(0).getType() == Schema.Type.NULL && types.get(1).getType() == Schema.Type.RECORD) {
                    schema = types.get(1);
                }
                else if (types.size() == 2 && types.get(0).getType() == Schema.Type.RECORD && types.get(1).getType() == Schema.Type.NULL) {
                    schema = types.get(0);
                }
                else {
                    throw new RuntimeException("Currently we only support schemas of the following form: UNION[null, RECORD] or UNION[RECORD, NULL] Given: " + schema);
                }
            } else if (schema.getType() != Schema.Type.RECORD) {
                throw new RuntimeException("Record type for row type expected. But is: " + schema);
            }
            final List<Schema.Field> fields = schema.getFields();
            final GenericRecord record = new GenericData.Record(schema);
            final Row row = (Row) rowObj;
            for (int i = 0; i < fields.size(); i++) {
                final Schema.Field field = fields.get(i);
                record.put(field.pos(), convertToRecord(field.schema(), row.getField(i)));
            }
            return record;
        } else if (rowObj instanceof String) {
            return new Utf8((String) rowObj);
        } else {
            return rowObj;
        }
    }
}
