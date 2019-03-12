package com.github.jeffbeagley.kafka.connect.transform.common;


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EpochtoTimestampTest {
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final Calendar EPOCH;
    private static final Calendar TIME;
    private static final Calendar DATE;
    private static final Calendar DATE_PLUS_TIME;
    private static final long DATE_PLUS_TIME_UNIX;
    private static final String STRING_DATE_FMT = "yyyy-MM-dd HH:mm:ss.SS";
    private static final String DATE_PLUS_TIME_STRING;

    private final EpochtoTimestamp<SourceRecord> xformKey = new EpochtoTimestamp.Key<>();
    private final EpochtoTimestamp<SourceRecord> xformValue = new EpochtoTimestamp.Value<>();

    static {
        EPOCH = GregorianCalendar.getInstance(UTC);
        EPOCH.setTimeInMillis(0L);

        TIME = GregorianCalendar.getInstance(UTC);
        TIME.setTimeInMillis(0L);
        TIME.add(Calendar.MILLISECOND, 1234);

        DATE = GregorianCalendar.getInstance(UTC);
        DATE.setTimeInMillis(0L);
        DATE.set(1970, Calendar.JANUARY, 1, 0, 0, 0);
        DATE.add(Calendar.DATE, 1);

        DATE_PLUS_TIME = GregorianCalendar.getInstance(UTC);
        DATE_PLUS_TIME.setTimeInMillis(0L);
        DATE_PLUS_TIME.add(Calendar.DATE, 1);
        DATE_PLUS_TIME.add(Calendar.MILLISECOND, 1234);

        DATE_PLUS_TIME_UNIX = DATE_PLUS_TIME.getTime().getTime();
        DATE_PLUS_TIME_STRING = "1970-01-02 00:00:01.234";

    }

    @AfterEach
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }


    @Test
    public void UnixTimestampConverts() {
        Map<String, String> config = new HashMap<>();
        config.put(EpochtoTimestamp.FIELD_CONFIG, "non_nullable_date");
        xformValue.configure(config);

        Schema payload_schema = SchemaBuilder.struct()
                .field("some_other_value", Schema.STRING_SCHEMA)
                .field("non_nullable_date", Schema.INT64_SCHEMA)
                .build();

        Struct payload_values = new Struct(payload_schema);

        payload_values.put("some_other_value", "jeff");
        payload_values.put("non_nullable_date", DATE_PLUS_TIME_UNIX);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, payload_schema, payload_values));

        Schema expectedSchema = SchemaBuilder.struct()
                .field("some_other_value", Schema.STRING_SCHEMA)
                .field("non_nullable_date", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        System.out.println((transformed.valueSchema()));
        assertEquals(expectedSchema.toString(), transformed.valueSchema().toString());
        assertEquals(DATE_PLUS_TIME_STRING, ((Struct) transformed.value()).get("non_nullable_date"));

    }

    @Test
    public void NullTimestampisDropped() {
        Map<String, String> config = new HashMap<>();
        config.put(EpochtoTimestamp.FIELD_CONFIG, "nullable_date");
        xformValue.configure(config);

        Schema payload_schema = SchemaBuilder.struct()
                .field("some_other_value", Schema.STRING_SCHEMA)
                .field("nullable_date", Schema.STRING_SCHEMA)
                .build();

        Struct payload_values = new Struct(payload_schema);

        payload_values.put("some_other_value", "jeff");
        payload_values.put("nullable_date", "null");

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, payload_schema, payload_values));

        Schema expectedSchema = SchemaBuilder.struct()
                .field("some_other_value", Schema.STRING_SCHEMA)
                .field("non_nullable_date", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        assertEquals(expectedSchema, transformed.valueSchema());
        assertEquals("jeff", ((Struct) transformed.value()).get("some_other_value"));
        assertEquals("null", ((Struct) transformed.value()).get("nullable_date"));
    }


}