package com.github.jeffbeagley.kafka.connect.transform.common;


import org.apache.kafka.connect.data.Schema;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.AfterEach;


import org.junit.jupiter.api.Test;

class TimestampConverterTest {
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final Calendar EPOCH;
    private static final Calendar TIME;
    private static final Calendar DATE;
    private static final Calendar DATE_PLUS_TIME;
    private static final long DATE_PLUS_TIME_UNIX;
    private static final String STRING_DATE_FMT = "yyyy-MM-dd HH:mm:ss.SSSSSS";

    private final TimestampConverter<SourceRecord> xformKey = new TimestampConverter.Key<>();
    private final TimestampConverter<SourceRecord> xformValue = new TimestampConverter.Value<>();

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

    }

    @AfterEach
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    // smalldatetime gets translated to adn stored in epoch within Kafka. Using Kafka connect, that epoch gets translated into a format via FORMAT_CONFIG to then be placed
    // back into smalldatetime format for SQL to accept
    @Test
    public void testUnixtoTimestampwithFormatfromField() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        config.put(TimestampConverter.FIELD_CONFIG, "ts");
        xformValue.configure(config);

        Schema structWithTimestampFieldSchema = SchemaBuilder.struct()
                .field("ts", Schema.INT64_SCHEMA)
                .field("other", Schema.STRING_SCHEMA)
                .build();
        Struct original = new Struct(structWithTimestampFieldSchema);
        original.put("ts", DATE_PLUS_TIME_UNIX);
        original.put("other", "test");

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, structWithTimestampFieldSchema, original));

        Schema expectedSchema = SchemaBuilder.struct()
                .field("ts", Timestamp.SCHEMA)
                .field("other", Schema.STRING_SCHEMA)
                .build();

        assertEquals(expectedSchema, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), ((Struct) transformed.value()).get("ts"));
        assertEquals("test", ((Struct) transformed.value()).get("other"));
    }

    @Test
    public void testNullUnixtoTimestampwithFormatfromField() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        config.put(TimestampConverter.FIELD_CONFIG, "ts");
        xformValue.configure(config);

        Schema structWithTimestampFieldSchema = SchemaBuilder.struct()
                .field("ts", Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        Struct original = new Struct(structWithTimestampFieldSchema);
        original.put("ts", null);

        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, structWithTimestampFieldSchema, original));

        System.out.println(original);
        System.out.println(transformed);

        Schema expectedSchema = SchemaBuilder.struct()
                .field("ts", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        assertEquals(expectedSchema, transformed.valueSchema());
        assertEquals(null, ((Struct) transformed.value()).get("ts"));
    }

}