/** 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.jeffbeagley.kafka.connect.transform.common;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import scala.Int;

import java.text.SimpleDateFormat;
import java.util.*;
import java.time.*;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.transforms.util.Requirements.*;
//import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class EpochtoTimestamp<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Convert timestamps between different formats such as Unix epoch, strings, and Connect Date/Timestamp types."
                    + "Applies to individual fields or to the entire value."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + EpochtoTimestamp.Key.class.getName() + "</code>) "
                    + "or value (<code>" + EpochtoTimestamp.Value.class.getName() + "</code>).";

    public static final String FIELD_CONFIG = "field";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                    "The field containing the timestamp");
    private static final String PURPOSE = "converting timestamp formats";
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private List<String> field;

    static final long NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

    private static class Config {
        Config(String field) {
            this.field = field;

        }
        String field;
    }
    private Config config;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig simpleConfig = new SimpleConfig(CONFIG_DEF, configs);
        final String field = simpleConfig.getString(FIELD_CONFIG);

        config = new Config(field);

    }

    @Override
    public R apply(R record) {
        return applyWithSchema(record);

    }

    private R applyWithSchema(R record) {
        final Schema original_schema = operatingSchema(record);
        final Struct original_value = requireStruct(operatingValue(record), PURPOSE);

        //create new schema
        final SchemaBuilder transformed_schema = SchemaUtil.copySchemaBasics(original_schema, SchemaBuilder.struct());

        for (Field field : original_schema.fields()) {
            if (field.name().equals(config.field)) {
                transformed_schema.field(field.name(), Schema.OPTIONAL_STRING_SCHEMA);

            } else {
                transformed_schema.field(field.name(), field.schema());
            }
        }

        Schema new_schema = transformed_schema.build();

        //manipulate values
        final Struct updatedValues = new Struct(new_schema);
        for (Field field : new_schema.fields()) {
            if(field.name().equals((config.field))) {
                //translate epoch to timestamp format
                if(original_value.get(field.name()) != "null" && original_value.get(field.name()) != null) {
                    //if length of date is longer than 13, then assume date is nanoseconds
                    Long d = (Long) original_value.get(field.name());
                    int l = String.valueOf(d).length();

                    String new_date;

                    if(l > 13) {
                        new_date = convertNS((Long) original_value.get(field.name()));
                    } else {
                        new_date = convertMS((Long) original_value.get(field.name()));
                    }

                    updatedValues.put(field.name(), new_date);
                } else {
                    updatedValues.put(field.name(), original_value.get(field.name()));
                }

            } else {
                updatedValues.put(field.name(), original_value.get(field.name()));
            }

        }

        return newRecord(record, new_schema, updatedValues);

    }

    public String convertMS(Long ms) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
        format.setTimeZone(UTC);

        return format.format(ms);

    }

    public String convertNS(long ns) {
        // long ms = TimeUnit.NANOSECONDS.toMillis(ns);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSS");
        format.setTimeZone(UTC);

        return format.format(ns);

    }

    public static class Key<R extends ConnectRecord<R>> extends EpochtoTimestamp<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends EpochtoTimestamp<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

}