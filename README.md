# EpochtoTimestamp converts Epoch to smalldatetime compatible format, or returns null
This was built to bypass the Null Pointer Exception (NPE) caused by the built in SMT TimeStampConverter when using nullable date fields

### How to build this project
1) Run command below to build:
mvn package -e

2) Look for the output jar file in "kafka-connect-transform\target":
For example "kafka-connect-transforms-1.2-SNAPSHOT-jar-with-dependencies.jar"

### How to use in a Kafka Connector Config
```
"transforms": "epoch_timestamp",
"transforms.epoch_timestamp.type": "com.github.jeffbeagley.kafka.connect.transform.common.EpochtoTimestamp$Value",
"transforms.epoch_timestamp.field": "non_nullable_date",
```