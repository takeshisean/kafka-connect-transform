# EpochtoTimestamp converts Epoch to smalldatetime compatible format, or returns null
This was built to bypass the Null Pointer Exception (NPE) caused by the built in SMT TimeStampConverter when using nullable date fields

### How to use in a Kafka Connector Config
```
"transforms": "epoch_timestamp",
"transforms.epoch_timestamp.type": "com.github.jeffbeagley.kafka.connect.transform.common.EpochtoTimestamp$Value",
"transforms.epoch_timestamp.field": "non_nullable_date",
```