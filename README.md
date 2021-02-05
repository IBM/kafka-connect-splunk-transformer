# Splunk-transformer

This repository contains two transformations which can be of help when dealing with the transfer of `JSON` records into Splunk.

## Transformer: com.ibm.garage.kafka.connect.transforms.Splunk

The intention of this transformer is to mimic internal Splunk transformations and configuration parameters; `SOURCE_KEY`, `DEST_KEY`, `FORMAT` and `REGEX`. This allows us to offload the processing of log messages into Kafka Connect workers. The transformer was designed to work in conjunction with [Kafka Connect Sink for Splunk](https://github.com/splunk/kafka-connect-splunk) in which you can enable the `splunk.header.support` configuration parameter to support the transfer of Kafka record header fields to Splunk metadata (e.g. `index`, `source`, `sourcetype`, `host`).

### Configuration Parameters

| Name                 | Description                                                                                                                                                          | Default Value |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| `sourceKey`          | Name of the field on which (either itself or its value) we want to apply some changes.                                                                               |               |
| `destKey`            | If `destKey` is specified, the transformation will rename `sourceKey` field to `destKey`.                                                                            |               |
| `isMetadata`         | Set to `true` for putting the `sourceKey` field into Kafka headers. By default, it will remove the field from body unless `preserveKeyInBody` is set to `true`.      | `false`       |
| `preserveKeyInBody`  | An option for preserving the original field in the body and thus available for the next transformation if `isMetadata` is set to `true`.                             | `false`       |
| `regex.pattern`      | An option to apply a regex to the value of the `sourceKey`. `regex.format` option needs to be specified and capture groups are supported.                            |               |
| `regex.format`       | An option to apply final formatting on the `sourceKey` value. Capture groups from the regex can be used using dollar syntax e.g. $1.                                 |               |
| `regex.defaultValue` | An option to provide a default value for the target field, if the source key does not match the regex pattern. `regex.pattern` and `regex.format` must be specified. |               |

### Notes on transformer behaviour

If `regex.pattern` is specified, but there is no match on the value of the `sourceKey` field, the `regex.defaultValue` is returned if it is specified, otherwise the kafka record is returned unchanged (without any other transformations).

### Flowchart Diagram

![Flowchart Diagram](/doc/architecture-flowchart.svg)

## Transformer: com.ibm.garage.kafka.connect.transforms.Filter

The intention of this transformer is to add filtering capabilities similar to those in Kafka Connect versions 2.6 and above using the `org.apache.kafka.connect.transforms.Filter` transformer. For earlier versions, the [KIP-585: Filter and Conditional SMTs](https://cwiki.apache.org/confluence/display/KAFKA/KIP-585%3A+Filter+and+Conditional+SMTs) is not yet included and `org.apache.kafka.connect.transforms.Filter` SMT in conjuction with `Predicate` interface cannot be used.

### Configuration Parameters

| Name        | Description                                                                                                                                                                | Default Value |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| `headerKey` | Name of the header key. If such a header key exists in the Kafka record, the whole message will be discarded unless `isNegate` is set to `true` to reverse this condition. |               |
| `isNegate`  | Set to `true` to negate filtering of messages with specifed. `headerkey`                                                                                                   | `false`       |

## Example transformation

Here is an example configuration for the Splunk and Filter transformers as discussed above - the `transforms` field contains an ordered list of transformers you want to apply.

```json
{
  "connector.class": "com.splunk.kafka.connect.SplunkSinkConnector",
  "tasks.max": "1",
  "topics": "LogDNATopic",
  "splunk.header.support": "true",
  "splunk.hec.uri": "<SPLUNK_HEC_URIs>",
  "splunk.hec.token": "<SPLUNK_TOKEN>",
  "transforms": "my_custom_transform_1,my_custom_transform_2,discard_if_no_index_in_header",
  "transforms.my_custom_transform_1.type": "com.ibm.garage.kafka.connect.transforms.Splunk",
  "transforms.my_custom_transform_1.sourceKey": "source_key1",
  "transforms.my_custom_transform_1.destKey": "splunk.header.index",
  "transforms.my_custom_transform_1.isMetadata": true,
  "transforms.my_custom_transform_1.regex.pattern": "^(.*)$",
  "transforms.my_custom_transform_1.regex.format": "my_custom_$1_format",
  "transforms.my_custom_transform_1.regex.defaultValue": "my default value",
  "transforms.my_custom_transform_1.preserveKeyInBody": true,
  "transforms.my_custom_transform_2.type": "com.ibm.garage.kafka.connect.transforms.Splunk",
  "transforms.my_custom_transform_2.sourceKey": "source_key1",
  "transforms.my_custom_transform_2.destKey": "source_key1_renamed",
  "transforms.discard_if_no_index_in_header.type": "com.ibm.garage.kafka.connect.transforms.Filter",
  "transforms.discard_if_no_index_in_header.headerKey": "splunk.header.index",
  "transforms.discard_if_no_index_in_header.isNegate": true
}
```

- The first transformation will take the value of the `source_key1` field, if found, and apply the `regex.pattern` `^(.*)$` with the value `my_custom_source_key1_regex.format` (as per the `regex.format` specified) if the regex pattern matches, if it does not then the `regex.defaultValue` if specified will be used as the value instead. It will then set the `splunk.header.index` header on the kafka record and, as `preserveKeyInBody` is specified, the original field will be left intact on the record and available to the next transformation.
- The second transformation will rename the field `source_key1` to `source_key1_renamed`.
- The last transformation will have no effect as a header with key `splunk.header.index` was set on the first transform and, as we have set `isNegate`, it will not filter the message.

## Build

```
gradle jar
```

## Test

```
gradle clean test
```

## Setting up Kafka Connect worker

### Worker properties

These transformers rely on the message being in JSON format and the connect worker to be configured to use JSON value and key converters. To do this, the connect worker will need to have the following properties set:

```
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
```

Unless you are specifying a schema, you will also need to set the following properties:

```
key.converter.schemas.enable=false
value.converter.schemas.enable=false
```

### Adding transformer

In order to make the transformers available to your Kafka connect worker, place the built jar on a supported plugin path in your worker environment.
