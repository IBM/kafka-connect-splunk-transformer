# Kafka Connect Splunk Transformer

This repository contains two transformations which can be of help when dealing with the transfer of `JSON` records into Splunk.

## Transformer: com.ibm.garage.kafka.connect.transforms.Splunk

The intention of this transformer is to mimic internal Splunk transformations and configuration parameters; `SOURCE_KEY`, `DEST_KEY`, `FORMAT` and `REGEX`. This allows us to offload the processing of log messages into Kafka Connect workers. The transformer was designed to work in conjunction with [Kafka Connect Sink for Splunk](https://github.com/splunk/kafka-connect-splunk) in which you can enable the `splunk.header.support` configuration parameter to support the transfer of Kafka record header fields to Splunk metadata (e.g. `index`, `source`, `sourcetype`, `host`).

### Configuration Parameters

| Name                 | Description                                                                                                                                                                                                                                                              | Default Value |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------- |
| `source.key`         | Name of the field on which (either itself or its value) we want to apply some changes. Nested fields are also supported utilizing the dotted form, e.g. `"config.app.id"`. If the `source.key` parameter contains a dot (`.`), it is automatically considered as nested. |               |
| `source.preserve`    | An option for preserving the original `source.key` field in the Kafka record body when the `dest.key` field is specified. The `source.key` field can thus be left unchanged in the Kafka record body.                                                                    | `false`       |
| `dest.key`           | If `dest.key` is specified, the transformation will rename `source.key` field to `dest.key`. `dest.key` cannot point to the same field as `source.key` does.                                                                                                             |               |
| `dest.toHeader`      | Set to `true` if you want to put the final field (`source.key` or `dest.key` if specified) into Kafka record headers. The final field is then removed from the Kafka record body.                                                                                        | `false`       |
| `regex.pattern`      | An option to apply a regex to the value of the `source.key`. `regex.format` option needs to be specified. Capture groups are supported.                                                                                                                                  |               |
| `regex.format`       | An option to apply final formatting on the `source.key` value. Capture groups from the regex can be used using dollar syntax e.g. `$1`.                                                                                                                                  |               |
| `regex.defaultValue` | An option to provide a default value for the target field, if the `source.key` value does not match the regex pattern. `regex.pattern` and `regex.format` must be specified.                                                                                             |               |

### Notes on transformer behaviour

#### Nested source.key

- If the `source.key` parameter contains a dot (`.`) character, it is automatically considered as nested.
- If the `source.key` is the only field nested in the parent object and the `source.key` is renamed by using the `dest.key` (`source.preserve` defaults to `false`) then the key is put into the root of the JSON message. The parent object of the `source.key` field is left empty, and it is not removed.
  - Example: `{"nested": {"renameMe": "value"}}` => `{"nested": {}, "renamedKey": "value"}`
- Despite the fact that the `dest.key` can contain dots (`.`), it is in NO way considered as the nested field and its key is always processed at once without creating any nested structures.
  - Example: `{"nested": {"renameMe": "value"}}` => `{"nested": {}, "renamed.key": "value"}`
- `source.key` parameter cannot point to the object. It must be the final element (field). If it points to the object, the Kafka record is returned unchanged.
  - Example: `{"nested": {"renameMe": "value"}}` and (`source.key="nested"`) => unchanged Kafka record
- `regex.pattern`, `regex.format` and `regex.defaultValue` are applied in place if `dest.key` is not specified.
  - Example: `{"nested": {"key": "value"}}` => `{"nested": {"key": "applied format or default value"}}`

#### Regex & format

- `java.util.regex` package is used to work with regular expressions.
- If `regex.pattern` is specified, but there is no match on the value of the `source.key` field, the `regex.defaultValue` is returned if it is specified. Otherwise, the Kafka record is returned unchanged (without any other transformations).

### Flowchart Diagram

![Flowchart Diagram](/doc/architecture-flowchart.svg)

## Transformer: com.ibm.garage.kafka.connect.transforms.Filter

The intention of this transformer is to add filtering capabilities similar to those in Kafka Connect versions 2.6 and above using the `org.apache.kafka.connect.transforms.Filter` transformer. For earlier versions, the [KIP-585: Filter and Conditional SMTs](https://cwiki.apache.org/confluence/display/KAFKA/KIP-585%3A+Filter+and+Conditional+SMTs) is not yet included and `org.apache.kafka.connect.transforms.Filter` SMT in conjuction with `Predicate` interface cannot be used.

### Configuration Parameters

| Name        | Description                                                                                                                                                                | Default Value |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| `headerKey` | Name of the header key. If such a header key exists in the Kafka record, the whole message will be discarded unless `isNegate` is set to `true` to reverse this condition. |               |
| `isNegate`  | Set to `true` to negate filtering of messages with specified `headerKey`.                                                                                                  | `false`       |

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
  "transforms.my_custom_transform_1.source.key": "source_key1",
  "transforms.my_custom_transform_1.source.preserve": true,
  "transforms.my_custom_transform_1.dest.key": "splunk.header.index",
  "transforms.my_custom_transform_1.dest.toHeader": true,
  "transforms.my_custom_transform_1.regex.pattern": "^(.*)$",
  "transforms.my_custom_transform_1.regex.format": "my_custom_$1_format",
  "transforms.my_custom_transform_1.regex.defaultValue": "my default value",

  "transforms.my_custom_transform_2.type": "com.ibm.garage.kafka.connect.transforms.Splunk",
  "transforms.my_custom_transform_2.source.key": "source_key1",
  "transforms.my_custom_transform_2.dest.key": "source_key1_renamed",

  "transforms.discard_if_no_index_in_header.type": "com.ibm.garage.kafka.connect.transforms.Filter",
  "transforms.discard_if_no_index_in_header.headerKey": "splunk.header.index",
  "transforms.discard_if_no_index_in_header.isNegate": true
}
```

- The configuration includes three transformations ordered in the pipeline (see `transforms` field).
- The first transformation (labeled `my_custom_transform_1`):
  - It takes the value of the `source_key1` field if it is found.
  - If the `regex.pattern` matches, it applies the `regex.format`, which results in `my_custom_<value of source_key1 field>_format`.
  - If the `regex.pattern` does not match, the `regex.defaultValue` is used as the value instead.
  - Since the `dest.key` is specified, a new field with `dest.key` key is created (which means `splunk.header.index`). The value is taken from the previous points.
  - `source.preserve` is set to `true`, so the original `source.key` is not removed from the Kafka record. It is left intact and available to the next transformation.
  - `dest.toHeader` is set to `true`, so newly created `splunk.header.index` field and its new value are moved to the Kafka record headers. The field is then removed from the Kafka record body.
- The second transformation (labeled `my_custom_transform_2`):
  - It creates a new `source_key1_renamed` field, and it takes the value from the original source key `source_key1`.
  - Since `source.preserve` defaults to `false`, the original `source_key1` field is removed from the Kafka record.
- The third transformation (labeled `discard_if_no_index_in_header`):
  - If the header with key `splunk.header.index` exists in the Kafka record, the message is not discarded (because the condition is negated by `isNegate` set to `true`). If the header with key `splunk.header.index` does not exist in the Kafka record, the message is discarded.

## Build

Please, use Gradle version 7.0+:

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
