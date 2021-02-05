/*
 Copyright 2021 IBM Inc. All rights reserved
 SPDX-License-Identifier: Apache2.0
*/

package com.ibm.garage.kafka.connect.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class SplunkTest {

	private static final String TEST_PURPOSE = "test purpose";

	@Nested
	@DisplayName("Configuration")
	class Configuration {
		private Transformation<SinkRecord> transformation;

		@Test
		@DisplayName("Should throw an exception if sourceKey configuration is null")
		public void configuration_throwsRuntimeException_sourceKey_Null() {
			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, null);
			props.put(Splunk.DESTINATION_KEY_CONFIG, "destKey");

			this.shouldThrow(props);
		}

		@Test
		@DisplayName("Should throw an exception if sourceKey configuration is empty")
		public void configuration_throwsRuntimeException_sourceKey_Empty() {
			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, "");
			props.put(Splunk.DESTINATION_KEY_CONFIG, "destKey");

			this.shouldThrow(props);
		}

		@Test
		@DisplayName("Should throw an exception if regex pattern is specified but regex format is not")
		public void configuration_throwsRuntimeException_only_regex_pattern_specified() {
			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, "sourceKey");
			props.put(Splunk.REGEX_PATTERN_CONFIG, "notnull");

			this.shouldThrow(props);
		}

		@Test
		@DisplayName("Should throw an exception if regex format is specified but regex pattern is not")
		public void configuration_throwsRuntimeException_only_regex_format_specified() {
			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, "sourceKey");
			props.put(Splunk.REGEX_FORMAT_CONFIG, "notnull");

			this.shouldThrow(props);
		}

		@Test
		@DisplayName("Should throw an exception if regex default value is specified but regex pattern is not")
		public void configuration_throwsRuntimeException_default_value_and_format_specified() {
			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, "sourceKey");
			props.put(Splunk.REGEX_FORMAT_CONFIG, "myformat");
			props.put(Splunk.REGEX_DEFAULT_VALUE_CONFIG, "my default Value");

			this.shouldThrow(props);
		}

		@Test
		@DisplayName("Should throw an exception if isPreserveInBody is true but isMetadata is false")
		public void configuration_throwsRuntimeException_isPreserveInBody_specified() {
			Map<String, Object> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, "sourceKey");
			props.put(Splunk.IS_METADATA_KEY_CONFIG, Boolean.FALSE);
			props.put(Splunk.PRESERVE_CONFIG, Boolean.TRUE);

			this.shouldThrow(props);
		}

		@Test
		@DisplayName("Should throw an exception if the regex.pattern specified is not valid")
		public void configuration_throwsRuntimeException_regexPattern_badFormat() {
			Map<String, Object> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, "sourceKey");
			props.put(Splunk.IS_METADATA_KEY_CONFIG, Boolean.FALSE);
			props.put(Splunk.REGEX_PATTERN_CONFIG, "^(.*$");

			this.shouldThrow(props);
		}

		private void shouldThrow(Map<String, ?> props) {
			this.transformation = new Splunk<>();

			assertThrows(RuntimeException.class, () -> {
				this.transformation.configure(props);
			});

		}
	}

	@Nested
	@DisplayName("Messages")
	class Messages {

		private Transformation<SinkRecord> transformation;

		@Test
		@DisplayName("Should return null if a message is null")
		public void message_returnOriginalNullMessage() {
			this.transformation = new Splunk<>();

			Map<String, Object> mapValue = null;

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = this.transformation.apply(record);

			Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			assertNull(resultMap);
		}

		@Test
		@DisplayName("Should return original message if a message is empty")
		public void message_returnOriginalEmptyMessage() {
			this.transformation = new Splunk<>();

			Map<String, Object> mapValue = new HashMap<>();

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = this.transformation.apply(record);

			Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			assertNotNull(resultMap);
			assertTrue(resultMap.isEmpty());
		}

		@Test
		@DisplayName("Should return original message if the sourceKey field is the only configuration specified")
		public void message_returnOrigMessage() {
			Map<String, String> props = new HashMap<>();
			final String FIELD_NAME = "fieldName";
			final String FIELD_VALUE = "test value";

			props.put(Splunk.SOURCE_KEY_CONFIG, FIELD_NAME);

			this.transformation = new Splunk<>();
			this.transformation.configure(props);

			Map<String, Object> originalMapValue = new HashMap<>();
			originalMapValue.put(FIELD_NAME, FIELD_VALUE);

			Map<String, Object> originalMapValueCopy = new HashMap<>();
			originalMapValueCopy.put(FIELD_NAME, FIELD_VALUE);

			final SinkRecord originalRecord = newRecord(originalMapValue);
			SinkRecord resultRecord = this.transformation.apply(originalRecord);

			Map<String, Object> resultMapValue = requireMapOrNull(resultRecord.value(), TEST_PURPOSE);
			assertEquals(originalMapValueCopy, resultMapValue);
		}

		@Test
		@DisplayName("Should rename a sourceKey to destKey field in a message if it is present")
		public void message_returnRenamedField() {
			Map<String, String> props = new HashMap<>();
			final String OLD_FIELD_NAME = "oldFieldName";
			final String NEW_FIELD_NAME = "newFieldName";
			final String FIELD_VALUE = "test value";

			props.put(Splunk.SOURCE_KEY_CONFIG, OLD_FIELD_NAME);
			props.put(Splunk.DESTINATION_KEY_CONFIG, NEW_FIELD_NAME);

			this.transformation = new Splunk<>();
			this.transformation.configure(props);

			Map<String, Object> mapValue = new HashMap<>();
			mapValue.put(OLD_FIELD_NAME, FIELD_VALUE);

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = this.transformation.apply(record);

			Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);

			assertTrue(resultMap.containsKey(NEW_FIELD_NAME));
			assertFalse(resultMap.containsKey(OLD_FIELD_NAME));
			assertEquals(FIELD_VALUE, resultMap.get(NEW_FIELD_NAME));
		}

		@Test
		@DisplayName("Should return unchanged message if the sourceKey field does not exist")
		public void message_returnUnchangedMessage() {
			Map<String, String> props = new HashMap<>();
			final String OLD_FIELD_NAME = "doesNotExist";
			final String NEW_FIELD_NAME = "newFieldName";
			final String FIELD_VALUE = "test value";

			props.put(Splunk.SOURCE_KEY_CONFIG, OLD_FIELD_NAME);
			props.put(Splunk.DESTINATION_KEY_CONFIG, NEW_FIELD_NAME);

			this.transformation = new Splunk<>();
			this.transformation.configure(props);

			Map<String, Object> mapValue = new HashMap<>();
			mapValue.put(OLD_FIELD_NAME, FIELD_VALUE);

			final SinkRecord originalRecord = newRecord(mapValue);
			SinkRecord resultRecord = this.transformation.apply(originalRecord);

			assertEquals(originalRecord, resultRecord);
		}

		@Test
		@DisplayName("Should preserve a sourceKey and it's original value in the body if preserveKeyInBody is set to true and isMetadata is set to true")
		public void message_preserveField() {
			Map<String, Object> props = new HashMap<>();
			final String OLD_FIELD_NAME = "oldFieldName";
			final String NEW_FIELD_NAME = "newFieldName";
			final Boolean IS_METADATA = Boolean.TRUE;
			final String FIELD_VALUE = "test value";
			final Boolean PRESERVE = Boolean.TRUE;

			props.put(Splunk.SOURCE_KEY_CONFIG, OLD_FIELD_NAME);
			props.put(Splunk.DESTINATION_KEY_CONFIG, NEW_FIELD_NAME);
			props.put(Splunk.IS_METADATA_KEY_CONFIG, IS_METADATA);
			props.put(Splunk.PRESERVE_CONFIG, PRESERVE);

			this.transformation = new Splunk<>();
			this.transformation.configure(props);

			Map<String, Object> mapValue = new HashMap<>();
			mapValue.put(OLD_FIELD_NAME, FIELD_VALUE);

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = this.transformation.apply(record);

			Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			assertTrue(resultMap.containsKey(OLD_FIELD_NAME));
			assertEquals(FIELD_VALUE, resultMap.get(OLD_FIELD_NAME));

			Iterator<Header> headerIterator = result.headers().allWithName(NEW_FIELD_NAME);
			assertTrue(headerIterator.hasNext());
			assertEquals(FIELD_VALUE, headerIterator.next().value());
		}

		@Nested
		@DisplayName("Regex & Format")
		class RegexFormat {
			private Transformation<SinkRecord> transformation;

			@Test
			@DisplayName("Should apply regex & format to the value of sourceKey field")
			public void message_returnRegexFormat() {
				Map<String, Object> props = new HashMap<>();
				final String FIELD_NAME = "aplyRegexAndFormat";
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String REGEX = "crn:(?:[^:]+:){3}([^:]+).*";
				final String FORMAT = "cloud_$1_logdna";
				final String EXPECTED_RESULT = "cloud_databases-for-mongodb_logdna";

				props.put(Splunk.SOURCE_KEY_CONFIG, FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);

				this.transformation = new Splunk<>();
				this.transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				mapValue.put(FIELD_NAME, FIELD_VALUE);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = this.transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertTrue(resultMap.containsKey(FIELD_NAME));
				assertEquals(EXPECTED_RESULT, String.valueOf(resultMap.get(FIELD_NAME)));
			}

			@Test
			@DisplayName("Should apply regex & format to the value of sourceKey field and put that field among headers")
			public void message_returnRegexFormatAsMetadata() {
				Map<String, Object> props = new HashMap<>();
				final String FIELD_NAME = "aplyRegexAndFormat";
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String REGEX = "crn:(?:[^:]+:){3}([^:]+).*";
				final String FORMAT = "cloud_$1_logdna";
				final Boolean iS_METADATA = true;
				final String EXPECTED_RESULT = "cloud_databases-for-mongodb_logdna";

				props.put(Splunk.SOURCE_KEY_CONFIG, FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.IS_METADATA_KEY_CONFIG, iS_METADATA);

				this.transformation = new Splunk<>();
				this.transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				mapValue.put(FIELD_NAME, FIELD_VALUE);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = this.transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertFalse(resultMap.containsKey(FIELD_NAME));

				Iterator<Header> headerIterator = result.headers().allWithName(FIELD_NAME);
				assertTrue(headerIterator.hasNext());
				assertEquals(EXPECTED_RESULT, headerIterator.next().value());
			}

			@Test
			@DisplayName("Should rename and apply regex & format to the value of sourceKey field and put that field among headers")
			public void message_returnRegexFormatAsMetadataAndRename() {
				Map<String, Object> props = new HashMap<>();
				final String FIELD_NAME = "aplyRegexAndFormat";
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String NEW_FIELD_NAME = "newFieldName";
				final String REGEX = "crn:(?:[^:]+:){3}([^:]+).*";
				final String FORMAT = "cloud_$1_logdna";
				final Boolean iS_METADATA = true;
				final String EXPECTED_RESULT = "cloud_databases-for-mongodb_logdna";

				props.put(Splunk.SOURCE_KEY_CONFIG, FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.IS_METADATA_KEY_CONFIG, iS_METADATA);
				props.put(Splunk.DESTINATION_KEY_CONFIG, NEW_FIELD_NAME);

				this.transformation = new Splunk<>();
				this.transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				mapValue.put(FIELD_NAME, FIELD_VALUE);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = this.transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertFalse(resultMap.containsKey(FIELD_NAME));
				assertFalse(resultMap.containsKey(NEW_FIELD_NAME));

				Iterator<Header> headerIterator = result.headers().allWithName(NEW_FIELD_NAME);
				assertTrue(headerIterator.hasNext());
				assertEquals(EXPECTED_RESULT, headerIterator.next().value());
			}

			@Test
			@DisplayName("Should return original value if regex does not match and no default value is specified")
			public void message_returnOriginalNoRegexFormat() {
				Map<String, Object> props = new HashMap<>();
				final String FIELD_NAME = "aplyRegexAndFormat";
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String REGEX = "noregex";
				final String FORMAT = "cloud_$1_logdna";
				final String EXPECTED_RESULT = FIELD_VALUE;

				props.put(Splunk.SOURCE_KEY_CONFIG, FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);

				this.transformation = new Splunk<>();
				this.transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				mapValue.put(FIELD_NAME, FIELD_VALUE);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = this.transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertTrue(resultMap.containsKey(FIELD_NAME));
				assertEquals(EXPECTED_RESULT, String.valueOf(resultMap.get(FIELD_NAME)));
			}

			@Test
			@DisplayName("Should return default value if regex does not match and a default value is specified")
			public void message_returnDefaultValueNoPatternMatch() {
				Map<String, Object> props = new HashMap<>();
				final String FIELD_NAME = "aplyRegexAndFormat";
				final String FIELD_VALUE = "/var/log/kublet.log";
				final String REGEX = "noregex";
				final String FORMAT = "cloud_$1_logdna";
				final String DEFAULT_VALUE = "my default value";
				final String EXPECTED_RESULT = DEFAULT_VALUE;

				props.put(Splunk.SOURCE_KEY_CONFIG, FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.REGEX_DEFAULT_VALUE_CONFIG, DEFAULT_VALUE);

				this.transformation = new Splunk<>();
				this.transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				mapValue.put(FIELD_NAME, FIELD_VALUE);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = this.transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertTrue(resultMap.containsKey(FIELD_NAME));
				assertEquals(EXPECTED_RESULT, String.valueOf(resultMap.get(FIELD_NAME)));
			}

			@Test
			@DisplayName("Should return original record unchanged if regex does not match and a default value is not specified")
			public void message_returnOrigRecordNoPatternMatch() {
				Map<String, Object> props = new HashMap<>();
				final String FIELD_NAME = "aplyRegexAndFormat";
				final String FIELD_VALUE = "/var/log/kublet.log";
				final Boolean IS_METADATA = true;
				final String REGEX = "noregex";
				final String FORMAT = "cloud_$1_logdna";

				props.put(Splunk.SOURCE_KEY_CONFIG, FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.IS_METADATA_KEY_CONFIG, IS_METADATA);

				this.transformation = new Splunk<>();
				this.transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				mapValue.put(FIELD_NAME, FIELD_VALUE);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = this.transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertTrue(resultMap.containsKey(FIELD_NAME));
				assertEquals(FIELD_VALUE, resultMap.get(FIELD_NAME));

				Iterator<Header> headerIterator = result.headers().allWithName(FIELD_NAME);
				assertFalse(headerIterator.hasNext());
			}
		}
	}

	@Nested
	@DisplayName("Metadata")
	class Metadata {

		private Transformation<SinkRecord> transformation;

		@Test
		@DisplayName("Should move field from body to header if there is no destKey, but isMetadata is true")
		public void message_returnHeaderField() {
			Map<String, Object> props = new HashMap<>();
			final String FIELD_NAME = "newFieldName";
			final String FIELD_VALUE = "test value";
			final Boolean IS_METADATA_VALUE = Boolean.TRUE;

			props.put(Splunk.SOURCE_KEY_CONFIG, FIELD_NAME);
			props.put(Splunk.IS_METADATA_KEY_CONFIG, IS_METADATA_VALUE);

			this.transformation = new Splunk<>();
			this.transformation.configure(props);

			Map<String, Object> mapValue = new HashMap<>();
			mapValue.put(FIELD_NAME, FIELD_VALUE);

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = this.transformation.apply(record);

			Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			assertFalse(resultMap.containsKey(FIELD_NAME));

			Headers headers = result.headers();
			assertTrue(headers.allWithName(FIELD_NAME).hasNext());
		}

		@Test
		@DisplayName("Should move and rename field from body to header, if destKey is present and isMetadata is true")
		public void message_returnRenamedHeaderField() {
			Map<String, Object> props = new HashMap<>();
			final String FIELD_NAME = "oldFiledName";
			final String NEW_FIELD_NAME = "newFieldName";
			final String FIELD_VALUE = "test value";
			final Boolean IS_METADATA_VALUE = Boolean.TRUE;

			props.put(Splunk.SOURCE_KEY_CONFIG, FIELD_NAME);
			props.put(Splunk.DESTINATION_KEY_CONFIG, NEW_FIELD_NAME);
			props.put(Splunk.IS_METADATA_KEY_CONFIG, IS_METADATA_VALUE);

			this.transformation = new Splunk<>();
			this.transformation.configure(props);

			Map<String, Object> mapValue = new HashMap<>();
			mapValue.put(FIELD_NAME, FIELD_VALUE);

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = this.transformation.apply(record);

			Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			assertFalse(resultMap.containsKey(FIELD_NAME));

			Headers headers = result.headers();
			assertTrue(headers.allWithName(NEW_FIELD_NAME).hasNext());
		}
	}

	private SinkRecord newRecord(Map<String, Object> value) {
		return new SinkRecord("topic", 1, null, null, null, value, 1L);
	}
}