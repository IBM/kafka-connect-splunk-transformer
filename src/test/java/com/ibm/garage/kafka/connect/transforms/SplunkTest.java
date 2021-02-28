/*
 Copyright 2021 IBM Inc. All rights reserved
 SPDX-License-Identifier: Apache2.0
*/

package com.ibm.garage.kafka.connect.transforms;

import static com.ibm.garage.kafka.connect.transforms.SplunkTestHelper.DEST_FIELD_NAME;
import static com.ibm.garage.kafka.connect.transforms.SplunkTestHelper.DEST_TO_HEADER_TRUE;
import static com.ibm.garage.kafka.connect.transforms.SplunkTestHelper.NESTED_SOURCE_FIELD_NAME;
import static com.ibm.garage.kafka.connect.transforms.SplunkTestHelper.SOURCE_FIELD_NAME;
import static com.ibm.garage.kafka.connect.transforms.SplunkTestHelper.SOURCE_FIELD_PARENT_OBJECT;
import static com.ibm.garage.kafka.connect.transforms.SplunkTestHelper.SOURCE_FIELD_VALUE;
import static com.ibm.garage.kafka.connect.transforms.SplunkTestHelper.SOURCE_PRESERVE_TRUE;
import static com.ibm.garage.kafka.connect.transforms.SplunkTestHelper.TEST_PURPOSE;
import static com.ibm.garage.kafka.connect.transforms.SplunkTestHelper.applyTransformation;
import static com.ibm.garage.kafka.connect.transforms.SplunkTestHelper.createNestedValueMap;
import static com.ibm.garage.kafka.connect.transforms.SplunkTestHelper.createValueMap;
import static com.ibm.garage.kafka.connect.transforms.SplunkTestHelper.getNestedValueMap;
import static com.ibm.garage.kafka.connect.transforms.SplunkTestHelper.processTransformation;
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
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class SplunkTest {

	private Transformation<SinkRecord> transformation;

	@Nested
	@DisplayName("SplunkTest - Configuration")
	class Configuration {

		@Test
		@DisplayName("Should throw an exception if source.key configuration is null")
		public void configuration_throwsRuntimeException_sourceKey_Null() {
			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, null);
			props.put(Splunk.DEST_KEY_CONFIG, DEST_FIELD_NAME);

			this.shouldThrow(props);
		}

		@Test
		@DisplayName("Should throw an exception if source.key configuration is empty")
		public void configuration_throwsRuntimeException_sourceKey_Empty() {
			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, "");
			props.put(Splunk.DEST_KEY_CONFIG, DEST_FIELD_NAME);

			this.shouldThrow(props);
		}

		@Test
		@DisplayName("Should throw an exception if regex pattern is specified but regex format is not")
		public void configuration_throwsRuntimeException_only_regex_pattern_specified() {
			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
			props.put(Splunk.REGEX_PATTERN_CONFIG, "notnull");

			this.shouldThrow(props);
		}

		@Test
		@DisplayName("Should throw an exception if regex format is specified but regex pattern is not")
		public void configuration_throwsRuntimeException_only_regex_format_specified() {
			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
			props.put(Splunk.REGEX_FORMAT_CONFIG, "notnull");

			this.shouldThrow(props);
		}

		@Test
		@DisplayName("Should throw an exception if regex default value is specified but regex pattern is not")
		public void configuration_throwsRuntimeException_default_value_and_format_specified() {
			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
			props.put(Splunk.REGEX_FORMAT_CONFIG, "myformat");
			props.put(Splunk.REGEX_DEFAULT_VALUE_CONFIG, "my default Value");

			this.shouldThrow(props);
		}

		@Test
		@DisplayName("Should throw an exception if source.preserve is true but dest.key is not specified")
		public void configuration_throwsRuntimeException_sourcePreserve_true_destKey_not_specified() {
			Map<String, Object> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
			props.put(Splunk.SOURCE_PRESERVE_CONFIG, SOURCE_PRESERVE_TRUE);

			this.shouldThrow(props);
		}

		@Test
		@DisplayName("Should throw an exception if the regex.pattern specified is not valid")
		public void configuration_throwsRuntimeException_regexPattern_badFormat() {
			Map<String, Object> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
			props.put(Splunk.DEST_TO_HEADER_CONFIG, Boolean.FALSE);
			props.put(Splunk.REGEX_PATTERN_CONFIG, "^(.*$");

			this.shouldThrow(props);
		}

		@Test
		@DisplayName("Should throw an exception if the source.key and dest.key point to the same field")
		public void configuration_throwsRuntimeException_sourceKey_destKey_are_the_same() {
			Map<String, Object> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
			props.put(Splunk.DEST_KEY_CONFIG, SOURCE_FIELD_NAME);

			this.shouldThrow(props);
		}

		private void shouldThrow(Map<String, ?> props) {
			transformation = new Splunk<>();

			assertThrows(RuntimeException.class, () -> {
				transformation.configure(props);
			});

		}
	}

	@Nested
	@DisplayName("SplunkTest - Messages")
	class Messages {

		@Test
		@DisplayName("Should return null if a message is null")
		public void message_returnOriginalNullMessage() {
			transformation = new Splunk<>();

			Map<String, Object> valueMap = null;

			Map<String, Object> resultValueMap = processTransformation(transformation, valueMap);

			assertNull(resultValueMap);
		}

		@Test
		@DisplayName("Should return original message if a message is empty")
		public void message_returnOriginalEmptyMessage() {
			transformation = new Splunk<>();

			Map<String, Object> valueMap = new HashMap<>();

			Map<String, Object> resultValueMap = processTransformation(transformation, valueMap);

			assertNotNull(resultValueMap);
			assertTrue(resultValueMap.isEmpty());
		}

		@Test
		@DisplayName("Should return original message if the source.key field is the only configuration specified")
		public void message_returnOrigMessage() {
			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> valueMap = createValueMap();
			Map<String, Object> valueMapCopy = createValueMap();

			Map<String, Object> resultValueMap = processTransformation(transformation, valueMapCopy);

			assertEquals(valueMap, resultValueMap);
		}

		@Test
		@DisplayName("Should return original message if the nested source.key field is the only configuration specified")
		public void message_returnOrigMessage_nestedSourceKey() {
			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_SOURCE_FIELD_NAME);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> valueMap = createNestedValueMap();
			Map<String, Object> valueMapCopy = createNestedValueMap();

			Map<String, Object> resultValueMap = processTransformation(transformation, valueMapCopy);

			assertEquals(valueMap, resultValueMap);
		}

		@Test
		@DisplayName("Should rename a source.key to dest.key field in a message if the source.key is present")
		public void message_returnRenamedField() {
			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
			props.put(Splunk.DEST_KEY_CONFIG, DEST_FIELD_NAME);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> valueMap = createValueMap();

			Map<String, Object> resultValueMap = processTransformation(transformation, valueMap);

			assertTrue(resultValueMap.containsKey(DEST_FIELD_NAME));
			assertFalse(resultValueMap.containsKey(SOURCE_FIELD_NAME));
			assertEquals(SOURCE_FIELD_VALUE, resultValueMap.get(DEST_FIELD_NAME));
		}

		@Test
		@DisplayName("Should rename a nested source.key to dest.key field (not nested!) in a message if the nested source.key is present")
		public void message_returnRenamedField_nestedSourceKey() {
			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_SOURCE_FIELD_NAME);
			props.put(Splunk.DEST_KEY_CONFIG, DEST_FIELD_NAME);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> valueMap = createNestedValueMap();

			Map<String, Object> resultValueMap = processTransformation(transformation, valueMap);
			Map<String, Object> nestedResultValueMap = getNestedValueMap(resultValueMap);

			assertFalse(nestedResultValueMap.containsKey(SOURCE_FIELD_NAME));
			assertTrue(resultValueMap.containsKey(DEST_FIELD_NAME));
			assertEquals(SOURCE_FIELD_VALUE, resultValueMap.get(DEST_FIELD_NAME));
		}

		@Test
		@DisplayName("Should return unchanged message if the source.key field does not exist")
		public void message_returnUnchangedMessage() {
			final String THIS_FIELD_EXIST = "thisOneExists";

			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
			props.put(Splunk.DEST_KEY_CONFIG, DEST_FIELD_NAME);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> valueMap = createValueMap(THIS_FIELD_EXIST, SOURCE_FIELD_VALUE);
			Map<String, Object> valueMapCopy = createValueMap(THIS_FIELD_EXIST, SOURCE_FIELD_VALUE);

			Map<String, Object> resultValueMap = processTransformation(transformation, valueMapCopy);

			assertEquals(valueMap, resultValueMap);
		}

		@Test
		@DisplayName("Should return unchanged message if the nested source.key field does not exist")
		public void message_returnUnchangedMessage_nestedSourceKey() {
			final String EXPECTED_NESTED_FIELD_NAME = "nested.my." + SOURCE_FIELD_NAME;

			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, EXPECTED_NESTED_FIELD_NAME);
			props.put(Splunk.DEST_KEY_CONFIG, DEST_FIELD_NAME);

			transformation = new Splunk<>();
			transformation.configure(props);

			// this exists: "my.nested.field"
			Map<String, Object> valueMap = createValueMap("my", createNestedValueMap());
			Map<String, Object> valueMapCopy = createValueMap("my", createNestedValueMap());

			Map<String, Object> resultValueMap = processTransformation(transformation, valueMapCopy);

			assertEquals(valueMap, resultValueMap);
		}

		@Test
		@DisplayName("Should return unchanged message if the source.key points to the object (i.e. a Map)")
		public void message_returnUnchangedMessage_pointingToMap() {
			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_PARENT_OBJECT);
			props.put(Splunk.DEST_KEY_CONFIG, DEST_FIELD_NAME);

			transformation = new Splunk<>();
			transformation.configure(props);

			// this exists: "nested.field"
			Map<String, Object> valueMap = createNestedValueMap();
			Map<String, Object> valueMapCopy = createNestedValueMap();

			Map<String, Object> resultValueMap = processTransformation(transformation, valueMapCopy);

			assertEquals(valueMap, resultValueMap);
		}

		@Test
		@DisplayName("Should return unchanged message if the nested source.key points to the object (i.e. a Map)")
		public void message_returnUnchangedMessage_nestedSourceKey_pointingToMap() {
			final String EXPECTED_NESTED_FIELD_NAME = "my." + SOURCE_FIELD_PARENT_OBJECT;

			Map<String, String> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, EXPECTED_NESTED_FIELD_NAME);
			props.put(Splunk.DEST_KEY_CONFIG, DEST_FIELD_NAME);

			transformation = new Splunk<>();
			transformation.configure(props);

			// this exists: "my.nested.field"
			Map<String, Object> valueMap = createValueMap("my", createNestedValueMap());
			Map<String, Object> valueMapCopy = createValueMap("my", createNestedValueMap());

			Map<String, Object> resultValueMap = processTransformation(transformation, valueMapCopy);

			assertEquals(valueMap, resultValueMap);
		}

		@Test
		@DisplayName("Should preserve a source.key and it's original value in the body if source.preserve is set to true and dest.key is specified")
		public void message_preserveField() {
			Map<String, Object> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
			props.put(Splunk.DEST_KEY_CONFIG, DEST_FIELD_NAME);
			props.put(Splunk.SOURCE_PRESERVE_CONFIG, SOURCE_PRESERVE_TRUE);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> valueMap = createValueMap();

			Map<String, Object> resultValueMap = processTransformation(transformation, valueMap);

			assertTrue(resultValueMap.containsKey(SOURCE_FIELD_NAME));
			assertEquals(SOURCE_FIELD_VALUE, resultValueMap.get(SOURCE_FIELD_NAME));
			assertTrue(resultValueMap.containsKey(DEST_FIELD_NAME));
			assertEquals(SOURCE_FIELD_VALUE, resultValueMap.get(DEST_FIELD_NAME));
		}

		@Test
		@DisplayName("Should preserve a nested source.key and it's original value in the body if source.preserve is set to true and dest.key is specified")
		public void message_preserveField_nestedSourceKey() {
			Map<String, Object> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_SOURCE_FIELD_NAME);
			props.put(Splunk.DEST_KEY_CONFIG, DEST_FIELD_NAME);
			props.put(Splunk.SOURCE_PRESERVE_CONFIG, SOURCE_PRESERVE_TRUE);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> valueMap = createNestedValueMap();

			Map<String, Object> resultValueMap = processTransformation(transformation, valueMap);
			Map<String, Object> nestedResultMap = getNestedValueMap(resultValueMap);

			assertTrue(nestedResultMap.containsKey(SOURCE_FIELD_NAME));
			assertEquals(SOURCE_FIELD_VALUE, nestedResultMap.get(SOURCE_FIELD_NAME));
			assertTrue(resultValueMap.containsKey(DEST_FIELD_NAME));
			assertEquals(SOURCE_FIELD_VALUE, resultValueMap.get(DEST_FIELD_NAME));

		}

		@Nested
		@DisplayName("SplunkTest - Regex & Format")
		class RegexFormat {

			@Test
			@DisplayName("Should apply regex & format to the value of source.key field")
			public void message_returnRegexFormat() {
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String REGEX = "crn:(?:[^:]+:){3}([^:]+).*";
				final String FORMAT = "cloud_$1_logdna";
				final String EXPECTED_RESULT = "cloud_databases-for-mongodb_logdna";

				Map<String, Object> props = new HashMap<>();
				props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> valueMap = createValueMap(SOURCE_FIELD_NAME, FIELD_VALUE);

				SinkRecord result = applyTransformation(transformation, valueMap);

				Map<String, Object> resultValueMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertTrue(resultValueMap.containsKey(SOURCE_FIELD_NAME));
				assertEquals(EXPECTED_RESULT, String.valueOf(resultValueMap.get(SOURCE_FIELD_NAME)));
			}

			@Test
			@DisplayName("Should apply regex & format to the value of nested source.key field")
			public void message_returnRegexFormat_nestedSourceKey() {
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String REGEX = "crn:(?:[^:]+:){3}([^:]+).*";
				final String FORMAT = "cloud_$1_logdna";
				final String EXPECTED_RESULT = "cloud_databases-for-mongodb_logdna";

				Map<String, Object> props = new HashMap<>();
				props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_SOURCE_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> valueMap = createNestedValueMap(SOURCE_FIELD_NAME, FIELD_VALUE);

				Map<String, Object> resultValueMap = processTransformation(transformation, valueMap);
				Map<String, Object> nestedResultMap = getNestedValueMap(resultValueMap);

				assertTrue(nestedResultMap.containsKey(SOURCE_FIELD_NAME));
				assertEquals(EXPECTED_RESULT, String.valueOf(nestedResultMap.get(SOURCE_FIELD_NAME)));
			}

			@Test
			@DisplayName("Should apply regex & format to the value of source.key field and put that field among headers")
			public void message_returnRegexFormat_toHeader() {
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String REGEX = "crn:(?:[^:]+:){3}([^:]+).*";
				final String FORMAT = "cloud_$1_logdna";
				final String EXPECTED_RESULT = "cloud_databases-for-mongodb_logdna";

				Map<String, Object> props = new HashMap<>();
				props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.DEST_TO_HEADER_CONFIG, DEST_TO_HEADER_TRUE);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> valueMap = createValueMap(SOURCE_FIELD_NAME, FIELD_VALUE);

				SinkRecord result = applyTransformation(transformation, valueMap);

				Map<String, Object> resultValueMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertFalse(resultValueMap.containsKey(SOURCE_FIELD_NAME));

				Iterator<Header> headerIterator = result.headers().allWithName(SOURCE_FIELD_NAME);
				assertTrue(headerIterator.hasNext());
				assertEquals(EXPECTED_RESULT, headerIterator.next().value());
			}

			@Test
			@DisplayName("Should apply regex & format to the value of nested source.key field and put that field among headers")
			public void message_returnRegexFormat_toHeader_nestedSourceKey() {
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String REGEX = "crn:(?:[^:]+:){3}([^:]+).*";
				final String FORMAT = "cloud_$1_logdna";
				final String EXPECTED_RESULT = "cloud_databases-for-mongodb_logdna";

				Map<String, Object> props = new HashMap<>();
				props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_SOURCE_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.DEST_TO_HEADER_CONFIG, DEST_TO_HEADER_TRUE);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> valueMap = createNestedValueMap(SOURCE_FIELD_NAME, FIELD_VALUE);

				SinkRecord result = applyTransformation(transformation, valueMap);

				Map<String, Object> resultValueMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				Map<String, Object> nestedResultMap = getNestedValueMap(resultValueMap);
				assertFalse(nestedResultMap.containsKey(SOURCE_FIELD_NAME));

				Iterator<Header> headerIterator = result.headers().allWithName(SOURCE_FIELD_NAME);
				assertTrue(headerIterator.hasNext());
				assertEquals(EXPECTED_RESULT, headerIterator.next().value());
			}

			@Test
			@DisplayName("Should rename and apply regex & format to the value of source.key field and put that field among headers")
			public void message_returnRegexFormatAs_toHeader_Rename() {
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String REGEX = "crn:(?:[^:]+:){3}([^:]+).*";
				final String FORMAT = "cloud_$1_logdna";
				final String EXPECTED_RESULT = "cloud_databases-for-mongodb_logdna";

				Map<String, Object> props = new HashMap<>();
				props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.DEST_TO_HEADER_CONFIG, DEST_TO_HEADER_TRUE);
				props.put(Splunk.DEST_KEY_CONFIG, DEST_FIELD_NAME);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> valueMap = createValueMap(SOURCE_FIELD_NAME, FIELD_VALUE);

				SinkRecord result = applyTransformation(transformation, valueMap);

				Map<String, Object> resultValueMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertFalse(resultValueMap.containsKey(SOURCE_FIELD_NAME));
				assertFalse(resultValueMap.containsKey(DEST_FIELD_NAME));

				Iterator<Header> headerIterator = result.headers().allWithName(DEST_FIELD_NAME);
				assertTrue(headerIterator.hasNext());
				assertEquals(EXPECTED_RESULT, headerIterator.next().value());
			}

			@Test
			@DisplayName("Should rename and apply regex & format to the value of nested source.key field and put that field among headers")
			public void message_returnRegexFormat_toHeader_Rename_nestedSourceKey() {
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String REGEX = "crn:(?:[^:]+:){3}([^:]+).*";
				final String FORMAT = "cloud_$1_logdna";
				final String EXPECTED_RESULT = "cloud_databases-for-mongodb_logdna";

				Map<String, Object> props = new HashMap<>();
				props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_SOURCE_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.DEST_TO_HEADER_CONFIG, DEST_TO_HEADER_TRUE);
				props.put(Splunk.DEST_KEY_CONFIG, DEST_FIELD_NAME);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> valueMap = createNestedValueMap(SOURCE_FIELD_NAME, FIELD_VALUE);

				SinkRecord result = applyTransformation(transformation, valueMap);

				Map<String, Object> resultValueMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				Map<String, Object> nestedResultMap = getNestedValueMap(resultValueMap);
				assertFalse(nestedResultMap.containsKey(SOURCE_FIELD_NAME));
				assertFalse(nestedResultMap.containsKey(DEST_FIELD_NAME));
				assertFalse(resultValueMap.containsKey(DEST_FIELD_NAME));

				Iterator<Header> headerIterator = result.headers().allWithName(DEST_FIELD_NAME);
				assertTrue(headerIterator.hasNext());
				assertEquals(EXPECTED_RESULT, headerIterator.next().value());
			}

			@Test
			@DisplayName("Should return original value if regex does not match and no default value is specified")
			public void message_returnOriginalNoRegexFormat() {
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String REGEX = "noregex";
				final String FORMAT = "cloud_$1_logdna";
				final String EXPECTED_RESULT = FIELD_VALUE;

				Map<String, Object> props = new HashMap<>();
				props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> valueMap = createValueMap(SOURCE_FIELD_NAME, FIELD_VALUE);

				Map<String, Object> resultValueMap = processTransformation(transformation, valueMap);

				assertTrue(resultValueMap.containsKey(SOURCE_FIELD_NAME));
				assertEquals(EXPECTED_RESULT, String.valueOf(resultValueMap.get(SOURCE_FIELD_NAME)));
			}

			@Test
			@DisplayName("Should return original value if regex does not match and no default value is specified (nested source.key)")
			public void message_returnOriginalNoRegexFormat_nestedSourceKey() {
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String REGEX = "noregex";
				final String FORMAT = "cloud_$1_logdna";
				final String EXPECTED_RESULT = FIELD_VALUE;

				Map<String, Object> props = new HashMap<>();
				props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_SOURCE_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> valueMap = createNestedValueMap(SOURCE_FIELD_NAME, FIELD_VALUE);

				Map<String, Object> resultValueMap = processTransformation(transformation, valueMap);
				Map<String, Object> nestedResultMap = getNestedValueMap(resultValueMap);

				assertTrue(nestedResultMap.containsKey(SOURCE_FIELD_NAME));
				assertEquals(EXPECTED_RESULT, String.valueOf(nestedResultMap.get(SOURCE_FIELD_NAME)));
			}

			@Test
			@DisplayName("Should return default value if regex does not match and a default value is specified")
			public void message_returnDefaultValueNoPatternMatch() {
				final String FIELD_VALUE = "/var/log/kublet.log";
				final String REGEX = "noregex";
				final String FORMAT = "cloud_$1_logdna";
				final String DEFAULT_VALUE = "my default value";
				final String EXPECTED_RESULT = DEFAULT_VALUE;

				Map<String, Object> props = new HashMap<>();
				props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.REGEX_DEFAULT_VALUE_CONFIG, DEFAULT_VALUE);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> valueMap = createValueMap(SOURCE_FIELD_NAME, FIELD_VALUE);

				Map<String, Object> resultValueMap = processTransformation(transformation, valueMap);

				assertTrue(resultValueMap.containsKey(SOURCE_FIELD_NAME));
				assertEquals(EXPECTED_RESULT, String.valueOf(resultValueMap.get(SOURCE_FIELD_NAME)));
			}

			@Test
			@DisplayName("Should return default value if regex does not match and a default value is specified (nested source.key)")
			public void message_returnDefaultValueNoPatternMatch_nestedSourceKey() {
				final String FIELD_VALUE = "/var/log/kublet.log";
				final String REGEX = "noregex";
				final String FORMAT = "cloud_$1_logdna";
				final String DEFAULT_VALUE = "my default value";
				final String EXPECTED_RESULT = DEFAULT_VALUE;

				Map<String, Object> props = new HashMap<>();
				props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_SOURCE_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.REGEX_DEFAULT_VALUE_CONFIG, DEFAULT_VALUE);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> valueMap = createNestedValueMap(SOURCE_FIELD_NAME, FIELD_VALUE);

				Map<String, Object> resultValueMap = processTransformation(transformation, valueMap);
				Map<String, Object> nestedResultMap = getNestedValueMap(resultValueMap);

				assertTrue(nestedResultMap.containsKey(SOURCE_FIELD_NAME));
				assertEquals(EXPECTED_RESULT, String.valueOf(nestedResultMap.get(SOURCE_FIELD_NAME)));
			}

			@Test
			@DisplayName("Should return original record unchanged if regex does not match and a default value is not specified")
			public void message_returnOrigRecordNoPatternMatch() {
				final String FIELD_VALUE = "/var/log/kublet.log";
				final String REGEX = "noregex";
				final String FORMAT = "cloud_$1_logdna";

				Map<String, Object> props = new HashMap<>();
				props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.DEST_TO_HEADER_CONFIG, DEST_TO_HEADER_TRUE);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> valueMap = createValueMap(SOURCE_FIELD_NAME, FIELD_VALUE);

				SinkRecord result = applyTransformation(transformation, valueMap);

				Map<String, Object> resultValueMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertTrue(resultValueMap.containsKey(SOURCE_FIELD_NAME));
				assertEquals(FIELD_VALUE, resultValueMap.get(SOURCE_FIELD_NAME));

				Iterator<Header> headerIterator = result.headers().allWithName(SOURCE_FIELD_NAME);
				assertFalse(headerIterator.hasNext());
			}

			@Test
			@DisplayName("Should return original record unchanged if regex does not match and a default value is not specified (nested source.key)")
			public void message_returnOrigRecordNoPatternMatch_nestedSourceKey() {
				final String FIELD_VALUE = "/var/log/kublet.log";
				final String REGEX = "noregex";
				final String FORMAT = "cloud_$1_logdna";

				Map<String, Object> props = new HashMap<>();
				props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_SOURCE_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.DEST_TO_HEADER_CONFIG, DEST_TO_HEADER_TRUE);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> valueMap = createNestedValueMap(SOURCE_FIELD_NAME, FIELD_VALUE);

				SinkRecord result = applyTransformation(transformation, valueMap);

				Map<String, Object> resultValueMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				Map<String, Object> nestedResultMap = getNestedValueMap(resultValueMap);

				assertTrue(nestedResultMap.containsKey(SOURCE_FIELD_NAME));
				assertEquals(FIELD_VALUE, nestedResultMap.get(SOURCE_FIELD_NAME));

				Iterator<Header> headerIterator = result.headers().allWithName(SOURCE_FIELD_NAME);
				assertFalse(headerIterator.hasNext());
			}
		}
	}

	@Nested
	@DisplayName("SplunkTest - toHeader")
	class ToHeader {

		@Test
		@DisplayName("Should move source.key field from body to header if there is no dest.key, but dest.toHeader is true")
		public void message_returnHeaderField() {
			Map<String, Object> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
			props.put(Splunk.DEST_TO_HEADER_CONFIG, DEST_TO_HEADER_TRUE);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> valueMap = createValueMap();

			SinkRecord result = applyTransformation(transformation, valueMap);

			Map<String, Object> resultValueMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			assertFalse(resultValueMap.containsKey(SOURCE_FIELD_NAME));

			Iterator<Header> headerIterator = result.headers().allWithName(SOURCE_FIELD_NAME);
			assertTrue(headerIterator.hasNext());
			assertEquals(SOURCE_FIELD_VALUE, headerIterator.next().value());
		}

		@Test
		@DisplayName("Should move nested source.key field from body to header if there is no dest.key, but dest.toHeader is true")
		public void message_returnHeaderField_nestedSourceKey() {
			Map<String, Object> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_SOURCE_FIELD_NAME);
			props.put(Splunk.DEST_TO_HEADER_CONFIG, DEST_TO_HEADER_TRUE);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> valueMap = createNestedValueMap();

			SinkRecord result = applyTransformation(transformation, valueMap);

			Map<String, Object> resultValueMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			Map<String, Object> nestedResultMap = getNestedValueMap(resultValueMap);
			assertFalse(nestedResultMap.containsKey(SOURCE_FIELD_NAME));

			Iterator<Header> headerIterator = result.headers().allWithName(SOURCE_FIELD_NAME);
			assertTrue(headerIterator.hasNext());
			assertEquals(SOURCE_FIELD_VALUE, headerIterator.next().value());
		}

		@Test
		@DisplayName("Should move and rename source.key field from body to header, if dest.key is present and dest.toHeader is true")
		public void message_returnRenamedHeaderField() {
			Map<String, Object> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
			props.put(Splunk.DEST_KEY_CONFIG, DEST_FIELD_NAME);
			props.put(Splunk.DEST_TO_HEADER_CONFIG, DEST_TO_HEADER_TRUE);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> valueMap = createValueMap();

			SinkRecord result = applyTransformation(transformation, valueMap);

			Map<String, Object> resultValueMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			assertFalse(resultValueMap.containsKey(SOURCE_FIELD_NAME));

			Iterator<Header> headerIterator = result.headers().allWithName(DEST_FIELD_NAME);
			assertTrue(headerIterator.hasNext());
			assertEquals(SOURCE_FIELD_VALUE, headerIterator.next().value());
		}

		@Test
		@DisplayName("Should move and rename nested source.key field from body to header, if dest.key is present and dest.toHeader is true")
		public void message_returnRenamedHeaderField_nestedSourceKey() {
			Map<String, Object> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_SOURCE_FIELD_NAME);
			props.put(Splunk.DEST_KEY_CONFIG, DEST_FIELD_NAME);
			props.put(Splunk.DEST_TO_HEADER_CONFIG, DEST_TO_HEADER_TRUE);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> valueMap = createNestedValueMap();

			SinkRecord result = applyTransformation(transformation, valueMap);

			Map<String, Object> resultValueMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			Map<String, Object> nestedResultMap = getNestedValueMap(resultValueMap);
			assertFalse(nestedResultMap.containsKey(SOURCE_FIELD_NAME));

			Iterator<Header> headerIterator = result.headers().allWithName(DEST_FIELD_NAME);
			assertTrue(headerIterator.hasNext());
			assertEquals(SOURCE_FIELD_VALUE, headerIterator.next().value());
		}
	}
}