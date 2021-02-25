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
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class SplunkTest {

	private static final String TEST_PURPOSE = "test purpose";
	private Transformation<SinkRecord> transformation;

	@Nested
	@DisplayName("SplunkTest - Configuration")
	class Configuration {

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
		@DisplayName("Should throw an exception if isPreserveInBody is true but destKey is not specified")
		public void configuration_throwsRuntimeException_isPreserveInBody_specified() {
			Map<String, Object> props = new HashMap<>();
			props.put(Splunk.SOURCE_KEY_CONFIG, "sourceKey");
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

			Map<String, Object> mapValue = null;

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = transformation.apply(record);

			Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			assertNull(resultMap);
		}

		@Test
		@DisplayName("Should return original message if a message is empty")
		public void message_returnOriginalEmptyMessage() {
			transformation = new Splunk<>();

			Map<String, Object> mapValue = new HashMap<>();

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = transformation.apply(record);

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

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> originalMapValue = new HashMap<>();
			originalMapValue.put(FIELD_NAME, FIELD_VALUE);

			Map<String, Object> originalMapValueCopy = new HashMap<>();
			originalMapValueCopy.put(FIELD_NAME, FIELD_VALUE);

			final SinkRecord originalRecord = newRecord(originalMapValue);
			SinkRecord resultRecord = transformation.apply(originalRecord);

			Map<String, Object> resultMapValue = requireMapOrNull(resultRecord.value(), TEST_PURPOSE);
			assertEquals(originalMapValueCopy, resultMapValue);
		}
		
		@Test
		@DisplayName("Should return original message if the nested sourceKey field is the only configuration specified")
		public void message_returnOrigMessage_nestedSourceKey() {
			Map<String, String> props = new HashMap<>();
			final String FIELD_NAME = "field";
			final String NESTED_FIELD_NAME = "nested." + FIELD_NAME;
			final String FIELD_VALUE = "test value";

			props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_FIELD_NAME);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> originalMapValue = new HashMap<>();
			Map<String, Object> nestedMapValue = new HashMap<>();
			nestedMapValue.put(FIELD_NAME, FIELD_VALUE);
			originalMapValue.put("nested", nestedMapValue);

			Map<String, Object> originalMapValueCopy = new HashMap<>();
			Map<String, Object> nestedMapValueCopy = new HashMap<>();
			nestedMapValueCopy.put(FIELD_NAME, FIELD_VALUE);
			originalMapValueCopy.put("nested", nestedMapValueCopy);
			
			final SinkRecord originalRecord = newRecord(originalMapValue);
			SinkRecord resultRecord = transformation.apply(originalRecord);

			Map<String, Object> resultMapValue = requireMapOrNull(resultRecord.value(), TEST_PURPOSE);
			assertEquals(originalMapValueCopy, resultMapValue);
		}

		@Test
		@DisplayName("Should rename a sourceKey to destKey field in a message if the sourceKey is present")
		public void message_returnRenamedField() {
			Map<String, String> props = new HashMap<>();
			final String OLD_FIELD_NAME = "oldFieldName";
			final String NEW_FIELD_NAME = "newFieldName";
			final String FIELD_VALUE = "test value";

			props.put(Splunk.SOURCE_KEY_CONFIG, OLD_FIELD_NAME);
			props.put(Splunk.DESTINATION_KEY_CONFIG, NEW_FIELD_NAME);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> mapValue = new HashMap<>();
			mapValue.put(OLD_FIELD_NAME, FIELD_VALUE);

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = transformation.apply(record);

			Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);

			assertTrue(resultMap.containsKey(NEW_FIELD_NAME));
			assertFalse(resultMap.containsKey(OLD_FIELD_NAME));
			assertEquals(FIELD_VALUE, resultMap.get(NEW_FIELD_NAME));
		}

		@Test
		@DisplayName("Should rename a nested sourceKey to destKey field (not nested!) in a message if the nested sourceKey is present")
		public void message_returnRenamedField_sourceKeyNested() {
			Map<String, String> props = new HashMap<>();
			final String FIELD_NAME = "field";
			final String NESTED_FIELD_NAME = "nested." + FIELD_NAME;
			final String NEW_FIELD_NAME = "newFieldName";
			final String FIELD_VALUE = "test value";

			props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_FIELD_NAME);
			props.put(Splunk.DESTINATION_KEY_CONFIG, NEW_FIELD_NAME);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> mapValue = new HashMap<>();
			Map<String, Object> nestedMapValue = new HashMap<>();
			nestedMapValue.put(FIELD_NAME, FIELD_VALUE);
			mapValue.put("nested", nestedMapValue);

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = transformation.apply(record);

			Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			@SuppressWarnings("unchecked")
			Map<String, Object> nestedResultMap = (Map<String, Object>) resultMap.get("nested");

			assertFalse(nestedResultMap.containsKey(FIELD_NAME));
			assertTrue(resultMap.containsKey(NEW_FIELD_NAME));
			assertEquals(FIELD_VALUE, resultMap.get(NEW_FIELD_NAME));
		}

		@Test
		@DisplayName("Should return unchanged message if the sourceKey field does not exist")
		public void message_returnUnchangedMessage() {
			Map<String, String> props = new HashMap<>();
			final String SOURCE_FIELD_NAME = "sourceField";
			final String SOURCE_FIELD_VALUE = "test value";
			final String DEST_FIELD_NAME = "destField";
			final String THIS_FIELD_EXIST = "thisOneExists";

			props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
			props.put(Splunk.DESTINATION_KEY_CONFIG, DEST_FIELD_NAME);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> mapValue = new HashMap<>();
			mapValue.put(THIS_FIELD_EXIST, SOURCE_FIELD_VALUE);

			
			Map<String, Object> mapValueCopy = new HashMap<>();
			mapValueCopy.put(THIS_FIELD_EXIST, SOURCE_FIELD_VALUE);

			final SinkRecord originalRecord = newRecord(mapValueCopy);
			SinkRecord resultRecord = transformation.apply(originalRecord);

			Map<String, Object> resultMapValue = requireMapOrNull(resultRecord.value(), TEST_PURPOSE);
			assertEquals(mapValue, resultMapValue);
		}
		
		@Test
		@DisplayName("Should return unchanged message if the nested sourceKey field does not exist")
		public void message_returnUnchangedMessage_sourceKeyNested() {
			Map<String, String> props = new HashMap<>();
			final String FIELD_NAME = "field";
			final String NESTED_FIELD_NAME = "nested.my." + FIELD_NAME;
			final String FIELD_VALUE = "test value";
			final String DEST_FIELD_NAME = "destField";

			props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_FIELD_NAME);
			props.put(Splunk.DESTINATION_KEY_CONFIG, DEST_FIELD_NAME);

			transformation = new Splunk<>();
			transformation.configure(props);

			// this exists: "nested.field.here";
			Map<String, Object> mapValue = new HashMap<>();
			Map<String, Object> nestedMapValue = new HashMap<>();
			Map<String, Object> nestedMapValue2lvl = new HashMap<>();
			nestedMapValue2lvl.put("here", FIELD_VALUE);
			nestedMapValue.put(FIELD_NAME, nestedMapValue2lvl);
			mapValue.put("nested", nestedMapValue);

			Map<String, Object> mapValueCopy = new HashMap<>();
			Map<String, Object> nestedMapValueCopy = new HashMap<>();
			Map<String, Object> nestedMapValueCopy2lvl = new HashMap<>();
			nestedMapValueCopy2lvl.put("here", FIELD_VALUE);
			nestedMapValueCopy.put(FIELD_NAME, nestedMapValueCopy2lvl);
			mapValueCopy.put("nested", nestedMapValueCopy);

			final SinkRecord originalRecord = newRecord(mapValueCopy);
			SinkRecord resultRecord = transformation.apply(originalRecord);

			Map<String, Object> resultMapValue = requireMapOrNull(resultRecord.value(), TEST_PURPOSE);
			assertEquals(mapValue, resultMapValue);
		}

		@Test
		@DisplayName("Should return unchanged message if the sourceKey points to the object (i.e. a Map)")
		public void message_returnUnchangedMessage_pointingToMap() {
			Map<String, String> props = new HashMap<>();
			final String SOURCE_FIELD_NAME = "sourceField";
			final String SOURCE_FIELD_VALUE = "test value";
			final String DEST_FIELD_NAME = "destField";

			props.put(Splunk.SOURCE_KEY_CONFIG, SOURCE_FIELD_NAME);
			props.put(Splunk.DESTINATION_KEY_CONFIG, DEST_FIELD_NAME);

			transformation = new Splunk<>();
			transformation.configure(props);

			// this exists: "sourceField.here";
			Map<String, Object> mapValue = new HashMap<>();
			Map<String, Object> nestedMapValue = new HashMap<>();
			nestedMapValue.put("here", SOURCE_FIELD_VALUE);
			mapValue.put("sourceField", nestedMapValue);

			Map<String, Object> mapValueCopy = new HashMap<>();
			Map<String, Object> nestedMapValueCopy = new HashMap<>();
			nestedMapValueCopy.put("here", SOURCE_FIELD_VALUE);
			mapValueCopy.put("sourceField", nestedMapValueCopy);

			final SinkRecord originalRecord = newRecord(mapValueCopy);
			SinkRecord resultRecord = transformation.apply(originalRecord);

			Map<String, Object> resultMapValue = requireMapOrNull(resultRecord.value(), TEST_PURPOSE);
			assertEquals(mapValue, resultMapValue);
			assertFalse(resultMapValue.containsKey(DEST_FIELD_NAME));
		}
		
		@Test
		@DisplayName("Should return unchanged message if the nested sourceKey points to the object (i.e. a Map)")
		public void message_returnUnchangedMessage_sourceKeyNested_pointingToMap() {
			Map<String, String> props = new HashMap<>();
			final String FIELD_NAME = "field";
			final String NESTED_FIELD_NAME = "nested." + FIELD_NAME;
			final String FIELD_VALUE = "test value";
			final String DEST_FIELD_NAME = "destField";

			props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_FIELD_NAME);
			props.put(Splunk.DESTINATION_KEY_CONFIG, DEST_FIELD_NAME);

			transformation = new Splunk<>();
			transformation.configure(props);

			// this exists: "nested.field.here";
			Map<String, Object> mapValue = new HashMap<>();
			Map<String, Object> nestedMapValue = new HashMap<>();
			Map<String, Object> nestedMapValue2lvl = new HashMap<>();
			nestedMapValue2lvl.put("here", FIELD_VALUE);
			nestedMapValue.put(FIELD_NAME, nestedMapValue2lvl);
			mapValue.put("nested", nestedMapValue);

			Map<String, Object> mapValueCopy = new HashMap<>();
			Map<String, Object> nestedMapValueCopy = new HashMap<>();
			Map<String, Object> nestedMapValueCopy2lvl = new HashMap<>();
			nestedMapValueCopy2lvl.put("here", FIELD_VALUE);
			nestedMapValueCopy.put(FIELD_NAME, nestedMapValueCopy2lvl);
			mapValueCopy.put("nested", nestedMapValueCopy);

			final SinkRecord originalRecord = newRecord(mapValueCopy);
			SinkRecord resultRecord = transformation.apply(originalRecord);

			Map<String, Object> resultMapValue = requireMapOrNull(resultRecord.value(), TEST_PURPOSE);
			assertEquals(mapValue, resultMapValue);
		}

		@Test
		@DisplayName("Should preserve a sourceKey and it's original value in the body if preserveKeyInBody is set to true and destKey is specified")
		public void message_preserveField() {
			Map<String, Object> props = new HashMap<>();
			final String OLD_FIELD_NAME = "oldFieldName";
			final String NEW_FIELD_NAME = "newFieldName";
			final String FIELD_VALUE = "test value";
			final Boolean PRESERVE = Boolean.TRUE;

			props.put(Splunk.SOURCE_KEY_CONFIG, OLD_FIELD_NAME);
			props.put(Splunk.DESTINATION_KEY_CONFIG, NEW_FIELD_NAME);
			props.put(Splunk.PRESERVE_CONFIG, PRESERVE);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> mapValue = new HashMap<>();
			mapValue.put(OLD_FIELD_NAME, FIELD_VALUE);

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = transformation.apply(record);

			Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			assertTrue(resultMap.containsKey(OLD_FIELD_NAME));
			assertEquals(FIELD_VALUE, resultMap.get(OLD_FIELD_NAME));
			assertTrue(resultMap.containsKey(NEW_FIELD_NAME));
			assertEquals(FIELD_VALUE, resultMap.get(NEW_FIELD_NAME));
		}
		
		@Test
		@DisplayName("Should preserve a nested sourceKey and it's original value in the body if preserveKeyInBody is set to true and destKey is specified")
		public void message_preserveField_sourceKeyNested() {
			Map<String, Object> props = new HashMap<>();
			final String FIELD_NAME = "field";
			final String NESTED_FIELD_NAME = "nested." + FIELD_NAME;
			final String NEW_FIELD_NAME = "newFieldName";
			final String FIELD_VALUE = "test value";
			final Boolean PRESERVE = Boolean.TRUE;

			props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_FIELD_NAME);
			props.put(Splunk.DESTINATION_KEY_CONFIG, NEW_FIELD_NAME);
			props.put(Splunk.PRESERVE_CONFIG, PRESERVE);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> mapValue = new HashMap<>();
			Map<String, Object> nestedMapValue = new HashMap<>();
			nestedMapValue.put(FIELD_NAME, FIELD_VALUE);
			mapValue.put("nested", nestedMapValue);

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = transformation.apply(record);

			Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			@SuppressWarnings("unchecked")
			Map<String, Object> nestedResultMap = (Map<String, Object>) resultMap.get("nested");
			assertTrue(nestedResultMap.containsKey(FIELD_NAME));
			assertEquals(FIELD_VALUE, nestedResultMap.get(FIELD_NAME));
			assertTrue(resultMap.containsKey(NEW_FIELD_NAME));
			assertEquals(FIELD_VALUE, resultMap.get(NEW_FIELD_NAME));

		}
		
		@Nested
		@DisplayName("SplunkTest - Regex & Format")
		class RegexFormat {
			
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

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				mapValue.put(FIELD_NAME, FIELD_VALUE);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertTrue(resultMap.containsKey(FIELD_NAME));
				assertEquals(EXPECTED_RESULT, String.valueOf(resultMap.get(FIELD_NAME)));
			}
			
			@Test
			@DisplayName("Should apply regex & format to the value of nested sourceKey field")
			public void message_returnRegexFormat_sourceKeyNested() {
				Map<String, Object> props = new HashMap<>();
				final String FIELD_NAME = "aplyRegexAndFormat";
				final String NESTED_FIELD_NAME = "nested." + FIELD_NAME;
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String REGEX = "crn:(?:[^:]+:){3}([^:]+).*";
				final String FORMAT = "cloud_$1_logdna";
				final String EXPECTED_RESULT = "cloud_databases-for-mongodb_logdna";

				props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				Map<String, Object> nestedMapValue = new HashMap<>();
				nestedMapValue.put(FIELD_NAME, FIELD_VALUE);
				mapValue.put("nested", nestedMapValue);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				@SuppressWarnings("unchecked")
				Map<String, Object> nestedResultMap = (Map<String, Object>) resultMap.get("nested");
				assertTrue(nestedResultMap.containsKey(FIELD_NAME));
				assertEquals(EXPECTED_RESULT, String.valueOf(nestedResultMap.get(FIELD_NAME)));
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

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				mapValue.put(FIELD_NAME, FIELD_VALUE);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertFalse(resultMap.containsKey(FIELD_NAME));

				Iterator<Header> headerIterator = result.headers().allWithName(FIELD_NAME);
				assertTrue(headerIterator.hasNext());
				assertEquals(EXPECTED_RESULT, headerIterator.next().value());
			}
			
			@Test
			@DisplayName("Should apply regex & format to the value of nested sourceKey field and put that field among headers")
			public void message_returnRegexFormatAsMetadata_sourceKeyNested() {
				Map<String, Object> props = new HashMap<>();
				final String FIELD_NAME = "aplyRegexAndFormat";
				final String NESTED_FIELD_NAME = "nested." + FIELD_NAME;
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String REGEX = "crn:(?:[^:]+:){3}([^:]+).*";
				final String FORMAT = "cloud_$1_logdna";
				final Boolean iS_METADATA = true;
				final String EXPECTED_RESULT = "cloud_databases-for-mongodb_logdna";

				props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.IS_METADATA_KEY_CONFIG, iS_METADATA);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				Map<String, Object> nestedMapValue = new HashMap<>();
				nestedMapValue.put(FIELD_NAME, FIELD_VALUE);
				mapValue.put("nested", nestedMapValue);
				
				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				@SuppressWarnings("unchecked")
				Map<String, Object> nestedResultMap = (Map<String, Object>) resultMap.get("nested");
				assertFalse(nestedResultMap.containsKey(FIELD_NAME));

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

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				mapValue.put(FIELD_NAME, FIELD_VALUE);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertFalse(resultMap.containsKey(FIELD_NAME));
				assertFalse(resultMap.containsKey(NEW_FIELD_NAME));

				Iterator<Header> headerIterator = result.headers().allWithName(NEW_FIELD_NAME);
				assertTrue(headerIterator.hasNext());
				assertEquals(EXPECTED_RESULT, headerIterator.next().value());
			}
			
			@Test
			@DisplayName("Should rename and apply regex & format to the value of nested sourceKey field and put that field among headers")
			public void message_returnRegexFormatAsMetadataAndRename_sourceKeyNested() {
				Map<String, Object> props = new HashMap<>();
				final String FIELD_NAME = "aplyRegexAndFormat";
				final String NESTED_FIELD_NAME = "nested." + FIELD_NAME;
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String NEW_FIELD_NAME = "newFieldName";
				final String REGEX = "crn:(?:[^:]+:){3}([^:]+).*";
				final String FORMAT = "cloud_$1_logdna";
				final Boolean iS_METADATA = true;
				final String EXPECTED_RESULT = "cloud_databases-for-mongodb_logdna";

				props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.IS_METADATA_KEY_CONFIG, iS_METADATA);
				props.put(Splunk.DESTINATION_KEY_CONFIG, NEW_FIELD_NAME);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				Map<String, Object> nestedMapValue = new HashMap<>();
				nestedMapValue.put(FIELD_NAME, FIELD_VALUE);
				mapValue.put("nested", nestedMapValue);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				@SuppressWarnings("unchecked")
				Map<String, Object> nestedResultMap = (Map<String, Object>) resultMap.get("nested");
				assertFalse(nestedResultMap.containsKey(FIELD_NAME));
				assertFalse(nestedResultMap.containsKey(NEW_FIELD_NAME));
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

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				mapValue.put(FIELD_NAME, FIELD_VALUE);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertTrue(resultMap.containsKey(FIELD_NAME));
				assertEquals(EXPECTED_RESULT, String.valueOf(resultMap.get(FIELD_NAME)));
			}

			@Test
			@DisplayName("Should return original value if regex does not match and no default value is specified (nested source key)")
			public void message_returnOriginalNoRegexFormat_nestedSourceKey() {
				Map<String, Object> props = new HashMap<>();
				final String FIELD_NAME = "aplyRegexAndFormat";
				final String NESTED_FIELD_NAME = "nested." + FIELD_NAME;
				final String FIELD_VALUE = "crn:v1:bluemix:public:databases-for-mongodb:us-south:a/123:11111111-2222-3333-4444-555555555555::";
				final String REGEX = "noregex";
				final String FORMAT = "cloud_$1_logdna";
				final String EXPECTED_RESULT = FIELD_VALUE;

				props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				Map<String, Object> nestedMapValue = new HashMap<>();
				nestedMapValue.put(FIELD_NAME, FIELD_VALUE);
				mapValue.put("nested", nestedMapValue);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				@SuppressWarnings("unchecked")
				Map<String, Object> nestedResultMap = (Map<String, Object>) resultMap.get("nested");
				assertTrue(nestedResultMap.containsKey(FIELD_NAME));
				assertEquals(EXPECTED_RESULT, String.valueOf(nestedResultMap.get(FIELD_NAME)));
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

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				mapValue.put(FIELD_NAME, FIELD_VALUE);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertTrue(resultMap.containsKey(FIELD_NAME));
				assertEquals(EXPECTED_RESULT, String.valueOf(resultMap.get(FIELD_NAME)));
			}
			
			@Test
			@DisplayName("Should return default value if regex does not match and a default value is specified (nested source key)")
			public void message_returnDefaultValueNoPatternMatch_nestedSourceKey() {
				Map<String, Object> props = new HashMap<>();
				final String FIELD_NAME = "aplyRegexAndFormat";
				final String NESTED_FIELD_NAME = "nested." + FIELD_NAME;
				final String FIELD_VALUE = "/var/log/kublet.log";
				final String REGEX = "noregex";
				final String FORMAT = "cloud_$1_logdna";
				final String DEFAULT_VALUE = "my default value";
				final String EXPECTED_RESULT = DEFAULT_VALUE;

				props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.REGEX_DEFAULT_VALUE_CONFIG, DEFAULT_VALUE);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				Map<String, Object> nestedMapValue = new HashMap<>();
				nestedMapValue.put(FIELD_NAME, FIELD_VALUE);
				mapValue.put("nested", nestedMapValue);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				@SuppressWarnings("unchecked")
				Map<String, Object> nestedResultMap = (Map<String, Object>) resultMap.get("nested");
				assertTrue(nestedResultMap.containsKey(FIELD_NAME));
				assertEquals(EXPECTED_RESULT, String.valueOf(nestedResultMap.get(FIELD_NAME)));
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

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				mapValue.put(FIELD_NAME, FIELD_VALUE);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				assertTrue(resultMap.containsKey(FIELD_NAME));
				assertEquals(FIELD_VALUE, resultMap.get(FIELD_NAME));

				Iterator<Header> headerIterator = result.headers().allWithName(FIELD_NAME);
				assertFalse(headerIterator.hasNext());
			}
			
			@Test
			@DisplayName("Should return original record unchanged if regex does not match and a default value is not specified (nested source key)")
			public void message_returnOrigRecordNoPatternMatch_nestedSourceKey() {
				Map<String, Object> props = new HashMap<>();
				final String FIELD_NAME = "aplyRegexAndFormat";
				final String NESTED_FIELD_NAME = "nested." + FIELD_NAME;
				final String FIELD_VALUE = "/var/log/kublet.log";
				final Boolean IS_METADATA = true;
				final String REGEX = "noregex";
				final String FORMAT = "cloud_$1_logdna";

				props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_FIELD_NAME);
				props.put(Splunk.REGEX_PATTERN_CONFIG, REGEX);
				props.put(Splunk.REGEX_FORMAT_CONFIG, FORMAT);
				props.put(Splunk.IS_METADATA_KEY_CONFIG, IS_METADATA);

				transformation = new Splunk<>();
				transformation.configure(props);

				Map<String, Object> mapValue = new HashMap<>();
				Map<String, Object> nestedMapValue = new HashMap<>();
				nestedMapValue.put(FIELD_NAME, FIELD_VALUE);
				mapValue.put("nested", nestedMapValue);

				final SinkRecord record = newRecord(mapValue);
				SinkRecord result = transformation.apply(record);

				Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
				@SuppressWarnings("unchecked")
				Map<String, Object> nestedResultMap = (Map<String, Object>) resultMap.get("nested");
				assertTrue(nestedResultMap.containsKey(FIELD_NAME));
				assertEquals(FIELD_VALUE, nestedResultMap.get(FIELD_NAME));

				Iterator<Header> headerIterator = result.headers().allWithName(FIELD_NAME);
				assertFalse(headerIterator.hasNext());
			}
		}
	}

	@Nested
	@DisplayName("SplunkTest - Metadata")
	class Metadata {
		
		@Test
		@DisplayName("Should move sourceKey field from body to header if there is no destKey, but isMetadata is true")
		public void message_returnHeaderField() {
			Map<String, Object> props = new HashMap<>();
			final String FIELD_NAME = "newFieldName";
			final String FIELD_VALUE = "test value";
			final Boolean IS_METADATA_VALUE = Boolean.TRUE;

			props.put(Splunk.SOURCE_KEY_CONFIG, FIELD_NAME);
			props.put(Splunk.IS_METADATA_KEY_CONFIG, IS_METADATA_VALUE);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> mapValue = new HashMap<>();
			mapValue.put(FIELD_NAME, FIELD_VALUE);

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = transformation.apply(record);

			Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			assertFalse(resultMap.containsKey(FIELD_NAME));

			Iterator<Header> headerIterator = result.headers().allWithName(FIELD_NAME);
			assertTrue(headerIterator.hasNext());
			assertEquals(FIELD_VALUE, headerIterator.next().value());
		}
		
		@Test
		@DisplayName("Should move nested sourceKey field from body to header if there is no destKey, but isMetadata is true")
		public void message_returnHeaderField_nestedSourceKey() {
			Map<String, Object> props = new HashMap<>();
			final String FIELD_NAME = "fieldName";
			final String NESTED_FIELD_NAME = "nested." + FIELD_NAME;
			final String FIELD_VALUE = "test value";
			final Boolean IS_METADATA_VALUE = Boolean.TRUE;

			props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_FIELD_NAME);
			props.put(Splunk.IS_METADATA_KEY_CONFIG, IS_METADATA_VALUE);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> mapValue = new HashMap<>();
			Map<String, Object> nestedMapValue = new HashMap<>();
			nestedMapValue.put(FIELD_NAME, FIELD_VALUE);
			mapValue.put("nested", nestedMapValue);

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = transformation.apply(record);

			Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			@SuppressWarnings("unchecked")
			Map<String, Object> nestedResultMap = (Map<String, Object>) resultMap.get("nested");
			assertFalse(nestedResultMap.containsKey(FIELD_NAME));

			Iterator<Header> headerIterator = result.headers().allWithName(FIELD_NAME);
			assertTrue(headerIterator.hasNext());
			assertEquals(FIELD_VALUE, headerIterator.next().value());
		}

		@Test
		@DisplayName("Should move and rename sourceKey field from body to header, if destKey is present and isMetadata is true")
		public void message_returnRenamedHeaderField() {
			Map<String, Object> props = new HashMap<>();
			final String FIELD_NAME = "oldFiledName";
			final String NEW_FIELD_NAME = "newFieldName";
			final String FIELD_VALUE = "test value";
			final Boolean IS_METADATA_VALUE = Boolean.TRUE;

			props.put(Splunk.SOURCE_KEY_CONFIG, FIELD_NAME);
			props.put(Splunk.DESTINATION_KEY_CONFIG, NEW_FIELD_NAME);
			props.put(Splunk.IS_METADATA_KEY_CONFIG, IS_METADATA_VALUE);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> mapValue = new HashMap<>();
			mapValue.put(FIELD_NAME, FIELD_VALUE);

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = transformation.apply(record);

			Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			assertFalse(resultMap.containsKey(FIELD_NAME));

			Iterator<Header> headerIterator = result.headers().allWithName(NEW_FIELD_NAME);
			assertTrue(headerIterator.hasNext());
			assertEquals(FIELD_VALUE, headerIterator.next().value());
		}
		
		@Test
		@DisplayName("Should move and rename nested sourceKey field from body to header, if destKey is present and isMetadata is true")
		public void message_returnRenamedHeaderField_nestedSourceKey() {
			Map<String, Object> props = new HashMap<>();
			final String FIELD_NAME = "fieldName";
			final String NESTED_FIELD_NAME = "nested." + FIELD_NAME;
			final String NEW_FIELD_NAME = "newFieldName";
			final String FIELD_VALUE = "test value";
			final Boolean IS_METADATA_VALUE = Boolean.TRUE;

			props.put(Splunk.SOURCE_KEY_CONFIG, NESTED_FIELD_NAME);
			props.put(Splunk.DESTINATION_KEY_CONFIG, NEW_FIELD_NAME);
			props.put(Splunk.IS_METADATA_KEY_CONFIG, IS_METADATA_VALUE);

			transformation = new Splunk<>();
			transformation.configure(props);

			Map<String, Object> mapValue = new HashMap<>();
			Map<String, Object> nestedMapValue = new HashMap<>();
			nestedMapValue.put(FIELD_NAME, FIELD_VALUE);
			mapValue.put("nested", nestedMapValue);

			final SinkRecord record = newRecord(mapValue);
			SinkRecord result = transformation.apply(record);

			Map<String, Object> resultMap = requireMapOrNull(result.value(), TEST_PURPOSE);
			@SuppressWarnings("unchecked")
			Map<String, Object> nestedResultMap = (Map<String, Object>) resultMap.get("nested");
			assertFalse(nestedResultMap.containsKey(FIELD_NAME));

			Iterator<Header> headerIterator = result.headers().allWithName(NEW_FIELD_NAME);
			assertTrue(headerIterator.hasNext());
			assertEquals(FIELD_VALUE, headerIterator.next().value());
		}
	}

	private SinkRecord newRecord(Map<String, Object> value) {
		return new SinkRecord("topic", 1, null, null, null, value, 1L);
	}
}