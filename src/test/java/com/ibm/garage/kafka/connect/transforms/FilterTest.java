/*
 Copyright 2021 IBM Inc. All rights reserved
 SPDX-License-Identifier: Apache2.0
*/

package com.ibm.garage.kafka.connect.transforms;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class FilterTest {

	private Transformation<SinkRecord> transformation;

	@Nested
	@DisplayName("FilterTest - Configuration")
	class Configuration {

		@Test
		@DisplayName("Should throw an exception if headerKey configuration is null")
		public void configuration_throwsRuntimeException_headerKey_Null() {
			Map<String, Object> props = new HashMap<>();
			props.put(Filter.HEADER_KEY_CONFIG, null);
			props.put(Filter.NEGATE_CONFIG, Boolean.FALSE);

			this.shouldThrow(props);
		}

		@Test
		@DisplayName("Should throw an exception if headerKey configuration is empty")
		public void configuration_throwsRuntimeException_headerKey_Empty() {
			Map<String, Object> props = new HashMap<>();
			props.put(Filter.HEADER_KEY_CONFIG, "");
			props.put(Filter.NEGATE_CONFIG, Boolean.FALSE);

			this.shouldThrow(props);
		}

		private void shouldThrow(Map<String, ?> props) {
			transformation = new Filter<>();

			assertThrows(RuntimeException.class, () -> {
				transformation.configure(props);
			});
		}
	}

	@Nested
	@DisplayName("FilterTest - Messages")
	class Messages {

		@Test
		@DisplayName("Should return null if the header key is found and negate is false")
		public void message_returnNullMessage() {
			transformation = new Filter<>();

			final String HEADER_KEY = "testHeaderKey";
			Map<String, String> props = new HashMap<>();
			props.put(Filter.HEADER_KEY_CONFIG, HEADER_KEY);

			Map<String, Object> mapValue = new HashMap<>();
			Headers headers = new ConnectHeaders();
			headers.add(HEADER_KEY, new SchemaAndValue(Schema.STRING_SCHEMA, "header value"));

			final SinkRecord record = newRecord(mapValue, headers);

			transformation.configure(props);
			SinkRecord result = transformation.apply(record);

			assertNull(result);
		}

		@Test
		@DisplayName("Should return null if the header key is not found but negate is true")
		public void message_returnNullMessage_negate() {
			transformation = new Filter<>();

			final String HEADER_KEY = "testHeaderKey";
			Map<String, Object> props = new HashMap<>();
			props.put(Filter.HEADER_KEY_CONFIG, HEADER_KEY);
			props.put(Filter.NEGATE_CONFIG, Boolean.TRUE);

			Map<String, Object> mapValue = new HashMap<>();
			Headers headers = new ConnectHeaders();
			headers.add("something_else", new SchemaAndValue(Schema.STRING_SCHEMA, "header value"));

			final SinkRecord record = newRecord(mapValue, headers);

			transformation.configure(props);
			SinkRecord result = transformation.apply(record);

			assertNull(result);
		}

		@Test
		@DisplayName("Should return a record if the header key is not found and negate is false")
		public void message_returnMessage() {
			transformation = new Filter<>();

			final String HEADER_KEY = "testHeaderKey";
			Map<String, Object> props = new HashMap<>();
			props.put(Filter.HEADER_KEY_CONFIG, HEADER_KEY);

			Map<String, Object> mapValue = new HashMap<>();
			Headers headers = new ConnectHeaders();
			headers.add("something_else", new SchemaAndValue(Schema.STRING_SCHEMA, "header value"));

			final SinkRecord record = newRecord(mapValue, headers);

			transformation.configure(props);
			SinkRecord result = transformation.apply(record);

			assertNotNull(result);
		}

		@Test
		@DisplayName("Should return a record if the header key is found and negate is true")
		public void message_returnMessage_negate() {
			transformation = new Filter<>();

			final String HEADER_KEY = "testHeaderKey";
			Map<String, Object> props = new HashMap<>();
			props.put(Filter.HEADER_KEY_CONFIG, HEADER_KEY);
			props.put(Filter.NEGATE_CONFIG, Boolean.TRUE);

			Map<String, Object> mapValue = new HashMap<>();
			Headers headers = new ConnectHeaders();
			headers.add(HEADER_KEY, new SchemaAndValue(Schema.STRING_SCHEMA, "header value"));

			final SinkRecord record = newRecord(mapValue, headers);

			transformation.configure(props);
			SinkRecord result = transformation.apply(record);

			assertNotNull(result);
		}
	}

	private SinkRecord newRecord(Map<String, Object> value, Iterable<Header> headers) {
		return new SinkRecord("topic", 1, null, null, null, value, 1L, 1L, TimestampType.NO_TIMESTAMP_TYPE, headers);
	}

}