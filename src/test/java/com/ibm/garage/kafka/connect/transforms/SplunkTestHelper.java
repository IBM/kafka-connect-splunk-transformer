/*
 Copyright 2021 IBM Inc. All rights reserved
 SPDX-License-Identifier: Apache2.0
*/

package com.ibm.garage.kafka.connect.transforms;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

public class SplunkTestHelper {

	public static final String TEST_PURPOSE = "test purpose";

	public static final Boolean DEST_TO_HEADER_TRUE = Boolean.TRUE;
	public static final Boolean SOURCE_PRESERVE_TRUE = Boolean.TRUE;
	
	public static final String SOURCE_FIELD_NAME = "sourceField";
	public static final String SOURCE_FIELD_VALUE = "sourceField value";

	public static final String DEST_FIELD_NAME = "destField";

	public static final String SOURCE_FIELD_PARENT_OBJECT = "nested";
	public static final String NESTED_SOURCE_FIELD_NAME = SOURCE_FIELD_PARENT_OBJECT + "." + SOURCE_FIELD_NAME;

	public static SinkRecord newRecord(Map<String, Object> value) {
		return new SinkRecord("topic", 1, null, null, null, value, 1L);
	}

	public static SinkRecord applyTransformation(Transformation<SinkRecord> transformation,
			Map<String, Object> valueMap) {
		return transformation.apply(newRecord(valueMap));
	}

	public static Map<String, Object> createValueMap() {
		Map<String, Object> valueMap = new HashMap<>();
		valueMap.put(SOURCE_FIELD_NAME, SOURCE_FIELD_VALUE);

		return valueMap;
	}

	public static Map<String, Object> createValueMap(String fieldName, Object fieldValue) {
		Map<String, Object> valueMap = new HashMap<>();
		valueMap.put(fieldName, fieldValue);

		return valueMap;
	}

	public static Map<String, Object> createNestedValueMap() {
		Map<String, Object> parentValueMap = new HashMap<>();
		Map<String, Object> nestedValueMap = new HashMap<>();
		nestedValueMap.put(SOURCE_FIELD_NAME, SOURCE_FIELD_VALUE);
		parentValueMap.put(SOURCE_FIELD_PARENT_OBJECT, nestedValueMap);

		return parentValueMap;
	}
	
	public static Map<String, Object> createNestedValueMap(String fieldName, Object fieldValue) {
		Map<String, Object> parentValueMap = new HashMap<>();
		Map<String, Object> nestedValueMap = new HashMap<>();
		nestedValueMap.put(fieldName, fieldValue);
		parentValueMap.put(SOURCE_FIELD_PARENT_OBJECT, nestedValueMap);

		return parentValueMap;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> getNestedValueMap(Map<String, Object> valueMap) {
		return (Map<String, Object>) valueMap.get(SOURCE_FIELD_PARENT_OBJECT);
	}
}