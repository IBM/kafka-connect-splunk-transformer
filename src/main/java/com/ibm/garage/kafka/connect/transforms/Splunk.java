/*
 Copyright 2021 IBM Inc. All rights reserved
 SPDX-License-Identifier: Apache2.0
*/

package com.ibm.garage.kafka.connect.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Splunk<R extends ConnectRecord<R>> implements Transformation<R> {

	private static final Logger log = LoggerFactory.getLogger(Splunk.class);

	private static final String JSON_KEY_DELIMITER_REGEX = "\\.";
	private static final String JSON_KEY_DELIMITER = ".";

	public static final String OVERVIEW_DOC = "Transformation of JSON messages to Splunk format";

	public static final String SOURCE_KEY_CONFIG = "sourceKey";
	public static final String DESTINATION_KEY_CONFIG = "destKey";
	public static final String IS_METADATA_KEY_CONFIG = "isMetadata";
	public static final String REGEX_PATTERN_CONFIG = "regex.pattern";
	public static final String REGEX_FORMAT_CONFIG = "regex.format";
	public static final String REGEX_DEFAULT_VALUE_CONFIG = "regex.defaultValue";
	public static final String PRESERVE_CONFIG = "preserveKeyInBody";

	private static ConfigDef.Validator PatternValidator() {
		return (regexKey, regexValue) -> {
			try {
				if (regexValue != null) {
					Pattern.compile(String.valueOf(regexValue));
				}
			} catch (PatternSyntaxException pe) {
				throw new ConfigException(regexKey, regexValue, "Regex pattern is not in the correct form.");
			}
		};
	}

	public static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(SOURCE_KEY_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM,
					"Source key")
			.define(DESTINATION_KEY_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Destination key")
			.define(IS_METADATA_KEY_CONFIG, ConfigDef.Type.BOOLEAN, Boolean.FALSE, ConfigDef.Importance.MEDIUM,
					"Is metadata key")
			.define(REGEX_PATTERN_CONFIG, ConfigDef.Type.STRING, null, PatternValidator(), ConfigDef.Importance.MEDIUM,
					"Regex pattern key")
			.define(REGEX_FORMAT_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Regex format key")
			.define(REGEX_DEFAULT_VALUE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
					"Regex default value key")
			.define(PRESERVE_CONFIG, ConfigDef.Type.BOOLEAN, Boolean.FALSE, ConfigDef.Importance.MEDIUM,
					"Preserve key in the body");

	private static final String PURPOSE = "field value modification";

	private String sourceKey;
	private String destKey;
	private Boolean isMetadata;
	private String regexPattern;
	private String regexFormat;
	private String regexDefaultValue;
	private Boolean preserveKeyInBody;

	private Boolean isSourceKeyNested;

	@Override
	public void configure(Map<String, ?> props) {
		log.info("Getting configuration for " + Splunk.class.getName() + " transformation...");

		final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

		this.sourceKey = config.getString(SOURCE_KEY_CONFIG);
		if (this.sourceKey == null || this.sourceKey.isEmpty()) {
			throw new RuntimeException("\"" + SOURCE_KEY_CONFIG + "\" configuration cannot be neither null nor empty");
		}
		this.isSourceKeyNested = isNested(this.sourceKey);

		this.destKey = config.getString(DESTINATION_KEY_CONFIG);
		this.isMetadata = config.getBoolean(IS_METADATA_KEY_CONFIG);
		this.regexPattern = config.getString(REGEX_PATTERN_CONFIG);
		this.regexFormat = config.getString(REGEX_FORMAT_CONFIG);
		this.regexDefaultValue = config.getString(REGEX_DEFAULT_VALUE_CONFIG);
		this.preserveKeyInBody = config.getBoolean(PRESERVE_CONFIG);

		if (this.regexPattern == null && this.regexFormat != null) {
			throw new RuntimeException(
					"Format: \"" + REGEX_FORMAT_CONFIG + "\" is configured but the regex is missing");
		}

		if (this.regexFormat == null && this.regexPattern != null) {
			throw new RuntimeException(
					"Regex: \"" + REGEX_FORMAT_CONFIG + "\" is configured but the format is missing");
		}

		if (this.regexDefaultValue != null && this.regexPattern == null) {
			throw new RuntimeException("Regex: \"" + REGEX_DEFAULT_VALUE_CONFIG
					+ "\" is configured but the regex format or pattern is missing");
		}

		if (this.preserveKeyInBody && this.destKey == null) {
			throw new RuntimeException("Config: \"" + PRESERVE_CONFIG + "\" is only applicable if \""
					+ DESTINATION_KEY_CONFIG + "\" is specified");
		}
		
		if (this.sourceKey.equals(this.destKey)) {
			throw new RuntimeException("Config: \"" + SOURCE_KEY_CONFIG + "\" and \"" + DESTINATION_KEY_CONFIG + "\" cannot point to the same field");
		}

		log.info(Splunk.class.getName() + " transformation has been successfully configured.");
	}

	@Override
	public R apply(R record) {
		log.debug("Processing a record...");
		final Map<String, Object> rootValueMap = requireMapOrNull(record.value(), PURPOSE);

		if (rootValueMap != null && !rootValueMap.isEmpty()) {
			Map<String, Object> ctxValueMap = rootValueMap;
			String ctxKey = this.sourceKey;

			if (this.isSourceKeyNested) {
				ctxKey = getLastNestedKey(this.sourceKey);
				ctxValueMap = getNestedValueMap(this.sourceKey, rootValueMap);

				if (ctxValueMap == null || ctxValueMap.isEmpty()) {
					log.debug("The record has been returned unchanged. Nested object is not found.");
					return record;
				}
			}

			if (ctxValueMap.containsKey(ctxKey)) {
				Object valueObject = ctxValueMap.get(ctxKey);

				if (!(valueObject instanceof Map)) {
					String value = String.valueOf(valueObject);

					if (this.regexPattern != null) {
						if (value.matches(this.regexPattern)) {
							value = value.replaceAll(this.regexPattern, this.regexFormat);
						} else if (this.regexDefaultValue != null) {
							value = this.regexDefaultValue;
						} else {
							log.debug(
									"The record has been returned unchanged because the regex.pattern does not match and there is no regex.defaulValue specified.");
							return record;
						}
					}

					if (this.destKey != null) {
						rootValueMap.put(this.destKey, value);
						if (!this.preserveKeyInBody) {
							ctxValueMap.remove(ctxKey);
						}
						ctxKey = this.destKey;
						ctxValueMap = rootValueMap;
					} else {
						ctxValueMap.put(ctxKey, value);
					}

					if (this.isMetadata) {
						record.headers().remove(ctxKey);
						record.headers().add(ctxKey, new SchemaAndValue(Schema.STRING_SCHEMA, ctxValueMap.get(ctxKey)));
						ctxValueMap.remove(ctxKey);
					}

					log.debug("The record has been modified.");
					return newRecord(record);
				}
				log.debug("The record has been returned unchanged. Source key field points to the object.");
				return record;
			}
			log.debug("The record has been returned unchanged. Nested source key field is not found.");
			return record;

		}
		log.debug("The record has been returned unchanged since it is empty.");
		return record;
	}

	@Override
	public void close() {
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	private R newRecord(R record) {
		return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
				record.valueSchema(), record.value(), record.timestamp(), record.headers());
	}

	private boolean isNested(String key) {
		return key.contains(JSON_KEY_DELIMITER);
	}

	private String getLastNestedKey(String dottedKey) {
		String[] splittedKeys = dottedKey.split(JSON_KEY_DELIMITER_REGEX);
		return splittedKeys[splittedKeys.length - 1];
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getNestedValueMap(String dottedKey, Map<String, Object> valueMap) {
		String[] splittedKeys = dottedKey.split(JSON_KEY_DELIMITER_REGEX);

		if (splittedKeys.length == 1) {
			return valueMap;
		}

		Map<String, Object> nestedValueMap = valueMap;

		for (int i = 0; i < splittedKeys.length - 1; i++) {
			Object value = nestedValueMap.get(splittedKeys[i]);
			if (value == null) {
				return null;
			}

			if (value instanceof Map) {
				nestedValueMap = (Map<String, Object>) value;
			} else {
				return null;
			}
		}

		return nestedValueMap;
	}
}
