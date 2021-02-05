/*
 Copyright 2021 IBM Inc. All rights reserved
 SPDX-License-Identifier: Apache2.0
*/

package com.ibm.garage.kafka.connect.transforms;

import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Filter<R extends ConnectRecord<R>> implements Transformation<R> {

	private static final Logger log = LoggerFactory.getLogger(Filter.class);

	public static final String OVERVIEW_DOC = "Filter transformation to discard a record if the header field exists";

	public static final String HEADER_KEY_CONFIG = "headerKey";
	public static final String NEGATE_CONFIG = "isNegate";

	public static final ConfigDef CONFIG_DEF = new ConfigDef().define(HEADER_KEY_CONFIG, ConfigDef.Type.STRING,
			ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM, "hasHeader key").define(NEGATE_CONFIG,
					ConfigDef.Type.BOOLEAN, Boolean.FALSE, ConfigDef.Importance.MEDIUM, "Negate the condition");

	private String headerName;
	private Boolean isNegate;

	@Override
	public void configure(Map<String, ?> props) {
		log.info("Getting configuration for " + Filter.class.getName() + " transformation...");

		final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

		this.headerName = config.getString(HEADER_KEY_CONFIG);
		if (this.headerName == null || this.headerName.isEmpty()) {
			throw new RuntimeException("\"" + HEADER_KEY_CONFIG + "\" configuration cannot be neither null nor empty");
		}

		this.isNegate = config.getBoolean(NEGATE_CONFIG);

		log.info(Filter.class.getName() + " transformation has been successfully configured.");
	}

	@Override
	public R apply(R record) {
		log.debug("Filtering a record...");

		Iterator<Header> header = record.headers().allWithName(this.headerName);

		if (this.isNegate) {
			if (!header.hasNext()) {
				log.debug("The record has been discarded.");
				return null;
			}
		} else {
			if (header.hasNext()) {
				log.debug("The record has been discarded.");
				return null;
			}
		}

		log.debug("The record has not been discarded.");
		return record;
	}

	@Override
	public void close() {
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}
}
