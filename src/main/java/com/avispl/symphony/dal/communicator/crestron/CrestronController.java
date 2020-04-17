/*
 * Copyright (c) 2017-2020 AVI-SPL Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.crestron;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.avispl.symphony.dal.communicator.AggregatorClient;
import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;
import com.avispl.symphony.dal.util.StringUtils;

/**
 * Implements Aggregator client for Crestron controllers.
 * 
 * @author Owen Gerig / Symphony Dev Team<br>
 *         Created on May 12, 2017
 * @since 4.0.2
 */
public class CrestronController extends AggregatorClient {
	static final String MAC_ADDRESS_KEY = "macAddress";

	/**
	 * Constructor.
	 */
	public CrestronController() {
		super();
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @since 4.7
	 */
	@Override
	public List<AggregatedDevice> retrieveMultipleStatistics() throws Exception {
		// call controlled device and get list of devices with their statistics
		List<AggregatedDevice> aggregatedDevices = super.retrieveMultipleStatistics();
		if (null != aggregatedDevices) {
			for (AggregatedDevice aggregatedDevice : aggregatedDevices) {
				Map<String, String> aggregatedDeviceProperties = aggregatedDevice.getProperties();
				if (null != aggregatedDeviceProperties && !aggregatedDeviceProperties.isEmpty()) {
					String aggregatedDeviceMacAddresses = aggregatedDeviceProperties.get(MAC_ADDRESS_KEY);
					if (!StringUtils.isNullOrEmpty(aggregatedDeviceMacAddresses)) {
						List<String> aggregatedDeviceMacAddressesList = new ArrayList<>();
						aggregatedDeviceMacAddressesList.add(aggregatedDeviceMacAddresses);
						aggregatedDevice.setMacAddresses(aggregatedDeviceMacAddressesList);
					}
				}
			}
		}
		return aggregatedDevices;
	}
}
