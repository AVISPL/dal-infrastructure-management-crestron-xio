/*
 * Copyright (c) 2017-2019 AVI-SPL Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.crestron;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.avispl.symphony.api.dal.dto.control.ControllableProperty;
import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;
import com.avispl.symphony.dal.util.ControllablePropertyFactory;
import org.junit.jupiter.api.Tag;

/**
 * Implements Crestron Controller test.
 * 
 * @author Owen Gerig / Symphony Dev Team<br>
 *         Created on May 12, 2017
 * @since 4.0.2
 */
@Tag("integration")
public class CrestronControllerTest {

	private static CrestronController crestronController = new CrestronController();

	/**
	 * Sets up any resources prior to test running.
	 * 
	 * @throws Exception if any error occurs
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		crestronController.setHost("portaldev.vnocsymphony.com");
		crestronController.setPort(80);
		crestronController.setProtocol("http");
		crestronController.init();

		// turn off logging for the duration of the test
		// for debugging, set level to DEBUG or TRACE (otherwise OFF)
		Logger.getLogger(CrestronController.class).setLevel(Level.OFF);
	}

	/**
	 * Tear down any resources opened during test
	 */
	@AfterClass
	public static void tearDownAfterClass() {
		try {
			crestronController.disconnect();
		} catch (Exception e) {
			// nothing we can do
		}
	}

	/**
	 * Test method for {@link com.avispl.symphony.dal.communicator.crestron.CrestronController#retrieveMultipleStatistics()}.
	 */
	@Test
	public void test01_GetMultipleStatistics() {

		try {
			List<AggregatedDevice> crestronDevices = crestronController.retrieveMultipleStatistics();
			assertNotNull("crestronDevices should not be null", crestronDevices);
			for (int i = 0; i < randomInt(1, 10); i++) {
				crestronDevices = crestronController.retrieveMultipleStatistics();
				assertNotNull("crestronDevices should not be null", crestronDevices);
				for (AggregatedDevice crestronDevice : crestronDevices) {
					assertNotNull("crestronDevice id should not be null", crestronDevice.getDeviceId());
				}
			}
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	/**
	 * Test method for {@link com.avispl.symphony.dal.communicator.crestron.CrestronController#controlProperty(ControllableProperty)}.
	 */
	@Test
	public void test02_ControlProperty() {
		try {
			List<AggregatedDevice> crestronDevices = crestronController.retrieveMultipleStatistics();
			for (AggregatedDevice aggregatedDevice : crestronDevices) {
				String aggregatedDeviceId = aggregatedDevice.getDeviceId();
				Map<String, String> controlMap = aggregatedDevice.getControl();
				if (null == controlMap || controlMap.isEmpty()) {
					continue;
				}
				Map<String, String> statistics = aggregatedDevice.getStatistics();
				for (Map.Entry<String, String> entry : controlMap.entrySet()) {
					String propertyName = entry.getKey();
					String statisticValue = statistics.get(propertyName);
					int existingIntegerValue = Integer.parseInt(statisticValue);
					int newIntegerValue = -1;
					Boolean existingBooleanValue = null;
					Boolean newBooleanValue = null;
					if (existingIntegerValue == 0) {
						existingBooleanValue = Boolean.FALSE;
						newIntegerValue = 1;
						newBooleanValue = Boolean.TRUE;
					} else if (existingIntegerValue == 1) {
						existingBooleanValue = Boolean.TRUE;
						newIntegerValue = 0;
						newBooleanValue = Boolean.FALSE;
					} else {
						newIntegerValue = existingIntegerValue + 1;
					}
					if (null != existingBooleanValue) {
						// pass boolean object
						ControllableProperty property = ControllablePropertyFactory.of(propertyName, newBooleanValue, aggregatedDeviceId);
						crestronController.controlProperty(property);
					} else {
						// pass int
						ControllableProperty property = ControllablePropertyFactory.of(propertyName, Integer.valueOf(newIntegerValue), aggregatedDeviceId);
						crestronController.controlProperty(property);
					}

					List<AggregatedDevice> crestronDevicesUpdated = crestronController.retrieveMultipleStatistics();
					AggregatedDevice matchedCrestronDevice = crestronDevicesUpdated.stream()
							.filter(cdu -> null != cdu.getDeviceId() && cdu.getDeviceId().equals(aggregatedDeviceId)).findFirst().orElse(null);
					Map<String, String> matchedCrestronDeviceStatistics = matchedCrestronDevice.getStatistics();
					String updatedValue = matchedCrestronDeviceStatistics.get(propertyName);
					int updatedIntegerValue = Integer.parseInt(updatedValue);
					// TODO: where is the updated boolean value test?
					if (updatedIntegerValue != newIntegerValue) {
						if (null != newBooleanValue && newBooleanValue.equals(existingBooleanValue)) {
							fail("control/statistics value was not updated property");
						}
					}
				}
			}

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public static int randomInt(int min, int max) {
		Random random = new Random();
		int n = max - min + 1;
		int i = random.nextInt(max) % n;
		return min + i;
	}

}
