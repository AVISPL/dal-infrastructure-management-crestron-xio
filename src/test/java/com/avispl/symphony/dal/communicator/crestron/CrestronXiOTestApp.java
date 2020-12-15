/*
 * Copyright (c) 2020 AVI-SPL, Inc. All Rights Reserved.
 */

package com.avispl.symphony.dal.communicator.crestron;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;
import com.avispl.symphony.dal.communicator.HttpCommunicator;
import com.avispl.symphony.dal.communicator.crestron.CrestronXiO.ScannedAggregatedDevice;
import com.avispl.symphony.dal.communicator.crestron.CrestronXiO.ScannedDeviceKey;
import com.avispl.symphony.dal.communicator.crestron.CrestronXiO.ScannedDeviceKeyComparator;

/**
 * Test app for CrestronXiO adapter.
 *
 * @author Vlad Kasyanenko / Symphony Dev Team <br>
 *         Created on Dec 7, 2020
 * @since 5.1
 */
public class CrestronXiOTestApp {

	/**
	 * Main entry of CrestronXiOTestApp. <br>
	 * NOTE: need to pause at least 2 min between consecuent runs of this app due to device list API pacing on XiO side.
	 *
	 * @param args test app arguments (reserved for future use)
	 */
	public static void main(String[] args) {
		System.out.println("Entering CrestronXiOTestApp");

		final CrestronXiO crestronXiO = new CrestronXiO();
		crestronXiO.setTrustAllCertificates(true);
		crestronXiO.setContentType("application/json");
		crestronXiO.setProtocol("https");
		crestronXiO.setPort(443);
		crestronXiO.setHost("api.crestron.io");
		crestronXiO.setAuthenticationScheme(HttpCommunicator.AuthenticationScheme.None);
		crestronXiO.setLogin("d65d142a-804f-4cd5-83a1-16b1d2f405c2"); // AVI-SPL XiO account
		crestronXiO.setPassword("3ccb51db641f4bb8b22fde31723a0d17");

		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

		try {
			crestronXiO.init();
			crestronXiO.retrieveMultipleStatistics(); // this is to un-pause monitoring

			TimeUnit.MILLISECONDS.sleep(1000); // give it some time to start up

			final long startTime = System.currentTimeMillis();
			System.out.println("Start time: " + dateFormat.format(new Date(startTime)));

			for (int i = 0; i < 60; ++i) {
				System.out.print("*");
				TimeUnit.MILLISECONDS.sleep(1000);
			}

			final long endTime = System.currentTimeMillis();
			System.out.println();
			System.out.println("End time: " + dateFormat.format(new Date(endTime)));

			List<AggregatedDevice> devices = crestronXiO.retrieveMultipleStatistics();
			int totalCount = devices.size();
			List<ScannedDeviceKey> deviceKeys = new ArrayList<>(totalCount);
			for (AggregatedDevice device : devices) {
				deviceKeys.add(new ScannedDeviceKey(device.getDeviceId(), ((ScannedAggregatedDevice) device).getScannedAt()));
			}
			deviceKeys.sort(new ScannedDeviceKeyComparator());

			int scannedCount = 0;
			for (ScannedDeviceKey deviceKey : deviceKeys) {
				Long scannedAt = deviceKey.getScannedAt();
				if (scannedAt != null) {
					++scannedCount;
					System.out.println("Device " + deviceKey.getDeviceId() + " scanned at " + dateFormat.format(new Date(scannedAt)));
				} else {
					System.out.println("Device " + deviceKey.getDeviceId() + " not scanned");
				}
			}
			System.out.println("Total aggregated devices: " + totalCount);
			System.out.println("Scanned aggregated devices: " + scannedCount);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				crestronXiO.destroy();
			} catch (Exception e) {
			}
		}

		System.out.println("Leaving CrestronXiOTestApp");
	}
}
