/*
 * Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.crestron;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;
import com.avispl.symphony.dal.communicator.HttpCommunicator;

/**
 * SYAL-4005: runs the adapter against a locally running symphony-crestron-xio-simulator while the simulator throttles
 * it, and checks that no 429 is ever reported to Symphony. <br>
 * The simulator is reset by every test. Its port defaults to 8085 and can be changed with {@code -Dxio.simulator.port}.
 *
 * @author Symphony Dev Team<br>
 * Created on Oct 28, 2026
 * @since 2.0.8
 */
@Tag("simulatorTest")
public class CrestronXiOSimulatorThrottlingTest {
	private static final int PORT = Integer.getInteger("xio.simulator.port", 8085);
	private static final String ADMIN = "http://localhost:" + PORT + "/api/simulator";
	private static final String ACCOUNT_ID = "4fc3d09b-cb3b-407c-9bf0-8ae3c80f7082";
	private static final String SUBSCRIPTION_ID = "40c66e5f01704bc0ab1010657d38a6f0";

	private final HttpClient admin = HttpClient.newHttpClient();
	private CrestronXiO crestronXiO;

	@BeforeEach
	public void init() throws Exception {
		send("POST", "/reset", null);
		crestronXiO = new CrestronXiO();
		crestronXiO.setProtocol("http");
		crestronXiO.setHost("localhost");
		crestronXiO.setPort(PORT);
		crestronXiO.setContentType("application/json");
		crestronXiO.setAuthenticationScheme(HttpCommunicator.AuthenticationScheme.None);
		crestronXiO.setLogin(ACCOUNT_ID);
		crestronXiO.setPassword(SUBSCRIPTION_ID);
	}

	@AfterEach
	public void cleanup() throws Exception {
		if (crestronXiO != null) {
			crestronXiO.destroy();
		}
		send("POST", "/reset", null);
	}

	/**
	 * The production shape: an account with more pages than its device status allowance, so part of every cycle is
	 * throttled by the API itself. Every device must still be collected, and no error ever reported.
	 */
	@Test
	public void accountOverItsAllowanceIsCollectedWithoutErrors() throws Exception {
		addDevices(70); // 4 pages
		// the adapter paces 4 pages at one per 15 s by itself, so only a tighter allowance than that throttles it
		setLimits(1, 30, 200, 60);
		crestronXiO.init();

		List<AggregatedDevice> devices = pumpWithoutErrors(240000, 70);

		assertEquals(70, devices.size());
	}

	/**
	 * Every device status request of a cycle is throttled, so the adapter spends all its retries: nothing is
	 * reported, and the devices collected before are kept.
	 */
	@Test
	public void cycleThrottledThroughoutKeepsDevicesAndReportsNothing() throws Exception {
		addDevices(5);
		setLimits(200, 60, 200, 60);
		crestronXiO.init();
		pumpWithoutErrors(30000, 5);

		addFault("{\"endpoint\":\"deviceStatusPage\",\"status\":429,\"trigger\":\"ALWAYS\",\"retryAfterSeconds\":1,\"retryAfterHeader\":false}");
		// 10 attempts, each throttled twice (adapter and HttpClient) for 1 s
		List<AggregatedDevice> devices = pumpWithoutErrors(40000, Integer.MAX_VALUE);

		assertEquals(5, devices.size(), "devices collected before the throttling must be kept");
		assertNull(crestronXiO.getApiError());
	}

	/**
	 * Throttled account groups degrade device naming only, and hold nothing back.
	 */
	@Test
	public void throttledGroupsDoNotHoldBackDeviceStatistics() throws Exception {
		addDevices(5);
		setLimits(200, 60, 200, 60);
		addFault("{\"endpoint\":\"groups\",\"status\":429,\"trigger\":\"ALWAYS\",\"retryAfterSeconds\":1}");
		crestronXiO.init();

		assertEquals(5, pumpWithoutErrors(30000, 5).size());
	}

	/**
	 * Calls retrieveMultipleStatistics() once a second, the way Symphony does, failing on the first error reported.
	 *
	 * @param durationMs how long to keep calling at most
	 * @param expectedDevices stop as soon as this many devices are reported
	 * @return devices reported by the last call
	 */
	private List<AggregatedDevice> pumpWithoutErrors(long durationMs, int expectedDevices) throws Exception {
		long deadline = System.currentTimeMillis() + durationMs;
		List<AggregatedDevice> devices = List.of();
		while (System.currentTimeMillis() < deadline) {
			try {
				devices = crestronXiO.retrieveMultipleStatistics();
			} catch (Exception e) {
				fail("the adapter reported an error while being throttled", e);
			}
			if (devices.size() >= expectedDevices) {
				return devices;
			}
			Thread.sleep(1000);
		}
		assertTrue(expectedDevices == Integer.MAX_VALUE, "expected " + expectedDevices + " devices, got " + devices.size());
		return devices;
	}

	private void addDevices(int count) throws Exception {
		for (int i = 0; i < count; i++) {
			send("POST", "/devices", "{\"model\":\"TSW-1070\",\"category\":\"TouchPanel\",\"manufacturer\":\"Crestron\"}");
		}
	}

	private void setLimits(int v2Limit, int v2WindowSeconds, int v1Limit, int v1WindowSeconds) throws Exception {
		send("PATCH", "/ratelimit", "{\"enabled\":true,\"buckets\":{"
				+ "\"v2\":{\"limit\":" + v2Limit + ",\"windowSeconds\":" + v2WindowSeconds + "},"
				+ "\"v1\":{\"limit\":" + v1Limit + ",\"windowSeconds\":" + v1WindowSeconds + "}}}");
	}

	private void addFault(String rule) throws Exception {
		send("POST", "/faults", rule);
	}

	private void send(String method, String path, String json) throws Exception {
		HttpRequest request = HttpRequest.newBuilder(URI.create(ADMIN + path))
				.header("Content-Type", "application/json")
				.method(method, json == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(json))
				.build();
		HttpResponse<String> response = admin.send(request, HttpResponse.BodyHandlers.ofString());
		assertTrue(response.statusCode() < 300, method + " " + path + " -> " + response.statusCode() + ": " + response.body());
	}
}
