/*
 * Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.crestron;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;
import com.avispl.symphony.api.dal.error.CommandFailureException;
import com.avispl.symphony.dal.communicator.HttpCommunicator;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.SingleRootFileSource;

/**
 * Drives a real device statistics collection cycle of {@link CrestronXiO} against a stubbed XiO API, to verify that the
 * API status published to Symphony follows the outcome of the most recent cycle.
 * <p>
 * Where {@link CrestronXiOApiStatusTest} covers the decision in isolation, this test covers the wiring around it: an
 * error captured by a page result reaching {@code apiError} through the collection cycle, and being dropped again once
 * a later cycle succeeds. That recovery is the point of SYAL-3868, where the adapter had to be deactivated and
 * reactivated by hand because a cached error could never be cleared.
 *
 * @author Symphony Dev Team<br>
 * Created on Sep 18, 2026
 * @since 2.0.6
 */
@Tag("Mock")
public class CrestronXiOApiStatusWireMockTest {
	private static final String ACCOUNT_ID = "d65d142a-804f-4cd5-83a1-16b1d2f405c2";
	private static final String SUBSCRIPTION_ID = "40c66e5f01704bc0ab1010657d38a6f0";
	private static final String STATISTICS_URL = "/api/V2/device/accountid/" + ACCOUNT_ID + "/pageno/1/pageSize/20/status";
	private static final String GROUPS_URL = "/api/v1/group/accountid/" + ACCOUNT_ID + "/groups";

	/** Upper bound for a cycle to run and publish its outcome. A cycle is driven by retrieveMultipleStatistics(). */
	private static final long CYCLE_TIMEOUT_MS = 30000;

	private static final String FORBIDDEN_BODY = "{\"Severity\":2,\"StatusCode\":403,"
			+ "\"Message\":\"The Public API feature is available with XiO Cloud Premium subscription.Please upgrade to access.\",\"ExceptionType\":\"\"}";

	private static final String PAGE_BODY = "{"
			+ "\"Pagination\":{\"TotalDevices\":2,\"TotalPages\":1,\"PageSize\":20,\"CurrentPageNumber\":1},"
			+ "\"DeviceList\":["
			+ device("5f3bc53205db4a48ab993b48", "TSW-1060-00107FBB8503") + ","
			+ device("5f3bc532c62411fa5cd84cfe", "TSW-1060-00107FBB8504")
			+ "]}";

	private WireMockServer wireMockServer;
	private CrestronXiO crestronXiO;

	@BeforeEach
	public void init() throws Exception {
		// an empty file source keeps the 6 MB of recorded mappings in src/test/resources out of this test
		Path emptyMappings = Files.createTempDirectory("xio-wiremock");
		wireMockServer = new WireMockServer(options().dynamicPort().bindAddress("127.0.0.1")
				.fileSource(new SingleRootFileSource(emptyMappings.toFile())));
		wireMockServer.start();

		crestronXiO = new CrestronXiO();
		crestronXiO.setTrustAllCertificates(true);
		crestronXiO.setProtocol("http");
		crestronXiO.setContentType("application/json");
		crestronXiO.setHost("127.0.0.1");
		crestronXiO.setPort(wireMockServer.port());
		crestronXiO.setAuthenticationScheme(HttpCommunicator.AuthenticationScheme.None);
		crestronXiO.setLogin(ACCOUNT_ID);
		crestronXiO.setPassword(SUBSCRIPTION_ID);
	}

	@AfterEach
	public void cleanup() {
		if (crestronXiO != null) {
			crestronXiO.destroy();
		}
		if (wireMockServer != null) {
			wireMockServer.stop();
		}
	}

	/**
	 * The SYAL-3868 recovery: the API rejects statistics requests with 403, the adapter reports it, and as soon as the
	 * API answers again the adapter clears the error by itself, with no deactivate/reactivate in between.
	 */
	@Test
	public void adapterRecoversFromApiErrorWithoutBeingRestarted() throws Exception {
		stubGroups(500);
		stubStatistics(403, FORBIDDEN_BODY);
		crestronXiO.init();

		runCyclesUntil(() -> crestronXiO.getApiError() != null, "the failing cycle to publish its error");

		Exception published = crestronXiO.getApiError();
		assertInstanceOf(CommandFailureException.class, published, "an answer from the API must be reported as such");
		assertEquals(403, ((CommandFailureException) published).getStatusCode());

		// the API starts answering again, without touching the adapter
		stubStatistics(200, PAGE_BODY);

		runCyclesUntil(() -> crestronXiO.getApiError() == null, "a successful cycle to clear the error");

		List<AggregatedDevice> devices = crestronXiO.retrieveMultipleStatistics();
		assertEquals(2, devices.size(), "statistics must be collected once the API answers again");
	}

	/**
	 * A failing account groups request degrades device naming only, and must never be reported as an API error: it is
	 * deliberately swallowed by the collection cycle.
	 */
	@Test
	public void accountGroupsFailureIsNotReportedAsApiError() throws Exception {
		stubGroups(500);
		stubStatistics(200, PAGE_BODY);
		crestronXiO.init();

		runCyclesUntil(() -> !pump().isEmpty(), "the first cycle to collect devices");

		assertNull(crestronXiO.getApiError(), "a failing account groups request must not affect the API status");
	}

	/**
	 * A transient failure is replaced by whatever the following cycle observes, so the reported cause is never older
	 * than the last cycle. This is the shape of the production incident, where a read timeout was reported for hours
	 * while the API was in fact answering 403.
	 */
	@Test
	public void laterCycleReplacesEarlierErrorCause() throws Exception {
		stubGroups(500);
		// connection reset stands in for the transport failure that used to get cached forever
		wireMockServer.stubFor(get(urlEqualTo(STATISTICS_URL))
				.willReturn(aResponse().withFault(com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER)));
		crestronXiO.init();

		runCyclesUntil(() -> crestronXiO.getApiError() != null, "the transport failure to be published");
		Exception transportError = crestronXiO.getApiError();
		assertNotNull(transportError);
		assertFalse(transportError instanceof CommandFailureException, "precondition: a transport failure, not an API answer");

		stubStatistics(403, FORBIDDEN_BODY);

		runCyclesUntil(() -> crestronXiO.getApiError() instanceof CommandFailureException, "the 403 to replace the transport failure");
		assertEquals(403, ((CommandFailureException) crestronXiO.getApiError()).getStatusCode());
	}

	private void stubStatistics(int status, String body) {
		wireMockServer.stubFor(get(urlEqualTo(STATISTICS_URL))
				.willReturn(aResponse().withStatus(status).withHeader("Content-Type", "application/json").withBody(body)));
	}

	private void stubGroups(int status) {
		wireMockServer.stubFor(get(urlEqualTo(GROUPS_URL))
				.willReturn(aResponse().withStatus(status).withHeader("Content-Type", "application/json").withBody("{}")));
	}

	/**
	 * Drives collection cycles until the given condition holds. <br>
	 * Calling retrieveMultipleStatistics() is what keeps the adapter from being considered paused and is what releases
	 * the data loader into its next cycle, so it is also how Symphony itself paces the adapter. It is expected to throw
	 * while an API error is published, which is exactly what this test asserts on.
	 */
	private void runCyclesUntil(BooleanSupplier condition, String what) throws Exception {
		long deadline = System.currentTimeMillis() + CYCLE_TIMEOUT_MS;
		while (System.currentTimeMillis() < deadline) {
			pump();
			if (condition.getAsBoolean()) {
				return;
			}
			Thread.sleep(200);
		}
		fail("Timed out after " + CYCLE_TIMEOUT_MS + " ms waiting for " + what);
	}

	/**
	 * Performs one retrieveMultipleStatistics() call the way the Symphony monitoring worker does, swallowing the error
	 * the adapter reports while an API error is published.
	 *
	 * @return devices currently held by the adapter, empty if the call reported an error
	 */
	private List<AggregatedDevice> pump() {
		try {
			return crestronXiO.retrieveMultipleStatistics();
		} catch (Exception e) {
			return Collections.emptyList();
		}
	}

	private static String device(String cid, String name) {
		return "{\"device\":{"
				+ "\"device-cid\":\"" + cid + "\","
				+ "\"device-id\":\"" + cid + "\","
				+ "\"device-name\":\"" + name + "\","
				+ "\"device-model\":\"TSW-1060\","
				+ "\"device-category\":\"TouchPanel\","
				+ "\"device-manufacturer\":\"Crestron\","
				+ "\"device-groupid\":\"fcb4d298-21b5-4132-a171-2c0f28409c44\","
				+ "\"device-status\":\"Online\","
				+ "\"serial-number\":\"1806JBH14888\"}}";
	}
}
