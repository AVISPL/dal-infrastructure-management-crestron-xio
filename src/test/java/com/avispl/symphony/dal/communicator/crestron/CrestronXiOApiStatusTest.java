/*
 * Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.crestron;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.net.SocketTimeoutException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.avispl.symphony.api.dal.error.CommandFailureException;

/**
 * Verifies how {@link CrestronXiO} evaluates its API status across device statistics collection cycles.
 * <p>
 * Regression coverage for SYAL-3868: the API error used to be cached by the first failing request and could never be
 * cleared or replaced afterwards, because the only code path clearing it required a successful request, which stopped
 * happening as soon as the XiO API started answering with HTTP errors. As a result the adapter kept reporting a stale
 * transport error for hours while the API was in fact answering 403, and only a full deactivate/reactivate of the
 * device recovered it.
 *
 * @author Symphony Dev Team<br>
 * Created on Sep 18, 2026
 * @since 2.0.6
 */
@Tag("Mock")
public class CrestronXiOApiStatusTest {
	private static final String ADDRESS = "api.crestron.io";
	private static final String REQUEST = "https://api.crestron.io:443/api/V2/device/accountid/acc/pageno/1/pageSize/20/status";

	private CrestronXiO crestronXiO;

	@BeforeEach
	public void init() {
		crestronXiO = new CrestronXiO();
	}

	/**
	 * The exact SYAL-3868 scenario: a transient read timeout is followed by cycles that fail with 403, because the
	 * account is not entitled to the public API. The adapter must report the 403 rather than the timeout it saw first.
	 */
	@Test
	public void forbiddenCycleReplacesEarlierTimeout() {
		Exception timeout = timeout();
		crestronXiO.updateApiStatusForCycle(false, timeout);
		assertSame(timeout, crestronXiO.getApiError());

		Exception forbidden = commandFailure(403);
		crestronXiO.updateApiStatusForCycle(false, forbidden);

		assertSame(forbidden, crestronXiO.getApiError(), "cycle outcome must replace the error reported by a previous cycle");
	}

	/**
	 * A cycle that retrieved data clears the error of previous cycles, so the adapter recovers on its own.
	 */
	@Test
	public void successfulCycleClearsPreviousError() {
		crestronXiO.updateApiStatusForCycle(false, commandFailure(403));
		assertNotNull(crestronXiO.getApiError(), "precondition: an error is published");

		crestronXiO.updateApiStatusForCycle(true, null);

		assertNull(crestronXiO.getApiError(), "a cycle that collected data must clear the API error");
	}

	/**
	 * Partially successful cycle: the API is reachable and the collected pages hold valid data, so no error is
	 * reported even though some pages failed.
	 */
	@Test
	public void partiallySuccessfulCycleReportsNoError() {
		crestronXiO.updateApiStatusForCycle(true, commandFailure(429));

		assertNull(crestronXiO.getApiError(), "at least one retrieved page means the API is reachable");
	}

	/**
	 * A cycle that attempted nothing (adapter paused, or stopped mid cycle) says nothing about the API and must leave
	 * the previously published status untouched.
	 */
	@Test
	public void cycleWithoutAttemptedPagesKeepsPreviousStatus() {
		Exception forbidden = commandFailure(403);
		crestronXiO.updateApiStatusForCycle(false, forbidden);

		crestronXiO.updateApiStatusForCycle(false, null);

		assertSame(forbidden, crestronXiO.getApiError());
	}

	/**
	 * An answer from the API names an actionable cause and is preferred over a transport failure, regardless of the
	 * order the two are observed in within the cycle.
	 */
	@Test
	public void apiAnswerIsPreferredOverTransportFailure() {
		Exception timeout = timeout();
		Exception forbidden = commandFailure(403);

		assertSame(forbidden, crestronXiO.preferredApiError(timeout, forbidden));
		assertSame(forbidden, crestronXiO.preferredApiError(forbidden, timeout));
	}

	/**
	 * With nothing else to compare against, whichever error is available is the one to report.
	 */
	@Test
	public void singleErrorIsAlwaysSelected() {
		Exception timeout = timeout();

		assertSame(timeout, crestronXiO.preferredApiError(null, timeout));
		assertSame(timeout, crestronXiO.preferredApiError(timeout, null));
		assertNull(crestronXiO.preferredApiError(null, null));
	}

	/**
	 * Tearing the adapter down drops the API error, so a following lifecycle does not start out reporting a failure
	 * that belongs to the previous one. This is what deactivating and reactivating the device used to achieve by
	 * discarding the adapter instance altogether.
	 */
	@Test
	public void destroyClearsApiError() {
		crestronXiO.updateApiStatusForCycle(false, commandFailure(403));
		assertNotNull(crestronXiO.getApiError(), "precondition: an error is published");

		crestronXiO.destroy();

		assertNull(crestronXiO.getApiError(), "destroy must not leave an API error behind");
	}

	private Exception commandFailure(int statusCode) {
		return new CommandFailureException(ADDRESS, REQUEST,
				"{\"StatusCode\":" + statusCode + ",\"Message\":\"The Public API feature is available with XiO Cloud Premium subscription.\"}", statusCode);
	}

	private Exception timeout() {
		return new SocketTimeoutException("Read timed out");
	}
}
