/*
 * Copyright (c) 2019-2021 AVI-SPL, Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.crestron;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import javax.security.auth.login.FailedLoginException;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;

import com.avispl.symphony.api.common.error.NotImplementedException;
import com.avispl.symphony.api.dal.control.Controller;
import com.avispl.symphony.api.dal.dto.control.ControllableProperty;
import com.avispl.symphony.api.dal.dto.monitor.ExtendedStatistics;
import com.avispl.symphony.api.dal.dto.monitor.Statistics;
import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;
import com.avispl.symphony.api.dal.error.CommandFailureException;
import com.avispl.symphony.api.dal.monitor.Monitorable;
import com.avispl.symphony.api.dal.monitor.aggregator.Aggregator;
import com.avispl.symphony.dal.aggregator.parser.AggregatedDeviceProcessor;
import com.avispl.symphony.dal.aggregator.parser.PropertiesMapping;
import com.avispl.symphony.dal.aggregator.parser.PropertiesMappingParser;
import com.avispl.symphony.dal.communicator.RestCommunicator;

/**
 * Implements Aggregator client for Crestron XiO controllers.
 * <p>
 * Remote API this adapter utilizes is REST based and uses aggressive rate limiting
 * by sending 429 response codes.
 * <p>
 * Remote API requires authentication by 2 parameters: accountId and subscriptionId.
 * These parameters must be set as JavaBean properties upon adapter initialization process.
 * <p>
 *
 * @author Symphony Dev Team<br>
 * Created on May 26, 2019
 */
public class CrestronXiO extends RestCommunicator implements Aggregator, Controller, Monitorable {
	/**
	 * Wraps metadata of a single page in a device status list.
	 * 
	 * @since 2.0
	 */
	static final class Page {
		final List<String> deviceIds;
		final String deviceModel;
		final int number;
		final int size;
		final int totalDevices;
		final int totalPages;
		final Exception error;

		/**
		 * Page constructor.
		 * 
		 * @param number page number
		 * @param size page size. Note that it is requested page size, not the actual one. The actual page size can be determined from {@code deviceIds} list
		 * @param deviceModel optional device model filter, can be {@code null}
		 * @param totalDevices total number of devices available
		 * @param totalPages total number of pages available
		 * @param deviceIds list of device ids wrapped in this page result
		 * @param error optional error (if any occurred while fetching page result)
		 */
		Page(int number, int size, String deviceModel, int totalDevices, int totalPages, List<String> deviceIds, Exception error) {
			super();
			this.number = number;
			this.size = size;
			this.deviceModel = deviceModel;
			this.totalDevices = totalDevices;
			this.totalPages = totalPages;
			this.deviceIds = deviceIds;
			this.error = error;
		}
	}

    /**
     * Account identifier to fetch devices for
     */
    private String accountId;

    /**
     * Crestron XIO subscription ID for authentication against their API
     */
    private String subscriptionId;

    /**
     * Devices this aggregator is responsible for
     */
    private Map<String, AggregatedDevice> aggregatedDevices = new ConcurrentHashMap<>();

    /**
     * Interceptor for RestTemplate that injects
     * authorization header and fixes malformed headers sent by XIO backend
     */
    private ClientHttpRequestInterceptor xioHeaderInterceptor = new CrestronXioHeaderInterceptor();

    /**
     * Instance of device data loader worker
     */
    private CrestronXioDeviceDataLoader deviceDataLoader;

    /**
     * This parameter holds timestamp of when we can perform next API request for retrieving devices metadata (device list)
     */
    private volatile long nextDevicesListRetryTs;

    /**
     * This parameter holds timestamp of when we can perform next API request for retrieving device statistics
     * So the maximal available fetch rate is utilized based on value from Retry-After response header
     */
    private volatile long nextDeviceStatusRetryTs;

    /**
     * This parameter holds timestamp of when we need to stop performing API calls
     * It used when device stop retrieving statistic. Updated each time of called #retrieveMultipleStatistics
     */
    private volatile long validRetrieveStatisticsTimestamp;

    /**
     * Aggregator inactivity timeout. If the {@link CrestronXiO#retrieveMultipleStatistics()}  method is not
     * called during this period of time - device is considered to be paused, thus the Cloud API
     * is not supposed to be called
     */
    private static final long retrieveStatisticsTimeOut = 3 * 60 * 1000;

    /**
     * Max size of device statistics that can be collected in a single request
     */
    private static final int deviceStatisticsCollectionBatchSize = 20;

    /**
     * Number of threads in a thread pool reserved for the device statistics collection
     */
    private static final int deviceStatisticsCollectionThreads = 5;
    
    /**
     * Interval of single device monitoring cycle.
     */
    private static final long deviceStatisticsMonitoringCycle = 60000; // ms

    /**
     * Indicates whether this device is considered as paused.
     * True by default so if the system is rebooted and the actual value is lost -> the device won't start statistics
     * collection unless the {@link CrestronXiO#retrieveMultipleStatistics()} method is called which will change it
     * to a correct value
     */
    private volatile boolean devicePaused = true;

    private AggregatedDeviceProcessor aggregatedDeviceProcessor;
    private ExecutorService devicesCollectionExecutor;

    private LinkedList<Future<Page>> devicesExecutionPool = new LinkedList<>();
    private Set<String> availableModels = new HashSet<>();
    private ReentrantLock controlLock = new ReentrantLock();

	/**
	 * Device model filter. If provided, only devices of given models will be monitored.
	 * @since 2.0
	 */
	List<String> deviceModelFilter;
	/**
	 * For {@link PacingMode.INTERVAL_BASED}, contains min interval between individual device status requests.
	 */
	long minDeviceStatusRequestInterval = 333; // ms, based on max rate of 200 requests in 60 seconds
	/**
	 * For {@link PacingMode.INTERVAL_BASED}, contains adjusted interval between individual device status requests. This interval is calculated as
	 * {@code (device monitoring cycle) / (number of aggregated devices)}. It cannot be less than {@code minDeviceStatusRequestInterval}.
	 */
	long deviceStatusRequestInterval = minDeviceStatusRequestInterval; // ms
	/**
	 * For {@link PacingMode.INTERVAL_BASED}, contains timestamp of next queued individual device status request.
	 */
	final AtomicLong nextDeviceStatusRequestTs = new AtomicLong();
	
	/**
	 * Holds last API error from device list request (if any).
	 */
	private Exception apiError;
	
	/** Whether service is running. */
	private volatile boolean serviceRunning;

    /**
     * Create executor which will handle background jobs for polling devices
     * since there's a thread running constantly that collects the general devices list and
     * is responsible for launching per-device statistics collection -
     * the thread pool size is 1+{@code CrestronXiO#deviceStatisticsCollectionThreads} since the
     * /status request rate is still limited
     *
     * @throws Exception while creating thread pool or during the {@code CrestronXiO#initAggregatedDevicesProcessor()}
     */
    @Override
    protected void internalInit() throws Exception {
    	// make sure we have enough connections for each thread communicating with XiO
    	final int workerThreads = deviceStatisticsCollectionThreads + 1;
    	setMaxConnectionsPerRoute(workerThreads);
    	setMaxConnectionsTotal(workerThreads);

        super.internalInit();

        devicesCollectionExecutor = Executors.newFixedThreadPool(workerThreads);
        devicesCollectionExecutor.submit(deviceDataLoader = new CrestronXioDeviceDataLoader());
        initAggregatedDevicesProcessor();
        serviceRunning = true;
    }

    /**
     * Initialize aggregated device processor based on the mapping stored in xio/model-mapping.yml
     * so the AggregatedDevice instances are created properly.
     * Also values from /aggregator.properties are used in order to configure the default behavior -
     * whether to keep or not the generic devices mapping (the devices that don't have explicitly defined mapping config
     * in a .yml file)
     *
     * @throws IOException in case mapping file and properties file are not available.
     */
    private void initAggregatedDevicesProcessor() throws IOException {
        Map<String, PropertiesMapping> models = new PropertiesMappingParser()
                .loadYML("xio/model-mapping.yml", getClass());

        Properties properties = new Properties();
        properties.load(getClass().getResourceAsStream("/aggregator.properties"));

        availableModels = models.keySet();
        if (Boolean.parseBoolean(properties.getProperty("skipUnknownModelsMapping"))) {
            models.remove("base");
            // TODO why do we clear the map?
            aggregatedDevices.clear();
        }
        aggregatedDeviceProcessor = new AggregatedDeviceProcessor(models);
    }

	@Override
	public void controlProperty(ControllableProperty controllableProperty) throws Exception {
		// checkApiStatus();

		throw new NotImplementedException(controllableProperty.getProperty() + " control is not suppported by " + getClass().getSimpleName() + " adapter");
	}

	@Override
	public void controlProperties(List<ControllableProperty> list) throws Exception {
		if (CollectionUtils.isEmpty(list)) {
			throw new IllegalArgumentException("Controllable properties cannot be null or empty");
		}

		for (ControllableProperty controllableProperty : list) {
			controlProperty(controllableProperty);
		}
	}

    /**
     * @return pingTimeout value if host is not reachable within
     * the pingTimeout, a ping time in milliseconds otherwise
     * if ping is 0ms it's rounded up to 1ms to avoid IU issues on Symphony portal
     * @throws Exception if any error occurs
     */
    @Override
    public int ping() throws Exception {
    	if (!isInitialized()) {
			throw new IllegalStateException("Cannot use CrestronXiO adapter without it being initialized first");
    	}

        long pingResultTotal = 0L;

        for (int i = 0; i < this.getPingAttempts(); i++) {
            long startTime = System.currentTimeMillis();

            try (Socket puSocketConnection = new Socket(this.getHost(), this.getPort())) {
                puSocketConnection.setSoTimeout(this.getPingTimeout());

                if (puSocketConnection.isConnected()) {
                    long endTime = System.currentTimeMillis();
                    long pingResult = endTime - startTime;
                    pingResultTotal += pingResult;
                    if (this.logger.isTraceEnabled()) {
                        this.logger.trace(String.format("PING OK: Attempt #%s to connect to %s on port %s succeeded in %s ms", i + 1, this.getHost(), this.getPort(), pingResult));
                    }
                } else {
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug(String.format("PING DISCONNECTED: Connection to %s did not succeed within the timeout period of %sms", this.getHost(), this.getPingTimeout()));
                    }
                    return this.getPingTimeout();
                }
            } catch (SocketTimeoutException tex) {
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug(String.format("PING TIMEOUT: Connection to %s did not succeed within the timeout period of %sms", this.getHost(), this.getPingTimeout()));
                }
                return this.getPingTimeout();
            }
        }
        return Math.max(1, Math.toIntExact(pingResultTotal / this.getPingAttempts()));
    }

    /**
     * Here we add additional interceptor to RestTemplate that performs following tasks
     * <ul>
     *     <li>add authentication headers to each request</li>
     *     <li>fixes malformed content-type headers that XiO sends in some types of responses</li>
     *     <li>tracks responses with code 429 and amount of seconds implementation must wait until next request might be performed</li>
     * </ul>
     */
    @Override
    protected RestTemplate obtainRestTemplate() throws Exception {
        RestTemplate restTemplate = super.obtainRestTemplate();

        if (restTemplate.getInterceptors() == null)
            restTemplate.setInterceptors(new ArrayList<>());

        if (!restTemplate.getInterceptors().contains(xioHeaderInterceptor))
            restTemplate.getInterceptors().add(xioHeaderInterceptor);

        return restTemplate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void internalDestroy() {
		serviceRunning = false;

		if (deviceDataLoader != null) {
			deviceDataLoader.stop();
			deviceDataLoader = null;
		}

		devicesExecutionPool.forEach(future -> future.cancel(true));
		devicesExecutionPool.clear(); // do not nullify, was not created in init()

		if (devicesCollectionExecutor != null) {
			devicesCollectionExecutor.shutdownNow();
			devicesCollectionExecutor = null;
		}
		
		aggregatedDevices.clear();

		super.internalDestroy();
    }


    /**
     * In order to control internal aggregator properties - controls are implemented which should
     * be populated within the statistics payload.
     *
     * @return List<Statistics> containing the controls and statistics properties
     */
    @Override
    public List<Statistics> getMultipleStatistics() throws Exception {
    	checkApiStatus();

        ExtendedStatistics extendedStatistics = new ExtendedStatistics();
        Map<String, String> stats = new HashMap<>();
        controlLock.lock();
        try {
        	// TODO add uptime and version
            //stats.put("", "");
        } finally {
            controlLock.unlock();
        }

        extendedStatistics.setStatistics(stats);
        return Collections.singletonList(extendedStatistics);
    }

    /**
     * {@inheritDoc}
     * The timestamp for collected devices has to be updated every monitoring cycle since large amount of
     * devices takes longer to go through (consider looping/requesting stats), so the device info is not
     * considered stale. The device info list is still relevant.
     */
    @Override
    public List<AggregatedDevice> retrieveMultipleStatistics() throws Exception {
    	// this has to be the fist call in this method to indicate that statistics retrieval attempt was made and device is not paused
        updateValidRetrieveStatisticsTimestamp();

    	checkApiStatus();

        return new ArrayList<>(aggregatedDevices.values());
    }

    private synchronized void updateValidRetrieveStatisticsTimestamp() {
        validRetrieveStatisticsTimestamp = System.currentTimeMillis() + retrieveStatisticsTimeOut;
        updateAggregatorStatus();
    }

    /**
     * Adds authentication headers to each request to Crestron XiO API
     */
    @Override
    protected HttpHeaders putExtraRequestHeaders(HttpMethod httpMethod, String uri, HttpHeaders headers) throws Exception {
        headers.add("XiO-subscription-key", getSubscriptionId());
        return headers;
    }

    /**
     * Update the status of the device.
     * The device is considered as paused if did not receive any retrieveMultipleStatistics()
     * calls during {@link CrestronXiO#validRetrieveStatisticsTimestamp}
     */
    private synchronized void updateAggregatorStatus() {
        devicePaused = validRetrieveStatisticsTimestamp < System.currentTimeMillis();
    }

	/**
	 * Fetches, deserializes and stores device details in the internal storage.
	 *
	 * @param pageNumber requested device statistics list page number
	 * @param pageSize requested device statistics list page size
	 * @param deviceModel requested device model filter (optional)
	 * @return metadata of processed device statistics list page
	 * @throws Exception if error occurs while fetching device statistics
	 */
	private Page processDeviceStatistics(int pageNumber, int pageSize, String deviceModel) throws Exception {
		JsonNode deviceStatisticsMap;
		long scannedAt;
		try {
			deviceStatisticsMap = fetchDeviceStatistics(pageNumber, pageSize, deviceModel);
			scannedAt = System.currentTimeMillis();
			updateApiStatus(null);
		} catch (CommandFailureException e) {
			// not related to API status
			throw e;
		} catch (Exception e) {
			updateApiStatus(e);
			throw e;
		}

		/* Response:
		{
			"Pagination": {       
				"TotalDevices": 248,     
				"TotalPages": 13,        
				"PageSize": 20,        
				"CurrentPageNumber": 1    
			},
			"DeviceList": {
				{Device 1 status},
				{Device 2 status},
				...
				{Device 20 status}
			}
		}*/
		List<String> processedDeviceIds = new ArrayList<>(pageSize);
		JsonNode deviceStatisticsList = deviceStatisticsMap.findValue("DeviceList");
		if (deviceStatisticsList != null && deviceStatisticsList.isArray()) {
			for (JsonNode deviceStatistics : deviceStatisticsList) {
				String deviceId = updateAggregatedDevice(deviceStatistics, scannedAt);
				if (deviceId != null && deviceId.length() > 0) {
					processedDeviceIds.add(deviceId);
				}
			}
		}

		int totalDevices = deviceStatisticsMap.findValue("TotalDevices").asInt();
		int totalPages = deviceStatisticsMap.findValue("TotalPages").asInt();
		return new Page(pageNumber, pageSize, deviceModel, totalDevices, totalPages, processedDeviceIds, null);
	}

	/**
	 * Retrieves detailed information about given devices including device statistics.
	 * 
	 * @param pageNumber page number to fetch
	 * @param pageSize page size to fetch
	 * @param deviceModel optional device model filter for fetch request
	 * @return {@link JsonNode} instance with list of device statistics
	 */
	private JsonNode fetchDeviceStatistics(int pageNumber, int pageSize, String deviceModel) throws Exception {
		// e.g. https://api.crestron.io/api/V2/device/accountid/a5683c5d-b47e-4a1d-8f3c-a7aa9bb3906f/pageno/1/pageSize/20/status
		// or https://api.crestron.io/api/V2/device/accountid/a5683c5d-b47e-4a1d-8f3c-a7aa9bb3906f/deviceModel/TSW-760/pageno/1/pageSize/20/status
		StringBuilder requestUri = new StringBuilder(144);
		requestUri.append("api/V2/device/accountid/");
		requestUri.append(accountId);
		// device model filter is optional
		if (deviceModel != null && deviceModel.length() > 0) {
			requestUri.append("/deviceModel/");
			requestUri.append(deviceModel);
		}
		requestUri.append("/pageno/");
		requestUri.append(pageNumber);
		requestUri.append("/pageSize/");
		requestUri.append(pageSize);
		requestUri.append("/status");

		paceDeviceStatusRequest();

		// need to check whether service was not stopped while thread was sleeping in paceDeviceStatusRequest
		if (serviceRunning) {
			// do not uncomment next section unless needed for debugging!
//			long start = System.currentTimeMillis();
//			java.text.DateFormat df = new java.text.SimpleDateFormat("HH:mm:ss.SSS");
//			try {
//				JsonNode result = doGet(requestUri.toString(), JsonNode.class);
//				long end = System.currentTimeMillis();
//				System.out.println(df.format(new java.util.Date(start)) + "\tFetched status of "
//						+ (deviceModel != null && deviceModel.length() > 0 ? deviceModel + " " : "") + "devices in " + (end - start) + " ms. Page number: "
//						+ pageNumber + ", page size: " + pageSize);
//				return result;
//			} catch (Exception e) {
//				if (serviceRunning) {
//					System.out.println(df.format(new java.util.Date(start)) + "\tError fetching status of "
//							+ (deviceModel != null && deviceModel.length() > 0 ? deviceModel + " " : "") + "devices. Page number: " + pageNumber
//							+ ", page size: " + pageSize);
//					e.printStackTrace();
//				}
//				throw e;
//			}

			return doGet(requestUri.toString(), JsonNode.class);
		} else {
			return null;
		}
	}

	/**
	 * Populates {@link AggregatedDevice} device statistics.
	 *
	 * @param deviceNode {@link JsonNode} instance to take statistics from
	 * @param scannedAt timestamp of when aggregated device was scanned
	 * @return aggregated device id, or {@code null} if device could not be parsed or model is not supported
	 */
	private String updateAggregatedDevice(JsonNode deviceNode, long scannedAt) {
		String deviceId = null;
		JsonNode deviceIdNode = deviceNode.findValue("device-cid");
		if (deviceIdNode != null) {
			deviceId = deviceIdNode.asText();
			if (deviceId != null && deviceId.length() > 0) {
				controlLock.lock();
				try {
					JsonNode modelNameNode = deviceNode.findValue("device-model");
					// the mapper will fall back to the "generic" detailed mapping if no "*-detailed" model mapping is created
					String modelName = modelNameNode == null ? "generic" : modelNameNode.asText() + "-detailed";
					AggregatedDevice aggregatedDevice = aggregatedDevices.get(deviceId);
					if (aggregatedDevice != null) {
						aggregatedDeviceProcessor.applyProperties(aggregatedDevice, deviceNode, modelName);
					} else {
						// new device
						aggregatedDevice = new AggregatedDevice();
						// TODO the below code used to add new device does not work, all base model filtering needs to be revisited
						aggregatedDeviceProcessor.applyProperties(aggregatedDevice, deviceNode, modelName);
						// String modelName = modelNameNode == null ? "base" : modelNameNode.asText();
						// the mapper will filter out devices if model is marked as skipped (???)
						//aggregatedDeviceProcessor.applyProperties(aggregatedDevice, deviceNode, availableModels.contains(modelName) ? modelName : "base");
						deviceId = aggregatedDevice.getDeviceId();
						if (deviceId != null && deviceId.length() > 0) {
							aggregatedDevices.put(deviceId, aggregatedDevice);
						}
					}
					aggregatedDevice.setTimestamp(scannedAt);
				} finally {
					controlLock.unlock();
				}
			}
		}
		return deviceId;
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AggregatedDevice> retrieveMultipleStatistics(List<String> deviceIds) throws Exception {
    	// this has to be the fist call in this method to indicate that statistics retrieval attempt was made and device is not paused
        updateValidRetrieveStatisticsTimestamp();

    	checkApiStatus();

        if (deviceIds == null || deviceIds.isEmpty())
            return Collections.emptyList();

        return retrieveMultipleStatistics().stream().filter(s ->
                deviceIds.contains(s.getDeviceId())).collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void authenticate() throws Exception {
        // trying to fetch account details to check if account/subscription identifiers are valid
        try {
            doGet("/api/v1/account/accountid/" + getAccountId() + "/account", JsonNode.class);
        } catch (CommandFailureException e) {
            if (e.getStatusCode() == 401)
                throw new FailedLoginException("Crestron XiO subscription ID is invalid " + getSubscriptionId());
            else if (e.getStatusCode() == 403)
                throw new FailedLoginException("Access denied to API with account " +
                        getAccountId() + " and subscription ID " + getSubscriptionId());
            throw e;
        } catch (Exception e) {
            throw e;
        }
    }

	/**
	 * This method will verify aggregated device list for any deleted devices. <br>
	 * If list contains any devices which did not appear in the last N monitored cycles, it will remove device from the list of monitored devices.
	 *
	 * @param monitoredDeviceIds map of monitored device ids per device model from last monitoring cycle
	 */
	void reconcileAggregatedDeviceList(Map<String, Set<String>> monitoredDeviceIds) {
		controlLock.lock();
		try {
			Iterator<AggregatedDevice> existingDevices = aggregatedDevices.values().iterator();
			while (existingDevices.hasNext()) {
				AggregatedDevice existingDevice = existingDevices.next();
				String existingDeviceId = existingDevice.getDeviceId();
				String existingDeviceModel = existingDevice.getDeviceModel();
				for (Map.Entry<String, Set<String>> monitoredDeviceIdsEntry : monitoredDeviceIds.entrySet()) {
					String modelFilter = monitoredDeviceIdsEntry.getKey();
					if (modelFilter == null || modelFilter.length() == 0 || modelFilter.equals(existingDeviceModel)) {
						if (!monitoredDeviceIdsEntry.getValue().contains(existingDeviceId)) {
							existingDevices.remove();
							break;
						}
					}
				}
			}
		} finally {
			controlLock.unlock();
		}
	}

    /**
     * Retrieves account identifier to fetch devices for
     *
     * @return account identifier to fetch devices for
     */
    public String getAccountId() {
        return accountId;
    }

    /**
     * Sets account identifiers to fetch devices for
     *
     * @param accountId account identifier to fetch devices for
     */
    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    /**
     * Retrieves Crestron XIO subscription ID for authentication against their API
     *
     * @return Crestron XIO subscription ID for authentication against their API
     */
    public String getSubscriptionId() {
        return subscriptionId;
    }

    /**
     * Sets Crestron XIO subscription ID for authentication against their API
     *
     * @param subscriptionId Crestron XIO subscription ID for authentication against their API
     */
    public void setSubscriptionId(String subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    /**
     * Returns timestamp of when we can perform next API request
     * to fetch the devices metadata
     *
     * @return long timestamp
     */
    public long getNextDevicesListRetryTs() {
        return nextDevicesListRetryTs;
    }

    /**
     * Returns timestamp of when we can perform next API request
     * to fetch the detailed statistics of the device
     *
     * @return long timestamp
     */
    public long getNextDeviceStatusRetryTs() {
        return nextDeviceStatusRetryTs;
    }

    /**
     * Check whether the device is considered as paused or not
     *
     * @return boolean
     */
    public boolean isDevicePaused() {
        return devicePaused;
    }

    /**
     * Sets timestamp of when we can perform next API request to get the devices metadata
     *
     * @param ts timestamp
     */
    public void setNextDevicesListRetryTs(long ts) {
        this.nextDevicesListRetryTs = ts;
    }

    /**
     * Sets timestamp of when we can perform next API request to get the device detailed statistics
     *
     * @param ts timestamp
     */
    public synchronized void setNextDeviceStatusRetryTs(long ts) {
        if (ts > this.nextDevicesListRetryTs) {
            this.nextDeviceStatusRetryTs = ts;
        }
    }

    /**
     * Interceptor for RestTemplate that injects
     * authorization header and fixes malformed headers sent by XIO backend
     */
    class CrestronXioHeaderInterceptor implements ClientHttpRequestInterceptor {
        @Override
        public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
            // workaround for fixing invalid headers from the server
        	// TODO check if we still need it
            ClientHttpResponse response = execution.execute(request, body);
            response.getHeaders().set("Content-Type", "application/json; charset=utf-8");
            return response;
        }
    }

    /**
     * Represents main background loop that fetches device
     * information from the Crestron XiO API
     */
    class CrestronXioDeviceDataLoader implements Runnable {
        /**
         * Controls whether CrestronXioDeviceDataLoader main loop should continue
         */
        private volatile boolean doProcess;

        /**
         * No-arg constructor
         */
        public CrestronXioDeviceDataLoader() {
            doProcess = true;
        }

        /**
         * Main processing loop
         */
        @Override
        public void run() {
			// scan loop has following boundaries: it starts by fetching first page of device statistics,
			// and lasts until all available pages of device statistics are retrieved
        	int lastTotalPages = 0;
        	long nextLoopTs = 0;
			mainloop: while (doProcess) {
				long sleepFor;
				if (nextLoopTs > 0) {
					sleepFor = nextLoopTs - System.currentTimeMillis();
					nextLoopTs += deviceStatisticsMonitoringCycle;
				} else {
					// for the very first loop give it 500 ms for adapter to initialize
					sleepFor = 500;
					nextLoopTs = System.currentTimeMillis() + deviceStatisticsMonitoringCycle;
				}

				if (sleepFor > 0) {
					try {
						TimeUnit.MILLISECONDS.sleep(sleepFor);
					} catch (InterruptedException e) {
						// ignore, if thread was requested to stop, main loop will break
					}
				}

				// if external process asked adapter to stop, we exit here immediately
				if (!doProcess) {
					break mainloop;
				}

				// next line will determine whether XiO monitoring was paused
				boolean deviceWasPaused = devicePaused;
				updateAggregatorStatus();
				if (devicePaused) {
					if (!deviceWasPaused) {
						logger.info("Device adapter did not receive retrieveMultipleStatistics call in more than " + retrieveStatisticsTimeOut / 1000
								+ " s. Statistics retrieval is suspended");
					}
					continue mainloop;
				}

				long collectionStartTs = System.currentTimeMillis();
				int collectedDevices = 0;

				if (logger.isDebugEnabled()) {
					logger.debug("Starting device statistics collection cycle");
				}

				try {
					// query the very first page(s) to calculate number of batches
					// if device model filter is in place, we need batches per model
					Map<String, AtomicInteger> batchCounts = new TreeMap<>();
					// monitored device ids per device model filter (from the results)
					Map<String, Set<String>> monitoredDeviceIds = new HashMap<>();
					if (deviceModelFilter == null) {
						batchCounts.put("", new AtomicInteger());
						monitoredDeviceIds.put("", new HashSet<String>());
					} else {
						for (String deviceModel : deviceModelFilter) {
							batchCounts.put(deviceModel, new AtomicInteger());
							monitoredDeviceIds.put(deviceModel, new HashSet<String>());
						}
					}

					int newTotalDeviceCount = 0;
					int newTotalPageCount = 0;

					// do a first loop to fetch first page from each batch
					for (String modelFilter : batchCounts.keySet()) {
						if (doProcess && !devicePaused) {
							devicesExecutionPool
									.add(devicesCollectionExecutor.submit(() -> retrieveDeviceStatistics(1, deviceStatisticsCollectionBatchSize, modelFilter)));
						} else {
							monitoredDeviceIds.clear();
							break;
						}
					}

					if (!doProcess) {
						// no need to wait for results
						break mainloop;
					}

					List<Page> collectedPages = collectDeviceStatiticResults(monitoredDeviceIds);
					for (Page page : collectedPages) {
						newTotalDeviceCount += page.totalDevices;
						collectedDevices += page.deviceIds.size();
						newTotalPageCount += page.totalPages;
						if (page.totalPages > 1) {
							batchCounts.get(page.deviceModel).set(page.totalPages);
						} else {
							// the only page available is already done
							batchCounts.remove(page.deviceModel);
						}
					}

					// adjust pacing interval if needed
					if (newTotalPageCount != lastTotalPages) {
						long interval = newTotalPageCount > 1
								? Math.max(deviceStatisticsMonitoringCycle / newTotalPageCount, minDeviceStatusRequestInterval)
								: minDeviceStatusRequestInterval;
						if (interval != deviceStatusRequestInterval) {
							logger.info("Adjusting device statistics pacing interval for " + newTotalPageCount + " aggregated devices from "
									+ deviceStatusRequestInterval + " to " + interval + " ms");
							deviceStatusRequestInterval = interval;
						}
						lastTotalPages = newTotalPageCount;
					}

					if (newTotalDeviceCount == 0) {
						// reconcile device list in case if any of devices were deleted
						if (!monitoredDeviceIds.isEmpty()) {
							reconcileAggregatedDeviceList(monitoredDeviceIds);
						}
						// no devices to monitor, wait for next cycle
						continue mainloop;
					}

					if (!batchCounts.isEmpty()) {
						// fetch remaining pages
						fetchLoop: for (Map.Entry<String, AtomicInteger> batchCount : batchCounts.entrySet()) {
							String model = batchCount.getKey();
							int count = batchCount.getValue().get();
							// start with 2 - first page was already collected
							for (int n = 2; n <= count; ++n) {
								if (devicePaused || !doProcess) {
									// stop fetching
									monitoredDeviceIds.clear();
									break fetchLoop;
								}
								final int batch = n;
								devicesExecutionPool.add(
										devicesCollectionExecutor.submit(() -> retrieveDeviceStatistics(batch, deviceStatisticsCollectionBatchSize, model)));
							}
						}
					}

					if (doProcess) {
						// wait until all worker threads have completed and gather processed device ids
						collectedPages = collectDeviceStatiticResults(monitoredDeviceIds);
						for (Page page : collectedPages) {
							collectedDevices += page.deviceIds.size();
						}

						if (doProcess && !monitoredDeviceIds.isEmpty()) {
							// reconcile device list in case if any of devices were deleted
							reconcileAggregatedDeviceList(monitoredDeviceIds);
						}
					}
				} catch (Throwable e) {
					logger.error("Uncaught error during device statistics collection cycle", e);
				} finally {
					long duration = System.currentTimeMillis() - collectionStartTs;
					logger.info("Finished device statistics collection cycle in " + duration + " ms. Devices collected: " + collectedDevices);
				}
			}
		}

		/**
		 * Triggers main loop to stop
		 */
		public void stop() {
			doProcess = false;
		}

		private List<Page> collectDeviceStatiticResults(Map<String, Set<String>> monitoredDeviceIds) {
			List<Page> collectedPages = new ArrayList<>();
			do {
				Future<Page> future = devicesExecutionPool.getFirst();
				try {
					Page page = future.get();
					if (page.error == null) {
						collectedPages.add(page);
						Set<String> deviceIds = monitoredDeviceIds.get(page.deviceModel);
						if (deviceIds != null) {
							deviceIds.addAll(page.deviceIds);
						}
					} else {
						// cannot reconcile list for device model if error
						monitoredDeviceIds.remove(page.deviceModel);
					}
				} catch (Exception e) {
					// either execution exception, or cancelled
					// in any case we cannot reconcile monitored device ids for deletion until we have the full list
					monitoredDeviceIds.clear();
				}
				devicesExecutionPool.removeFirst();
			} while (doProcess && !devicesExecutionPool.isEmpty());

			return collectedPages;
		}
	}

    private void paceDeviceStatusRequest() {
		long now = System.currentTimeMillis();
		long next = nextDeviceStatusRequestTs.getAndAccumulate(now, (x, y) -> (Math.max(x, y) + deviceStatusRequestInterval));
		long timeToWait = next - now;

		if (timeToWait > 0) {
			try {
				TimeUnit.MILLISECONDS.sleep(timeToWait);
			} catch (InterruptedException e) {
				// the only interruption would be when service being stopped
			}
		}
    }

	/**
	 * Retrieve device statistics for given device ids and save it to indicate that the device data has been fetched successfully. <br>
	 * Note that if an error occurs, this method will not report it directly but rather log it and update {@code apiError} instance variable.
	 *
	 * @param pageNumber page number to retrieve statistics for
	 * @param pageSize page number to retrieve statistics for
	 * @param deviceModel optional device model filter to retrieve statistics with
	 */
	Page retrieveDeviceStatistics(int pageNumber, int pageSize, String deviceModel) {
		int retryAttempts = 0;
		Exception lastError = null;
		while (retryAttempts++ < 10 && serviceRunning) {
			try {
				Page processedDeviceIds = processDeviceStatistics(pageNumber, pageSize, deviceModel);
				if (logger.isDebugEnabled()) {
					logger.debug(String.format("Retrieved device statistics for " + pageSize + " devices on page " + pageNumber
							+ (deviceModel != null && deviceModel.length() > 0 ? " (device model: " + deviceModel + ")" : "") + "; attempt: " + retryAttempts));
				}
				return processedDeviceIds;
			} catch (CommandFailureException e) {
				lastError = e;
				if (e.getStatusCode() != 429) {
					// Might be 401, 403 or any other error code here so the code will just get stuck
					// cycling this failed request until it's fixed. So we need to skip this scenario.
					logger.error("Crestron XiO API error " + e.getStatusCode() + " while retrieving statistics for " + pageSize + " devices on page "
							+ pageNumber + (deviceModel != null && deviceModel.length() > 0 ? " (device model: " + deviceModel + ")" : ""), e);
					break;
				}
			} catch (Exception e) {
				lastError = e;
				// if service is running, log error
				if (serviceRunning) {
					logger.error("Crestron XiO API error while retrieving statistics for " + pageSize + " devices on page " + pageNumber
							+ (deviceModel != null && deviceModel.length() > 0 ? " (device model: " + deviceModel + ")" : ""), e);
				}
				break;
			}
		}
		if (retryAttempts == 10 && serviceRunning) {
			// if we got here, all 10 attempts failed
			logger.error("Failed to retrieve statistic due to Crestron XiO API throttling for " + pageSize + " devices on page " + pageNumber
					+ (deviceModel != null && deviceModel.length() > 0 ? " (device model: " + deviceModel + ")" : ""));
		}
		return new Page(pageNumber, pageSize, deviceModel, 0, 0, null, lastError);
	}

	/**
	 * Check status of adapter APIs.
	 *
	 * @throws IllegalStateException if adapter is used without being initialized first
	 * @throws Exception if API call to XiO produced an error
	 */
	private void checkApiStatus() throws Exception {
		if (!isInitialized()) {
			throw new IllegalStateException("Cannot use CrestronXiO adapter without it being initialized first");
		}

		Exception error;
		controlLock.lock();
		try {
			error = apiError;
		} finally {
			controlLock.unlock();
		}

		if (error != null) {
			throw error;
		}
	}

	/**
	 * Check status of adapter APIs.
	 *
	 * @throws IllegalStateException if adapter is used without being initialized first
	 * @throws Exception if API call to XiO produced an error
	 */
	private void updateApiStatus(Exception error) {
		controlLock.lock();
		try {
			apiError = error;
		} finally {
			controlLock.unlock();
		}
	}

	/**
	 * Retrieves {@code deviceModelFilter} property. <br>
	 * If provided and not empty, this property instructs adapter to only monitor device of given models. <br>
	 * Note that this getter returns unmodifiable view of device models list.
	 *
	 * @return list of device models to monitor
	 * @since 2.0
	 */
	public List<String> setDeviceModelFilter() {
		return deviceModelFilter != null ? Collections.unmodifiableList(deviceModelFilter) : null;
	}

	/**
	 * Sets {@code deviceModelFilter} property. <br>
	 * If provided and not empty, this property instructs adapter to only monitor device of given models.
	 *
	 * @param deviceModelFilter list of device models to monitor
	 * @since 2.0
	 */
	public void setDeviceModelFilter(List<String> deviceModelFilter) {
		this.deviceModelFilter = deviceModelFilter != null ? new ArrayList<>(deviceModelFilter) : null;
	}

    /**
     * This method is for compatibility with Symphony communicator infrastructure
     * as Symphony is not capable of setting arbitrary properties on the communicator class.
     * In order to propagate accountId required by Crestron we use "login" property that Symphony is aware of
     *
     * @return value of Crestron API account ID
     */
    @Override
    public String getLogin() {
        return getAccountId();
    }

    /**
     * This method is for compatibility with Symphony communicator infrastructure
     * as Symphony is not capable of setting arbitrary properties on the communicator class.
     * In order to propagate accountId required by Crestron we use "login" property that Symphony is aware of
     *
     * @param login value of Crestron API account ID
     */
    @Override
    public void setLogin(String login) {
        setAccountId(login);
    }

    /**
     * This method is for compatibility with Symphony communicator infrastructure
     * as Symphony is not capable of setting arbitrary properties on the communicator class.
     * In order to propagate subscriptionId required by Crestron we use "password" property that Symphony is aware of
     *
     * @return value of Crestron API subscription ID
     */
    @Override
    public String getPassword() {
        return getSubscriptionId();
    }

    /**
     * This method is for compatibility with Symphony communicator infrastructure
     * as Symphony is not capable of setting arbitrary properties on the communicator class.
     * In order to propagate subscriptionId required by Crestron we use "password" property that Symphony is aware of
     *
     * @param password value of Crestron API subscription ID
     */
    @Override
    public void setPassword(String password) {
        setSubscriptionId(password);
    }
}