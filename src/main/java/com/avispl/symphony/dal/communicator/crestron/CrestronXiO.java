/*
 * Copyright (c) 2019-2020 AVI-SPL, Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.crestron;

import com.avispl.symphony.api.dal.control.Controller;
import com.avispl.symphony.api.dal.dto.control.AdvancedControllableProperty;
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import javax.security.auth.login.FailedLoginException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
 * @author Sergey Blazchenko / Symphony Dev Team<br>
 * Created on May 26, 2019
 * @since 4.7
 */
public class CrestronXiO extends RestCommunicator implements Aggregator, Controller, Monitorable {
	/**
	 * Wraps aggregated device and timestamp when it was last scanned for status.
	 */
	static final class ScannedAggregatedDevice extends AggregatedDevice {
		@JsonIgnore
		private Long scannedAt;
		
		ScannedAggregatedDevice() {
			super();
		}
		
		Long getScannedAt() {
			return scannedAt;
		}
		
		void setScannedAt(Long scannedAt) {
			this.scannedAt = scannedAt;
		}
	}

	/**
	 * Wraps key of aggregated device scanned for status.
	 */
	static final class ScannedDeviceKey {
		final String deviceId;
		final Long scannedAt;

		ScannedDeviceKey(String deviceId, Long scannedAt) {
			super();
			this.deviceId = deviceId;
			this.scannedAt = scannedAt;
		}

		String getDeviceId() {
			return deviceId;
		}

		Long getScannedAt() {
			return scannedAt;
		}

		@Override
		public String toString() {
			return "{\"deviceId\":\"" + deviceId + "\",\"scannedAt\":" + scannedAt + "}";
		}
	}

	/**
	 * Comparator for aggregated devices scanned for status.
	 */
	static final class ScannedDeviceKeyComparator implements Comparator<ScannedDeviceKey> {
		/**
		 * Comparison of {@code ScannedDeviceKey} objects is done based on value of {@code scannedAt} property using logic and requirements below.<br>
		 * <br>
		 * {@inheritDoc}
		 */
		@Override
		final public int compare(ScannedDeviceKey o1, ScannedDeviceKey o2) {
			final long scannedAt1 = o1 != null && o1.scannedAt != null ? o1.scannedAt.longValue() : 0;
			final long scannedAt2 = o2 != null && o2.scannedAt != null ? o2.scannedAt.longValue() : 0;
			final long delta = scannedAt1 - scannedAt2;
			if (delta > Integer.MAX_VALUE) {
				return Integer.MAX_VALUE;
			}
			if (delta < Integer.MIN_VALUE) {
				return Integer.MIN_VALUE;
			}
			return (int) delta;
		}
	}

    private enum CallContext {DEVICE_LIST, DEVICE_STATUS}
    private enum PacingMode {INTERVAL_BASED, RETRY_AFTER}

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
    private Map<String, ScannedAggregatedDevice> aggregatedDevices = new ConcurrentHashMap<>();

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
     * Time period within which the device metadata (basic devices information) cannot be refreshed.
     * If ignored if device list is not yet retrieved or the cached device list is empty {@link CrestronXiO#aggregatedDevices}
     */
    private volatile long validDeviceMetaDataRetrievalPeriodTimestamp;

    /**
     * Aggregator inactivity timeout. If the {@link CrestronXiO#retrieveMultipleStatistics()}  method is not
     * called during this period of time - device is considered to be paused, thus the Cloud API
     * is not supposed to be called
     */
    private static final long retrieveStatisticsTimeOut = 3 * 60 * 1000;

    /**
     * If the {@link CrestronXiO#deviceMetaDataInformationRetrievalTimeout} is set to a value that is too small -
     * devices list will be fetched too frequently. In order to avoid this - the minimal value is based on this value.
     */
    private static final long defaultMetaDataTimeout = 2 * 60 * 1000;

    /**
     * Number of threads in a thread pool reserved for the device statistics collection
     */
    private static final int deviceStatisticsCollectionThreads = 20;
    
    /**
     * Interval of single device monitoring cycle.
     */
    private static final long deviceStatisticsMonitoringCycle = 60000; // ms

    /**
     * Device metadata retrieval timeout. The general devices list is retrieved once during this time period.
     */
    private long deviceMetaDataInformationRetrievalTimeout = 30 * 60 * 1000;

    /**
     * Indicates whether a device is considered as paused.
     * True by default so if the system is rebooted and the actual value is lost -> the device won't start stats
     * collection unless the {@link CrestronXiO#retrieveMultipleStatistics()} method is called which will change it
     * to a correct value
     */
    private volatile boolean devicePaused = true;

    private AggregatedDeviceProcessor aggregatedDeviceProcessor;
    private ExecutorService devicesCollectionExecutor;

    private List<Future> devicesExecutionPool = new ArrayList<>();
    private Set<String> availableModels = new HashSet<>();
    private ReentrantLock controlLock = new ReentrantLock();

	/**
	 * Defines pacing mode for device status requests. <br>
	 * For {@link PacingMode.INTERVAL_BASED}, pacing is done by maintaining min interval between consecutive device status requests sent. E.g., to achieve 20
	 * requests per second rate, individual requests will be paced with 50 ms interval. <br>
	 * For {@link PacingMode.RETRY_AFTER}, pacing is done based on value {@code Retry-After} header from {@code 429 (Too Many Requests)} server response. Note
	 * that precision of that header is in seconds, therefore such pacing cannot be done if multiple requests per second are required.
	 */
	PacingMode deviceStatusPacingMode = PacingMode.INTERVAL_BASED;
	/**
	 * For {@link PacingMode.INTERVAL_BASED}, contains min interval between individual device status requests.
	 */
	long minDeviceStatusRequestInterval = 50; // ms
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
     * the thread pool size is 1+{@link CrestronXiO#deviceStatisticsCollectionThreads} since the
     * /status request rate is still limited
     *
     * @throws Exception while creating thread pool or during the {@link CrestronXiO#initAggregatedDevicesProcessor()}
     */
    @Override
    protected void internalInit() throws Exception {
    	// make sure we have enough connections for each thread
    	setMaxConnectionsPerRoute(deviceStatisticsCollectionThreads);
    	setMaxConnectionsTotal(deviceStatisticsCollectionThreads);
    	
        super.internalInit();

        devicesCollectionExecutor = Executors.newFixedThreadPool(1 + deviceStatisticsCollectionThreads);
        devicesCollectionExecutor.submit(deviceDataLoader = new CrestronXioDeviceDataLoader());
        initAggregatedDevicesProcessor();
        serviceRunning = true;
    }

    /**
     * Initialize aggregated device processor based on the mapping stored in xio/model-mapping.yml
     * so the AggregatedDevice instances are created properly.
     * Also values from /aggregator.properties are used in order to configure the default behaviour -
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
            aggregatedDevices = new ConcurrentHashMap<>();
        }
        aggregatedDeviceProcessor = new AggregatedDeviceProcessor(models);
        validDeviceMetaDataRetrievalPeriodTimestamp = System.currentTimeMillis();
    }

    @Override
    public void controlProperty(ControllableProperty controllableProperty) throws Exception {
    	checkApiStatus();

        String property = controllableProperty.getProperty();
        String value = String.valueOf(controllableProperty.getValue());

        controlLock.lock();
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Received control property %s with value %s", property, value));
            }
            switch (property) {
                case "Device List Retrieval Timeout (ms)":
                    deviceMetaDataInformationRetrievalTimeout = Long.parseLong(String.valueOf(value));
                    break;
                default:
                    logger.debug("Operation " + property + " is not implemented.");
                    break;
            }
        } finally {
            controlLock.unlock();
        }
    }

    @Override
    public void controlProperties(List<ControllableProperty> list) throws Exception {
    	checkApiStatus();

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

		if (devicesCollectionExecutor != null) {
			devicesCollectionExecutor.shutdown();
			devicesCollectionExecutor = null;
		}

		devicesExecutionPool.forEach(future -> future.cancel(true));
		devicesExecutionPool.clear(); // do not nullify, was not created in init()
		
		aggregatedDevices.clear();

		super.internalDestroy();
    }


    /**
     * In order to control internal aggregator properties - controls are implemented which should
     * be populated within the statistics payload.
     * Currently there is a single setting - {@link CrestronXiO#deviceMetaDataInformationRetrievalTimeout}
     *
     * @return List<Statistics> containing the controls and statistics properties
     */
    @Override
    public List<Statistics> getMultipleStatistics() throws Exception {
    	checkApiStatus();

        ExtendedStatistics extendedStatistics = new ExtendedStatistics();
        Map<String, String> stats = new HashMap<>();
        List<AdvancedControllableProperty> controls = new ArrayList<>();
        controlLock.lock();
        try {
            stats.put("Device List Retrieval Timeout (ms)", "");

            AdvancedControllableProperty metadataTimeout = new AdvancedControllableProperty();
            AdvancedControllableProperty.Numeric timeoutControl = new AdvancedControllableProperty.Numeric();
            metadataTimeout.setType(timeoutControl);
            metadataTimeout.setName("Device List Retrieval Timeout (ms)");
            metadataTimeout.setValue(deviceMetaDataInformationRetrievalTimeout);
            metadataTimeout.setTimestamp(new Date());

            controls.add(metadataTimeout);
        } finally {
            controlLock.unlock();
        }

        extendedStatistics.setStatistics(stats);
        extendedStatistics.setControllableProperties(controls);
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
    	checkApiStatus();

        updateValidRetrieveStatisticsTimestamp();
        // TODO this should be removed and we should use actual status retrieval time after we get API from Crestron for bulk status retrieval!
        aggregatedDevices.values().forEach(aggregatedDevice -> aggregatedDevice.setTimestamp(System.currentTimeMillis()));
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
     * Loads, deserializes and stores devices metadata in the internal storage
     */
    private void processAvailableDevicesMetadata() throws Exception {
        if (aggregatedDevices.size() > 0 && validDeviceMetaDataRetrievalPeriodTimestamp > System.currentTimeMillis()) {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("General devices metadata retrieval is in cooldown. %s seconds left",
                        (validDeviceMetaDataRetrievalPeriodTimestamp - System.currentTimeMillis()) / 1000));
            }
            return;
        }

        JsonNode availableDevices;
        try {
	        availableDevices = fetchAvailableDevices();
	        if (logger.isDebugEnabled()) {
	            logger.debug(String.format("Received devices metadata at %s. Devices list size: %s", new Date(), availableDevices.size()));
	        }
	        // reset API error if any
       		updateApiStatus(null);
       } catch (Exception e) {
        	if (e instanceof CommandFailureException && ((CommandFailureException) e).getStatusCode() == 429) {
        		// XiO throttling error, will recover
        	} else {
        		// API error, need to report
        		updateApiStatus(e);
        	}
        	throw e;
        }

        // retrieving list of active device ids
        Set<String> liveDevices = StreamSupport
                .stream(availableDevices.spliterator(), false)
                .map(n -> n.findPath("device-cid").asText())
                .collect(Collectors.toSet());

        // parse the device list
        List<ScannedAggregatedDevice> devices = new ArrayList<>();
        availableDevices.forEach(jsonNode -> {
            JsonNode modelNameNode = jsonNode.get("device-model");
            String modelName = modelNameNode == null ? "base" : modelNameNode.asText();
            ScannedAggregatedDevice device = new ScannedAggregatedDevice();
            aggregatedDeviceProcessor.applyProperties(device, jsonNode, availableModels.contains(modelName) ? modelName : "base");
            if (!StringUtils.isEmpty(device.getDeviceId())) {
                devices.add(device);
            }
        });
        devices.sort(Comparator.comparing(AggregatedDevice::getDeviceId));

        controlLock.lock();
        try {
        	// remove stale devices which are not reported by XiO anymore
        	Iterator<Map.Entry<String, ScannedAggregatedDevice>> existingList = aggregatedDevices.entrySet().iterator();
        	while (existingList.hasNext()) {
        		Map.Entry<String, ScannedAggregatedDevice> entry = existingList.next();
        		if (!liveDevices.contains(entry.getKey())) {
        			ScannedAggregatedDevice value = entry.getValue();
        			logger.info("Removing device " + value.getDeviceName() + " with id " + value.getDeviceId() + " - it is not reported by XiO device list API anymore");
        			existingList.remove();
        		}
        	}

        	// update remaining devices
			devices.forEach(aggregatedDevice -> {
                if (aggregatedDevices.containsKey(aggregatedDevice.getDeviceId())) {
                    aggregatedDevices.get(aggregatedDevice.getDeviceId()).setDeviceOnline(aggregatedDevice.getDeviceOnline());
                } else {
                    aggregatedDevices.put(aggregatedDevice.getDeviceId(), aggregatedDevice);
                }
            });
        } finally {
            controlLock.unlock();
        }
    }

    /**
     * Checks whether we can issue an API request
     * taking into account Too-Many-Requests response from the server and seconds to wait
     *
     * @return boolean value indicating if the api is not supposed to be called at the time
     */
    private boolean isApiBlocked(CallContext context) {
        switch (context) {
            case DEVICE_LIST:
                if (getNextDevicesListRetryTs() == 0) {
                    return false;
                }
                return getNextDevicesListRetryTs() > System.currentTimeMillis();
            case DEVICE_STATUS:
                if (getNextDeviceStatusRetryTs() == 0) {
                    return false;
                }
                return getNextDeviceStatusRetryTs() > System.currentTimeMillis();
            default:
                logger.debug("Unsupported call context: " + context);
                return false;
        }
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
	 * Loads, deserializes and stores device details in the internal storage
	 *
	 * @param deviceId Device ID to process statistics for
	 */
	private void processDeviceStatistics(String deviceId) throws Exception {
		JsonNode deviceStatistics;
		long scannedAt;
		try {
			deviceStatistics = fetchDeviceStatistics(deviceId);
			scannedAt = System.currentTimeMillis();
			updateApiStatus(null);
		} catch (CommandFailureException e) {
			// not related to API status
			throw e;
		} catch (Exception e) {
			updateApiStatus(e);
			throw e;
		}
		updateAggregatedDevice(deviceId, deviceStatistics, scannedAt);
	}

    /**
     * Retrieves information about available devices.
     *
     * @return {@link JsonNode} instance with information about available devices
     */
    private JsonNode fetchAvailableDevices() throws Exception {
        JsonNode response = doGet("api/v1/device/accountid/" + getAccountId() + "/devices", JsonNode.class);
        controlLock.lock();
        try {
            if (response != null && !response.isNull() && response.size() > 0) {
                validDeviceMetaDataRetrievalPeriodTimestamp = System.currentTimeMillis() + Math.max(defaultMetaDataTimeout, deviceMetaDataInformationRetrievalTimeout);
            }
        } finally {
            controlLock.unlock();
        }
        return response;
    }

    /**
     * Retrieves detailed information about given device including device statistics
     *
     * @param deviceId Device ID to fetch statistics for
     * @return {@link JsonNode} instance with information about available devices
     */
    private JsonNode fetchDeviceStatistics(String deviceId) throws Exception {
		paceDeviceStatusRequest();
		return doGet("api/v2/device/accountid/" + getAccountId() + "/devicecid/" + deviceId + "/status", JsonNode.class);
    }

    /**
     * Populates {@link AggregatedDevice} device statistics
     *
     * @param deviceId device id used to query device info
     * @param deviceNode {@link JsonNode} instance to take statistics from
     * @param scannedAt timestamp of when aggregated device was scanned
     */
    private void updateAggregatedDevice(String deviceId, JsonNode deviceNode, long scannedAt) {
        controlLock.lock();
        try {
            ScannedAggregatedDevice aggregatedDevice = aggregatedDevices.get(deviceId);
            boolean newDevice = false;
            if (aggregatedDevice == null) {
                aggregatedDevice = new ScannedAggregatedDevice();
                newDevice = true;
            }
            JsonNode modelNameNode = deviceNode.findValue("device-model");
            String modelName = modelNameNode == null ? "generic" : modelNameNode.asText() + "-detailed";
            // The mapper will fall back to the "generic" detailed mapping if no "*-detailed" model mapping is created
            aggregatedDeviceProcessor.applyProperties(aggregatedDevice, deviceNode, modelName);
            // it was noted that for few devices XiO returns no detailed info (not even device id)
            // in this case use device id from the request - it is needed to build proper list of device keys
            String parsedDeviceId = aggregatedDevice.getDeviceId();
            if (parsedDeviceId == null || parsedDeviceId.length() == 0) {
            	aggregatedDevice.setDeviceId(deviceId);
            }
            aggregatedDevice.setScannedAt(scannedAt);
            Map<String, String> deviceProperties = aggregatedDevice.getProperties();
            if (deviceProperties == null) {
            	aggregatedDevice.setProperties(deviceProperties = new HashMap<>());
            }
            deviceProperties.put("~StatusQueried", new SimpleDateFormat("MM/dd/yyyy HH:mm:ss z").format(new Date(scannedAt)));
            if (newDevice) {
            	aggregatedDevices.put(deviceId, aggregatedDevice);
            }
        } finally {
            controlLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AggregatedDevice> retrieveMultipleStatistics(List<String> deviceIds) throws Exception {
    	checkApiStatus();

        updateValidRetrieveStatisticsTimestamp();

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
            ClientHttpResponse response = execution.execute(request, body);
            response.getHeaders().set("Content-Type", "application/json; charset=utf-8");

            if (response.getHeaders().containsKey("Retry-After")) {
                if (!request.getURI().getPath().endsWith("/status")) {
                    setNextDevicesListRetryTs(System.currentTimeMillis() +
                            (Integer.parseInt(response.getHeaders().get("Retry-After").get(0)) * 1000));
                } else {
                    setNextDeviceStatusRetryTs(System.currentTimeMillis() +
                            (Integer.parseInt(response.getHeaders().get("Retry-After").get(0)) * 1000));
                }
            }

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
			// scan loop has following boundaries, - it starts by fetching all device metadata
			// and lasts until all statistics are retrieved for each individual device
			mainloop: while (doProcess) {
				try {
					TimeUnit.MILLISECONDS.sleep(500);
				} catch (InterruptedException e) {
					// ignore, if thread was requested to stop, main loop will break
				}

				// if external process asked adapter to stop, we exit here immediately
				if (!doProcess) {
					break mainloop;
				}

				// next line will determine whether XiO monitoring was paused
				updateAggregatorStatus();
				if (devicePaused) {
					if (logger.isDebugEnabled()) {
						logger.debug(String.format(
								"Device adapter did not receive retrieveMultipleStatistics call in %s s. Statistics retrieval and device metadata retrieval is suspended.",
								retrieveStatisticsTimeOut / 1000));
					}
					continue mainloop;
				}

				// load all device metadata
				if (!isApiBlocked(CallContext.DEVICE_LIST)) {
					try {
						processAvailableDevicesMetadata();						
					} catch (Exception e) {
						logger.error("Error happened upon Crestron XiO API access when loading metadata for all available devices", e);
						// do not break - even if we cannot obtain the new list, we should try updating status of devices from the existing list
					}
				}

				if (!doProcess) {
					// service stopped
					break mainloop;
				}

				if (devicePaused) {
					continue mainloop;
				}

				// in case metadata was retrieved, we can scan devices by device
				int aggregatedDeviceCount = aggregatedDevices.size();
				if (aggregatedDeviceCount == 0) {
					// either caused by API error, or there are no devices in the account
					// in either case, no reason to repeat immediately
					long sleepFor = deviceStatisticsMonitoringCycle - 500; // there is extra 500 ms sleep at the beginning of the loop
					if (sleepFor > 0) {
						try {
							TimeUnit.MILLISECONDS.sleep(sleepFor);
						} catch (InterruptedException e) {
						}
					}
					continue mainloop;
				}

				List<ScannedDeviceKey> scannedDeviceKeys = new ArrayList<>(aggregatedDeviceCount);
				for (ScannedAggregatedDevice aggregatedDevice : aggregatedDevices.values()) {
					scannedDeviceKeys.add(new ScannedDeviceKey(aggregatedDevice.getDeviceId(), aggregatedDevice.getScannedAt()));
				}
				scannedDeviceKeys.sort(new ScannedDeviceKeyComparator());
				if (logger.isDebugEnabled()) {
					logger.debug(String.format("Starting the device statistics collection cycle at %s with the device list: %s", new Date(), scannedDeviceKeys));
				}

				if (deviceStatusPacingMode == PacingMode.INTERVAL_BASED) {
					// adjust pacing interval if needed
					long interval = Math.max(deviceStatisticsMonitoringCycle / aggregatedDeviceCount, minDeviceStatusRequestInterval);
					if (interval != deviceStatusRequestInterval) {
						logger.info("Adjusting device statistics pacing interval for " + aggregatedDeviceCount + " aggregated devices from "
								+ deviceStatusRequestInterval + " to " + interval + " ms");
						deviceStatusRequestInterval = interval;
					}
				}

				for (ScannedDeviceKey key : scannedDeviceKeys) {
					if (devicePaused || !doProcess) {
						break;
					}
					devicesExecutionPool.add(devicesCollectionExecutor.submit(() -> retrieveDeviceStatistics(key.getDeviceId())));
				}
				
				// wait until all worker threads have completed
				do {
					try {
						TimeUnit.MILLISECONDS.sleep(500);
					} catch (InterruptedException e) {
						if (!doProcess) {
							break mainloop;
						}
					}
					devicesExecutionPool.removeIf(Future::isDone);
				} while (!devicesExecutionPool.isEmpty());
				
				if (logger.isDebugEnabled()) {
					logger.debug("Finished collecting devices statistics cycle at " + new Date());
				}
			}
		}

        /**
         * Triggers main loop to stop
         */
        public void stop() {
            doProcess = false;
        }
    }

    private void paceDeviceStatusRequest() {
		long timeToWait = 0;
		switch (deviceStatusPacingMode) {
			case INTERVAL_BASED:
				long now = System.currentTimeMillis();
				long next = nextDeviceStatusRequestTs.getAndAccumulate(now, (x, y) -> (Math.max(x, y) + deviceStatusRequestInterval));
				timeToWait = next - now;
				break;

			case RETRY_AFTER:
				timeToWait = nextDeviceStatusRetryTs - System.currentTimeMillis();
				break;
		}

		if (timeToWait > 0) {
			try {
				TimeUnit.MILLISECONDS.sleep(timeToWait);
			} catch (InterruptedException e) {
				// the only interruption would be when service being stopped
			}
		}
    }

	/**
	 * Retrieve device statistics by given device id and save it to indicate that the device data has been fetched successfully
	 *
	 * @param devicesScanned map to put deviceId:collected pair to
	 * @param deviceId that has to be used for the device retrieval
	 */
	private void retrieveDeviceStatistics(String deviceId) {
		int retryAttempts = 0;
		while (retryAttempts++ < 10 && serviceRunning) {
			try {
				processDeviceStatistics(deviceId);
				if (logger.isDebugEnabled()) {
					logger.debug(String.format("Retrieved device statistics for device " + deviceId + "; attempts: " + retryAttempts));
				}
				return;
			} catch (CommandFailureException e) {
				if (e.getStatusCode() != 429) {
					// Might be 401, 403 or any other error code here so the code will just get stuck
					// cycling this failed request until it's fixed. So we need to skip this scenario.
					updateAggregatedDeviceOnlineStatus(deviceId, false); // TODO this does not look correct!
					logger.error("Crestron XiO API error " + e.getStatusCode() + " while retrieving statistics for device " + deviceId, e);
					break;
				}
			} catch (Exception e) {
				// if service is running, log error
				if (serviceRunning) {
					logger.error("Crestron XiO API error while retrieving statistics for device " + deviceId, e);
				}
				break;
			}
		}
		if (retryAttempts == 10 && serviceRunning) {
			// if we got here, all 10 attempts failed
			logger.error("Failed to retrieve statistic due to Crestron XiO API throttling for device " + deviceId);
		}
	}

    private void updateAggregatedDeviceOnlineStatus(String id, boolean onlineStatus) {
        AggregatedDevice aggregatedDevice = aggregatedDevices.get(id);
        if (aggregatedDevice != null) {
            aggregatedDevice.setDeviceOnline(onlineStatus);
        }
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

    @Override
    public int getPort() {
        return super.getPort();
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