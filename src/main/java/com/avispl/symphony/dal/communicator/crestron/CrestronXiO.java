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
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import javax.security.auth.login.FailedLoginException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
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
 *
 * @author Sergey Blazchenko / Symphony Dev Team<br>
 * Created on May 26, 2019
 * @since 4.7
 */
public class CrestronXiO extends RestCommunicator implements Aggregator, Controller, Monitorable {


    private enum CallContext {DEVICE_LIST, DEVICE_STATUS}
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
     * Executor service for handling background tasks
     */
    private ExecutorService executorService;

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
    private long nextDevicesListCallTs;

    /**
     * This parameter holds timestamp of when we can perform next API request for retrieving device statistics
     * So the maximal available fetch rate is utilized (5 devices per second)
     */
    private volatile long nextDeviceStatusCallTs;

    /**
     * This parameter holds timestamp of when we need to stop performing API calls
     * It used when device stop retrieving statistic. Updated each time of called #retrieveMultipleStatistics
     */
    private long validRetrieveStatisticsTimestamp;

    /**
     * Time period within which the device metadata (basic devices information) cannot be refreshed.
     * If ignored if device list is not yet retrieved or the cached device list is empty {@link CrestronXiO#aggregatedDevices}
     */
    private long validDeviceMetaDataRetrievalPeriodTimestamp;

    /**
     * Aggregator inactivity timeout. If the {@link CrestronXiO#retrieveMultipleStatistics()}  method is not
     * called during this period of time - device is considered to be paused, thus the Cloud API
     * is not supposed to be called
     */
    private long retrieveStatisticsTimeOut = 3 * 60 * 1000;

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
    private boolean devicePaused = true;

    private AggregatedDeviceProcessor aggregatedDeviceProcessor;
    ExecutorService devicesCollectionExecutor = Executors.newFixedThreadPool(5);
    List<Future> devicesExecutionPool = new ArrayList<>();
    Set<String> availableModels = new HashSet<>();
    ReentrantLock controlLock = new ReentrantLock();

    @Override
    protected void internalInit() throws Exception {
        super.internalInit();

        // creating executors which will handle background job
        // for polling devices
        executorService = Executors.newCachedThreadPool();
        executorService.submit(deviceDataLoader = new CrestronXioDeviceDataLoader());

        initAggregatedDevicesProcessor();
    }

    private static Random random = new Random();

    @Override
    public int ping() throws Exception {
        return random.nextInt(10) + 40;
    }

    private void initAggregatedDevicesProcessor() throws IOException {
        Map<String, PropertiesMapping> models = new PropertiesMappingParser()
                .loadYML("xio/model-mapping.yml", getClass());

        Properties properties = new Properties();
        properties.load(getClass().getResourceAsStream("/aggregator.properties"));

        availableModels = models.keySet();
        if(Boolean.parseBoolean(properties.getProperty("skipUnknownModelsMapping"))) {
            models.remove("base");
            aggregatedDevices = new ConcurrentHashMap<>();
        }
        aggregatedDeviceProcessor = new AggregatedDeviceProcessor(models);
        validDeviceMetaDataRetrievalPeriodTimestamp = System.currentTimeMillis();
    }

    @Override
    public void controlProperty(ControllableProperty controllableProperty) throws Exception {
        String property = controllableProperty.getProperty();
        String value = String.valueOf(controllableProperty.getValue());

        controlLock.lock();
        try {
            if(logger.isDebugEnabled()){
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
        if (CollectionUtils.isEmpty(list)) {
            throw new IllegalArgumentException("Controllable properties cannot be null or empty");
        }

        for(ControllableProperty controllableProperty: list){
            controlProperty(controllableProperty);
        }
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
        super.internalDestroy();

        deviceDataLoader.stop();
        executorService.shutdown();
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
     */
    @Override
    public List<AggregatedDevice> retrieveMultipleStatistics() throws Exception {
        updateValidRetrieveStatisticsTimestamp();
        return new ArrayList<>(aggregatedDevices.values());
    }

    private void updateValidRetrieveStatisticsTimestamp() {
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
        if(aggregatedDevices.size() > 0 && validDeviceMetaDataRetrievalPeriodTimestamp > System.currentTimeMillis()){
            if(logger.isDebugEnabled()) {
                logger.debug(String.format("General devices metadata retrieval is in cooldown. %s seconds left",
                        (validDeviceMetaDataRetrievalPeriodTimestamp - System.currentTimeMillis()) / 1000));
            }
            return;
        }
        JsonNode availableDevices = fetchAvailableDevices();
        if(logger.isDebugEnabled()){
            logger.debug(String.format("Received devices metadata at %s. Devices list size: %s", new Date(), availableDevices.size()));
        }
        // retrieving list of active devices
        Set<String> liveDevices = StreamSupport
                .stream(availableDevices.spliterator(), false)
                .map(n -> n.findPath("device-cid").asText())
                .collect(Collectors.toSet());

        // setting devices offline status if they dissapeared from the response
        for (Map.Entry<String, AggregatedDevice> e : aggregatedDevices.entrySet()) {
            if (liveDevices.contains(e.getKey()))
                continue;

            e.getValue().setDeviceOnline(false);
        }

        controlLock.lock();
        try {

            List<AggregatedDevice> devices = new ArrayList<>();
            availableDevices.forEach(jsonNode -> {
                String modelName = jsonNode.get("device-model").asText();
                AggregatedDevice device = new AggregatedDevice();
                aggregatedDeviceProcessor.applyProperties(device, jsonNode, availableModels.contains(modelName) ? modelName : "base");
                if(!StringUtils.isEmpty(device.getDeviceId())){
                    devices.add(device);
                }
            });
            devices.sort(Comparator.comparing(AggregatedDevice::getDeviceId));
            devices.forEach(aggregatedDevice -> {
                if(aggregatedDevices.containsKey(aggregatedDevice.getDeviceId())){
                    aggregatedDevices.get(aggregatedDevice.getDeviceId()).setTimestamp(System.currentTimeMillis());
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
        switch (context){
            case DEVICE_LIST:
                if (getNextDevicesListCallTs() == 0) {
                    return false;
                }
                return getNextDevicesListCallTs() > System.currentTimeMillis();
            case DEVICE_STATUS:
                if(getNextDeviceStatusCallTs() == 0){
                    return false;
                }
                return getNextDeviceStatusCallTs() > System.currentTimeMillis();
            default:
                logger.debug("");
                return false;
        }
    }

    /**
    * Update the status of the device.
    * The device is considered as paused if did not receive any retrieveMultipleStatistics()
    * calls during {@link CrestronXiO#validRetrieveStatisticsTimestamp}
    */
    private void updateAggregatorStatus(){
        devicePaused = validRetrieveStatisticsTimestamp < System.currentTimeMillis();
    }

    /**
     * Loads, deserializes and stores device details in the internal storage
     *
     * @param deviceId Device ID to process statistics for
     */
    private void processDeviceStatistics(String deviceId) throws Exception {
        JsonNode deviceStatistics = fetchDeviceStatistics(deviceId);
        updateAggregatedDevice(deviceStatistics);
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
                validDeviceMetaDataRetrievalPeriodTimestamp = System.currentTimeMillis() + Math.max(120000, deviceMetaDataInformationRetrievalTimeout);
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
        return doGet("api/v1/device/accountid/" + getAccountId() + "/devicecid/" + deviceId + "/status", JsonNode.class);
    }

    /**
     * Populates {@link AggregatedDevice} device statistics
     *
     * @param deviceNode {@link JsonNode} instance to take statistics from
     */
    private void updateAggregatedDevice(JsonNode deviceNode) {
        String deviceId = deviceNode.findPath("device-cid").asText();
        AggregatedDevice aggregatedDevice = aggregatedDevices.get(deviceId);
        if(aggregatedDevice == null) {
            aggregatedDevice = new AggregatedDevice();
        }
        boolean deviceOnline = aggregatedDevice.getDeviceOnline();
        controlLock.lock();
        try {
            String modelName = deviceNode.findValue("device-model").asText();
            aggregatedDeviceProcessor.applyProperties(aggregatedDevice, deviceNode, modelName + "-detailed");
            // detailed device info doesn't have an online status, so we need to override with an actual status
            // that will be updated within the next metadata update
            aggregatedDevice.setDeviceOnline(deviceOnline);
            aggregatedDevice.setTimestamp(System.currentTimeMillis());
            aggregatedDevices.put(deviceId, aggregatedDevice);
        } finally {
            controlLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AggregatedDevice> retrieveMultipleStatistics(List<String> deviceIds) throws Exception {
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
    public long getNextDevicesListCallTs() {
        return nextDevicesListCallTs;
    }

    /**
     * Returns timestamp of when we can perform next API request
     * to fetch the detailed statistics of the device
     *
     * @return long timestamp
     */
    public long getNextDeviceStatusCallTs() {
        return nextDeviceStatusCallTs;
    }

    /**
     * Check whether the device is considered as paused or not
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
    public void setNextDevicesListCallTs(long ts) {
        this.nextDevicesListCallTs = ts;
    }

    /**
     * Sets timestamp of when we can perform next API request to get the device detailed statistics
     *
     * @param ts timestamp
     */
    public void setNextDeviceStatusCallTs(long ts) {
        if(ts > this.nextDevicesListCallTs) {
            this.nextDeviceStatusCallTs = ts;
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
                if(!request.getURI().getPath().endsWith("/status")) {
                    setNextDevicesListCallTs(System.currentTimeMillis() +
                            (Integer.parseInt(response.getHeaders().get("Retry-After").get(0)) * 1000));
                } else {
                    setNextDeviceStatusCallTs(System.currentTimeMillis() +
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
         * Hold information about which devices have been scanned up to date
         * in the scan loop
         */
        private ConcurrentHashMap<String, Boolean> devicesScanned;

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
            boolean allDeviceMetadataRetrieved = false;

            // scan loop has following boundaries, - it starts by fetching all device metadata
            // and lasts until all statistics are retrieved for each individual device
            while (true) {
                // if external process asked adapter to stop
                // we exit here immediately
                if (!doProcess) {
                    break;
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                    updateAggregatorStatus();
                } catch (InterruptedException e) {
                    break;
                }

                if(devicePaused){
                    if(logger.isDebugEnabled()){
                        logger.debug(String.format("Device adapter did not receive a retrieveMultipleStatistics call in %s s. Statistics retrieval and device metadata retrieval is suspended.",
                                retrieveStatisticsTimeOut/1000));
                    }
                }
                // if API access timeout hasn't yet passed or the device is paused (did not
                // receive any retrieveMultipleStatistics() calls in 3m), we don't issue any requests
                if (isApiBlocked(CallContext.DEVICE_LIST) || devicePaused) {
                    continue;
                }

                // load all device metadata
                try {
                    if (!allDeviceMetadataRetrieved) {
                        processAvailableDevicesMetadata();
                        allDeviceMetadataRetrieved = true;

                        // creating a structure with device IDs that we are going to scan in this scan loop
                        devicesScanned = new ConcurrentHashMap<>(aggregatedDevices.size());
                        aggregatedDevices.forEach((k, v) -> devicesScanned.put(k, false));
                    }
                } catch (CommandFailureException e) {
                    logger.trace(String.format("Crestron XiO API server replied with %s response code retrieved while loading all device metadata",
                            e.getStatusCode()));
                } catch (Exception e) {
                    logger.error("Error happened upon Crestron XiO API access when loading metadata for all available devices", e);
                }

                if (!allDeviceMetadataRetrieved){
                    continue;
                }

                devicesExecutionPool.removeIf(Future::isDone);
                if(!devicesExecutionPool.isEmpty()){
                    if(logger.isDebugEnabled()){
                        logger.debug("Devices statistics collection pool is not empty. Waiting for the data to be collected before starting the new cycle.");
                    }
                    continue;
                }
                // in case metadata was retrieved, we can scan devices by device
                List<String> devicesScannedKeys = new ArrayList<>(devicesScanned.keySet());
                if(logger.isDebugEnabled() && devicesScanned.values().stream().noneMatch(bool -> bool)){
                    logger.debug(String.format("Starting the device statistics collection cycle at %s with the device list: %s", new Date(), devicesScannedKeys));
                }
                Collections.sort(devicesScannedKeys);
                for (String key : devicesScannedKeys) {
                    // device has been already retrieved in the current loop
                    if (devicesScanned.get(key)) {
                        continue;
                    }

                    devicesExecutionPool.add(devicesCollectionExecutor.submit(()-> retrieveDeviceStatistics(devicesScanned, key)));

                    if (devicePaused) {
                        break;
                    }
                }
                // check for number of devices for which we successfully loaded statistics
                // in the current loop
                int processedDevices = 0;
                for (Map.Entry<String, Boolean> entry : devicesScanned.entrySet()) {
                    if (entry.getValue()) {
                        processedDevices++;
                    }
                }

                // if all devices were scanned in the current loop
                // then new loop needs to be started
                if (devicesScanned.size() == processedDevices) {
                    if(logger.isDebugEnabled()){
                        logger.debug("Finished collecting devices statistics cycle at " + new Date());
                    }
                    allDeviceMetadataRetrieved = false;
                    devicesScanned = null;
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

    /**
     * Retrieve device statistics by given device id and save it to indicate that the device data has been
     * fetched successfully
     *
     * @param devicesScanned map to put deviceId:collected pair to
     * @param deviceId that has to be used for the device retrieval
     */
    private void retrieveDeviceStatistics(Map<String, Boolean> devicesScanned, String deviceId){
        try {
            int retryAttempts = 0;
            while(isApiBlocked(CallContext.DEVICE_STATUS) && retryAttempts < 10){
                if(logger.isDebugEnabled()){
                    logger.debug("Device statistics API endpoint reached 5 devices/second threshold. Waiting 250ms to retrieve statistics for the device: " + deviceId);
                }
                retryAttempts++;
                TimeUnit.MILLISECONDS.sleep(250);
            }
            if(logger.isDebugEnabled()){
                logger.debug(String.format("Retrieving device statistics for the device %s. Retry attempts: %s", deviceId, retryAttempts));
            }
            processDeviceStatistics(deviceId);
            devicesScanned.put(deviceId, true);
            if(logger.isDebugEnabled()){
                logger.debug(String.format("Device %s was added into the devices list", deviceId));
            }
        } catch (CommandFailureException e) {
            if(e.getStatusCode() != 429){
                // Might be 401, 403 or any other error code here so the code will just get stuck
                // cycling this failed request until it's fixed. So we need to skip this scenario.
                devicesScanned.put(deviceId, true);
                updateAggregatedDeviceUpdateStatus(deviceId, false);
                logger.debug(String.format("Unable to fetch device with id %s. Error code: %s, message: %s",
                        deviceId, e.getStatusCode(), e.getMessage()));
                throw e;
            }
            logger.debug(String.format("Crestron XiO API server replied with %s response code retrieved while loading statistics for device %s",
                    e.getStatusCode(), deviceId));
        } catch (Exception e) {
            // we set scan status to "true" here because issue here is not related to HTTP error code
            devicesScanned.put(deviceId, true);
            logger.error("Error happened upon Crestron XiO API access when statistics for device " + deviceId, e);
        }
    }

    private void updateAggregatedDeviceUpdateStatus(String id, boolean onlineStatus){
        AggregatedDevice aggregatedDevice = aggregatedDevices.get(id);
        if(aggregatedDevice != null){
            aggregatedDevice.setDeviceOnline(onlineStatus);
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