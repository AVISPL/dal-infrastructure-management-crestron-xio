/*
 * Copyright (c) 2019-2020 AVI-SPL, Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.crestron;

import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;
import com.avispl.symphony.api.dal.error.CommandFailureException;
import com.avispl.symphony.api.dal.monitor.aggregator.Aggregator;
import com.avispl.symphony.dal.communicator.RestCommunicator;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.RestTemplate;

import javax.security.auth.login.FailedLoginException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
public class CrestronXiO extends RestCommunicator implements Aggregator {

    /**
     * Account identifier to fetch devices for
     */
    private String accountId;

    /**
     * Crestron XIO subscription ID for authentication against their API
     */
    private String subscriptionId;

    /**
     * Logger which can be re-used by derived classes (if any)
     */
    protected final Log logger = LogFactory.getLog(getClass());

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
     * This parameter holds timestamp of when we can perform next API request
     */
    private long nextApiCallTs;

    @Override
    protected void internalInit() throws Exception {
        super.internalInit();

        // creating executors which will handle background job
        // for polling devices
        executorService = Executors.newCachedThreadPool();
        executorService.submit(deviceDataLoader = new CrestronXioDeviceDataLoader());
    }

    private static Random random = new Random();

    @Override
    public int ping() throws Exception {
        return random.nextInt(10) + 40;
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
     * {@inheritDoc}
     */
    @Override
    public List<AggregatedDevice> retrieveMultipleStatistics() throws Exception {
        return new ArrayList<>(aggregatedDevices.values());
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
        JsonNode availableDevices = fetchAvailableDevices();

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

        // creating and updating devices
        availableDevices.forEach(jsonNode -> {
            String deviceId = jsonNode.findPath("device-cid").asText();
            boolean deviceAlreadyExistsInStats = aggregatedDevices.containsKey(deviceId);

            AggregatedDevice aggregatedDevice;
            if (deviceAlreadyExistsInStats) {
                aggregatedDevice = aggregatedDevices.get(deviceId);
            } else {
                aggregatedDevice = new AggregatedDevice();
                aggregatedDevice.setDeviceId(deviceId);
                aggregatedDevices.put(deviceId, aggregatedDevice);
            }

            aggregatedDevice.setDeviceType(jsonNode.findPath("device-category").asText());
            aggregatedDevice.setDeviceName(jsonNode.findPath("device-name").asText());
            aggregatedDevice.setDeviceMake(jsonNode.findPath("device-manufacturer").asText());
            aggregatedDevice.setDeviceModel(jsonNode.findPath("device-model").asText());
            aggregatedDevice.setSerialNumber(jsonNode.findPath("serial-number").asText());
            aggregatedDevice.setDeviceOnline(true);
        });
    }

    private void addIfExist(JsonNode jsonNode, Map<String, String> properties, String propertyName) {
        String property = jsonNode.findPath(propertyName).asText();
        if (property != null && !(property.equals("null") || property.equals(""))) {
            properties.put("device-id", property);
        }
    }

    /**
     * Checks whether we can issue an API request
     * taking into account Too-Many-Requests response from the server and seconds to wait
     *
     * @return
     */
    private boolean isApiBlocked() {
        if (getNextApiCallTs() == 0)
            return false;

        return getNextApiCallTs() > System.currentTimeMillis();
    }

    /**
     * Loads, deserializes and stores device details in the internal storage
     *
     * @param deviceId Device ID to process statistics for
     */
    private void processDeviceStatistics(String deviceId) throws Exception {
        getAggregatedDevice(fetchDeviceStatistics(deviceId), aggregatedDevices.get(deviceId));
    }

    /**
     * Retrieves information about available devices.
     *
     * @return {@link JsonNode} instance with information about available devices
     */
    private JsonNode fetchAvailableDevices() throws Exception {
        return doGet("api/v1/device/accountid/" + getAccountId() + "/devices", JsonNode.class);
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
     * @param deviceNode       {@link JsonNode} instance to take statistics from
     * @param aggregatedDevice device instance where to put statistics to
     */
    private void getAggregatedDevice(JsonNode deviceNode, AggregatedDevice aggregatedDevice) {
        // TODO change mapping

        Map<String, String> propertiesMap = new HashMap<>(4);
        propertiesMap.put("device.device-builddate", deviceNode.findPath("device-builddate").asText());
        propertiesMap.put("device.device-key", deviceNode.findPath("device-key").asText());
        propertiesMap.put("device.firmware-version", deviceNode.findPath("firmware-version").asText());
        propertiesMap.put("device.displayed-input", deviceNode.findPath("displayed-input").asText());
        aggregatedDevice.setProperties(propertiesMap);

        Map<String, String> statisticsMap = new HashMap<>(16);
        statisticsMap.put("device.call-status", deviceNode.findPath("call-status").asText());
        statisticsMap.put("device.occupancy-status", deviceNode.findPath("occupancy-status").asText());
        statisticsMap.put("device.sleep-status", deviceNode.findPath("sleep-status").asText());
        statisticsMap.put("device.skype-presence", deviceNode.findPath("skype-presence").asText());
        statisticsMap.put("audio.volume", deviceNode.findPath("volume").asText());
        statisticsMap.put("audio.mute-status", deviceNode.findPath("mute-status").asText());
        statisticsMap.put("connections.bluetooth", deviceNode.findPath("bluetooth").asText());
        statisticsMap.put("connections.usb-in", deviceNode.findPath("usb-in").asText());
        statisticsMap.put("services.calendar-connection", deviceNode.findPath("calendar-connection").asText());
        statisticsMap.put("services.skype-connection", deviceNode.findPath("skype-connection").asText());
        statisticsMap.put("hdmi-input.hdmi-input-horizontal-resolution", deviceNode.findPath("hdmi-input-horizontal-resolution").asText());
        statisticsMap.put("hdmi-input.hdmi-input-vertical-resolution", deviceNode.findPath("hdmi-input-vertical-resolution").asText());
        statisticsMap.put("hdmi-input.hdmi-input-frames-per-second", deviceNode.findPath("hdmi-input-frames-per-second").asText());
        statisticsMap.put("hdmi-output.hdmi-output-horizontal-resolution", deviceNode.findPath("skype-connection").asText());
        statisticsMap.put("hdmi-output.hdmi-output-vertical-resolution", deviceNode.findPath("skype-connection").asText());
        statisticsMap.put("hdmi-output.hdmi-output-frames-per-second", deviceNode.findPath("skype-connection").asText());
        aggregatedDevice.setStatistics(statisticsMap);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AggregatedDevice> retrieveMultipleStatistics(List<String> deviceIds) throws Exception {
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
     *
     * @return timestamp of when we can perform next API request
     */
    public long getNextApiCallTs() {
        return nextApiCallTs;
    }

    /**
     * Sets timestamp of when we can perform next API request
     *
     * @param nextApiCallTs timestamp of when we can perform next API request
     */
    public void setNextApiCallTs(long nextApiCallTs) {
        this.nextApiCallTs = nextApiCallTs;
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
                setNextApiCallTs(System.currentTimeMillis() +
                        (Integer.parseInt(response.getHeaders().get("Retry-After").get(0)) * 1000));
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
        private Map<String, Boolean> devicesScanned;

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
                if (!doProcess)
                    break;

                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    break;
                }

                // if API access timeout hasn't yet passed, we don't issue any requests
                if (isApiBlocked())
                    continue;

                // load all device metadata
                try {
                    if (!allDeviceMetadataRetrieved) {
                        processAvailableDevicesMetadata();
                        allDeviceMetadataRetrieved = true;

                        // creating a structure with device IDs that we are going to scan in this scan loop
                        devicesScanned = new HashMap<>(aggregatedDevices.size());
                        aggregatedDevices.forEach((k, v) -> devicesScanned.put(k, false));
                    }
                } catch (CommandFailureException e) {
                    logger.trace("Crestron XiO API server replied with " + e.getStatusCode() +
                            " response code retrieved while loading all device metadata");
                } catch (Exception e) {
                    logger.error("Error happened upon Crestron XiO API access when loading metadata for all available devices", e);
                }

                if (!allDeviceMetadataRetrieved || isApiBlocked())
                    continue;

                // in case metadata was retrieved, we can scan deviec by device
                for (Map.Entry<String, Boolean> entry : devicesScanned.entrySet()) {
                    // device has been already retrieved in the current loop
                    if (entry.getValue())
                        continue;

                    try {
                        processDeviceStatistics(entry.getKey());
                        entry.setValue(true);
                    } catch (CommandFailureException e) {
                        logger.trace("Crestron XiO API server replied with " + e.getStatusCode() +
                                " response code retrieved while loading statistics for device " + entry.getKey());
                    } catch (Exception e) {
                        // we set scan status to "true" here because issue here is not related to
                        // HTTP code 429 and it looks that API responds with wrong data
                        entry.setValue(true);

                        logger.error("Error happened upon Crestron XiO API access when statistics for device " + entry.getKey(), e);
                    }

                    if (isApiBlocked()) {
                        break;
                    }
                }

                // check for number of devices for which we successfully loaded statistics
                // in the current loop
                int processedDevices = 0;
                for (Map.Entry<String, Boolean> entry : devicesScanned.entrySet()) {
                    if (entry.getValue())
                        processedDevices++;
                }

                // if all devices were scanned in the current loop
                // then new loop needs to be started
                if (devicesScanned.size() == processedDevices) {
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

    /**
     * Temporarily used method for testing purposes
     */
    /*
    public static void main(String[] args) throws Exception {
        CrestronXiO xio = new CrestronXiO();

        xio.setHost("api.crestron.io");
        xio.setProtocol("https");
        xio.setPort(443);
        xio.setLogin("d65d142a-804f-4cd5-83a1-16b1d2f405c2");
        xio.setPassword("***REMOVED***");

        try {
            xio.init();
        } catch (Exception e) {
            e.printStackTrace();
        }

        while (true) {
            List<AggregatedDevice> aggregatedDevices = xio.retrieveMultipleStatistics();
            System.out.println("*aggregatedDevices = " + aggregatedDevices.toString());
            Thread.sleep(10 * 1000);
        }
    }
    */
}