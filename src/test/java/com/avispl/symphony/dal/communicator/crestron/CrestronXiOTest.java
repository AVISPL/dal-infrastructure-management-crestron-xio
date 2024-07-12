package com.avispl.symphony.dal.communicator.crestron;

import com.atlassian.ta.wiremockpactgenerator.WireMockPactGenerator;
import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;
import com.avispl.symphony.dal.communicator.HttpCommunicator;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

@Tag("test")
public class CrestronXiOTest {
    static CrestronXiO crestronXiO;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort().dynamicHttpsPort().bindAddress("127.0.0.1"));

    {
        wireMockRule.addMockServiceRequestListener(WireMockPactGenerator
                .builder("xio-adapter", "xio")
                .withRequestHeaderWhitelist("authorization", "content-type").build());
        wireMockRule.start();
    }

    @BeforeEach
    public void init() throws Exception {
        crestronXiO = new CrestronXiO();
        crestronXiO.setTrustAllCertificates(true);
        crestronXiO.setProtocol("http");
        crestronXiO.setContentType("application/json");
        crestronXiO.setPort(wireMockRule.port());
        crestronXiO.setHost("127.0.0.1");
        crestronXiO.setAuthenticationScheme(HttpCommunicator.AuthenticationScheme.None);
        crestronXiO.setLogin("");
        crestronXiO.setPassword("");
    }

    @Test
    public void getDevicesTest() throws Exception {
        // 5f3bc532c62411fa5cd84cfe device without model name for both basic and detailed payload
        // 5f3bc532295d07ba76c546a7 device without model name for basic only
        // 5f3bc532f9ddfe0451480957 device without model name for detailed payload
        crestronXiO.init();
        List<AggregatedDevice> devices = crestronXiO.retrieveMultipleStatistics();
        Thread.currentThread().join(60000);
        Assert.assertEquals(299, crestronXiO.retrieveMultipleStatistics().size());
        Thread.currentThread().join(60000);
        Assert.assertEquals(299, crestronXiO.retrieveMultipleStatistics().size());
        Thread.currentThread().join(60000);
        Assert.assertEquals(299, crestronXiO.retrieveMultipleStatistics().size());
        Thread.currentThread().join(60000);
        devices = crestronXiO.retrieveMultipleStatistics();
        long collectedDevices = devices.stream().filter(aggregatedDevice -> aggregatedDevice.getProperties() != null && aggregatedDevice.getProperties().size() != 0).count();
        Assert.assertEquals(299, devices.size());
        Assert.assertEquals(299, collectedDevices);
    }

    @Test
    public void pingTest() throws Exception {
        crestronXiO.init();
        List<AggregatedDevice> devices = crestronXiO.retrieveMultipleStatistics();
        Thread.currentThread().join(10000);
        int pingValue = crestronXiO.ping();
        Assert.assertTrue(pingValue < crestronXiO.getPingTimeout());
        Thread.currentThread().join(1000);
        Assert.assertTrue(crestronXiO.ping() < crestronXiO.getPingTimeout());
        Thread.currentThread().join(1000);
        Assert.assertTrue(crestronXiO.ping() < crestronXiO.getPingTimeout());
        Thread.currentThread().join(1000);
        Assert.assertTrue(crestronXiO.ping() < crestronXiO.getPingTimeout());
    }

    @Test
    public void getDevicesTestWithPause() throws Exception {
        crestronXiO.init();
        List<AggregatedDevice> devices = crestronXiO.retrieveMultipleStatistics();
        Thread.currentThread().join(60000);
        devices = crestronXiO.retrieveMultipleStatistics();
        Thread.currentThread().join(250000);
        long collectedDevicesWithPause = devices.stream().filter(aggregatedDevice -> aggregatedDevice.getProperties() != null).count();
        Assert.assertEquals(299, collectedDevicesWithPause);
        Assert.assertTrue(crestronXiO.isDevicePaused());
        Thread.currentThread().join(60000);
        devices = crestronXiO.retrieveMultipleStatistics();
        long collectedDevices = devices.stream().filter(aggregatedDevice -> aggregatedDevice.getProperties() != null).count();
        Thread.currentThread().join(60000);
        devices = crestronXiO.retrieveMultipleStatistics();
        Thread.currentThread().join(5000);
        Assert.assertFalse(crestronXiO.isDevicePaused());
        Assert.assertEquals(299, devices.size());
        Assert.assertEquals(299, collectedDevices);
    }
}
