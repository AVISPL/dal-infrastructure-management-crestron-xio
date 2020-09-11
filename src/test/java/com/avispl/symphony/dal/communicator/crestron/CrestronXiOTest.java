package com.avispl.symphony.dal.communicator.crestron;

import com.atlassian.ta.wiremockpactgenerator.WireMockPactGenerator;
import com.avispl.symphony.api.dal.dto.control.ControllableProperty;
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
        crestronXiO.setLogin("d65d142a-804f-4cd5-83a1-16b1d2f405c2");
        crestronXiO.setPassword("***REMOVED***");
    }

    @Test
    public void getDevicesTest() throws Exception {
        crestronXiO.init();
        List<AggregatedDevice> devices = crestronXiO.retrieveMultipleStatistics();
        Thread.currentThread().join(300000);
        devices = crestronXiO.retrieveMultipleStatistics();
        long collectedDevices = devices.stream().filter(aggregatedDevice -> aggregatedDevice.getProperties() != null && aggregatedDevice.getProperties().size() != 0).count();
        Assert.assertEquals(360, devices.size());
        Assert.assertEquals(360, collectedDevices);
    }


    @Test
    public void getDevicesTestWithPause() throws Exception {
        crestronXiO.init();
        List<AggregatedDevice> devices = crestronXiO.retrieveMultipleStatistics();
        Thread.currentThread().join(60000);
        devices = crestronXiO.retrieveMultipleStatistics();
        Thread.currentThread().join(181000);
        long collectedDevicesWithPause = devices.stream().filter(aggregatedDevice -> aggregatedDevice.getProperties() != null).count();
        Assert.assertEquals(360, collectedDevicesWithPause);
        Assert.assertTrue(crestronXiO.isDevicePaused());
        Thread.currentThread().join(60000);
        devices = crestronXiO.retrieveMultipleStatistics();
        long collectedDevices = devices.stream().filter(aggregatedDevice -> aggregatedDevice.getProperties() != null).count();
        Thread.currentThread().join(60000);
        devices = crestronXiO.retrieveMultipleStatistics();
        Thread.currentThread().join(5000);
        Assert.assertFalse(crestronXiO.isDevicePaused());
        Assert.assertEquals(360, devices.size());
        Assert.assertEquals(360, collectedDevices);
    }

    @Test
    public void testRemovingGenericDeviceMappings() throws Exception {
        crestronXiO.init();
        ControllableProperty controllableProperty = new ControllableProperty();
        controllableProperty.setProperty("Skip unknown device models");
        controllableProperty.setValue(1);
        crestronXiO.controlProperty(controllableProperty);
        List<AggregatedDevice> devices = crestronXiO.retrieveMultipleStatistics();
        Thread.currentThread().join(60000);
        devices = crestronXiO.retrieveMultipleStatistics();
        Assert.assertEquals(6, devices.size());
    }
}
