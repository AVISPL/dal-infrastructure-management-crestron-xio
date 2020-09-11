package com.avispl.symphony.dal.communicator.crestron;

import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;
import com.avispl.symphony.dal.communicator.HttpCommunicator;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

@Tag("integrationTest")
public class CrestronXiOIntegrationTest {
    static CrestronXiO crestronXiO;

    @BeforeEach
    public void init() throws Exception {
        crestronXiO = new CrestronXiO();
        crestronXiO.setTrustAllCertificates(true);
        crestronXiO.setProtocol("http");
        crestronXiO.setContentType("application/json");
        crestronXiO.setPort(443);
        crestronXiO.setHost("api.crestron.io");
        crestronXiO.setAuthenticationScheme(HttpCommunicator.AuthenticationScheme.None);
        crestronXiO.setLogin("d65d142a-804f-4cd5-83a1-16b1d2f405c2");
        crestronXiO.setPassword("3ccb51db641f4bb8b22fde31723a0d17");
    }

    @Test
    public void getDevicesTest() throws Exception {
        crestronXiO.init();
        List<AggregatedDevice> devices = crestronXiO.retrieveMultipleStatistics();
        Thread.currentThread().join(60000);
        devices = crestronXiO.retrieveMultipleStatistics();
        Thread.currentThread().join(60000);
        devices = crestronXiO.retrieveMultipleStatistics();
        Thread.currentThread().join(60000);
        devices = crestronXiO.retrieveMultipleStatistics();
        Thread.currentThread().join(60000);
        devices = crestronXiO.retrieveMultipleStatistics();
        Thread.currentThread().join(60000);
        devices = crestronXiO.retrieveMultipleStatistics();
        long collectedDevices = devices.stream().filter(aggregatedDevice -> aggregatedDevice.getProperties() != null).count();
        Assert.assertEquals(67, devices.size());
        Assert.assertEquals(67, collectedDevices);
    }
}
