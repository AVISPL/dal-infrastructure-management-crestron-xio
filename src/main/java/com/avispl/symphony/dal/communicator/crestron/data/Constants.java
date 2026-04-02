/*
 * Copyright (c) 2024 AVI-SPL, Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.crestron.data;

/**
 * Properties used in code, to provide more consistency.
 *
 * @author Maksym.Rossiytsev/SymphonyDevTeam
 * @since 2.0.2
 */
public interface Constants {
    /**
     * Extended property names used in main communicator class
     *
     * @author Maksym.Rossiytsev/SymphonyDevTeam
     * @since 2.0.2
     * */
    interface Properties {
        String ADAPTER_BUILD_DATE = "AdapterBuildDate";
        String ADAPTER_VERSION = "AdapterVersion";
        String ADAPTER_UPTIME = "AdapterUptime";
        String ADAPTER_UPTIME_MIN = "AdapterUptime(min)";
        String MONITORED_DEVICES_TOTAL = "MonitoredDevicesTotal";
        String LAST_MONITORING_CYCLE_DURATION = "LastMonitoringCycleDuration(sec)";
        String MONITORING_CYCLE_INTERVAL = "MonitoringCycleInterval(min)";
        String DEVICE_UPDATE_TIME = "UpdateTime";
    }

    /**
     * Json Paths used in main communicator class for data navigation
     *
     * @author Maksym.Rossiytsev/SymphonyDevTeam
     * @since 2.0.2
     * */
    interface JsonPaths {
        String DEVICE_LIST = "DeviceList";
        String TOTAL_DEVICES = "TotalDevices";
        String TOTAL_PAGES = "TotalPages";
        String DEVICE_CID = "device-cid";
        String DEVICE_MODEL = "device-model";
    }

    /**
     * Headers used in main communicator class
     *
     * @author Maksym.Rossiytsev/SymphonyDevTeam
     * @since 2.0.2
     * */
    interface Headers {
        String XIO_SUBSCRIPTION_KEY = "XiO-subscription-key";
        String CONTENT_TYPE = "Content-Type";
    }

    /**
     * URIs used in main class during http communication with XiO API
     *
     * @author Maksym.Rossiytsev/SymphonyDevTeam
     * @since 2.0.2
     * */
    interface URI {
        String V1_DEVICE_ACCOUNT_ID = "api/V2/device/accountid/";
        String ACCOUNT_GROUPS = "api/v1/group/accountid/%s/groups";
        String DEVICE_MODEL = "/deviceModel/";
        String DEVICE_PAGE_NO = "/pageno/";
        String DEVICE_PAGE_SIZE = "/pageSize/";
        String DEVICE_STATUS = "/status";
        String V2_DEVICE_ACCOUNT_ID = "/api/v1/account/accountid/";
        String ACCOUNT = "/account";
    }
}
