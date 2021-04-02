/*
 * Copyright (c) 2021 AVI-SPL, Inc. All Rights Reserved.
 */

package com.avispl.symphony.dal.communicator.crestron;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;
import com.avispl.symphony.dal.communicator.HttpCommunicator;

/**
 * Test app for CrestronXiO adapter.
 *
 * @author Vlad Kasyanenko / Symphony Dev Team <br>
 *         Created on Dec 7, 2020
 * @since 1.1
 */
public class CrestronXiOTestApp {

	/**
	 * Comparator for aggregated devices based on monitored time stamps.
	 */
	static final class MonitoredTimestampComparator implements Comparator<AggregatedDevice> {
		/**
		 * Comparison of {@code AggregatedDevice} objects is done based on value of {@code monitored} property using logic and requirements below.<br>
		 * <br>
		 * {@inheritDoc}
		 */
		@Override
		final public int compare(AggregatedDevice o1, AggregatedDevice o2) {
			final long scannedAt1 = o1 != null && o1.getTimestamp() != null ? o1.getTimestamp().longValue() : 0;
			final long scannedAt2 = o2 != null && o2.getTimestamp() != null ? o2.getTimestamp().longValue() : 0;
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

	/**
	 * Main entry of CrestronXiOTestApp. <br>
	 * NOTE: <br>
	 * - need to stop monitoring of AVI-SPL XiO account in Symphony in order to run the app <br>
	 * - need to pause at least 2 min between consequent runs of this app due to device list API pacing on XiO side.
	 *
	 * @param args test app arguments (reserved for future use)
	 */
	public static void main(String[] args) {
		System.out.println("Entering CrestronXiOTestApp");

		final CrestronXiO crestronXiO = new CrestronXiO();
		crestronXiO.setTrustAllCertificates(true);
		crestronXiO.setContentType("application/json");
		crestronXiO.setProtocol("https");
		crestronXiO.setPort(443);
		crestronXiO.setHost("api.crestron.io");
		crestronXiO.setAuthenticationScheme(HttpCommunicator.AuthenticationScheme.None);
		crestronXiO.setLogin("d65d142a-804f-4cd5-83a1-16b1d2f405c2"); // AVI-SPL XiO account
		crestronXiO.setPassword("***REMOVED***");
		crestronXiO.setDeviceModelFilter(Arrays.asList("MERCURY", "TSW-1060"));

		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

		try {
			crestronXiO.init();
			crestronXiO.retrieveMultipleStatistics(); // this is to un-pause monitoring

			final long startTime = System.currentTimeMillis();
			System.out.println("Start time: " + dateFormat.format(new Date(startTime)));

			// scan for 1 min
			for (int i = 0; i < 60; ++i) {
				System.out.print("*");
				TimeUnit.MILLISECONDS.sleep(1000);
			}

			final long endTime = System.currentTimeMillis();
			System.out.println();
			System.out.println("End time: " + dateFormat.format(new Date(endTime)));

			List<AggregatedDevice> devices = crestronXiO.retrieveMultipleStatistics();
			int totalCount = devices.size();
			if (totalCount > 1) {
				Collections.sort(devices, new MonitoredTimestampComparator());
			}

			for (AggregatedDevice device : devices) {
				System.out.println(dateFormat.format(new Date(device.getTimestamp())) + "\tDevice id: " + device.getDeviceId() + ", name: "
						+ device.getDeviceName() + ", online: " + device.getDeviceOnline() + ", properties: " + device.getProperties() + ", statistics: "
						+ device.getStatistics());
			}

			System.out.println("Scanned aggregated devices: " + totalCount);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				crestronXiO.destroy();
			} catch (Exception e) {
			}
		}

		System.out.println("Leaving CrestronXiOTestApp");
	}
}

// device status API response example
/*
{
  "Pagination": {
    "TotalDevices": 99,
    "TotalPages": 99,
    "PageSize": 1,
    "CurrentPageNumber": 1
  },
  "DeviceList": [
    {
      "device": {
        "device-cid": "54-b2-03-08-59-60_-1",
        "device-accountid": "d65d142a-804f-4cd5-83a1-16b1d2f405c2",
        "device-groupid": "3cd9d50f-d2b5-4d72-8199-53aa74f8c2e0",
        "device-model": "UC-ENGINE",
        "device-category": "",
        "device-manufacturer": "Crestron Electronics",
        "device-id": null,
        "device-name": "C4-54B203085960",
        "device-builddate": "9/13/2020 12:48:06 PM",
        "device-key": null,
        "user-device-name": "ADD_Air Force_ENGINE",
        "serial-number": "UCE16552860",
        "call-status": null,
        "displayed-input": null,
        "occupancy-status": null,
        "firmware-version": "1.00.16.872",
        "sleep-status": null,
        "skype-presence": null,
        "cloudownedsettings-maximumcapacity": null,
        "hardware-type": "UCE",
        "license-type": "Premium",
        "device-status": "Offline"
      },
      "audio": {
        "mic-mute-status": null,
        "mute-status": null,
        "volume": null
      },
      "connections": {
        "bluetooth": null,
        "bluetooth-pairing": null,
        "display-power": null,
        "hdmi-in": null,
        "hdmi-out": null,
        "usb-in": null,
        "dm-in": null
      },
      "hdmi-input": {
        "hdmi-input-resolution": null,
        "hdmi-input-interlaced-detected": null,
        "hdmi-input-is-source-hdcp-active": null,
        "hdmi-input-source-hdcp-state": null,
        "hdmi-input-horizontal-resolution": null,
        "hdmi-input-vertical-resolution": null,
        "hdmi-input-frames-per-second": null,
        "hdmi-input-aspect-ratio": null,
        "hdmi-input-audio-format": null,
        "hdmi-input-audio-channels": null,
        "hdmi-sync-detected": null
      },
      "hdmi-output": {
        "hdmi-output-resolution": null,
        "hdmi-output-interlaced-detected": null,
        "hdmi-output-hdcp-disabled-by-hdcp": null,
        "hdmi-output-horizontal-resolution": null,
        "hdmi-output-vertical-resolution": null,
        "hdmi-output-frames-per-second": null,
        "hdmi-output-aspect-ratio": null,
        "hdmi-output-audio-format": null,
        "hdmi-output-audio-channels": null,
        "hdmi-output-is-sink-connected": null,
        "hdmi-output-manufacturer-string": null,
        "hdmi-output-serial-number-string": null
      },
      "network": {
        "status-host-name": "C4-54B203085960",
        "status-domain-name": "TWG.WHITLOCK.COM",
        "nic-1-dns-servers": [
          "10.60.4.2",
          "10.4.3.208"
        ],
        "nic-1-dhcp-enabled": true,
        "nic-1-ip-address": "10.60.1.142",
        "nic-1-subnet-mask": "255.255.255.0",
        "nic-1-def-router": "10.60.1.1",
        "nic-1-mac-address": "54.b2.03.08.59.60",
        "nic-1-link": true,
        "nic-2-dhcp-enabled": null,
        "nic-2-ip-address": null,
        "nic-2-subnet-mask": null,
        "nic-2-def-router": null,
        "nic-2-mac-address": null,
        "nic-2-link": null,
        "proxy-enabled": null,
        "wifi-status": null,
        "ieee8021x": null
      },
      "services": {
        "calendar-connection": null,
        "fusion-connection-type": null,
        "fusion-status": null,
        "skype-connection": null
      },
      "Conferencing": {
        "sip-server-status": null,
        "static-sip-qos": null,
        "static-voice-qos": null,
        "lldp-voice-vlan-id": null,
        "lldp-voice-vlan-priority": null,
        "vlan-ip-address": null
      },
      "DMInput": {
        "dm-input-resolution": null,
        "hdmi-input-interlaced-detected": null,
        "dm-input-is-source-hdcp-active": null,
        "dm-input-source-hdcp-state": null,
        "dm-input-horizontal-resolution": null,
        "dm-input-vertical-resolution": null,
        "dm-input-frames-per-second": null,
        "dm-input-aspect-ratio": null,
        "dm-input-audio-format": null,
        "dm-input-audio-channels": null
      },
      "audioVideo": {
        "miracast-isEnabled": null,
        "miracast-defaultwindowsexperience": null,
        "miracast-wifidirectMode": null,
        "hdmi-disabled-by-hdcp": null,
        "airboard-isenabled": null,
        "airboard-connection-info": null,
        "airboard-pairing-status": null,
        "wifidonglestatus": null
      },
      "usb": {
        "statusTab.label.usbConnected": null,
        "statusTab.label.usbType": null
      },
      "roomScheduling": {
        "statusTab.label.roomSchedulingRoomStatus": null,
        "statusTab.label.roomSchedulingCalendarSync": null,
        "statusTab.label.roomSchedulingFusionStatus": null
      },
      "schedule": {
        "statusTab.scheduling.enableModernAuth": null,
        "statusTab.schedule.schedulingSource": null,
        "statusTab.label.schedulingConnectionStatus": null,
        "statusTab.schedule.connectionStatusMessage": null,
        "statusTab.schedule.occupancyStatus": null,
        "statusTab.schedule.occupancySource": null,
        "statusTab.schedule.pushRegistrationStatus": null,
        "statusTab.schedule.googleRegistrationStatus": null,
        "statusTab.schedule.googleRegistrationUrl": null,
        "statusTab.schedule.googleRegistrationCode": null,
        "statusTab.schedule.exchangeRegistrationStatus": null,
        "statusTab.schedule.exchangeRegistrationUrl": null,
        "statusTab.schedule.exchangeRegistrationCode": null,
        "statusTab.schedule.currentMeeting.scheduled": null,
        "statusTab.schedule.currentMeetingSubject": null,
        "statusTab.schedule.currentMeetingEndTime": null,
        "statusTab.schedule.currentMeetingRecurring": null,
        "statusTab.schedule.currentMeetingOrganizer": null,
        "statusTab-schedule-attendees": null,
        "statusTab.schedule.currentMeetingCheckInStatus": null,
        "statusTab.schedule.currentMeetingMeetingPrivacyLevel": null
      },
      "categoryccs-cam-usb-f-400": {
        "ccs-serial-number": "NOT CONNECTED",
        "ccs-camera-version": "NOT CONNECTED",
        "ccs-software-version": "NOT CONNECTED",
        "ccs-upgrade-version": "NOT CONNECTED",
        "ccs-room-occupancy": 0,
        "ccs-genius-framing-enabled": true,
        "ccs-occupant-count-enabled": true
      },
      "security": {
        "statusTab-encryptConnection": false
      },
      "Peripherals": {
        "features-conference-microphone": "Healthy",
        "features-conference-speaker": "Healthy",
        "features-default-speaker": "Healthy",
        "features-camera": "NotApplicable",
        "features-front-room-display": "Healthy",
        "features-hdmiingest": "Healthy",
        "ucprsettings-isConnected": null,
        "category-ucpr-serialnumber": null,
        "category-ucpr-firmwareversion": null,
        "category-ucpr-usbA": null,
        "category-ucpr-usbC": null,
        "category-ucpr-hdmi": null,
        "category-ucpr-byod": null
      },
      "logitech": {
        "logitech-product-model": "Not Connected",
        "logitech-product-name": "",
        "logitech-serial-number": "",
        "logitech-firmware-version": "",
        "hidden-firmwareUpdate": false,
        "logitech-sync-software-version": "",
        "logitech-status": "",
        "logitech-rightsight": false,
        "ConnectedToIoTHub": "Offline"
      },
      "Applications": {
        "application-mode-status": "SkypeRoom",
        "teams-video-address": null
      }
    }
  ]
}
*/