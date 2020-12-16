/*
 * Copyright (c) 2020 AVI-SPL, Inc. All Rights Reserved.
 */

package com.avispl.symphony.dal.communicator.crestron;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;
import com.avispl.symphony.dal.communicator.HttpCommunicator;
import com.avispl.symphony.dal.communicator.crestron.CrestronXiO.ScannedAggregatedDevice;
import com.avispl.symphony.dal.communicator.crestron.CrestronXiO.ScannedDeviceKey;
import com.avispl.symphony.dal.communicator.crestron.CrestronXiO.ScannedDeviceKeyComparator;

/**
 * Test app for CrestronXiO adapter.
 *
 * @author Vlad Kasyanenko / Symphony Dev Team <br>
 *         Created on Dec 7, 2020
 * @since 5.1
 */
public class CrestronXiOTestApp {

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
		crestronXiO.setPassword("3ccb51db641f4bb8b22fde31723a0d17");

		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

		try {
			crestronXiO.init();
			crestronXiO.retrieveMultipleStatistics(); // this is to un-pause monitoring

			TimeUnit.MILLISECONDS.sleep(1000); // give it some time to start up

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
			
			TimeUnit.MILLISECONDS.sleep(1000); // give it some time to finish

			List<AggregatedDevice> devices = crestronXiO.retrieveMultipleStatistics();
			int totalCount = devices.size();
			List<ScannedDeviceKey> deviceKeys = new ArrayList<>(totalCount);
			for (AggregatedDevice device : devices) {
				deviceKeys.add(new ScannedDeviceKey(device.getDeviceId(), ((ScannedAggregatedDevice) device).getScannedAt()));
				System.out.println("Device id: " + device.getDeviceId() + "; name: " + device.getDeviceName() + "; properties: " + device.getProperties() + "; statistics: " + device.getStatistics());
			}
			deviceKeys.sort(new ScannedDeviceKeyComparator());

			int scannedCount = 0;
			for (ScannedDeviceKey deviceKey : deviceKeys) {
				Long scannedAt = deviceKey.getScannedAt();
				if (scannedAt != null) {
					++scannedCount;
					System.out.println("Device " + deviceKey.getDeviceId() + " scanned at " + dateFormat.format(new Date(scannedAt)));
				} else {
					System.out.println("Device " + deviceKey.getDeviceId() + " not scanned");
				}
			}
			System.out.println("Total aggregated devices: " + totalCount);
			System.out.println("Scanned aggregated devices: " + scannedCount);
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

// device list API response example
/*
[
 {
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
   "device-status": "Offline"
 },
 ...
 ]
 */

// device status API response example
/*
{
  "device": {
    "device-cid": "00-10-7f-bb-85-03_-1",
    "device-accountid": "d65d142a-804f-4cd5-83a1-16b1d2f405c2",
    "device-groupid": "9d0bbf1b-7cf0-4766-8b23-4ca250bf9491",
    "device-model": "TSW-1060",
    "device-category": "TouchPanel",
    "device-manufacturer": "Crestron",
    "device-id": "@E-00107fbb8503",
    "device-name": "TSW-1060-00107FBB8503",
    "device-builddate": "Thu Jul 16 10:59:30 EDT 2020  (366047)",
    "device-key": "No SystemKey Server",
    "user-device-name": "CHI-MAGMILE-B160-TSW-1060-00107FBB8503",
    "serial-number": "1806JBH14888",
    "call-status": null,
    "displayed-input": null,
    "occupancy-status": null,
    "firmware-version": "2.009.0122",
    "sleep-status": null,
    "skype-presence": null,
    "cloudownedsettings-maximumcapacity": null,
    "hardware-type": null,
    "license-type": null
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
    "status-host-name": "TSW-1060-00107FBB8503",
    "status-domain-name": "aviinc.local",
    "nic-1-dns-servers": [
      "10.8.30.7(DHCP)",
      "10.1.10.27(DHCP)"
    ],
    "nic-1-dhcp-enabled": true,
    "nic-1-ip-address": "10.8.40.117",
    "nic-1-subnet-mask": "255.255.255.0",
    "nic-1-def-router": "10.8.40.1",
    "nic-1-mac-address": "00.10.7f.bb.85.03",
    "nic-1-link": true,
    "nic-2-dhcp-enabled": null,
    "nic-2-ip-address": null,
    "nic-2-subnet-mask": null,
    "nic-2-def-router": null,
    "nic-2-mac-address": null,
    "nic-2-link": null,
    "proxy-enabled": false,
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
    "statusTab.label.usbConnected": false,
    "statusTab.label.usbType": ""
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
    "ccs-serial-number": null,
    "ccs-camera-version": null,
    "ccs-software-version": null,
    "ccs-upgrade-version": null,
    "ccs-room-occupancy": null,
    "ccs-genius-framing-enabled": null,
    "ccs-occupant-count-enabled": null
  },
  "security": {
    "statusTab-encryptConnection": false
  },
  "Peripherals": {
    "features-conference-microphone": null,
    "features-conference-speaker": null,
    "features-default-speaker": null,
    "features-camera": null,
    "features-front-room-display": null,
    "features-hdmiingest": null
  },
  "logitech": {
    "logitech-product-model": null,
    "logitech-product-name": null,
    "logitech-serial-number": null,
    "logitech-firmware-version": null,
    "hidden-firmwareUpdate": null,
    "logitech-sync-software-version": null,
    "logitech-status": null,
    "logitech-rightsight": null,
    "ConnectedToIoTHub": true
  },
  "Applications": {
    "application-mode-status": "SkypeRoom",
    "teams-video-address": "10.8.40.116"
  },
  "Device Pairing": {
    "pairing-status": null
  },
  "Occupancy": {
    "statusOccupied": null,
    "statusTimeOut": null
  },
  "display": {
    "statusTab.label.displayStatus": "Standby"
  }
}
*/