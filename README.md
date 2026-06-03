# Crestron XiO Cloud Integration - Capabilities & Configuration
This document covers Crestron XiO Cloud Aggregator Capabilities and Configuration.

Note: Not to be confused with with Crestron Touch Panel Adapter.

Symphony integrates with Crestron XiO Cloud to provide comprehensive monitoring and control of Crestron devices across an environment.
Main features are: real-time device health monitoring, device metadata and status tracking, network data, and HDMI input/output information across a wide range of Crestron device types.

## Main use cases
- **Monitor** device online status, call status, firmware versions, and network data
- **Track** individual device details - mute status, sleep status, volume, connections (USB, Bluetooth), HDMI input/output
- **Inventory** keep Crestron devices registered in XiO Cloud in check
- **Control** devices metadata retrieval timeout to manage API request frequency

## Prerequisites and where to start
Crestron XiO Cloud Aggregator communicates with the Crestron XiO Cloud API at `api.crestron.io`.

To get started, you need a Crestron XiO Cloud account with API access. Obtain the following credentials from your XiO Cloud account:
- **Account ID** - used as the Username in Symphony device configuration
- **Subscription Key** - used as the Password in Symphony device configuration

To obtain your Subscription Key, please contact your Crestron Technical Support or Account Representative.

## Crestron XiO Device Configuration and Provisioning
Once the XiO Cloud API credentials are obtained, use the Account ID and Subscription Key for the Symphony device configuration.

The aggregated (Crestron) devices will be available as aggregated devices with different models.
Devices that have the Crestron XiO Cloud Aggregator device set as Monitoring Proxy are devices monitored by the XiO aggregator.

Once the XiO device is created with Monitoring Service -> Advanced Monitoring, the following configuration must be applied:

| Field | Value |
|---|---|
| Monitoring Source | Direct |
| Management Address | api.crestron.io |
| Protocol | HTTP |
| Username | XiO Cloud API Account ID |
| Password | XiO Cloud API Subscription Key |
| Port Number | 443 by default |

When the device is configured, saved and set active, the Crestron XiO Cloud Aggregator will start communicating with the XiO Cloud API to retrieve data about registered devices.
By default, unprovisioned devices will appear on Aggregated Devices -> Unprovisioned Devices tab.
To import a Poly Lens aggregated device for monitoring:
1. Open Aggregated Devices
2. Select unprovisioned devices
3. Fill required provisioning fields
4. Import devices into Symphony

Devices and available device data can be tuned by adapter configuration properties:

| Property | Description | Value |
|---|---|---|
| deviceMetaDataRetrievalTimeout | Timeout that defines the frequency of retrieving the Crestron device metadata from XiO Cloud API | Configurable, milliseconds |

Note: Adjusting the retrieval timeout helps reduce the number of requests made to the XiO Cloud API.

For detailed information on the aggregator and its configuration, please refer to our knowledgebase -> https://symphony.knowledgeowl.com/help/crestron-xio-cloud-aggregator-technical-breakdown

## Available Monitored Data
Crestron XiO Cloud Aggregator monitored data consists of 2 parts: Aggregator extended properties and Device extended properties.

Aggregator properties include service/adapter metadata:

| Property | Description |
|---|---|
| AdapterBuildDate | The build date of the adapter |
| AdapterUptime | How long the adapter has been running |
| AdapterVersion | Current version of the adapter |
| LastMonitoringCycleDuration(s) | Duration of the last monitoring cycle in seconds |
| MonitoredDevicesTotal | Total number of devices being monitored |
| MonitoringCycleInterval(min) | Interval between monitoring cycles in minutes |

Aggregated Devices provide the following monitoring capabilities:

| Property Type | Description |
|---|---|
| Device Metadata | Name, Serial Number, CID, Model, Manufacturer |
| Online Status | Whether the device is online or offline |
| Call Status | Active call status of the device |
| Sleep Status | Whether the device is in sleep mode |
| Mute Status | Audio mute state of the device |
| Firmware Version | Installed firmware version |
| Build Date | Firmware build date |
| Network Data | IP address, Subnet mask, MAC Address |
| Volume | Current volume level |
| Skype Presence | Skype presence state |
| Connections Info | USB and Bluetooth connection details |
| Services | Calendar and Sky Connection service status |
| HDMI Input/Output Information | Resolution and FPS for HDMI inputs/outputs |

## Supported Models and Devices

The Crestron XiO Cloud Aggregator supports a wide range of Crestron device categories, including:

- Presentation Systems
- Power Amplifiers
- Control Systems
- Crestron Mercury Tabletop
- Occupancy Sensors
- Automation Processor
- Network AV Encoders/Decoders
- Media Presentation Controller
- Digital Signal Processors
- AV Switch/Receiver
- Desk Phone
- UC Engines
- Touch Screens

For a complete list of Crestron XiO Cloud supported devices and models, refer to: https://symphony.knowledgeowl.com/help/crestron-xio-cloud-general-info

Note: Monitoring and Control capabilities may depend on the specific device model.

## Troubleshooting
**Login Error**
- Verify your XiO Cloud API Account ID and Subscription Key are correct
- Ensure the Symphony XiO Cloud Aggregator device is configured with the correct management address (api.crestron.io), protocol (HTTP), port (443), and credentials

**API Error**
- Check the API error description
- If it mentions configuration mismatches, verify all property values and data formats (API hostname, timeout limits, etc.)

**Link Error/Ping Timeout**
- Make sure your Cloud Connector can reach api.crestron.io
- Check the Ping Protocol in the Symphony XiO Cloud Aggregator device configuration
- Try switching between ICMP/TCP modes, as certain protocols may be blocked by proxy settings

If none of the recommended steps help, please enter an SOS ticket at {https://avi-spl.atlassian.net/servicedesk/customer/portals}

## What AI Assistant can do with it:
- Find Crestron XiO Aggregated Devices (XiO Aggregator as Monitoring Proxy)
- Verify Crestron XiO Cloud Aggregator configuration

## What AI Assistant cannot do with it:
- Provision the devices
