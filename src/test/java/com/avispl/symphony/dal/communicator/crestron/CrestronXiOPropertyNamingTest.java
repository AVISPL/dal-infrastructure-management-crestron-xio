/*
 * Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.crestron;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Aggregated device property names in model-mapping.yml follow the naming policy (SYAL-3882):
 * acronyms in uppercase, no truncated words.
 */
@Tag("Mock")
public class CrestronXiOPropertyNamingTest {

	private static final Pattern PROPERTY_KEY = Pattern.compile("^ {8}([A-Za-z0-9()%#]+):", Pattern.MULTILINE);
	private static final Pattern MIXED_CASE_ACRONYM = Pattern.compile("(Ip|Mac|Dhcp|Usb|Hdmi|Dns|Ntp|Cpu)(?![a-z])");

	private List<String> readPropertyNames() throws Exception {
		try (InputStream in = getClass().getResourceAsStream("/xio/model-mapping.yml")) {
			Assertions.assertNotNull(in, "xio/model-mapping.yml not found");
			String yml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
			String section = yml.substring(yml.indexOf("properties:"));
			List<String> names = new ArrayList<>();
			Matcher m = PROPERTY_KEY.matcher(section);
			while (m.find()) {
				names.add(m.group(1));
			}
			return names;
		}
	}

	@Test
	void testNoMixedCaseAcronyms() throws Exception {
		for (String name : readPropertyNames()) {
			Assertions.assertFalse(MIXED_CASE_ACRONYM.matcher(name).find(), "Acronym is not uppercase in property name: " + name);
		}
	}

	@Test
	void testRenamedPropertiesPresent() throws Exception {
		List<String> names = readPropertyNames();
		for (String expected : Arrays.asList("IPAddress", "MACAddress", "DHCPEnabled", "USBIn", "DeviceBuildDate", "DefaultRouter",
				"HDMIInputHorizontalResolution", "HDMIInputVerticalResolution", "HDMIInputFramesPerSecond",
				"HDMIOutputHorizontalResolution", "HDMIOutputVerticalResolution", "HDMIOutputFramesPerSecond")) {
			Assertions.assertTrue(names.contains(expected), "Missing property " + expected + " in " + names);
		}
	}
}
