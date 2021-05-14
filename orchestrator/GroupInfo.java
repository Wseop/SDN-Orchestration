package com.ss.sdnproject.main;

import java.util.HashMap;
import java.util.Map;

import org.onosproject.net.DeviceId;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class GroupInfo {
	public static int groupCount = 1;
	
	public static Map <String, Integer> IpToGroupId = new HashMap<>();
	public static Map <DeviceId, Integer> deviceIdToGroupId = new HashMap<>();
	public static Multimap<Integer, DeviceId> groupTable = HashMultimap.create();
}
