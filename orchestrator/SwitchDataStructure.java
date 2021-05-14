package com.ss.sdnproject.main;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.jboss.netty.channel.Channel;
import org.onosproject.core.ApplicationId;
import org.onosproject.net.DeviceId;
import org.onosproject.net.PortNumber;
import org.onosproject.net.flowobjective.ForwardingObjective;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class SwitchDataStructure {
	public static Map <DeviceId, OpenflowBuilder> deviceToBuilder = new HashMap<>();
	public static Map <Integer, DeviceId> channelIdToDevice = new HashMap<>();
	public static Map <Integer, Channel> deviceToChannel = new HashMap<>();			//key : Integer.parseInt(DeviceId.toString().subString(7))
	public static Map <LinkMapper, LinkMapper> linkSrcToDst = new HashMap<>();
	public static Set <Integer> outsideSwitch = new HashSet<>();		//key : Integer.parseInt(DeviceId.toString().subString(7))
	
	/* table for outside path */
	public static Map <Pair<DeviceId, PortNumber>, Pair<DeviceId, PortNumber>> pathTable = new HashMap<>();
	
	public static void setLink(int sourceSwitchNo, int sourcePortNo, int destSwitchNo, int destPortNo) {
		
		/* check already mapped */
		Iterator<LinkMapper> keys = SwitchDataStructure.linkSrcToDst.keySet().iterator();
		while(keys.hasNext()) {
			LinkMapper link = keys.next();
			if((link.dpid == sourceSwitchNo) && (link.portNo == sourcePortNo)) {
				SwitchDataStructure.linkSrcToDst.put(new LinkMapper(sourceSwitchNo,sourcePortNo),
						new LinkMapper(destSwitchNo, destPortNo));
				SwitchDataStructure.linkSrcToDst.remove(link);
				return;
			}
		}
		SwitchDataStructure.linkSrcToDst.put(new LinkMapper(sourceSwitchNo,sourcePortNo),
				new LinkMapper(destSwitchNo, destPortNo));
	}
	
	public static int getOverLinkDpid(int sourceSwitchNo, int sourcePortNo) {
		Iterator<LinkMapper> keys = SwitchDataStructure.linkSrcToDst.keySet().iterator();
		while(keys.hasNext()) {
			LinkMapper link = keys.next();
			if((link.dpid == sourceSwitchNo) && (link.portNo == sourcePortNo)) {
				return SwitchDataStructure.linkSrcToDst.get(link).dpid;
			}
		}
		return -1;
	}
}
