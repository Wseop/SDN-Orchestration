package com.ss.sdnproject.main;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.StringTokenizer;

import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Port;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.PortStatistics;
import org.projectfloodlight.openflow.protocol.OFBucket;
import org.projectfloodlight.openflow.protocol.OFBucketCounter;
import org.projectfloodlight.openflow.protocol.OFCapabilities;
import org.projectfloodlight.openflow.protocol.OFDescStatsReply;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFeaturesReply;
import org.projectfloodlight.openflow.protocol.OFFlowMod;
import org.projectfloodlight.openflow.protocol.OFFlowRemoved;
import org.projectfloodlight.openflow.protocol.OFFlowStatsEntry;
import org.projectfloodlight.openflow.protocol.OFFlowStatsReply;
import org.projectfloodlight.openflow.protocol.OFGroupDescStatsEntry;
import org.projectfloodlight.openflow.protocol.OFGroupDescStatsReply;
import org.projectfloodlight.openflow.protocol.OFGroupStatsEntry;
import org.projectfloodlight.openflow.protocol.OFGroupStatsReply;
import org.projectfloodlight.openflow.protocol.OFGroupType;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.protocol.OFPortDescStatsReply;
import org.projectfloodlight.openflow.protocol.OFPortStatsEntry;
import org.projectfloodlight.openflow.protocol.OFPortStatsReply;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.OFAuxId;
import org.projectfloodlight.openflow.types.OFGroup;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.U64;

import com.google.common.collect.ImmutableList;

public class OpenflowBuilder {
	public static OFFactory FACTORY = OFFactories.getFactory(OFVersion.OF_13);

	private DeviceId deviceId;
	private OFFeaturesReply.Builder featureReply;
	private OFPortDescStatsReply.Builder portDescStatsReply;
	private OFDescStatsReply.Builder descStatsReply;
	private OFPortStatsReply.Builder portStatsReply;
	private OFGroupDescStatsReply.Builder groupDescStatsReply;
	private OFGroupStatsReply.Builder groupStatsReply;
	
	private List<OFFlowStatsEntry> flowStatsEntries;
	private OFFlowStatsReply.Builder flowStatsReply;

	public OpenflowBuilder(DeviceId id) {
		deviceId = id;
		featureReply = null;
		portDescStatsReply = null;
		descStatsReply = null;
		portStatsReply = null;
		groupDescStatsReply = null;
		groupStatsReply = null;
		flowStatsEntries = new ArrayList<>();
		flowStatsReply = null;
	}
	
	/* feature build */
	public boolean setFeatureReply(Device device) {
		if (device == null) {
			return false;
		} else if (featureReply != null) {
			return true;
		}
		
		featureReply = buildFeatureReply(device);
		return true;
	}
	private OFFeaturesReply.Builder buildFeatureReply(Device device) {
		return FACTORY.buildFeaturesReply()
				.setDatapathId(DatapathId.of(device.chassisId().value()))
				.setNBuffers(256).setNTables((short) 254)
				.setAuxiliaryId(OFAuxId.of(0))
				.setCapabilities(EnumSet.<OFCapabilities>of(
						OFCapabilities.FLOW_STATS, 
						OFCapabilities.TABLE_STATS, 
						OFCapabilities.PORT_STATS, 
						OFCapabilities.GROUP_STATS, 
						OFCapabilities.QUEUE_STATS));
	}

	/* Desc Stats build */
	public boolean setDescStatsReply(Device device) {
		if (device == null) {
			return false;
		} else if (descStatsReply != null) {
			return true;
		}
		
		descStatsReply = buildDescStatsReply(device);
		return true;
	}
	private OFDescStatsReply.Builder buildDescStatsReply(Device device) {
		return FACTORY.buildDescStatsReply()
				.setMfrDesc(device.manufacturer())
				.setHwDesc(device.hwVersion())
				.setSwDesc(device.swVersion())
				.setSerialNum(device.serialNumber());
	}
	
	/* Port Desc Stats build */
	public boolean setPortDescStatsReply(List<Port> ports) {
		if (ports.isEmpty()) {
			return false;
		}
		
		portDescStatsReply = buildPortDescStatsReply(ports);
		return true;
	}
	private OFPortDescStatsReply.Builder buildPortDescStatsReply(List<Port> ports) {
		List<OFPortDesc> list = new ArrayList<OFPortDesc>();

		for (Port p : ports) {
			
			/* extract port name */
			int idx = p.annotations().toString().indexOf("portName=") + 9;
			String annotation = p.annotations().toString().substring(idx);
			StringTokenizer token = new StringTokenizer(annotation, "}");
			String name = token.nextToken();
			
			if (p.isEnabled()) {
				if (p.number() == PortNumber.LOCAL) {
					list.add(FACTORY.buildPortDesc()
							.setPortNo(OFPort.LOCAL)
							.setCurrSpeed(p.portSpeed())
							.setMaxSpeed(10000)
							.setName(name).build());
				} else {
					
					list.add(FACTORY.buildPortDesc()
							.setPortNo(OFPort.of((int) p.number().toLong()))
							.setCurrSpeed(p.portSpeed())
							.setMaxSpeed(10000)
							.setName(name).build());
				}
			}
		}
		return FACTORY.buildPortDescStatsReply().setEntries(ImmutableList.<OFPortDesc>copyOf(list));
	}

	/* Port Stats build */
	public boolean setPortStatsReply(List<PortStatistics> portStats) {
		if (portStats.isEmpty()) {
			return false;
		} 
		
		portStatsReply = buildPortStatsReply(portStats);
		return true;
	}
	private OFPortStatsReply.Builder buildPortStatsReply(List<PortStatistics> portStats) {
		List<OFPortStatsEntry> list = new ArrayList<OFPortStatsEntry>();

		for (PortStatistics p : portStats) {
			OFPortStatsEntry pse = FACTORY.buildPortStatsEntry()
					.setPortNo(OFPort.of(p.port()))
					.setDurationSec(p.durationSec())
					.setDurationNsec(p.durationNano())
					.setRxBytes(U64.of(p.bytesReceived()))
					.setRxDropped(U64.of(p.packetsRxDropped()))
					.setRxErrors(U64.of(p.packetsRxErrors()))
					.setRxPackets(U64.of(p.packetsReceived()))
					.setTxBytes(U64.of(p.bytesSent()))
					.setTxDropped(U64.of(p.packetsTxDropped()))
					.setTxErrors(U64.of(p.packetsTxErrors()))
					.setTxPackets(U64.of(p.packetsSent()))
					.build();
			list.add(pse);
		}
		return FACTORY.buildPortStatsReply().setEntries(ImmutableList.<OFPortStatsEntry>copyOf(list));
	}
	
	/* Group Desc Stats build */
	public boolean setGroupDescStatsReply(OFGroup id, OFGroupType type, List<OFBucket> buckets) {
		groupDescStatsReply = buildGroupDescStatsReply(id, type, buckets);
		return true;
	}
	private OFGroupDescStatsReply.Builder buildGroupDescStatsReply(OFGroup id, OFGroupType type, List<OFBucket> ofBuckets) {
		List<OFGroupDescStatsEntry> list = new ArrayList<>();
		List<OFBucket> buckets = new ArrayList<>();
	
		/* build bucket */
		for (OFBucket b : ofBuckets) {
			buckets.add(b);
		}
		
		/* build entries */
		OFGroupDescStatsEntry gdse = FACTORY.buildGroupDescStatsEntry()
				.setGroupType(type)
				.setGroup(id)
				.setBuckets(buckets).build();
		list.add(gdse);
		
		return FACTORY.buildGroupDescStatsReply().setEntries(ImmutableList.<OFGroupDescStatsEntry>copyOf(list));
	}
	
	/* Group Stats build */
	public boolean setGroupStatsReply(OFGroup id) {
		groupStatsReply = buildGroupStatsReply(id);
		return true;
	}
	private OFGroupStatsReply.Builder buildGroupStatsReply(OFGroup id) {
		List<OFGroupStatsEntry> list = new ArrayList<>();
		List<OFBucketCounter> counters = new ArrayList<>();
		
		/* build counters */
		OFBucketCounter c = FACTORY.buildBucketCounter()
				.setByteCount(U64.of((long)0))
				.setPacketCount(U64.of((long)0)).build();
		counters.add(c);
		
		/* build entries */
		OFGroupStatsEntry gse = FACTORY.buildGroupStatsEntry()
				.setBucketStats(counters)
				.setByteCount(U64.of((long)0))
//				.setDurationNsec(255)
//				.setDurationSec(254)
				.setGroup(id)
				.setPacketCount(U64.of((long)0))
				.setRefCount(0).build();
		list.add(gse);
		
		return FACTORY.buildGroupStatsReply().setEntries(ImmutableList.<OFGroupStatsEntry>copyOf(list));
	}
	
	/* Flow Stats Entries build (modify) */
	public void addFlowStatsEntry(OFFlowMod flowMod) {
		
		/* duplication process */
		for (int i = 0; i < flowStatsEntries.size(); i++) {
			OFFlowStatsEntry e = flowStatsEntries.get(i);
			
			if (flowMod.getCookie().equals(e.getCookie()) && 
				flowMod.getInstructions().equals(e.getInstructions()) &&
				flowMod.getMatch().equals(e.getMatch()) &&
				flowMod.getPriority() == e.getPriority()) {
				return;
			}
		}
		
		OFFlowStatsEntry flowStatsEntry = FACTORY.buildFlowStatsEntry()
//				.setActions(flowMod.getActions())
				.setByteCount(U64.of(0))
				.setCookie(flowMod.getCookie())
				.setDurationNsec(0)
				.setDurationSec(0)
				.setFlags(flowMod.getFlags())
				.setHardTimeout(flowMod.getHardTimeout())
				.setIdleTimeout(flowMod.getIdleTimeout())
//				.setImportance(0)
				.setInstructions(flowMod.getInstructions())
				.setMatch(flowMod.getMatch())
				.setPacketCount(U64.of(0))
				.setPriority(flowMod.getPriority())
				.setTableId(flowMod.getTableId()).build();
		flowStatsEntries.add(flowStatsEntry);
	}
	
	public void removeFlowStatsEntry(OFFlowRemoved flowRemoved) {
		for (int i = 0; i < flowStatsEntries.size(); i++) {
			OFFlowStatsEntry e = flowStatsEntries.get(i);
			
			if (flowRemoved.getCookie().equals(e.getCookie()) &&
				flowRemoved.getMatch().equals(e.getMatch()) &&
				flowRemoved.getPriority() == e.getPriority()) {
				flowStatsEntries.remove(i);
				return;
			}
		}
	}
	
	/* Flow Stats build */
	public boolean setFlowStatsReply() {
		flowStatsReply = buildFlowStatsReply();
		return true;
	}
	private OFFlowStatsReply.Builder buildFlowStatsReply() {
		return FACTORY.buildFlowStatsReply().setEntries(ImmutableList.<OFFlowStatsEntry>copyOf(flowStatsEntries));
	}

	public DeviceId getDeviceId() {
		return deviceId;
	}
	
	public OFFeaturesReply.Builder getFeatureReply() {
		return featureReply;
	}

	public OFPortDescStatsReply.Builder getPortDescStatsReply() {
		return portDescStatsReply;
	}

	public OFDescStatsReply.Builder getDescStatsReply() {
		return descStatsReply;
	}

	public OFPortStatsReply.Builder getPortStatsReply() {
		return portStatsReply;
	}
	
	public OFGroupDescStatsReply.Builder getGroupDescStatsReply() {
		return groupDescStatsReply;
	}
	
	public OFGroupStatsReply.Builder getGroupStatsReply() {
		return groupStatsReply;
	}
	
	public OFFlowStatsReply.Builder getFlowStatsReply() {
		return flowStatsReply;
	}
}