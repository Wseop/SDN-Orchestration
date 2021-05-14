package com.ss.sdnproject.main;

import static org.slf4j.LoggerFactory.getLogger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.jboss.netty.channel.Channel;
import org.onlab.packet.Ethernet;
import org.onlab.packet.MacAddress;
import org.onlab.packet.MplsLabel;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Host;
import org.onosproject.net.HostId;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceEvent;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.flow.DefaultFlowRule;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.group.DefaultGroupBucket;
import org.onosproject.net.group.DefaultGroupDescription;
import org.onosproject.net.group.DefaultGroupKey;
import org.onosproject.net.group.GroupBucket;
import org.onosproject.net.group.GroupBuckets;
import org.onosproject.net.group.GroupDescription;
import org.onosproject.net.group.GroupKey;
import org.onosproject.net.group.GroupService;
import org.onosproject.net.group.GroupStore;
import org.onosproject.net.host.HostEvent;
import org.onosproject.net.host.HostListener;
import org.onosproject.net.host.HostService;
import org.onosproject.net.link.LinkEvent;
import org.onosproject.net.link.LinkListener;
import org.onosproject.net.link.LinkService;
import org.onosproject.net.packet.InboundPacket;
import org.onosproject.net.packet.OutboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketPriority;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.topology.TopologyEvent;
import org.onosproject.net.topology.TopologyListener;
import org.onosproject.net.topology.TopologyService;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFOxmList;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFPacketInReason;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.oxm.OFOxmInPort;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TableId;
import org.slf4j.Logger;

import com.esotericsoftware.minlog.Log;

@Component(immediate = true)
public class Orchestrator {
	private ApplicationId appId;
	public final Logger log = getLogger(getClass());
	
	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	protected CoreService coreService;
	
	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	protected DeviceService deviceService;
	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	protected LinkService linkService;
	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	protected PacketService packetService;
	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	protected FlowObjectiveService flowObjectiveService;
	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	protected HostService hostService;
	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	protected TopologyService topologyService;
	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	protected FlowRuleService flowRuleService;
	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	protected GroupService groupService;
	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	protected GroupStore groupStore;
	
	private static boolean isParent = false;
	private Vector<LinkMapper> hostConnectedPoint = new Vector<LinkMapper>();
	private Map<String, Vector<FlowRule>> ipToFlowRule = new HashMap<String, Vector<FlowRule>>();

	private static boolean proactiveMode = false;
	private static DeviceId srcDeviceId = DeviceId.deviceId("of:0000000000000006");
	private static DeviceId dstDeviceId = DeviceId.deviceId("of:0000000000000005");
	
	@Activate
	public void activate() {
		appId = coreService.registerApplication("org.onosproject.app.orchestrator");
		deviceService.addListener(deviceListener);
		linkService.addListener(linkListener);

		/* packet handler */
		packetService.addProcessor(processor, PacketProcessor.director(0));
		TrafficSelector.Builder selector = DefaultTrafficSelector.builder();
		selector.matchEthType(Ethernet.TYPE_IPV4);
		packetService.requestPackets(selector.build(), PacketPriority.CONTROL, appId);
		
		hostService.addListener(hostListener);
		topologyService.addListener(topologyListener);
	}
	
	@Deactivate()
	public void deactivate() {
		deviceService.removeListener(deviceListener);
		linkService.removeListener(linkListener);
		packetService.removeProcessor(processor);
		hostService.removeListener(hostListener);
		topologyService.removeListener(topologyListener);
	}
	
	DeviceListener deviceListener = new DeviceListener() {
		
		@Override
		public void event(DeviceEvent event) {
			// TODO Auto-generated method stub
			Iterable<Device> devices;
			
			switch (event.type()) {
			case DEVICE_ADDED:
				devices = deviceService.getDevices();
				
				for (Device d : devices) {
					if (deviceService.isAvailable(d.id())) {
						if (isParent) {
							enrollDeviceToGroup(d);							
						} else {
							startNT(d);
						}
					}
				}
				break;
			case DEVICE_AVAILABILITY_CHANGED:
				devices = deviceService.getDevices();
				
				for (Device d : devices) {
					if (!deviceService.isAvailable(d.id())) {
						if (isParent) {
							deleteDeviceFromGroup(d.id());							
						} else {
							closeNT(d.id());
						}
					}
				}
				break;
			case DEVICE_UPDATED:
				devices = deviceService.getDevices();
				
				for (Device d : devices) {
					if (deviceService.isAvailable(d.id())) {
						if (isParent) {
							enrollDeviceToGroup(d);							
						} else {
							startNT(d);
						}
					}
				}
				break;
			case DEVICE_REMOVED:
				Set <DeviceId> keys = null;
				Iterator <DeviceId> key = null;
				devices = deviceService.getDevices();
				
				if (isParent) {
					keys = GroupInfo.deviceIdToGroupId.keySet();
					key = keys.iterator();					
				} else {
					keys = SwitchDataStructure.deviceToBuilder.keySet();
					key = keys.iterator();
				}
				
				/* find removed device then closeNT or delete from group */
				while (key.hasNext()) {
					boolean exist = false;
					DeviceId id = key.next();
					
					for (Device d : devices) {
						if (d.id().equals(id)) {
							exist = true;
							break;
						}
					}
					if (!exist) {
						if (isParent) {
							deleteDeviceFromGroup(id);							
						} else {
							closeNT(id);
						}
					}
				}
				break;
			case PORT_UPDATED:
				if (!isParent) {
					devices = deviceService.getDevices();
					
					for (Device d : devices) {
						OpenflowBuilder b = SwitchDataStructure.deviceToBuilder.get(d.id());
						
						b.setPortDescStatsReply(deviceService.getPorts(d.id()));
						SwitchDataStructure.deviceToBuilder.put(d.id(), b);
					}
				}
				break;
			case PORT_STATS_UPDATED:
				if (!isParent) {
					devices = deviceService.getDevices();
					
					for (Device d : devices) {
						OpenflowBuilder b = SwitchDataStructure.deviceToBuilder.get(d.id());
						
						b.setPortStatsReply(deviceService.getPortStatistics(d.id()));
						SwitchDataStructure.deviceToBuilder.put(d.id(), b);
					}
				}
				break;
			default:
				break;
			}
		}
	};
	
	/**
	 * Start netty thread that connects to controller
	 * 
	 * @param d 
	 */
	public void startNT(Device d) {
		
		boolean isNew = true;
		
		/* check if 'd' is a new device */
		if (SwitchDataStructure.deviceToBuilder.containsKey(d.id())) {
			isNew = false;
		}
		
		/*
		 * if 'd' is a new device then create data structure and
		 * start connection establishment
		 */
		if (isNew) {
			OpenflowBuilder builder = new OpenflowBuilder(d.id());
			
			/* while loop until receiving all info */
			while (true) {
				if (!builder.setFeatureReply(d)) {
					continue;
				}
				if (!builder.setDescStatsReply(d)) {
					continue;
				}
				if (!builder.setPortDescStatsReply(deviceService.getPorts(d.id()))) {
					continue;
				}
				if (!builder.setPortStatsReply(deviceService.getPortStatistics(d.id()))) {
					continue;
				}
				
				log.info("Openflow builder created (id : " + d.id().toString() + ")");
				SwitchDataStructure.deviceToBuilder.put(d.id(), builder);
				break;
			}
			
			/* start netty */
			NettyThread NT = new NettyThread("192.168.56.101", d.id());
			NT.start();
		}
	}
	
	/**
	 * close netty channel if 'd' is unavailable and
	 * delete related SwtichDataStructure map info
	 * 
	 * @param d 
	 */
	public void closeNT(DeviceId d) {
		if (!SwitchDataStructure.deviceToBuilder.containsKey(d)) {
			return;
		}
		Channel c = SwitchDataStructure.deviceToChannel.get(Integer.parseInt(d.toString().substring(7), 16));
		
		if (SwitchDataStructure.deviceToBuilder.remove(d) != null) {
			log.info("closeNT :: deviceToBuilder value removed (" + d.toString() + ")");
		}
		if (SwitchDataStructure.deviceToChannel.remove(Integer.parseInt(d.toString().substring(7), 16)) != null) {
			log.info("closeNT :: deviceToChannel value removed (" + d.toString() + ")");
		}
		if (SwitchDataStructure.channelIdToDevice.remove(c.getId()) != null) {
			log.info("closeNT :: channelIdToDevice value removed (" + d.toString() + ")");
		}
		
		/* outside switch info reset */
		SwitchDataStructure.outsideSwitch.clear();
		c.close();
	}
	
	LinkListener linkListener = new LinkListener() {
		
		@Override
		public void event(LinkEvent event) {
			// TODO Auto-generated method stub
			Iterable<Link> links;
			
			switch (event.type()) {
			case LINK_ADDED:
				if (!isParent) {
					links = linkService.getLinks();
					
					SwitchDataStructure.linkSrcToDst.clear();
					
					/* link info setting to switch data structure map */
					for (Link l : links) {
						linkingStart(l);
					}
				}
				break;
			case LINK_REMOVED:
				if (!isParent) {
					links = linkService.getLinks();
					
					SwitchDataStructure.linkSrcToDst.clear();
					
					/* link info setting to switch data structure map */
					for (Link l : links) {
						linkingStart(l);
					}
				}
				break;
			default:
				break;
			}
		}
	};
	
	/**
	 * link info mapping to SwitchDataStructure map 
	 * 
	 * @param l 
	 */
	public void linkingStart(Link l) {
		
		Integer src = Integer.parseInt(l.src().deviceId().toString().substring(7), 16);
		Integer srcPort = Integer.parseInt(l.src().port().toString());
		Integer dst = Integer.parseInt(l.dst().deviceId().toString().substring(7), 16);
		Integer dstPort = Integer.parseInt(l.dst().port().toString());
		
		/* if src is outside network */
		if (!SwitchDataStructure.deviceToBuilder.containsKey(l.src().deviceId())) {
			SwitchDataStructure.outsideSwitch.add(Integer.parseInt(l.src().deviceId().toString().substring(7), 16));
		}
		
		/* link mapping */
		SwitchDataStructure.setLink(src, srcPort, dst, dstPort);
	}

	
	PacketProcessor processor = new PacketProcessor() {
		
		@Override
		public void process(PacketContext context) {
			// TODO Auto-generated method stub
			if (!isParent) {
				processOutsidePacket(context.outPacket());
				if (!proactiveMode) {
					processFlows(context);
				}
			}
		}
	};
	
	/**
	 * process packet from outside
	 * @param packetOut
	 */
	private void processOutsidePacket(OutboundPacket packetOut) {
		ByteBuffer lldpData = packetOut.data();
		byte[] temp = new byte[6];

		/* get switch's dpid from lldp */
		temp[0] = lldpData.get(17);	temp[1] = lldpData.get(18);	temp[2] = lldpData.get(19);
		temp[3] = lldpData.get(20);	temp[4] = lldpData.get(21);	temp[5] = lldpData.get(22);
		Integer dpid = byteArrayToInt(temp);
		
		/* if dpid is outside switch then process packet */
		if (SwitchDataStructure.outsideSwitch.contains(dpid)) {
			OFFactory FACTORY = OFFactories.getFactory(OFVersion.OF_13);
			OFOxmInPort oxe = null;
			Iterator<LinkMapper> keys = SwitchDataStructure.linkSrcToDst.keySet().iterator();
			
			while (keys.hasNext()) {
				LinkMapper link = keys.next();
				if ((link.dpid == dpid) && (link.portNo == lldpData.get(29))) {
					int portNumber = SwitchDataStructure.linkSrcToDst.get(link).portNo;
					oxe = FACTORY.oxms().inPort(OFPort.of(portNumber));
					break;
				}
			}
			if (oxe == null) {
				return;
			} else {
				OFOxmList oxmList = OFOxmList.of(oxe);
				Match m = FACTORY.buildMatchV3().setOxmList(oxmList).build();
				
				OFPacketIn ofpi = FACTORY.buildPacketIn().setMatch(m).setBufferId(OFBufferId.NO_BUFFER).setTotalLen(67)
						.setReason(OFPacketInReason.ACTION).setTableId(TableId.of(0)).setData(lldpData.array())
						.build();
				if (ofpi == null) {
					return;
				}
				
				Channel c = SwitchDataStructure.deviceToChannel.get(
						Integer.parseInt(packetOut.sendThrough().toString().substring(7), 16));
				c.write(ofpi);
			}
		}
	}
	
	/**
	 * @param context
	 */
	private void processFlows(PacketContext context) {
		
		if (context.isHandled()) {
            return;
        }

        InboundPacket pkt = context.inPacket();
        Ethernet ethPkt = pkt.parsed();
                
        if (ethPkt == null) {
            return;
        }

        // Bail if this is deemed to be a control packet.
        if (ethPkt.getEtherType() == Ethernet.TYPE_LLDP || ethPkt.getEtherType() == Ethernet.TYPE_BSN) {
            return;
        }

        // Skip IPv6 multicast packet when IPv6 forward is disabled.
        if (ethPkt.getEtherType() == Ethernet.TYPE_IPV6) {
            return;
        }

        HostId id_dst = HostId.hostId(ethPkt.getDestinationMAC());
        HostId id_src = HostId.hostId(ethPkt.getSourceMAC());
        
        // Do not process link-local addresses in any way.
        if (id_dst.mac().isLinkLocal() || id_src.mac().isLinkLocal()) {
            return;
        }

        // Do not process IPv4 multicast packets, let mfwd handle them
        if (ethPkt.getEtherType() == Ethernet.TYPE_IPV4) {
            if (id_dst.mac().isMulticast() || id_src.mac().isMulticast()) {
                return;
            }
        }
		
        Host dst = hostService.getHost(id_dst);
        Host src = hostService.getHost(id_src);
        
        // extracts ip from parsed version
        int idx = pkt.parsed().toString().indexOf("nw_dst");
		StringTokenizer ip = new StringTokenizer(pkt.parsed().toString().substring(idx)); ip.nextToken();
		String strIp = ip.nextToken().trim();
//		Log.info("strIp : " + strIp);
        
        /* If don't know destination host then forward flows */
        if (dst == null && src != null) {   	
        	
        	// apply only if packet is from 'host connected port'
			if (isFromInsideHost(pkt.receivedFrom())) {
				applyFlows(src, strIp);
			}
        }
        else if (dst != null && src != null) {

			// remove only if packet is from 'host connected port'
			if (isFromInsideHost(pkt.receivedFrom())) {
				if (ipToFlowRule.containsKey(strIp)) {
					removeIpFlowRules(strIp);
				}
			}

        }
	}
	
	HostListener hostListener = new HostListener() {
		
		@Override
		public void event(HostEvent event) {
			// TODO Auto-generated method stub
			switch (event.type()) {
			case HOST_ADDED:
				Iterable<Host> host = hostService.getHosts();
				
				if (proactiveMode) {
					for (Host h : host) {
						if (srcDeviceId.equals(h.location().deviceId())) {
							applyHostFlow(h);
						}
					}
				} else {
					
					// hostConnectedPoint
					for(Host h : host) {
						if(hostConnectedPointContains(h) == false) {
							LinkMapper createLinkMapper = new LinkMapper(h.location().deviceId(), h.location().port());
							hostConnectedPoint.addElement(createLinkMapper);
							//Log.info("Host Connected DeviceId And Port Number Added: " + h.location().deviceId() + " " + h.location().port().toLong());
						}
					}
				}
				break;
			default:
				break;
			}
		}
	};
	
	TopologyListener topologyListener = new TopologyListener() {
		
		@Override
		public void event(TopologyEvent event) {
			// TODO Auto-generated method stub
			
			/* Apply flows */
			if (proactiveMode) {
				if ((deviceService.getDevice(srcDeviceId) != null) && (deviceService.getDevice(dstDeviceId) != null)) {
					applyFlows();
				}
			}
		}
	};
	
	private boolean hostConnectedPointContains(Host h) {
		boolean doesSameExist = false;
		for(int i = 0; i < hostConnectedPoint.size(); i++) {
			LinkMapper linkMapper = hostConnectedPoint.elementAt(i);
			if(h.location().deviceId().equals(linkMapper.deviceId) && h.location().port().equals(linkMapper.portNum))
				doesSameExist = true;
		}
		return doesSameExist;
	}

	/**
	 * Proactive
	 * @param h
	 */
	private void applyHostFlow(Host h) {
		FlowRule rule = null;
		Set<Path> paths = topologyService.getPaths(topologyService.currentTopology(), srcDeviceId, dstDeviceId);
		Path path = null;
		Iterator<Link> links = null;
		Link l = null;

		/* To outside */
		if (!paths.iterator().hasNext()) {
			return;
		}
		path = paths.iterator().next();
		links = path.links().iterator();
		if (!links.hasNext()) {
			return;
		}
		l = links.next(); 
		
		rule = buildSrcFlowRule(srcDeviceId, Ethernet.TYPE_ARP, l.src().port(), h.location().port(), h.mac());
		flowRuleService.applyFlowRules(rule);
//		Log.info(rule.toString());
		
		/* From outside */
		rule = buildDstFlowRule(srcDeviceId, Ethernet.TYPE_ARP, h.location().port(), l.src().port(), h.mac());
		flowRuleService.applyFlowRules(rule);
	}
	
	private void applyFlows() {
		FlowRule rule = null;
		Set<Path> paths = topologyService.getPaths(topologyService.currentTopology(), srcDeviceId, dstDeviceId);
		Path path = null;
		Iterator<Link> links = null;
		Link l = null;
		DeviceId src = null;
		PortNumber inPort = null, outPort = null;
		
		if (!paths.iterator().hasNext()) {
			return;
		}
		path = paths.iterator().next();
		links = path.links().iterator();
		if (!links.hasNext()) {
			return;
		}
		
		/* Pass host point */
		l = links.next(); 
		
		/* Forward remains point */
		src = l.dst().deviceId();
		inPort = l.dst().port();
		while (links.hasNext()) {
			l = links.next();
			
			outPort = l.src().port();
			
			/* To outside */
			rule = buildFlowRule(src, Ethernet.TYPE_ARP, outPort, inPort);
			flowRuleService.applyFlowRules(rule);
//			Log.info(rule.toString());
			
			/* From outside */
			rule = buildFlowRule(src, Ethernet.TYPE_ARP, inPort, outPort);
			flowRuleService.applyFlowRules(rule);
//			Log.info(rule.toString());
			
			src = l.dst().deviceId();
			inPort = l.dst().port();
		}
		
		links = linkService.getLinks().iterator();
		while (links.hasNext()) {
			l = links.next();
			
			if (l.dst().deviceId().equals(src)) {
				if (SwitchDataStructure.outsideSwitch.contains(Integer.parseInt(l.src().deviceId().toString().substring(7), 16))) {
					outPort = l.dst().port();
					
					/* To outside */
					rule = buildFlowRule(src, Ethernet.TYPE_ARP, outPort, inPort);
					flowRuleService.applyFlowRules(rule);
//					Log.info(rule.toString());
					
					/* From outside */
					rule = buildFlowRule(src, Ethernet.TYPE_ARP, inPort, outPort);
					flowRuleService.applyFlowRules(rule);
//					Log.info(rule.toString());
				}
			}
		}
	}
	
	/**
	 * For reactive
	 */
	private void applyFlows(Host h, String ip) {
		Iterable<Device> devices = deviceService.getAvailableDevices();
		FlowRule rule = null;
		
		Vector<FlowRule> flowRuleVector = new Vector<FlowRule>(); 
		DeviceId device_src = h.location().deviceId();
		
		for (Device device_dst : devices) {
			if (!h.location().deviceId().equals(device_dst.id())) {
				Set<Path> paths = topologyService.getPaths(topologyService.currentTopology(), device_src, device_dst.id());
				Path path = null;
				Iterator<Link> links = null;
				Link l = null;
				DeviceId src = null;
				PortNumber inPort = null, outPort = null;
				
				if (!paths.iterator().hasNext()) {
					continue;
				}
				path = paths.iterator().next();
				links = path.links().iterator();
				if (!links.hasNext()) {
					continue;
				}
				l = links.next(); 
				
				/* Forward start point */
				rule = buildSrcFlowRule(device_src, Ethernet.TYPE_ARP, PortNumber.FLOOD, h.location().port(), h.mac());
				flowRuleService.applyFlowRules(rule);
//				Log.info(rule.toString());
				flowRuleVector.addElement(rule);
				
				/* Forward remains point */
				src = l.dst().deviceId();
				inPort = l.dst().port();
				while (links.hasNext()) {
					l = links.next();
					
					outPort = l.src().port();
					rule = buildSrcFlowRule(src, Ethernet.TYPE_ARP, outPort, inPort, h.mac());
					flowRuleService.applyFlowRules(rule);
//					Log.info(rule.toString());
					flowRuleVector.addElement(rule);
					
					src = l.dst().deviceId();
					inPort = l.dst().port();
				}
				
				links = linkService.getLinks().iterator();
				while (links.hasNext()) {
					l = links.next();
					
					if (l.dst().deviceId().equals(src)) {
						if (SwitchDataStructure.outsideSwitch.contains(Integer.parseInt(l.src().deviceId().toString().substring(7), 16))) {
							outPort = l.dst().port();
							rule = buildSrcFlowRule(src, Ethernet.TYPE_ARP, outPort, inPort, h.mac());
							flowRuleService.applyFlowRules(rule);
//							Log.info(rule.toString());
							flowRuleVector.addElement(rule);
						}
					}
				}
			}
		}
		ipToFlowRule.put(ip, flowRuleVector);
//		Log.info("One Flow Path Done");
	}
	

	private FlowRule buildSrcFlowRule(DeviceId deviceId, short ethType, PortNumber outPort, PortNumber inPort, MacAddress src_host) {
		FlowRule rule = null;
		TrafficSelector.Builder match = DefaultTrafficSelector.builder();
		TrafficTreatment.Builder action = DefaultTrafficTreatment.builder();
		
		if (inPort != null) {
			match.matchInPort(inPort)
				 .matchEthSrc(src_host);
		}
		match.matchEthType(ethType);
		action.setOutput(outPort);
		
		rule = DefaultFlowRule.builder()
							  .forDevice(deviceId)
							  .withSelector(match.build())
							  .withTreatment(action.build())
							  .withPriority(40010)
							  .fromApp(appId)
//							  .makeTemporary(5)
							  .makePermanent()
							  .build();
//		Log.info("buildFlowRule : " + deviceId.toString() + ", " + inPort + " -> " + outPort);
		return rule;
	}
	
	private FlowRule buildDstFlowRule(DeviceId deviceId, short ethType, PortNumber outPort, PortNumber inPort, MacAddress dst_host) {
		FlowRule rule = null;
		TrafficSelector.Builder match = DefaultTrafficSelector.builder();
		TrafficTreatment.Builder action = DefaultTrafficTreatment.builder();
		
		if (inPort != null) {
			match.matchInPort(inPort)
				 .matchEthDst(dst_host);
		}
		match.matchEthType(ethType);
		action.setOutput(outPort);
		
		rule = DefaultFlowRule.builder()
							  .forDevice(deviceId)
							  .withSelector(match.build())
							  .withTreatment(action.build())
							  .withPriority(40010)
							  .fromApp(appId)
//							  .makeTemporary(5)
							  .makePermanent()
							  .build();
//		Log.info("buildFlowRule : " + deviceId.toString() + ", " + inPort + " -> " + outPort);
		return rule;
	}
	
	/* For proactive */
	private FlowRule buildFlowRule(DeviceId deviceId, short ethType, PortNumber outPort, PortNumber inPort) {
		FlowRule rule = null;
		TrafficSelector.Builder match = DefaultTrafficSelector.builder();
		TrafficTreatment.Builder action = DefaultTrafficTreatment.builder();
		
		if (inPort != null) {
			match.matchInPort(inPort);
		}
		match.matchEthType(ethType);
		action.setOutput(outPort);
		
		rule = DefaultFlowRule.builder()
							  .forDevice(deviceId)
							  .withSelector(match.build())
							  .withTreatment(action.build())
							  .withPriority(40010)
							  .fromApp(appId)
							  .makePermanent()
							  .build();
//		Log.info("buildFlowRule : " + deviceId.toString() + ", " + inPort + " -> " + outPort);
		return rule;
	}
	
	private boolean isFromInsideHost(ConnectPoint connectPoint) {
		boolean isTrue = false;
		for(int i = 0; i < hostConnectedPoint.size(); i++) {
    		LinkMapper linkMapper = hostConnectedPoint.elementAt(i);
    		if(connectPoint.deviceId().equals(linkMapper.deviceId) && connectPoint.port().equals(linkMapper.portNum)) {
    			isTrue = true;
    		}
		}
		return isTrue;
	}
	
	private void removeIpFlowRules(String strIp) {
		Vector<FlowRule> flowRules = new Vector<FlowRule>();
		flowRules = ipToFlowRule.get(strIp);
		for (int j = 0; j < flowRules.size(); j++) {
			flowRuleService.removeFlowRules(flowRules.elementAt(j));
//			Log.info("DELETE : " + flowRules.elementAt(j).toString());
		}
		ipToFlowRule.remove(strIp);
	}
	
	public int byteArrayToInt(byte bytes[]) {
		return ((((int)bytes[0] & 0xff) << 40) | 
				(((int)bytes[1] & 0xff) << 32) | 
				(((int)bytes[2] & 0xff) << 24) | 
				(((int)bytes[3] & 0xff) << 16) |
				(((int)bytes[4] & 0xff) << 8) | 
				((int)bytes[5] & 0xff)
				);
	}
	
	/** enroll device to group */
	private void enrollDeviceToGroup(Device d) {
		
		/* if new device is added then enroll */
		if (!GroupInfo.deviceIdToGroupId.containsKey(d.id())) {
			String ip = extractIpAddress(d.annotations().toString());
			
			/* if new ip then mapping new ip & new groupId */
			if (!GroupInfo.IpToGroupId.containsKey(ip)) {
				GroupInfo.IpToGroupId.put(ip, GroupInfo.groupCount++);
			}
			
			/* add device to group */
			int groupId = GroupInfo.IpToGroupId.get(ip);
			createGroup(d.id(), groupId);
			GroupInfo.deviceIdToGroupId.put(d.id(), groupId);
			GroupInfo.groupTable.put(groupId, d.id());
		}
	}
	
	/** delete device from group */
	private void deleteDeviceFromGroup(DeviceId id) {
		Integer groupId = GroupInfo.deviceIdToGroupId.get(id);
		
		GroupInfo.deviceIdToGroupId.remove(id);
		GroupInfo.groupTable.remove(groupId, id);
	}
	
	/** create indirect group (just classify devices) */
	private void createGroup(DeviceId deviceId, int groupId) {
		List<GroupBucket> buckets = new ArrayList<>();
		
		TrafficTreatment.Builder tBuilder = DefaultTrafficTreatment.builder();
		tBuilder.setOutput(PortNumber.ALL)
				.setEthDst(MacAddress.BROADCAST)
				.setEthSrc(MacAddress.BROADCAST)
				.pushMpls()
				.setMpls(MplsLabel.mplsLabel(MplsLabel.MAX_MPLS));
		buckets.add(DefaultGroupBucket.createIndirectGroupBucket(tBuilder.build()));
		
		GroupBuckets groupBuckets = new GroupBuckets(buckets);
		
		String key = "orchestration";
		GroupKey groupKey = new DefaultGroupKey(key.getBytes());
		
		GroupDescription groupDesc = new DefaultGroupDescription(deviceId, GroupDescription.Type.INDIRECT, groupBuckets, groupKey, groupId, appId);
		
		groupService.addGroup(groupDesc);
		groupStore.storeGroupDescription(groupDesc);
	}
	
	/** extract Ip address from annotations */
	private String extractIpAddress(String str) {
		int idx = str.indexOf("channelId=") + 10;	//ip start index
		StringTokenizer ip = new StringTokenizer(str.substring(idx), ":");
		
		return ip.nextToken();
	}

}