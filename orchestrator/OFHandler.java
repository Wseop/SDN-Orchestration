package com.ss.sdnproject.main;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang3.tuple.Pair;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.onosproject.net.DeviceId;
import org.onosproject.net.PortNumber;
import org.projectfloodlight.openflow.protocol.OFActionType;
import org.projectfloodlight.openflow.protocol.OFBadRequestCode;
import org.projectfloodlight.openflow.protocol.OFDescStatsReply;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFeaturesReply;
import org.projectfloodlight.openflow.protocol.OFFlowMod;
import org.projectfloodlight.openflow.protocol.OFFlowRemoved;
import org.projectfloodlight.openflow.protocol.OFFlowStatsReply;
import org.projectfloodlight.openflow.protocol.OFGroupDescStatsReply;
import org.projectfloodlight.openflow.protocol.OFGroupMod;
import org.projectfloodlight.openflow.protocol.OFGroupStatsEntry;
import org.projectfloodlight.openflow.protocol.OFGroupStatsReply;
import org.projectfloodlight.openflow.protocol.OFHello;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFOxmList;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFPacketInReason;
import org.projectfloodlight.openflow.protocol.OFPacketOut;
import org.projectfloodlight.openflow.protocol.OFPortDescStatsReply;
import org.projectfloodlight.openflow.protocol.OFPortStatsEntry;
import org.projectfloodlight.openflow.protocol.OFPortStatsReply;
import org.projectfloodlight.openflow.protocol.OFRoleReply;
import org.projectfloodlight.openflow.protocol.OFRoleRequest;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.oxm.OFOxmInPort;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFErrorCauseData;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TableId;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;

public class OFHandler extends IdleStateAwareChannelHandler implements ChannelHandler {

	public final Logger log = getLogger(getClass());
	
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent event) throws Exception {

		OFFactory FACTORY = OFFactories.getFactory(OFVersion.OF_13);
		OFHello hello = FACTORY.buildHello().build();

		if (ctx.getChannel().isOpen()) {
			ctx.getChannel().write(hello);
		}
	}

	@SuppressWarnings("null")
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) throws Exception {
		OFMessage ofMessage = (OFMessage) event.getMessage();
		OFFactory FACTORY = OFFactories.getFactory(OFVersion.OF_13);

		switch (ofMessage.getType()) {
		case HELLO:
			HELLO_Handler(ofMessage, FACTORY, ctx);
			break;

		case ECHO_REQUEST:
			ECHO_REQUEST_Handler(ofMessage, FACTORY, ctx);
			break;

		case FEATURES_REQUEST:
			FEATURES_REQUEST_Handler(ofMessage, FACTORY, ctx);
			break;

		case STATS_REQUEST:
			STATS_REQUEST_Handler(ofMessage, FACTORY, ctx);
			break;

		case ROLE_REQUEST:
			ROLE_REQUEST_Handler(ofMessage, FACTORY, ctx);
			break;

		case FLOW_MOD:
			FLOW_MOD_Handler(ofMessage, FACTORY, ctx);
			break;
			
		case FLOW_REMOVED:
			FLOW_REMOVED_Handler(ofMessage, FACTORY, ctx);
			break;

		case BARRIER_REQUEST:
			BARRIER_REQUEST_Handler(ofMessage, FACTORY, ctx);
			break;

		case GET_CONFIG_REQUEST:
			GET_CONFIG_REQUEST_Handler(ofMessage, FACTORY, ctx);
			break;

		case PACKET_OUT:
			PACKET_OUT_Handler(ofMessage, FACTORY, ctx);
			break;

		case GROUP_MOD:
			GROUP_MOD_Handler(ofMessage, FACTORY, ctx);
			break;
		default:
//			Log.info(ofMessage.getType().toString());
			break;
		}
	}

	/**
	 * This method makes OFPacketIn packet for link discovery. This packet
	 * contains lldp and is sent to controller.
	 * 
	 * @param ofMessage
	 * @param FACTORY
	 * @param data
	 * @param switchNo
	 * @param portNo
	 * @return
	 */
	private OFPacketIn makePacketIn(OFMessage ofMessage, OFFactory FACTORY, byte[] data, DeviceId id, int portNo) {

		/* In this case switchNo == dpid */
		OFOxmInPort oxe = matchPortAndSwitchNo(id, portNo, FACTORY);
		if (oxe == null){
			return null;
		}

		OFOxmList oxmList = OFOxmList.of(oxe);
		Match m = FACTORY.buildMatchV3().setOxmList(oxmList).build();

		OFPacketIn ofpi = FACTORY.buildPacketIn().setMatch(m).setBufferId(OFBufferId.NO_BUFFER).setTotalLen(67)
				.setXid(ofMessage.getXid()).setReason(OFPacketInReason.ACTION).setTableId(TableId.of(0)).setData(data)
				.build();

		return ofpi;
	}

	/**
	 * This method matches link with appropriate ports and switches.
	 * 
	 * @param switchNo is current switch number
	 * @param portNo is port number from lldp package
	 * @param FACTORY is from OFHANDLER
	 * @return OFOxmInport value
	 */
	private OFOxmInPort matchPortAndSwitchNo(DeviceId id, int portNo, OFFactory FACTORY) {

		OFOxmInPort oxe = null;
		int dpid = Integer.parseInt(id.toString().substring(7), 16);

		/* For Middle Controller */
		Iterator<LinkMapper> keys = SwitchDataStructure.linkSrcToDst.keySet().iterator();
		while (keys.hasNext()) {
			LinkMapper link = keys.next();
			if ((link.dpid == dpid) && (link.portNo == portNo)) {
				int portNumber = SwitchDataStructure.linkSrcToDst.get(link).portNo;
				oxe = FACTORY.oxms().inPort(OFPort.of(portNumber));
				break;
			}
		}

		if (oxe == null) {
			return null;
		}

		return oxe;
	}

	private void ECHO_REQUEST_Handler(OFMessage ofMessage, OFFactory FACTORY, ChannelHandlerContext ctx) {
		ctx.getChannel().write(FACTORY.buildEchoReply().setXid(ofMessage.getXid()).build());
	}

	private void HELLO_Handler(OFMessage ofMessage, OFFactory FACTORY, ChannelHandlerContext ctx) {

	}

	private void ECHO_REPLY_Handler(OFMessage ofMessage, OFFactory FACTORY, ChannelHandlerContext ctx) {

	}

	private void FEATURES_REQUEST_Handler(OFMessage ofMessage, OFFactory FACTORY, ChannelHandlerContext ctx) {
		DeviceId id = SwitchDataStructure.channelIdToDevice.get(ctx.getChannel().getId());
		OFFeaturesReply.Builder buildFeatureReply = SwitchDataStructure.deviceToBuilder.get(id).getFeatureReply();
		
		ctx.getChannel().write(buildFeatureReply.setXid(ofMessage.getXid()).build());
	}

	private void STATS_REQUEST_Handler(OFMessage ofMessage, OFFactory FACTORY, ChannelHandlerContext ctx) {
		DeviceId id = SwitchDataStructure.channelIdToDevice.get(ctx.getChannel().getId());
		OpenflowBuilder builder = SwitchDataStructure.deviceToBuilder.get(id);
		
		/* OFPortDescStats */
		if (ofMessage.toString().contains("OFPortDescStats")) {
			OFPortDescStatsReply.Builder buildPortDescStats = builder.getPortDescStatsReply();
			buildPortDescStats.setXid(ofMessage.getXid());
			OFPortDescStatsReply portDescStatsReply = buildPortDescStats.build();

			ctx.getChannel().write(portDescStatsReply);
		}

		/* OFDescStats */
		else if (ofMessage.toString().contains("OFDescStats")) {
			OFDescStatsReply.Builder buildDescStats = builder.getDescStatsReply();

			ctx.getChannel().write(buildDescStats.setXid(ofMessage.getXid()).build());
		}

		/* OFPortStats */
		else if (ofMessage.toString().contains("OFPortStats")) {
			OFPortStatsReply.Builder buildPortStats = builder.getPortStatsReply();
			buildPortStats.setXid(ofMessage.getXid());
			OFPortStatsReply portStatReply = buildPortStats.build();

			ctx.getChannel().write(portStatReply);
		}

		/* OFMeterStatsRequest */
		else if (ofMessage.toString().contains("OFMeterStatsRequest")) {
			ctx.getChannel().write(FACTORY.buildMeterStatsReply().setXid(ofMessage.getXid()).build());
		}

		/* OFGroupDescStatsRequest */
		else if (ofMessage.toString().contains("OFGroupDescStatsRequest")) {
			OFGroupDescStatsReply.Builder buildGroupDescStats = builder.getGroupDescStatsReply();
			if (buildGroupDescStats == null) {
				buildGroupDescStats = FACTORY.buildGroupDescStatsReply();
			}
			buildGroupDescStats.setXid(ofMessage.getXid());
			OFGroupDescStatsReply groupDescStatsReply = buildGroupDescStats.build();
			
			ctx.getChannel().write(groupDescStatsReply);
		}

		/* OFGroupStatsRequest */
		else if (ofMessage.toString().contains("OFGroupStatsRequest")) {
			OFGroupStatsReply.Builder buildGroupStats_old = builder.getGroupStatsReply();
			OFGroupStatsReply.Builder buildGroupStats = FACTORY.buildGroupStatsReply();
			
			if (buildGroupStats_old != null) {

				/* renew reply message (renew duration) */
				List<OFPortStatsEntry> pse = builder.getPortStatsReply().getEntries();
				
				List<OFGroupStatsEntry> groupStatsEntries_old = buildGroupStats_old.getEntries();
				List<OFGroupStatsEntry> groupStatsEntries = new ArrayList<>();
				for (OFGroupStatsEntry e : groupStatsEntries_old) {
					OFGroupStatsEntry gse = FACTORY.buildGroupStatsEntry()
							.setBucketStats(e.getBucketStats())
							.setByteCount(e.getByteCount())
							.setGroup(e.getGroup())
							.setPacketCount(e.getPacketCount())
							.setRefCount(e.getRefCount())
							.setDurationNsec(pse.iterator().next().getDurationNsec())
							.setDurationSec(pse.iterator().next().getDurationSec()).build();
					groupStatsEntries.add(gse);
					
					buildGroupStats.setEntries(ImmutableList.<OFGroupStatsEntry>copyOf(groupStatsEntries));
				}
			}
			buildGroupStats.setXid(ofMessage.getXid());
			
			OFGroupStatsReply groupStatsReply = buildGroupStats.build();
			ctx.getChannel().write(groupStatsReply);
		}

		/* OFFlowStats*/
		else if (ofMessage.toString().contains("OFFlowStats")) {
			builder.setFlowStatsReply();
			OFFlowStatsReply.Builder buildFlowStats = builder.getFlowStatsReply();
			if (buildFlowStats == null) {
				buildFlowStats = FACTORY.buildFlowStatsReply();
			}
			buildFlowStats.setXid(ofMessage.getXid());
			
			OFFlowStatsReply flowStatsReply = buildFlowStats.build();
			ctx.getChannel().write(flowStatsReply);
		}
		
		/* OFTableFeaturesStats */
		else if (ofMessage.toString().contains("OFTableFeaturesStats")) {
			ctx.getChannel()
					.write(FACTORY.errorMsgs().buildBadRequestErrorMsg().setCode(OFBadRequestCode.BAD_TYPE)
							.setXid(ofMessage.getXid())
							.setData(OFErrorCauseData.of("0007000000000000".getBytes(), FACTORY.getVersion())).build());
		}
	}

	private void ROLE_REQUEST_Handler(OFMessage ofMessage, OFFactory FACTORY, ChannelHandlerContext ctx) {
		OFRoleRequest roleRequest = (OFRoleRequest) ofMessage;
		OFRoleReply roleReply;

		roleReply = FACTORY.buildRoleReply().setXid(ofMessage.getXid()).setRole(roleRequest.getRole())
				.setGenerationId(roleRequest.getGenerationId()).build();
		ctx.getChannel().write(roleReply);
	}

	private void FLOW_MOD_Handler(OFMessage ofMessage, OFFactory FACTORY, ChannelHandlerContext ctx) {
		DeviceId id = SwitchDataStructure.channelIdToDevice.get(ctx.getChannel().getId());
		OpenflowBuilder builder = SwitchDataStructure.deviceToBuilder.get(id);
		OFFlowMod flowMod = (OFFlowMod) ofMessage;
		
		/* pathTable set (proactive) */
		if (flowMod.getPriority() == 10) {
			setPathTable(flowMod);
		} else {
			builder.addFlowStatsEntry(flowMod);
		}
	}
	
	private void setPathTable(OFFlowMod flowMod) {
		PortNumber outPort = null, inPort = null;
		DeviceId srcId = null, dstId = null;
		List<OFAction> actions = flowMod.getActions();
		
		for (OFAction action : actions) {
			if (action.getType() == OFActionType.OUTPUT) {
				
				/* extract outPort */
				int idx = action.toString().indexOf("port=");
				StringTokenizer tmp = new StringTokenizer(action.toString().substring(idx), "port=");
				StringTokenizer port = new StringTokenizer(tmp.nextToken(), ",");
				
				outPort = PortNumber.portNumber(Long.parseLong(port.nextToken()));
			} else if (action.getType() == OFActionType.SET_FIELD){
				
				/* extract src, dst MacAddress */ 
				int idx = action.toString().indexOf("value=") + 6;
				String mac = action.toString().substring(idx, idx + 17);
				
				/* transform MacAddress to DeviceId */
				String id_str = "of:0000";
				StringTokenizer token_m = new StringTokenizer(mac, ":");
				
				while (token_m.hasMoreTokens()) {
					id_str += token_m.nextToken();
				}
				
				if (action.toString().contains("EthSrc")) {
					srcId = DeviceId.deviceId(id_str);
				} else if (action.toString().contains("EthDst")) {
					dstId = DeviceId.deviceId(id_str);
				}
			} else {
				return;
			}
		}
		
		/* extract inPort */
		StringTokenizer tmp = new StringTokenizer(flowMod.getMatch().toString(), "=");
		tmp.nextToken();
		StringTokenizer token_in = new StringTokenizer(tmp.nextToken(), ")");
		
		inPort = PortNumber.portNumber(token_in.nextToken());
		
		/* put in pathTable */
		if (srcId == null || outPort == null || dstId == null || inPort == null) {
			log.error("path setting fail");
			return;
		}
		Pair<DeviceId, PortNumber> src = Pair.of(srcId, outPort);
		Pair<DeviceId, PortNumber> dst = Pair.of(dstId, inPort);
		
		SwitchDataStructure.pathTable.put(src, dst);
	}
	
	private void FLOW_REMOVED_Handler(OFMessage ofMessage, OFFactory FACTORY, ChannelHandlerContext ctx) {
		DeviceId id = SwitchDataStructure.channelIdToDevice.get(ctx.getChannel().getId());
		OpenflowBuilder builder = SwitchDataStructure.deviceToBuilder.get(id);
		OFFlowRemoved flowRemoved = (OFFlowRemoved) ofMessage;
		
		builder.removeFlowStatsEntry(flowRemoved);
	}

	private void BARRIER_REQUEST_Handler(OFMessage ofMessage, OFFactory FACTORY, ChannelHandlerContext ctx) {
		ctx.getChannel().write(FACTORY.buildBarrierReply().setXid(ofMessage.getXid()).build());
	}

	private void GET_CONFIG_REQUEST_Handler(OFMessage ofMessage, OFFactory FACTORY, ChannelHandlerContext ctx) {
		ctx.getChannel().write(FACTORY.buildGetConfigReply().setXid(ofMessage.getXid()).setMissSendLen(65535).build());
	}

	private void PACKET_OUT_Handler(OFMessage ofMessage, OFFactory FACTORY, ChannelHandlerContext ctx) {
		DeviceId id = SwitchDataStructure.channelIdToDevice.get(ctx.getChannel().getId());

		byte[] data = null;

		OFPacketOut packetOut = (OFPacketOut) ofMessage;
		byte[] lldp = packetOut.getData();
		
		/* process of lldp */
		if (((lldp[12] == -120 && lldp[13] == -52) || (lldp[12] == -119 && lldp[13] == 66))) {

			data = new byte[81];
			for (int i = 0; i < lldp.length; i++) {
				data[i] = lldp[i];
			}
			
			OFPacketIn ofpi = makePacketIn(ofMessage, FACTORY, data, id, lldp[29]);
						
			if (ofpi == null) {
				return;
			}

			int switch_adjacent = SwitchDataStructure.getOverLinkDpid(Integer.parseInt(id.toString().substring(7), 16), lldp[29]);
			if (switch_adjacent == -1)
				return;

			Channel ctx_adjacent = SwitchDataStructure.deviceToChannel.get(switch_adjacent);
			ctx_adjacent.write(ofpi);
		}
	}
	
	private void GROUP_MOD_Handler(OFMessage ofMessage, OFFactory FACTORY, ChannelHandlerContext ctx) {
		DeviceId id = SwitchDataStructure.channelIdToDevice.get(ctx.getChannel().getId());
		OpenflowBuilder builder = SwitchDataStructure.deviceToBuilder.get(id);
		OFGroupMod groupMod = (OFGroupMod) ofMessage;
		
		builder.setGroupStatsReply(groupMod.getGroup());
		builder.setGroupDescStatsReply(groupMod.getGroup(), groupMod.getGroupType(), groupMod.getBuckets());
	}
}
