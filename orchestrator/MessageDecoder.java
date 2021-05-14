package com.ss.sdnproject.main;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFMessageReader;

public class MessageDecoder extends FrameDecoder implements ChannelHandler {

	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buffer) throws Exception {
		// TODO Auto-generated method stub
		OFMessageReader<OFMessage> oFMessageReader = OFFactories
				.getGenericReader();
		OFMessage ofMessage = oFMessageReader.readFrom(buffer);
				
		return ofMessage;
	}
	
}
