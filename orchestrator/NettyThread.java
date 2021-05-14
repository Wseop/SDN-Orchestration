package com.ss.sdnproject.main;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.onosproject.net.DeviceId;

public class NettyThread extends Thread {
	private final InetSocketAddress InetAddress;
	private final DeviceId deviceId;

	public NettyThread(String ip, DeviceId deviceId) {
		InetAddress = new InetSocketAddress(ip, 6633);
		this.deviceId = deviceId;
	}

	public void run() {
		// TODO Auto-generated method stub
		// Configure the client.
		ClientBootstrap bootstrap = new ClientBootstrap(
				new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));

		// Set up the pipeline factory.
		bootstrap.setPipelineFactory(new PiplineFactory());

		// Start the connection attempt.
		ChannelFuture future = bootstrap.connect(InetAddress);

		/* 
		 * switch data structure setting
		 * channel id <--> device id
		 */
		SwitchDataStructure.channelIdToDevice.put(future.getChannel().getId(), deviceId);
		SwitchDataStructure.deviceToChannel.put(Integer.parseInt(deviceId.toString().substring(7), 16), future.getChannel());

		// Wait until the connection is closed or the connection attempt fails.
		future.getChannel().getCloseFuture().awaitUninterruptibly();

		// Shut down thread pools to exit.
		bootstrap.releaseExternalResources();
	}
}
