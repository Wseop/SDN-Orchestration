package com.ss.sdnproject.main;

import org.onosproject.net.DeviceId;
import org.onosproject.net.PortNumber;

public class LinkMapper {

	public int dpid = 0;
	public int portNo = 0;
	public DeviceId deviceId;
	public PortNumber portNum;
	
	/* constructor */
	LinkMapper() {
		
	}
	
	/* constructor */
	LinkMapper(int dpid, int portNo) {
		this.dpid = dpid;
		this.portNo = portNo;
	}
	
	/* constructor */
	LinkMapper(DeviceId deviceId, PortNumber portNum) {
		this.deviceId = deviceId;
		this.portNum = portNum;
	}
}
