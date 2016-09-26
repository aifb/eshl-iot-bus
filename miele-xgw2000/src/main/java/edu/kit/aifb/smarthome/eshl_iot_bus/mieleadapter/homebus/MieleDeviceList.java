package edu.kit.aifb.smarthome.eshl_iot_bus.mieleadapter.homebus;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import org.eclipse.persistence.oxm.annotations.XmlPath;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)

/**
 * The XML homebus root node
 * 
 * @author Kaibin Bao
 *
 */
@XmlRootElement(name="DEVICES")
public class MieleDeviceList {
	@XmlPath("device")
	@JsonProperty("devices")
	private List<MieleDeviceHomeBusData> devices;
	
	@JsonProperty("devices")
	public List<MieleDeviceHomeBusData> getDevices() {
		return devices;
	}
	
	@JsonProperty("devices")
	public void setDevices(List<MieleDeviceHomeBusData> devices) {
		this.devices = devices;
	}
}
