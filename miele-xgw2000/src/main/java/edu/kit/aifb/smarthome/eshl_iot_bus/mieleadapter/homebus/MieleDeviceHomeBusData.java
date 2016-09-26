package edu.kit.aifb.smarthome.eshl_iot_bus.mieleadapter.homebus;

import java.util.HashSet;
import java.util.Set;

import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.eclipse.persistence.oxm.annotations.XmlPath;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)

/**
 * A Miele device on the XML homebus
 * 
 * @author Kaibin Bao
 *
 */
@XmlType
public class MieleDeviceHomeBusData {
	@XmlPath("class/text()")
	@JsonProperty("class")
	private int deviceClass;

	@XmlPath("UID/text()")
	@JsonProperty("uid")
	private int uid;
	
	@XmlPath("type/text()")
	@JsonProperty("type")
	private String type;
	
	@XmlPath("name/text()")
	@JsonProperty("name")
	private String name;
	
	@XmlPath("state/text()")
	@JsonProperty("state")
	private int state;
	
	@XmlPath("additionalName/text()")
	@JsonProperty("additionalName")
	private String additionalName;

	@XmlPath("room/text()")
	@JsonProperty("room")
	private String roomName;
	
	@XmlPath("room[@id]")
	private String roomId;

	@XmlPath("room[@level]")
	private String roomLevel;

	@XmlPath("information/key[@name='State']/@value")
	@JsonProperty("stateName")
	private String stateName;
	
	@XmlPath("information/key[@name='Phase']/@value")
	@JsonProperty("phaseName")
	private String phaseName;
	
	@XmlPath("information/key[@name='Duration']/@value")
	@JsonIgnore
	private MieleDuration duration;

	@XmlPath("information/key[@name='Start Time']/@value")
	@JsonIgnore
	private MieleDuration startTime;
	
	@XmlPath("information/key[@name='Remaining Time']/@value")
	@JsonIgnore
	private MieleDuration remainingTime;
	
	@XmlPath("actions/action[@name='Details']/@URL")
	@JsonIgnore
	private String detailsUrl;

	@XmlTransient
	@JsonProperty("deviceDetails")
	private MieleApplianceRawData deviceDetails;
	
	@XmlTransient
	@JsonProperty("actions")
	private Set<String> actions;
	
	/* GETTERS */
	
	public int getDeviceClass() {
		return deviceClass;
	}

	public int getUid() {
		return uid;
	}

	public String getType() {
		return type;
	}

	public String getName() {
		return name;
	}

	public int getState() {
		return state;
	}

	public String getAdditionalName() {
		return additionalName;
	}
	
	public String getRoomName() {
		return roomName;
	}

	public String getRoomId() {
		return roomId;
	}

	public String getRoomLevel() {
		return roomLevel;
	}

	public String getStateName() {
		return stateName;
	}

	public String getPhaseName() {
		return phaseName;
	}

	@JsonProperty("duration")
	public Integer getDuration() {
		if( duration == null )
			return null;
		return duration.duration();
	}
	
	@JsonProperty("startTime")
	public Integer getStartTime() {
		if( startTime == null )
			return null;
		return startTime.duration();
	}

	@JsonProperty("remainingTime")
	public Integer getRemainingTime() {
		if( remainingTime == null )
			return null;
		return remainingTime.duration();
	}

	@JsonIgnore
	public String getDetailsUrl() {
		return detailsUrl;
	}
	
	public MieleApplianceRawData getDeviceDetails() {
		return deviceDetails;
	}
	
	public Set<String> getActions() {
		return actions;
	}
	
	/* SETTERS */
	
	public void setDeviceDetails(MieleApplianceRawData deviceDetails) {
		this.deviceDetails = deviceDetails;
	}
	
	public void addAction(String action) {
		if( actions == null ) {
			actions = new HashSet<>();
		}
		actions.add(action);
	}
	
	@Override
	public String toString() {
		return String.format("miele device %x, class %x, state %d", uid, deviceClass, state);
	}
}
