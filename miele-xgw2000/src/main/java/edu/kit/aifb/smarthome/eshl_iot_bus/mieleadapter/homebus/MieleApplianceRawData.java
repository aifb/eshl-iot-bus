package edu.kit.aifb.smarthome.eshl_iot_bus.mieleadapter.homebus;

import java.net.URL;

import javax.xml.bind.annotation.XmlRootElement;

import org.eclipse.persistence.oxm.annotations.XmlPath;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)

/**
 * The XML homebus device detail root node
 * 
 * @author Kaibin Bao
 *
 */
@XmlRootElement(name="device")
public class MieleApplianceRawData {
	@XmlPath("information/key[@name='Appliance Type']/@value")
	private String applianceTypeName;
	
	@XmlPath("information/key[@name='State']/@value")
	private String stateName;

	@XmlPath("information/key[@name='Program']/@value")
	private String programName;
	
	@XmlPath("information/key[@name='Phase']/@value")
	private String phaseName;
	
	@XmlPath("information/key[@name='Start Time']/@value")
	@JsonIgnore
	private MieleDuration startTime;
	
	@XmlPath("information/key[@name='Smart Start']/@value")
	@JsonIgnore
	private MieleDuration smartStartTime;
	
	@XmlPath("information/key[@name='Remaining Time']/@value")
	@JsonIgnore
	private MieleDuration remainingTime;
	
	@XmlPath("information/key[@name='Duration']/@value")
	@JsonIgnore
	private MieleDuration duration;
	
	@XmlPath("information/key[@name='End Time']/@value")
	@JsonIgnore
	private MieleDuration endTime;
	
	/* SPECIFIC INFORMATION */
	
	
	
	/* COMMAND URLS */
	
	@XmlPath("actions/action[@name='Stop']/@URL")
	@JsonIgnore
	private URL stopCommandUrl;

	@XmlPath("actions/action[@name='Start']/@URL")
	@JsonIgnore
	private URL startCommandUrl;
	
	@XmlPath("actions/action[@name='Light On']/@URL")
	@JsonIgnore
	private URL lightOnCommandUrl;

	@XmlPath("actions/action[@name='Light Off']/@URL")
	@JsonIgnore
	private URL lightOffCommandUrl;
	
	/* GETTERS */
	
	public String getApplianceTypeName() {
		return applianceTypeName;
	}

	public String getStateName() {
		return stateName;
	}

	public String getProgramName() {
		return programName;
	}

	public String getPhaseName() {
		return phaseName;
	}

	@JsonProperty("startTime")
	public Integer getStartTime() {
		if( startTime == null )
			return null;
		return startTime.duration();
	}

	@JsonProperty("smartStartTime")
	public Integer getSmartStartTime() {
		if( smartStartTime == null )
			return null;
		return smartStartTime.duration();
	}

	@JsonProperty("remainingTime")
	public Integer getRemainingTime() {
		if( remainingTime == null )
			return null;
		return remainingTime.duration();
	}

	@JsonProperty("duration")
	public Integer getDuration() {
		if( duration == null )
			return null;
		return duration.duration();
	}

	@JsonProperty("endTime")
	public Integer getEndTime() {
		if( endTime == null )
			return null;
		return endTime.duration();
	}

	@JsonIgnore
	public URL getStopCommandUrl() {
		return stopCommandUrl;
	}

	@JsonIgnore
	public URL getStartCommandUrl() {
		return startCommandUrl;
	}

	@JsonIgnore
	public URL getLightOnCommandUrl() {
		return lightOnCommandUrl;
	}

	@JsonIgnore
	public URL getLightOffCommandUrl() {
		return lightOffCommandUrl;
	}
}
