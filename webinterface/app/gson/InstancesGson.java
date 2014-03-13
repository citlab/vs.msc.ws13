package gson;

public class InstancesGson {
	private String InstanceId;
	private String ImageId;
	private String InstanceType;
	private String LaunchTime;
	private String PublicIpAddress;
	private StateGson State;

	public String getInstanceId() {
		return InstanceId;
	}

	public void setInstanceId(String instanceId) {
		InstanceId = instanceId;
	}

	public String getImageId() {
		return ImageId;
	}

	public void setImageId(String imageId) {
		ImageId = imageId;
	}

	public StateGson getState() {
		return State;
	}

	public void setState(StateGson state) {
		State = state;
	}

	public String getInstanceType() {
		return InstanceType;
	}

	public void setInstanceType(String instanceType) {
		InstanceType = instanceType;
	}

	public String getLaunchTime() {
		return LaunchTime;
	}

	public void setLaunchTime(String launchTime) {
		LaunchTime = launchTime;
	}

	public String getPublicIpAddress() {
		return PublicIpAddress;
	}

	public void setPublicIpAddress(String publicIpAddress) {
		PublicIpAddress = publicIpAddress;
	}
}
