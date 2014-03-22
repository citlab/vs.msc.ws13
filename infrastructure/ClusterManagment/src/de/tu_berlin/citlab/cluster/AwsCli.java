package de.tu_berlin.citlab.cluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

import de.tu_berlin.citlab.cluster.gson.DescribeInstancesGson;
import de.tu_berlin.citlab.cluster.gson.InstancesGson;
import de.tu_berlin.citlab.cluster.gson.ReservationsGson;
import de.tu_berlin.citlab.cluster.gson.RunInstances;
import de.tu_berlin.citlab.cluster.instances.Instance;

public class AwsCli {
	public static synchronized Instance runInstance(String imageId,
			String instanceType, String keyName, String availabilityZone) {

		return runInstances(imageId, instanceType, keyName, availabilityZone, 1)[0];
	}

	public static synchronized Instance[] runInstances(String imageId,
			String instanceType, String keyName, String availabilityZone,
			int count) {
		String result = "";
		ProcessBuilder pb = new ProcessBuilder("aws", "ec2", "run-instances",
				"--image-id", imageId, "--instance-type", instanceType,
				"--key-name", keyName, "--placement", "AvailabilityZone="
						+ availabilityZone, "--count", count + "");

		try {
			Process p = pb.start();

			BufferedReader br = new BufferedReader(new InputStreamReader(
					p.getInputStream()));
			String line;

			while ((line = br.readLine()) != null) {
				result += line + "\n";
			}

			p.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Gson gson = new Gson();
		RunInstances gsonObj = gson.fromJson(result, RunInstances.class);
		List<Instance> instances = new ArrayList<Instance>();

		for (InstancesGson i : gsonObj.getInstances()) {
			instances.add(Instance.createInstance(i));
		}

		return instances.toArray(new Instance[instances.size()]);
	}

	public static synchronized Instance[] describeInstances() {
		String result = "";

		ProcessBuilder pb = new ProcessBuilder("aws", "ec2",
				"describe-instances");

		try {
			Process p = pb.start();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					p.getInputStream()));
			String line;

			while ((line = br.readLine()) != null) {
				result += line + "\n";
			}

			p.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Gson gson = new Gson();
		DescribeInstancesGson gsonObj = gson.fromJson(result,
				DescribeInstancesGson.class);
		List<Instance> instances = new ArrayList<Instance>();

		for (ReservationsGson r : gsonObj.getReservations()) {
			for (InstancesGson ig : r.getInstances()) {
				Instance i = Instance.createInstance(ig);
				if (!i.getPublicIp().equals(
						ClusterDatabase.getInstance().getProperty("public-ip"))) {
					instances.add(i);
				}

			}
		}

		return instances.toArray(new Instance[instances.size()]);
	}

	public static synchronized void terminateInstance(String instanceId) {
		ProcessBuilder pb = new ProcessBuilder("aws", "ec2",
				"terminate-instances", "--instance-id", instanceId);

		try {
			Process p = pb.start();

			p.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static synchronized void rebootInstance(String instanceId) {
		ProcessBuilder pb = new ProcessBuilder("aws", "ec2",
				"reboot-instances", "--instance-id", instanceId);

		try {
			Process p = pb.start();

			p.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
