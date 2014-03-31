package de.tu_berlin.citlab.ws;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class NodeStartupService {

	public static void main(String[] args) {
		String url = "http://54.195.243.38:9000/";

		String nimbus = null;
		System.out.println("Fetching nimbus address");
		while ((nimbus = HttpTools.httpGet(url + "lookup?type=nimbus")) == null) {
			try {
				System.out
						.println("Error in retrieving nimbus ip! Trying again in 10 seconds.");
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Nimbus address is: " + nimbus);
		writeCfgFile(nimbus, "/home/ubuntu/storm-0.8.2/conf/storm.yaml");
	}

	private static void writeCfgFile(String nimbus, String path) {
		File f = new File(path);

		String cfg = "storm.zookeeper.servers:\n" + "\n"
				+ "  - \"54.195.243.38\"\n" + "\n"
				+ "storm.local.dir: \"~/storm\"\n" + "\n" + "nimbus.host: \""
				+ nimbus + "\"\n" + "\n" + "supervisor.slots.ports:\n"
				+ "    - 6700\n" + "    - 6701\n" + "    - 6702\n"
				+ "    - 6703\n";
		FileWriter fw = null;

		try {
			fw = new FileWriter(f);
			fw.append(cfg);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
				}
			}
		}
	}
}
