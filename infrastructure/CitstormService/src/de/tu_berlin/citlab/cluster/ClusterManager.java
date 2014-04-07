package de.tu_berlin.citlab.cluster;

import java.io.IOException;
import java.net.UnknownHostException;

import de.tu_berlin.citlab.database.ClusterDatabase;
import de.tu_berlin.citlab.database.RegisterDatabase;

public class ClusterManager extends Thread {
	boolean running = true;

	@Override
	public void run() {
		while (running) {

			checkIP(RegisterDatabase.TYPE_CASSANDRA);
			checkIP(RegisterDatabase.TYPE_NIMBUS);

			try {
				Thread.sleep(Long.parseLong(ClusterDatabase.getInstance()
						.getProperty("manager.ip-check-interval")));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void checkIP(int type) {
		try {
			String cAddr = RegisterDatabase.getInstance().getIP(type);
			Process p1 = java.lang.Runtime.getRuntime().exec(
					"ping -c 1 " + cAddr);
			int returnVal = p1.waitFor();
			boolean reachable = (returnVal == 0);

			if (!reachable) {
				RegisterDatabase.getInstance().updateIP(type, "null");
			}

		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void terminate() {
		running = false;
	}

}
