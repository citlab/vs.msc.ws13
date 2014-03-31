package de.tu_berlin.citlab;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;

import de.tu_berlin.citlab.cluster.ClusterManager;
import de.tu_berlin.citlab.register.servlets.LookupServlet;
import de.tu_berlin.citlab.register.servlets.RegisterServlet;

public class CitstormServer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		startRegisterService();
		startClusterManager();
	}

	private static void startRegisterService() {
		Server server = new Server(9000);

		ContextHandlerCollection handlers = new ContextHandlerCollection();

		ServletContextHandler handler = new ServletContextHandler();
		handler.setContextPath("/");
		handler.addServlet(LookupServlet.class, "/lookup");
		handler.addServlet(RegisterServlet.class, "/register");

		handlers.addHandler(handler);

		server.setHandler(handlers);

		try {
			server.start();
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("Register Server started");
	}

	private static void startClusterManager() {
		ClusterManager cMan = new ClusterManager();
		cMan.start();

		System.out.println("Cluster Manager started");
	}
}
