package de.tu_berlin.citlab.cluster;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;

import de.tu_berlin.citlab.cluster.servlets.MainServlet;

public class ClusterManagerWeb {

	public static void main(String[] args) throws Exception {
		Server server = new Server(8080);

		ContextHandlerCollection handlers = new ContextHandlerCollection();

		ServletContextHandler handler = new ServletContextHandler(
				ServletContextHandler.SESSIONS);
		handler.setContextPath("/");
		handler.addServlet(MainServlet.class, "/");

		handlers.addHandler(handler);

		server.setHandler(handlers);

		server.start();
		server.join();
	}

}
