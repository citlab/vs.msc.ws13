package de.tu_berlin.citlab.web;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;

import de.tu_berlin.citlab.web.servlets.DeployServlet;

public class NimbusFileUploadStart {

	public static void main(String[] args) throws Exception {
		Server server = new Server(8081);

		ContextHandlerCollection handlers = new ContextHandlerCollection();

		ServletContextHandler handler = new ServletContextHandler();
		handler.setContextPath("/");
		handler.addServlet(DeployServlet.class, "/");

		handlers.addHandler(handler);

		server.setHandler(handlers);

		server.start();
		server.join();
	}

}
