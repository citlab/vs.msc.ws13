package de.tu_berlin.citlab.ws;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;

import de.tu_berlin.citlab.ws.servlets.LookupServlet;
import de.tu_berlin.citlab.ws.servlets.RegisterServlet;

public class RegisterServer extends Thread {

	public static void main(String args[]) throws java.lang.Exception {

		Server server = new Server(9000);

		ContextHandlerCollection handlers = new ContextHandlerCollection();

		ServletContextHandler handler = new ServletContextHandler();
		handler.setContextPath("/");
		handler.addServlet(LookupServlet.class, "/lookup");
		handler.addServlet(RegisterServlet.class, "/register");

		handlers.addHandler(handler);

		server.setHandler(handlers);

		server.start();
		server.join();
	}
}
