package de.tu_berlin.citlab.web.servlets;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.tu_berlin.citlab.web.tools.HTMLBuilder;

public class MainServlet extends HttpServlet {

	private static final long serialVersionUID = -2397786012814720043L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		PrintWriter pw = resp.getWriter();

		HTMLBuilder.htmlHead(resp);
		pw.println("<html>");

		HTMLBuilder.titledHeader(resp, "CIT-Storm - Web Deployment");

		pw.println(" <frameset rows=\"25%,*\">");
		pw.println("   <frame src=\"/deploy\">");
		pw.println("   <frame src=\"http://54.195.243.38:8080\">");
		pw.println(" </frameset>");

		pw.println("</html>");
	}
}
