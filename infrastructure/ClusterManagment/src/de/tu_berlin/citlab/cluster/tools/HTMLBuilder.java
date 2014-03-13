package de.tu_berlin.citlab.cluster.tools;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServletResponse;

public class HTMLBuilder {
	public static void titledHeader(HttpServletResponse resp, String title)
			throws IOException {
		PrintWriter pw = resp.getWriter();

		pw.println("<head>");
		pw.println("<title>" + title + "</title>");
		pw.println("</head>");
	}

	public static void htmlHead(HttpServletResponse resp) throws IOException {
		PrintWriter pw = resp.getWriter();
		resp.setContentType("text/html");
		pw.println("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\"");
		pw.println("\"http://www.w3.org/TR/html4/strict.dtd\">");
	}
}
