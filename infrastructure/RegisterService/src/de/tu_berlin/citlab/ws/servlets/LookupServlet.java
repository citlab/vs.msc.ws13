package de.tu_berlin.citlab.ws.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.tu_berlin.citlab.ws.dbase.Database;

public class LookupServlet extends HttpServlet {

	private static final long serialVersionUID = 4776264869192668458L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		String param = req.getParameter("type");
		if (param != null) {
			String ip = null;

			if (param.equals("cassandra")) {
				ip = Database.getInstance().getIP(1);
			} else if (param.equals("nimbus")) {
				ip = Database.getInstance().getIP(0);
			}
			if (ip == null) {
				ip = "null";
			}

			resp.getOutputStream().println(ip);
		}
	}
}
