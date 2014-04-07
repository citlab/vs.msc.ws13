package de.tu_berlin.citlab.register.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.tu_berlin.citlab.database.RegisterDatabase;

public class RegisterServlet extends HttpServlet {

	private static final long serialVersionUID = 8331955987974889558L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {

		String ip = req.getRemoteAddr();
		String param = req.getParameter("type");
		if (param != null) {
			String result = "null";
			if (param.equals("cassandra")) {
				result = RegisterDatabase.getInstance().updateIP(1, ip);
			} else if (param.equals("nimbus")) {
				result = RegisterDatabase.getInstance().updateIP(0, ip);
			} else if (param.equals("supervisor")) {
				result = RegisterDatabase.getInstance().addIP(2, ip);
			}

			resp.getOutputStream().println(result);
		}
	}
}
