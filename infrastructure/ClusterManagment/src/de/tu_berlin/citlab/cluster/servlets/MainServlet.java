package de.tu_berlin.citlab.cluster.servlets;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import de.tu_berlin.citlab.cluster.AwsCli;
import de.tu_berlin.citlab.cluster.Cluster;
import de.tu_berlin.citlab.cluster.Config;
import de.tu_berlin.citlab.cluster.Instance;
import de.tu_berlin.citlab.cluster.Instance.Role;
import de.tu_berlin.citlab.cluster.Instance.State;
import de.tu_berlin.citlab.cluster.tools.HTMLBuilder;

public class MainServlet extends HttpServlet {

	private static final long serialVersionUID = -7357464832672041437L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {

		HttpSession s = req.getSession();

		if (req.getParameterNames().hasMoreElements()) {
			String cmd = req.getParameter("start_nimbus");
			if (cmd != null) {
				Cluster.startNimbus();
			}
			cmd = req.getParameter("start_supervisor");
			if (cmd != null) {
				int num = 1;
				cmd = req.getParameter("n_sv");
				if (cmd != null) {
					num = Integer.parseInt(cmd);
				}
				Cluster.startSupervisor(num);
			}
			cmd = req.getParameter("reboot_cluster");
			if (cmd != null) {
				Cluster.rebootCluster();
			}
			cmd = req.getParameter("kill_cluster");
			if (cmd != null) {
				Cluster.killCluster();
			}
			cmd = req.getParameter("reboot_supervisors");
			if (cmd != null) {
				Cluster.rebootSupervisors();
			}
			cmd = req.getParameter("kill_supervisors");
			if (cmd != null) {
				Cluster.killSupervisors();
			}
			cmd = req.getParameter("terminate");
			if (cmd != null) {
				cmd = req.getParameter("instance");
				if (cmd != null) {
					Cluster.terminateInstance(cmd);
				}
			}
			cmd = req.getParameter("reboot");
			if (cmd != null) {
				cmd = req.getParameter("instance");
				if (cmd != null) {
					Cluster.rebootInstance(cmd);
				}
			}
			cmd = req.getParameter("assignIp");
			if (cmd != null) {
				cmd = req.getParameter("instance");
				if (cmd != null) {
					Cluster.updatePublicIPAddress(cmd);
				}
			}
			cmd = req.getParameter("hide_terminated");
			if (cmd != null) {
				s.setAttribute("hide_terminated", cmd);
			}
			resp.sendRedirect(req.getServletPath());
		} else {
			buildHTML(s, resp);
		}
	}

	private void buildHTML(HttpSession session, HttpServletResponse resp)
			throws ServletException, IOException {

		PrintWriter pw = resp.getWriter();
		HTMLBuilder.htmlHead(resp);
		pw.println("<html>");

		HTMLBuilder.titledHeader(resp, "CIT-Storm - Cluster Managment");

		pw.println("<body>");
		printControls(pw);
		pw.println("<hr>");

		String hideTerminated = (String) session
				.getAttribute("hide_terminated");
		if (hideTerminated != null && hideTerminated.equals("true")) {
			listInstances(pw, true);
		} else {
			listInstances(pw, false);
		}
		pw.println("<hr>");
		pw.println("</body>");
		pw.println("</html>");
	}

	private void printControls(PrintWriter pw) {
		pw.println("<h2>Manage Cluster:</h2>");

		pw.println("<form action=\"/\">");
		pw.println("<input type=\"submit\" name=\"start_nimbus\" value=\"Start Nimbus\"><br>");
		pw.println("<input type=\"submit\" name=\"start_supervisor\" value=\"Start Supervisors\">");
		pw.println("<input type=\"number\" name=\"n_sv\" />");
		pw.println("<p>");
		pw.println("<input type=\"submit\" name=\"kill_cluster\" value=\"Shutdown Cluster\"><br>");
		pw.println("<input type=\"submit\" name=\"reboot_cluster\" value=\"Reboot Cluster\"><br>");
		pw.println("<input type=\"submit\" name=\"kill_supervisors\" value=\"Terminate Supervisors\"><br>");
		pw.println("<input type=\"submit\" name=\"reboot_supervisors\" value=\"Reboot Supervisors\"><br>");
		pw.println("</form>");

		pw.println("<p>");

		if (Cluster.isNimbusUp()) {
			pw.println("<a href=\"http://"
					+ Config.getInstance().getProperty("nimbus.public-ip")
					+ ":8081\" target=\"_blank\">Web Deploy Page</a>");
		} else {
			pw.println("<br>");
		}
	}

	private void listInstances(PrintWriter pw, boolean hideTerminated) {
		pw.println("<h2>Manage EC2 Instances:</h2>");

		List<Instance> instances = Arrays.asList(AwsCli.describeInstances());

		pw.println("<form action=\"/\">");
		if (!hideTerminated) {
			pw.println("<input type=\"radio\" name=\"hide_terminated\" value=\"true\" onclick=\"this.form.submit();\"> Hide terminated<br>");
			pw.println("<input type=\"radio\" name=\"hide_terminated\" value=\"false\" onclick=\"this.form.submit();\" checked=\"checked\"> Show terminated<br>");
		} else {
			pw.println("<input type=\"radio\" name=\"hide_terminated\" value=\"true\" onclick=\"this.form.submit();\" checked=\"checked\"> Hide terminated<br>");
			pw.println("<input type=\"radio\" name=\"hide_terminated\" value=\"false\" onclick=\"this.form.submit();\"> Show terminated<br>");
		}

		pw.println("</form>");
		pw.println("<br>");

		if (hideTerminated) {
			List<Instance> filtered = new ArrayList<Instance>();
			for (Instance i : instances) {
				if (i.getState() != State.TERMINATED) {
					filtered.add(i);
				}
			}

			if (!filtered.isEmpty()) {
				pw.println("<table border=\"1\">");
				printTableHeader(pw);

				for (Instance i : filtered) {
					printTableInstance(pw, i);
				}

				pw.println("</table>");
			} else {
				pw.println("There are currently no instances viewable");
			}
		} else {
			if (!instances.isEmpty()) {
				pw.println("<table border=\"1\">");
				printTableHeader(pw);

				for (Instance i : instances) {
					printTableInstance(pw, i);
				}

				pw.println("</table>");
			} else {
				pw.println("There are currently no instances active");
			}
		}
	}

	private void printTableHeader(PrintWriter pw) {
		pw.println("<tr>");
		pw.println("<th>InstanceID</th>");
		pw.println("<th>Role</th>");
		pw.println("<th>ImageID</th>");
		pw.println("<th>InstanceType</th>");
		pw.println("<th>PublicIP</th>");
		pw.println("<th>LaunchTime</th>");
		pw.println("<th>State</th>");
		pw.println("<th>Controls</th>");
		pw.println("</tr>");
	}

	private void printTableInstance(PrintWriter pw, Instance i) {
		pw.println("<tr>");
		pw.println("<td>" + i.getInstanceId() + "</td>");
		pw.println("<td>" + i.getRole() + "</td>");
		pw.println("<td>" + i.getImageId() + "</td>");
		pw.println("<td>" + i.getInstanceType() + "</td>");
		if (i.getPublicIp() != null
				&& i.getPublicIp().equals(
						Config.getInstance().getProperty("nimbus.public-ip"))) {
			pw.println("<td><font color=\"#0000FF\">" + i.getPublicIp()
					+ "</font></td>");
		} else {
			pw.println("<td>" + i.getPublicIp() + "</td>");
		}
		pw.println("<td>" + i.getLaunchTime() + "</td>");

		switch (i.getState()) {
		case PENDING:
			pw.println("<td bgcolor=\"#FFFF00\">" + i.getState() + "</td>");
			break;
		case RUNNING:
			pw.println("<td bgcolor=\"#00FF00\">" + i.getState() + "</td>");
			break;
		case SHUTTING_DOWN:
			pw.println("<td bgcolor=\"#FFFF00\">" + i.getState() + "</td>");
			break;
		case TERMINATED:
			pw.println("<td bgcolor=\"#FF0000\">" + i.getState() + "</td>");
			break;
		case STOPPING:
			pw.println("<td bgcolor=\"#FF0000\">" + i.getState() + "</td>");
			break;
		case STOPPED:
			pw.println("<td bgcolor=\"#FF0000\">" + i.getState() + "</td>");
			break;
		default:
			pw.println("<td>" + i.getState() + "</td>");
			break;
		}

		pw.println("<td>");
		pw.println("<form action=\"/\">");
		pw.println(" <input type=\"hidden\" name=\"instance\" value=\""
				+ i.getInstanceId() + "\">");
		pw.println(" <input type=\"submit\" name=\"terminate\" value=\"Terminate\">");
		pw.println(" <input type=\"submit\" name=\"reboot\" value=\"Reboot\">");
		if (i.getRole() == Role.NIMBUS
				&& (i.getState() == State.RUNNING || i.getState() == State.PENDING)) {
			pw.println(" <input type=\"submit\" name=\"assignIp\" value=\"Assign EIP\">");
		}
		pw.println("</form>");
		pw.println("</td>");

		pw.println("</tr>");
	}
}
