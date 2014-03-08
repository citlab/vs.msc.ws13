package de.tu_berlin.citlab.web.servlets;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FilenameUtils;

import de.tu_berlin.citlab.web.Config;
import de.tu_berlin.citlab.web.JarTools;
import de.tu_berlin.citlab.web.tools.HTMLBuilder;

public class DeployServlet extends HttpServlet {

	private static final long serialVersionUID = -7357464832672041437L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		PrintWriter pw = resp.getWriter();

		String cmd = req.getParameter("delete");
		if (cmd != null) {
			deleteFile(req);
		}
		cmd = req.getParameter("run");
		if (cmd != null) {
			runJob(req);
		}
		cmd = req.getParameter("kill");
		if (cmd != null) {
			killJob(req);
		}

		HTMLBuilder.htmlHead(resp);
		pw.println("<html>");

		HTMLBuilder.titledHeader(resp, "CIT-Storm - Web Deployment");

		pw.println("<body>");

		generateUploadForm(pw);

		listAvailableJobs(pw);

		pw.println("</body>");
		pw.println("</html>");
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {

		uploadFileToServer(req);

		resp.sendRedirect("/deploy");
	}

	private void generateUploadForm(PrintWriter pw) {
		// create file upload form
		pw.println(" <form action=\"/deploy\" method=\"post\" enctype=\"multipart/form-data\">");
		pw.println("  <p>Choose a storm topology jar file:");
		pw.println("   <br>");
		pw.println("   <input type=\"file\" name=\"file\" />");
		pw.println("   <br>");
		pw.println("   <input type=\"submit\" />");
		pw.println("  </p>");
		pw.println(" </form>");
	}

	private void listAvailableJobs(PrintWriter pw) {
		// list available jobs
		File[] files = new File(Config.getInstance().getPath("job.path"))
				.listFiles();
		if (files.length == 0) {
			pw.println("No jobs uploaded");
		} else {
			for (File f : files) {
				String name = f.getName();
				if (name.endsWith(".jar")) {
					String topologyName = JarTools.getManifestAttributeFromJar(
							"Topology-Name", f);

					pw.println("<form action=\"/deploy\">");
					pw.println(" <input type=\"hidden\" name=\"file\" value=\""
							+ name + "\">");
					pw.println(" <input type=\"hidden\" name=\"topology\" value=\""
							+ topologyName + "\">");
					pw.println(" <input type=\"submit\" name=\"run\" value=\"Run\">");
					pw.println(" <input type=\"submit\" name=\"kill\" value=\"Kill\">");
					pw.println(" <input type=\"submit\" name=\"delete\" value=\"Delete\">");

					pw.println(name);
					pw.println("Topology-Name: " + topologyName);
					pw.println("</form>");
				}
			}
		}
	}

	private void deleteFile(HttpServletRequest req) {
		String cmd = req.getParameter("file");
		if (cmd != null) {
			String folder = Config.getInstance().getPath("job.path");
			File f = new File(folder + "/" + cmd);
			if (f.getAbsolutePath().startsWith(folder)) {
				f.delete();
			}
		}
	}

	private void runJob(HttpServletRequest req) {
		String cmd = req.getParameter("file");
		if (cmd != null) {
			String folder = Config.getInstance().getPath("job.path");
			String stormPath = Config.getInstance().getPath("storm.path");
			File file = new File(folder + "/" + cmd);
			String filePath = file.getAbsolutePath();
			String mainClass = JarTools.getManifestAttributeFromJar(
					"Storm-Main-Class", file);
			String topologyName = JarTools.getManifestAttributeFromJar(
					"Topology-Name", file);
			if (filePath.startsWith(folder)) {
				ProcessBuilder pb = new ProcessBuilder("sudo", "bin/storm",
						"jar", filePath, mainClass, topologyName);
				pb.directory(new File(stormPath));

				try {
					Process p = pb.start();
					p.waitFor();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		}
	}

	private void killJob(HttpServletRequest req) {
		String cmd = req.getParameter("file");
		if (cmd != null) {
			String folder = Config.getInstance().getPath("job.path");
			String stormPath = Config.getInstance().getPath("storm.path");
			File file = new File(folder + "/" + cmd);
			String filePath = file.getAbsolutePath();
			String topologyName = JarTools.getManifestAttributeFromJar(
					"Topology-Name", file);
			if (filePath.startsWith(folder)) {
				ProcessBuilder pb = new ProcessBuilder("sudo", "bin/storm",
						"kill", topologyName);
				pb.directory(new File(stormPath));

				try {
					Process p = pb.start();
					p.waitFor();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		}
	}

	private void uploadFileToServer(HttpServletRequest req)
			throws ServletException, IOException {
		// code from:
		// http://stackoverflow.com/questions/2422468/how-to-upload-files-to-server-using-jsp-servlet
		try {
			List<FileItem> items = new ServletFileUpload(
					new DiskFileItemFactory()).parseRequest(req);
			for (FileItem item : items) {
				if (!item.isFormField()) {
					String fileName = FilenameUtils.getName(item.getName());
					InputStream fileContent = item.getInputStream();
					saveFileToDisk(fileContent, new File(Config.getInstance()
							.getPath("job.path")
							+ "/"
							+ getTimeStamp()
							+ "_"
							+ fileName));
				}
			}
		} catch (FileUploadException e) {
			throw new ServletException("Cannot parse multipart request.", e);
		}
	}

	private void saveFileToDisk(InputStream is, File outputFile) {
		int read = 0;
		byte[] bytes = new byte[1024];

		FileOutputStream fos = null;

		try {
			fos = new FileOutputStream(outputFile);
			while ((read = is.read(bytes)) != -1) {
				fos.write(bytes, 0, read);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private String getTimeStamp() {
		Calendar cal;
		String DATE_FORMAT_NOW = "dd_MM_yyyy_HH_mm_ss";
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
		cal = Calendar.getInstance();
		return sdf.format(cal.getTime());
	}
}
