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

public class DeployServlet extends HttpServlet {

	private static final long serialVersionUID = -7357464832672041437L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		PrintWriter pw = resp.getWriter();

		String uri = req.getRequestURI();
		String result;

		if (uri.equals("/delete")) {
			System.out.println("Delete requested");
			String filename = req.getParameter("file");
			if (filename != null) {
				result = deleteFile(filename);
			} else {
				result = "No filename";
			}
		} else if (uri.equals("/run")) {
			System.out.println("Run requested");
			String filename = req.getParameter("file");
			if (filename != null) {
				result = runJob(filename);
			} else {
				result = "No filename";
			}
		} else if (uri.equals("/kill")) {
			System.out.println("Kill requested");
			String filename = req.getParameter("file");
			if (filename != null) {
				result = killJob(filename);
			} else {
				result = "No filename";
			}
		} else {
			result = "Nothing happened";
		}

		pw.println(result);
		System.out.println(result);
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		System.out.println("Upload requested");
		PrintWriter pw = resp.getWriter();

		String uri = req.getRequestURI();
		String result;

		if (uri.equals("/upload")) {
			result = uploadFileToServer(req);
		} else {
			result = "Nothing happened";
		}

		pw.println(result);
		System.out.println(result);
	}

	private String deleteFile(String filename) {
		String folder = Config.getInstance().getPath("job.path");
		File file = new File(folder + "/" + filename);
		if (file.getAbsolutePath().startsWith(folder)) {
			if (file.exists()) {
				file.delete();
				return "File deleted";
			} else {
				return "File does not exist";
			}
		} else {
			return "Invalid file path";
		}
	}

	private String runJob(String filename) {
		String folder = Config.getInstance().getPath("job.path");
		String stormPath = Config.getInstance().getPath("storm.path");
		File file = new File(folder + "/" + filename);

		if (!file.exists()) {
			return "File does not exist";
		}

		String filePath = file.getAbsolutePath();
		String mainClass = JarTools.getManifestAttributeFromJar(
				"Storm-Main-Class", file);
		if (mainClass == null) {
			return "No main class in Manifest";
		}

		String topologyName = JarTools.getManifestAttributeFromJar(
				"Topology-Name", file);

		if (topologyName == null) {
			return "No topology name in Manifest";
		}

		if (filePath.startsWith(folder)) {
			ProcessBuilder pb = new ProcessBuilder("sudo", "bin/storm", "jar",
					filePath, mainClass, topologyName);
			pb.directory(new File(stormPath));

			try {
				Process p = pb.start();
				p.waitFor();
			} catch (IOException | InterruptedException e) {
				return "Failed to start topology";
			}
			return "Topology started";
		} else {
			return "Invalid file path";
		}
	}

	private String killJob(String filename) {
		String folder = Config.getInstance().getPath("job.path");
		String stormPath = Config.getInstance().getPath("storm.path");
		File file = new File(folder + "/" + filename);

		if (!file.exists()) {
			return "File does not exist";
		}

		String filePath = file.getAbsolutePath();

		String topologyName = JarTools.getManifestAttributeFromJar(
				"Topology-Name", file);
		if (topologyName == null) {
			return "No topology name in Manifest";
		}

		if (filePath.startsWith(folder)) {
			ProcessBuilder pb = new ProcessBuilder("sudo", "bin/storm", "kill",
					topologyName);
			pb.directory(new File(stormPath));

			try {
				Process p = pb.start();
				p.waitFor();
			} catch (IOException | InterruptedException e) {
				return "Failed to stop topology";
			}
			return "Topology stopped";
		} else {
			return "Invalid file path";
		}
	}

	private String uploadFileToServer(HttpServletRequest req)
			throws IOException {
		// code from:
		// http://stackoverflow.com/questions/2422468/how-to-upload-files-to-server-using-jsp-servlet

		String name = null;

		try {
			List<FileItem> items = new ServletFileUpload(
					new DiskFileItemFactory()).parseRequest(req);
			for (FileItem item : items) {
				if (!item.isFormField()) {
					String fileName = FilenameUtils.getName(item.getName());
					InputStream fileContent = item.getInputStream();
					name = getTimeStamp() + "_" + fileName;
					saveFileToDisk(fileContent, new File(Config.getInstance()
							.getPath("job.path") + "/" + name));
				}
			}
			if (name != null) {
				return name;
			}
		} catch (FileUploadException e) {
			e.printStackTrace();
		}

		return "null";
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
