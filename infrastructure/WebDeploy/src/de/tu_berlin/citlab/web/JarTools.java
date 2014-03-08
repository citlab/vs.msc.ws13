package de.tu_berlin.citlab.web;

import java.io.File;
import java.io.IOException;
import java.util.jar.JarFile;

public class JarTools {
	public static String getManifestAttributeFromJar(String name, File file) {
		JarFile jar = null;
		String value = null;
		try {
			jar = new JarFile(file);
			value = jar.getManifest().getMainAttributes().getValue(name);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (jar != null) {
				try {
					jar.close();
				} catch (IOException e) {
				}
			}
		}

		return value;
	}
}
