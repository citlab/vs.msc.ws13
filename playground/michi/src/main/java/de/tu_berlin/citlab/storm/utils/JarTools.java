package de.tu_berlin.citlab.storm.utils;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.jar.Manifest;

public class JarTools {

	/**
	 * Reads an attribute from the Manifest of the Jar file
	 * 
	 * @param attributeName
	 *            The attribute to find in the manifest
	 * @return The value of the attribute, or null, if the attribute is not set
	 */

	public static String getAttributeFromManifest(String attributeName) {
		Manifest mf = null;
		URLClassLoader cl = (URLClassLoader) JarTools.class.getClassLoader();
		try {
			URL url = cl.findResource("META-INF/MANIFEST.MF");
			mf = new Manifest(url.openStream());

		} catch (IOException e) {
			e.printStackTrace();
		}

		return mf.getMainAttributes().getValue(attributeName);
	}
}
