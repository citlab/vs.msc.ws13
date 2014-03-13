package de.tu_berlin.citlab.ws;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpTools {
	public static String httpGet(final String url) {
		String result = "";
		try {
			URL obj = new URL(url);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			con.setConnectTimeout(2000);
			con.setRequestMethod("GET");

			BufferedReader in = new BufferedReader(new InputStreamReader(
					con.getInputStream()));
			String inputLine;

			while ((inputLine = in.readLine()) != null) {
				result += inputLine;
			}
			in.close();
		} catch (IOException e) {
			return null;
		}

		if (result.equals("null")) {
			result = null;
		}

		return result;
	}
}
