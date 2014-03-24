package de.tu_berlin.citlab.storm.helpers;

public class StringHelper {

	/**
	 * removes all unicode characters that are outside the Basic multilingual plane (BMP)
	 * 
	 * @see http://en.wikipedia.org/wiki/Unicode_block
	 * 
	 * @param str
	 * @return
	 */
	public static String removeAllNonBMPCharacters(String str) {
		/*
		 * java converts utf-8 to utf-16 for internal usage. (http://docs.oracle.com/javase/7/docs/api/java/nio/charset/Charset.html)
		 * Thus, there are only 2-byte and 4-byte characters. (http://en.wikipedia.org/wiki/UTF-16)
		 * All BMP characters are 2-bytes long, so there are inside of { 0x00, 0x00 } - { 0xFF, 0xFF }
		 * So the following regular expression replaces all characters, that do not fit into this range, with an empty string.
		 * (don't get confused: ^ inside [] is negation!)
		 */
		return str.replaceAll("[^\u0000-\uFFFF]", "");
	}

	public static void main(String[] args) {
		byte[] bytes = { (byte) 0xf0, (byte) 0x9F, (byte) 0x99, (byte) 0x8C };
		String str = "Spare me your stupid kittens " + new String(bytes) + " !";
		System.out.println(str);
		System.out.println(removeAllNonBMPCharacters(str));
	}

}
