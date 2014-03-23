package de.tu_berlin.citlab.storm.helpers;

public class StringHelper {

	/**
	 * this function removes all UTF-8 characters, that are more than 3 bytes
	 * long and therefore outside the BMP.
	 * 
	 * @param str
	 * @return
	 */
	public static String removeAllNonBMPCharacters(String str) {
		// don't get confused: ^ inside [] is negation!
		return str.replaceAll("[^\u0000-\uFFFF]", "");
	}
	
	public static void main(String[] args) {
		byte[] bytes = { (byte) 0xf0, (byte) 0x9F, (byte) 0x99, (byte) 0x8C };
		String str = "Spare me your stupid kittens " + new String(bytes) + " !";
		System.out.println(str);
		System.out.println(removeAllNonBMPCharacters(str));
	}

}
