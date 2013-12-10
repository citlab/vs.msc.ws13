package de.tu_berlin.citlab.twitter;

public class InvalidTwitterConfigurationException extends Exception {

	private static final long serialVersionUID = -8893179293249648967L;

	public InvalidTwitterConfigurationException() {
		super();
	}

	public InvalidTwitterConfigurationException(String message) {
		super(message);
	}

	public InvalidTwitterConfigurationException(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidTwitterConfigurationException(Throwable cause) {
		super(cause);
	}
}
