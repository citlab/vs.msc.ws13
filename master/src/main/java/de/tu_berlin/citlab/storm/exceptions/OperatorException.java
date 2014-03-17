package de.tu_berlin.citlab.storm.exceptions;

/**
 * Created by Constantin on 3/5/14.
 */
public class OperatorException extends Exception
{
	public OperatorException() { super(); }
	public OperatorException(String message) { super(message); }
	public OperatorException(String message, Throwable cause) { super(message, cause); }
	public OperatorException(Throwable cause) { super(cause); }
}
