package de.tu_berlin.citlab.storm.bolts;

public interface UDFStreaming<I> 
{
	public void groupBy(I input);
	public void emissionRequest();
}
