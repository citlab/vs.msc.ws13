package de.tu_berlin.citlab.storm.bolts;

import java.util.LinkedList;

public class SlidingWindow<WindowEntry> 
{
	private final LinkedList<WindowEntry> _slidingWindow;
	private WindowEntry _actEntry;
	private long _windowSize;
	
	
	public SlidingWindow(long windowSize)
	{
		_slidingWindow = new LinkedList<WindowEntry>();
		_actEntry = null;
		_windowSize = windowSize;
	}
	
	
	public boolean appendEntry(WindowEntry entry)
	{
		boolean added = _slidingWindow.add(entry);
		if(added)
			_actEntry = entry;
		
		return added;
	}

	
	//public WindowEntry[] TODO: write a prepare_emission() function
	
	
	public boolean check_sizeReached()
	{
		if(_slidingWindow.size() < _windowSize)
			return false;
		else return true;
	}
}
