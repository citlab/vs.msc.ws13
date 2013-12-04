package de.tu_berlin.citlab.storm.bolts.windows;


import java.util.ArrayList;
import java.util.List;


public class TimeWindow<WindowEntry> extends SlidingWindow<WindowEntry>
{
/* Global Constants: */
/* ================= */
	
	private final long _startTime;
	private final long _endTime;
	
	private ArrayList<Long> _winEntry_timers;
	
/* Constructor: */
/* ============ */
	
	public TimeWindow(int secs_windowLength)
	{
		super();
		_startTime = System.currentTimeMillis();
		_endTime = _startTime + secs_windowLength * 1000;
		
		_winEntry_timers = new ArrayList<Long>();
	}
	
	
	
/* Public-Methods: */
/* =============== */
	
	@Override
	public boolean appendEntry(WindowEntry entry)
	{
		boolean added = super.appendEntry(entry);
		_winEntry_timers.add(System.currentTimeMillis());
		
		return added;
	}
	
	public boolean check_windowLimit()
	{
		long currentTime = System.currentTimeMillis();
		if(currentTime < _endTime)
			return false;
		else return true;
	}

	
	public List<WindowEntry> flush_winSlide()
	{
		int windowLimiter;
		for(windowLimiter = 0; windowLimiter < _windowEntries.size() ; windowLimiter++ ){
			if(_winEntry_timers.get(windowLimiter) >= _endTime){
				break;
			}
		}
		List<WindowEntry> winSlide = new ArrayList<WindowEntry>(_windowEntries.subList(0, windowLimiter));
		if(_windowEntries.size() == windowLimiter)
			_windowEntries = new ArrayList<WindowEntry>();
		else
			_windowEntries = new ArrayList<WindowEntry>(_windowEntries.subList(windowLimiter, _windowEntries.size()));
		
		return winSlide;
	}
}
