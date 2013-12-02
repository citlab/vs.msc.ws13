package units.bucketstore;


import java.util.ArrayList;
import java.util.List;


public class CountWindow<WindowEntry> extends SlidingWindow<WindowEntry>
{
/* Global Constants: */
/* ================= */
	
	private final int _windowSize;
	//private final int _windowSlide;	
	
/* Constructor: */
/* ============ */
	
	public CountWindow(int windowSize)//, int windowSlide)
	{
		super();
		_windowSize = windowSize;
		//_windowSlide = windowSlide;
	}
	
	
	
/* Public-Methods: */
/* =============== */
		
	public boolean check_windowLimit()
	{
		if(_windowEntries.size() < _windowSize)
			return false;
		else return true;
	}

	
	public List<WindowEntry> flush_winSlide()
	{
		List<WindowEntry> winSlide;
	//If Window is just filled to _windowSize:
		if(_windowSize >= _windowEntries.size()){
			winSlide = new ArrayList<WindowEntry>(_windowEntries.subList(0, _windowEntries.size()));
			_windowEntries = new ArrayList<WindowEntry>(); // Window-Entries has been flushed completely. No entries left.
		}
		
	//If Window is not completely filled up to _windowSize:
		else{
			winSlide = new ArrayList<WindowEntry>(_windowEntries.subList(0, _windowSize));
			_windowEntries = new ArrayList<WindowEntry>(_windowEntries.subList(_windowSize, _windowEntries.size()));
		}
		/*
		if(_windowEntries.size() == _windowSize)
			_windowEntries = new ArrayList<WindowEntry>();
		else
			_windowEntries = new ArrayList<WindowEntry>(_windowEntries.subList(_windowSize, _windowEntries.size()));
		*/
		return winSlide;
	}
}
