package units.bucketstore;

import java.util.ArrayList;
import java.util.List;

public abstract class SlidingWindow<WindowEntry>
{
	/* Global Variables: */
	/* ================= */
		
		protected ArrayList<WindowEntry> _windowEntries;
		
		
	/* Constructor: */
	/* ============ */
			
		public SlidingWindow()
		{
			_windowEntries = new ArrayList<WindowEntry>();
		}
		
		
		
	/* Public-Methods: */
	/* =============== */
		
		public boolean appendEntry(WindowEntry entry)
		{
			boolean added = _windowEntries.add(entry);
			return added;
		}

		
		abstract public boolean check_windowLimit();

		abstract public List<WindowEntry> flush_winSlide();
}
