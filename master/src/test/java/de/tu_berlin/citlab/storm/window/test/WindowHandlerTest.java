package de.tu_berlin.citlab.storm.window.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.WindowHandler;
import de.tu_berlin.citlab.testsuite.mocks.TupleMock;

public class WindowHandlerTest {

	private WindowHandler handler;
	private static Fields fields = new Fields("windowKey", "groupKey", "value");
	
	private static Tuple getTuple(String windowKey, String groupKey) {
		return TupleMock.mockTupleByFields(new Values(windowKey, groupKey, ""), fields);
	}
	
	private static Tuple getTuple(String windowKey, String groupKey, String value) {
		return TupleMock.mockTupleByFields(new Values(windowKey, groupKey, value), fields);
	}
	
	@Before
	public void setUp() {

	}
	
	@Test
	public void testOverflowExceptionSingleWindow() {
		handler = new WindowHandler(
			new CountWindow<Tuple>(1)
		);
		handler.add(getTuple("1", "1"));
		boolean exceptionCaught = false;
		try {
			handler.add(getTuple("1", "1"));
		}
		catch(ArrayIndexOutOfBoundsException e) {
			exceptionCaught = true;
		}
		assertTrue(exceptionCaught);
	}
	
	@Test
	public void testOverflowExceptionMultipleWindows() {
		handler = new WindowHandler(
			new CountWindow<Tuple>(1),
			KeyConfigFactory.ByFields("windowKey")
		);
		handler.add(getTuple("1", "1"));
		handler.add(getTuple("2", "1"));
		boolean exceptionCaught1 = false;
		try {
			handler.add(getTuple("1", "1"));
		}
		catch(ArrayIndexOutOfBoundsException e) {
			exceptionCaught1 = true;
		}
		boolean exceptionCaught2 = false;
		try {
			handler.add(getTuple("2", "1"));
		}
		catch(ArrayIndexOutOfBoundsException e) {
			exceptionCaught2 = true;
		}
		assertTrue(exceptionCaught1);
		assertTrue(exceptionCaught2);
	}
	
	@Test
	public void testSatisfactionSingleWindows() {
		handler = new WindowHandler(
			new CountWindow<Tuple>(1),
			KeyConfigFactory.ByFields("windowKey")
		);
		
		assertFalse(handler.isSatisfied());
		
		handler.add(getTuple("1", "1"));
		
		assertTrue(handler.isSatisfied());
		
		handler.flush();
		
		assertFalse(handler.isSatisfied());
	}
	
	/**
	 * test "at least one is satisfied semantic"
	 */
	@Test
	public void testSatisfactionMultipleWindows() {
		handler = new WindowHandler(
			new CountWindow<Tuple>(2),
			KeyConfigFactory.ByFields("windowKey")
		);
		

		handler.add(getTuple("1", "1", "v1"));
		assertFalse(handler.isSatisfied());
		
		handler.add(getTuple("2", "1", "v2"));
		assertFalse(handler.isSatisfied());
		
		handler.add(getTuple("3", "1", "v3"));
		assertFalse(handler.isSatisfied());
		
		handler.add(getTuple("1", "1", "v4"));
		assertTrue(handler.isSatisfied());
		
		handler.add(getTuple("2", "1", "v5"));
		assertTrue(handler.isSatisfied());
		
		handler.add(getTuple("3", "1", "v6"));
		assertTrue(handler.isSatisfied());
		
		handler.flush();
		assertFalse(handler.isSatisfied());

		handler.add(getTuple("1", "1", "v1"));
		assertFalse(handler.isSatisfied());
		
		handler.add(getTuple("2", "1", "v2"));
		assertFalse(handler.isSatisfied());
		
		handler.add(getTuple("3", "1", "v3"));
		assertFalse(handler.isSatisfied());
		
		handler.add(getTuple("1", "1", "v4"));
		handler.flush();
		assertFalse(handler.isSatisfied());
		
		handler.add(getTuple("2", "1", "v5"));
		handler.flush();
		assertFalse(handler.isSatisfied());
		
		handler.add(getTuple("3", "1", "v6"));
		handler.flush();
		assertFalse(handler.isSatisfied());
	}

	
	@Test
	public void testObjectConsistency() {
			
		Tuple t = getTuple("1", "1", "v1");
		Tuple t2 = getTuple("1", "1", "v2");
		
		assertFalse(t.equals(t2));
		
		ArrayList<Tuple> l1 = new ArrayList<Tuple>();
		l1.add(t);
		
		ArrayList<Tuple> l2 = new ArrayList<Tuple>();
		l2.add(t);
		
		assertTrue(l1.equals(l2));

	}
	
	@Test
	public void testFlushSingleWindowsSingleGroup() {
		handler = new WindowHandler(
			new CountWindow<Tuple>(2)
		);
		
		ArrayList<Tuple> l1 = new ArrayList<Tuple>();
		l1.add(getTuple("1", "1", "v1"));
		l1.add(getTuple("1", "1", "v2"));
		
		for(Tuple t : l1) {
			handler.add(t);
		}
		
		List<List<Tuple>> result = handler.flush();
		
		assertTrue(result.size() == 1);
		assertTrue(result.get(0).equals(l1));
		
	}
	
	@Test
	public void testFlushSingleWindowsMultipleGroup() {
		handler = new WindowHandler(
			new CountWindow<Tuple>(2),
			KeyConfigFactory.DefaultKey(),
			KeyConfigFactory.ByFields("groupKey")
		);
		
		ArrayList<Tuple> l1 = new ArrayList<Tuple>();
		l1.add(getTuple("1", "1", "v1"));
		l1.add(getTuple("1", "2", "v1"));
		
		for(Tuple t : l1) {
			handler.add(t);
		}
		
		List<List<Tuple>> result = handler.flush();
		
		assertTrue(result.size() == 2);
		assertTrue(result.get(0).get(0).equals(l1.get(1)));
		assertTrue(result.get(1).get(0).equals(l1.get(0)));
		System.out.println(result);
		
		l1 = new ArrayList<Tuple>();
		l1.add(getTuple("1", "1", "v1"));
		l1.add(getTuple("3", "2", "v3"));
		
		for(Tuple t : l1) {
			handler.add(t);
		}
		
		result = handler.flush();
		
		assertTrue(result.size() == 2);
		assertTrue(result.get(0).get(0).equals(l1.get(1)));
		assertTrue(result.get(1).get(0).equals(l1.get(0)));
		System.out.println(result);
		
	}

}
