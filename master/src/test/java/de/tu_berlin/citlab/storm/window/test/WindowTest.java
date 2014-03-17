package de.tu_berlin.citlab.storm.window.test;

import de.tu_berlin.citlab.storm.window.CountWindow;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class WindowTest {

	@Test
	public void testNotSlidingWindowContent() {
		CountWindow<Integer> window = new CountWindow<Integer>(2);
		window.add(1);
		window.add(2);
		List<Integer> result = window.flush();
		List<Integer> compare = new ArrayList<Integer>();
		compare.add(1);
		compare.add(2);
		assertTrue(result.equals(compare));
	}

	@Test
	public void testNotSlidingWindowEmptyAfterFlush() {
		List<Integer> empty = new ArrayList<Integer>();
		CountWindow<Integer> window = new CountWindow<Integer>(2);
		assertTrue(empty.equals(window.flush()));
		window.add(1);
		window.flush();
		assertTrue(empty.equals(window.flush()));
		window.add(1);
		window.add(2);
		window.flush();
		assertTrue(empty.equals(window.flush()));
	}

	@Test
	public void testSlidingWindowOne() {
		List<Integer> compare = new ArrayList<Integer>();
		CountWindow<Integer> window = new CountWindow<Integer>(3, 1);
		window.add(1);
		window.add(2);
		window.add(3);
		window.flush();
		compare.add(2);
		compare.add(3);
		assertTrue(compare.equals(window.flush()));
		compare.clear();
		compare.add(3);
		assertTrue(compare.equals(window.flush()));
	}

	@Test
	public void testSlidingWindowTwo() {
		List<Integer> compare = new ArrayList<Integer>();
		CountWindow<Integer> window = new CountWindow<Integer>(3, 2);
		window.add(1);
		window.add(2);
		window.add(3);
		window.flush();
		compare.add(3);
		assertTrue(compare.equals(window.flush()));
		compare.clear();
		assertTrue(compare.equals(window.flush()));
	}

	@Test
	public void testWindowOverflowException() {
		CountWindow<Integer> window = new CountWindow<Integer>(3);
		window.add(1);
		window.add(2);
		window.add(3);
		boolean exceptionCaught = false;
		try {
			window.add(4);
		} catch (ArrayIndexOutOfBoundsException e) {
			exceptionCaught = true;
		}
		assertTrue(exceptionCaught);
	}

	@Test
	public void testWindowImpossibleOffsetException() {
		boolean exceptionCaught = false;
		try {
			new CountWindow<Integer>(0, 1);
		} catch (IllegalArgumentException e) {
			exceptionCaught = true;
		}
		assertTrue(exceptionCaught);
	}

}
