package de.tu_berlin.citlab.testsuite.testSkeletons.interfaces;


import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.testsuite.testSkeletons.BoltTest;
import de.tu_berlin.citlab.testsuite.testSkeletons.OperatorTest;

import java.util.List;


/**
 * <p>
 *     Interface that shows the abstract methods for the {@link de.tu_berlin.citlab.testsuite.testSkeletons.StandaloneTest StandaloneTest}
 * 	   that the Test-designer needs to implement for each specific test of that type.
 * </p>
 * Created by Constantin on 3/16/14.
 */
public interface StandaloneTestMethods<B extends BoltTest, O extends OperatorTest>
{
	/**
	 * The test-designer has to return an own implementation of a {@link BoltTest}
	 * via this method for the StandaloneTest.
	 * @return The test-specific implementation of a {@link BoltTest}.
	 */
	public abstract B initBoltTestDescr();

	/**
	 * The test-designer has to return an own implementation of an {@link OperatorTest}
	 * via this method for the StandaloneTest.
	 * @return The test-specific implementation of an {@link OperatorTest}.
	 */
	public abstract O initOpTestDescr();

	/**
	 * The test-designer needs to specify some input Tuples that are Mock-ups of the
	 * Storm specific {@link backtype.storm.tuple.Tuple}. This could be done in two ways:
	 * <ul>
	 *     <li> Using the {@link de.tu_berlin.citlab.testsuite.mocks.TupleMock TupleMock} implementation
	 *     		and methods like {@link de.tu_berlin.citlab.testsuite.mocks.TupleMock#mockTupleByFields(backtype.storm.tuple.Values, backtype.storm.tuple.Fields) mockTupleByFields(..)}
	 *     		for example,
	 *     </li>
	 *     <li>
	 *         or using the {@link de.tu_berlin.citlab.testsuite.helpers.TupleMockFactory TupleMockFactory} that gives
	 *         access to more specific tuple-generators, like a {@link de.tu_berlin.citlab.testsuite.helpers.TupleMockFactory#generateTwitterTuples(String[], String[], int, int) Twitter-Tuple-Generator}.
	 *     </li>
	 * </ul>
	 *
	 * @return A {@link java.util.List} of {@link de.tu_berlin.citlab.testsuite.mocks.TupleMock InputTuple-Mocks}.
	 */
	public abstract List<Tuple> generateInputTuples();

	/**
	 * As it is important for UDFBolts with a {@link de.tu_berlin.citlab.storm.window.TimeWindow TimeWindow}
	 * to simulate a time gap between two consecutive tuple inputs, this method gives the test-designer a chance
	 * to do so. <br />
	 * The regular behaviour in a test without a Bolt, using {@link de.tu_berlin.citlab.storm.window.TimeWindow TimeWindows}
	 * should be to return 0, otherwise a higher value should be taken, dependent of both, the number of input tuples and
	 * the time-difference, defined in the TimeWindow's {@link de.tu_berlin.citlab.storm.window.TimeWindow#timeSlot timeSlot}.
	 * @return the <b>sleepTimer</b> between to consecutive Tuples, in milliseconds.
	 */
	public abstract int setSleepTimerBetweenTuples();
}
