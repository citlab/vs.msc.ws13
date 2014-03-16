package de.tu_berlin.citlab.testsuite.testSkeletons.interfaces;


import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.window.Window;
import de.tu_berlin.citlab.storm.window.WindowHandler;

import java.util.List;


/**
 * <p>
 *     Interface that shows the abstract methods for the {@link de.tu_berlin.citlab.testsuite.testSkeletons.OperatorTest OperatorTest}
 * 	   that the Test-designer needs to implement for each specific test of that type.
 * </p>
 * Created by Constantin on 1/20/14.
 */
public interface UDFBoltTestMethods
{

	/**
	 * The Test-designer has to define the {@link Window} here, used by the UDFBolt.
	 * <p>
	 *     Currently there are two types of Windows available: {@link de.tu_berlin.citlab.storm.window.CountWindow CountWindows}
	 *     and {@link de.tu_berlin.citlab.storm.window.TimeWindow TimeWindows}. Both have their own parameters, which are
	 *     needed in their constructors. A new instance of one of those window types have to be returned via that method.
	 * </p>
	 * @return A new instance of a specific {@link Window}-type.
	 */
    public Window<Tuple, List<Tuple>> initWindow();

	/**
	 * The Test-designer has to define the {@link WindowHandler} here, used by the UDFBolt.
	 * @return A new instance of a {@link WindowHandler}.
	 */
    public WindowHandler initWindowHandler();

	/**
	 * The Test-designer could assert the output from the UDFBolt's {@link de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock OutputCollectorMock}
	 * here, if the automatic output assertion should be tested. Otherwise, the return value could also be <b>null</b>,
	 * stating that no output assertion testing should take place.
	 * <p>
	 *     If however, output assertion should be established for the test, the guessed output that the
	 *     {@link de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock OutputCollectorMock} should have after complete
	 *     execution of every input Tuple needs to be returned here as a {@link java.util.List} of {@link java.util.List} of
	 *     {@link Object Objects}, as each input Tuple could generate a {@link java.util.List} of {@link Object Objects}
	 *     via the OutputCollector.
	 * </p>
	 * @param inputTuples Only an optional input parameter.
	 *                    If the TupleInput was created via a generator of the
	 *                    {@link de.tu_berlin.citlab.testsuite.helpers.TupleMockFactory TupleMockFactory},
	 *                    then the inputTuples may be needed to guess the asserted transformation of input -> output here.
	 * @return The Output that the {@link de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock OutputCollectorMock} should have
	 * 					  or null, if no output assertion testing is needed.
	 */
    public List<List<Object>> assertWindowedOutput(final List<Tuple> inputTuples);
}
