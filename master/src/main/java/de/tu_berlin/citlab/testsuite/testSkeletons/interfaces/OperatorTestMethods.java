package de.tu_berlin.citlab.testsuite.testSkeletons.interfaces;


import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;

import java.util.List;


/**
 * <p>
 *     Interface that shows the abstract methods for the {@link de.tu_berlin.citlab.testsuite.testSkeletons.OperatorTest OperatorTest}
 * 	   that the Test-designer needs to implement for each specific test of that type.
 * </p>
 * Created by Constantin on 1/20/14.
 */
public interface OperatorTestMethods
{
	/**
	 * The Test-designer need to define a new instance of an {@link IOperator} here.
	 * @return The Operator that is later used in the {@link de.tu_berlin.citlab.storm.bolts.UDFBolt UDFBolt}.
	 */
    public IOperator initOperator();

	/**
	 * The Test-designer could assert the output from the {@link IOperator IOperator's} {@link de.tu_berlin.citlab.testsuite.mocks.OutputCollectorMock OutputCollectorMock}
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
    public List<List<Object>> assertOperatorOutput(final List<Tuple> inputTuples);
}
