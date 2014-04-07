package de.tu_berlin.citlab.testsuite.testSkeletons.interfaces;


import de.tu_berlin.citlab.testsuite.helpers.BoltEmission;
import de.tu_berlin.citlab.testsuite.helpers.BoltTestConfig;

import java.util.List;


/**
 * <p>
 *     Interface that shows the abstract methods for the {@link de.tu_berlin.citlab.testsuite.testSkeletons.TopologyTest TopologyTest}
 * 	   that the Test-designer needs to implement for each specific test of that type.
 * </p>
 * @author Constantin on 3/16/14.
 */
public interface TopologyTestMethods
{
	/**
	 * The Test-Designer needs to specify the first TupleInput as a {@link java.util.List} of {@link de.tu_berlin.citlab.testsuite.mocks.TupleMock TupleMocks}
	 * and the {@link backtype.storm.tuple.Fields OutputFields} of the output for the first UDFBolt that will be tested
	 * inside a {@link de.tu_berlin.citlab.testsuite.testSkeletons.TopologyTest TopologyTest}.
	 *
	 * @return 	A new instance of a BoltEmission with a {@link java.util.List} of {@link de.tu_berlin.citlab.testsuite.mocks.TupleMock TupleMocks}
	 * 			as the constructor parameters.
	 */
	public abstract BoltEmission defineFirstBoltsInput();


    /**
     * The Test-Designer has to return the name of the Topology-Test here. Needed for plotting and testDir-creation.
     * @return The name of the Topology as a {@link String}.
     */
    public abstract String nameTopologyTest();

	/**
	 * The Test-Designer needs to specify the topology-chain, represented by a {@link java.util.List} of {@link BoltTestConfig BoltTestConfigs}.
	 * Each BoltTestConfig defines the TestName for the regarding {@link de.tu_berlin.citlab.testsuite.testSkeletons.BoltTest BoltTest}
	 * and the {@link de.tu_berlin.citlab.storm.bolts.UDFBolt UDFBolt} itself.
	 * @return 	The chain of {@link BoltTestConfig BoltTestConfigs}, which will be tested by the {@link de.tu_berlin.citlab.testsuite.testSkeletons.TopologyTest TopologyTest}
	 * 			in the order the Configs were added to the List.
	 */
	public abstract List<BoltTestConfig> defineTopologySetup();
}
