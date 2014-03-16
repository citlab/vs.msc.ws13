package de.tu_berlin.citlab.testsuite.helpers;


import de.tu_berlin.citlab.storm.bolts.UDFBolt;

import java.util.List;


/**
 * <p>
 *     Represents the configuration of a {@link de.tu_berlin.citlab.testsuite.testSkeletons.BoltTest BoltTest}
 *     in a topology chain of a {@link de.tu_berlin.citlab.testsuite.testSkeletons.TopologyTest TopologyTest}.
 * </p>
 * <p>
 *     Each configuration consists of several construction parameters:
 *     <ul>
 *			<li>the Name of the BoltTest,</li>
 *			<li>the {@link de.tu_berlin.citlab.storm.bolts.UDFBolt UDFBolt} to be tested,</li>
 *			<li>a sleepTimer that is used to simulate a timing offset between two consecutive {@link de.tu_berlin.citlab.testsuite.mocks.TupleMock InputTuples},
 *     			<ul>
 *					<li>used for {@link de.tu_berlin.citlab.storm.window.TimeWindow TimeWindow}-handling</li>
 *					<li>should be set to 0, if no {@link de.tu_berlin.citlab.storm.window.TimeWindow TimeWindow} is tested in the current UDFBolt</li>
 *				</ul>
 * 			</li>
 *			<li>The asserted Output as an <em>optional</em> parameter. <b>Null</b>, if no output-Assertion should happen.</li>
 *	   </ul>
 * </p>
 * @author Constantin on 12.03.14.
 */
public class BoltTestConfig
{
    public final String testName;
    public final UDFBolt testBolt;
	public final int boltSleepTimer;
    public final List<List<Object>> assertedOutput;


	/**
	 * The constructor of a {@link BoltTestConfig}.
	 * @param testName the Name of the BoltTest
	 * @param testBolt the {@link de.tu_berlin.citlab.storm.bolts.UDFBolt UDFBolt} to be tested
	 * @param boltSleepTimer a sleepTimer that is used to simulate a timing offset between two consecutive {@link de.tu_berlin.citlab.testsuite.mocks.TupleMock InputTuples}.
	 *                       Used for {@link de.tu_berlin.citlab.storm.window.TimeWindow TimeWindow}-handling.
	 *                       Should be set to 0, if no {@link de.tu_berlin.citlab.storm.window.TimeWindow TimeWindow} is tested in the current UDFBolt.
	 * @param assertedOutput The asserted Output as an <em>optional</em> parameter. <b>Null</b>, if no output-Assertion should happen.
	 */
    public BoltTestConfig(String testName, UDFBolt testBolt, int boltSleepTimer,
                          List<List<Object>> assertedOutput)
    {
        this.testName = testName;
        this.testBolt = testBolt;
		this.boltSleepTimer = boltSleepTimer;
        this.assertedOutput = assertedOutput;
    }
}
