package de.tu_berlin.citlab.testsuite.mocks;


import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.testsuite.helpers.DebugLogger;
import de.tu_berlin.citlab.testsuite.helpers.LogPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;


public final class OutputCollectorMock
{

/* Global Private Constants: */
/* ========================= */

    private static final Logger LOGGER = LogManager.getLogger(DebugLogger.OCOLLMOCK_ID);
    private static final Marker DETAILED = DebugLogger.getDetailedMarker();


/* Global Public Static Vars: */
/* ========================= */

    public static List<List<Object>> output;



/* Public Static Methods: */
/* ====================== */

    public static void resetOutput()
    {
        if(output != null)
            output.clear();
    }


    public static OutputCollector mockOutputCollector()
    {
        OutputCollector oColl = Mockito.mock(OutputCollector.class);
        output = new ArrayList<List<Object>>();

        class IsAnyObjectList extends ArgumentMatcher<List<Object>>
        {
            @Override
            public boolean matches(Object argument)
            {
                List<Object> objList = (List) argument;
                if(argument.getClass().equals(objList.getClass())){
                    return true;
                }
                else return false;
            }

        }

        class IsAnyTuple extends ArgumentMatcher<Tuple>
        {
            @Override
            public boolean matches(Object argument)
            {
                if(argument.getClass().equals(Tuple.class))
                    return true;
                else return false;
            }

        }

        Mockito.when(oColl.emit(Matchers.argThat(new IsAnyObjectList()))).thenAnswer(new Answer<List<Object>>(){
            public List<Object> answer(InvocationOnMock invocation)
                    throws Throwable {

                //the objectList which was emitted by the Output-Coll Mock:
                List<Object> emissionList = (List<Object>) invocation.getArguments()[0];
                output.add(emissionList);

                LOGGER.debug(DETAILED, "OutputCollector emitted: \n\t {}", LogPrinter.toObjectListString(emissionList));

                return emissionList;
            }
        });

        Mockito.doNothing().when(oColl).ack(Matchers.argThat(new IsAnyTuple()));

        return oColl;
    }
}
