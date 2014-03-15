package de.tu_berlin.citlab.testsuite.mocks;


import backtype.storm.Constants;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.window.IKeyConfig;
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

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.anyInt;


public final class TupleMock
{

/* Global Private Constants: */
/* ========================= */

    private static final Logger LOGGER = LogManager.getLogger(DebugLogger.TUPLEMOCK_ID);
    private static final Marker DETAILED = DebugLogger.getDetailedMarker();


/* Global Private Enumerations: */
/* ============================ */

    enum IKeyType {UNDEFINED, DEFAULT, BY_FIELDS, BY_SOURCE}



/* Public Static Methods: */
/* ====================== */

    public static Tuple mockTickTuple()
    {
        LOGGER.debug(DETAILED, "Created Tick-Tuple Mock. \n\t SYS_COMP_ID: {} \n\t SYS_TICK-STREAM_ID: {}",
                    Constants.SYSTEM_TICK_STREAM_ID,
                    Constants.SYSTEM_TICK_STREAM_ID);
        return TupleMock.mockSysIDTuple(Constants.SYSTEM_COMPONENT_ID, Constants.SYSTEM_TICK_STREAM_ID);
    }

    public static Tuple mockSysIDTuple(String componentID, String streamID)
    {
        LOGGER.debug(DETAILED, "Created Source-Tuple Mock. \n\t SYS_COMP_ID: {} \n\t SYS_TICK-STREAM_ID: {}",
                    componentID,
                    streamID);
        return TupleMock.mockTuple(componentID, streamID, null, KeyConfigFactory.DefaultKey(), IKeyType.UNDEFINED);
    }


    public static Tuple mockTuple(Values vals)
    {
        LOGGER.debug(DETAILED, "Created Tuple Mock by DefaultKey. \n\t Vals: {} \n\t IKeyConfig: DEFAULT-Key",
                    LogPrinter.toValString(vals));
        return TupleMock.mockTuple(null, null, vals, KeyConfigFactory.DefaultKey(), IKeyType.DEFAULT);
    }

    public static Tuple mockTupleBySource(String componentID, Values vals)
    {
        LOGGER.debug(DETAILED, "Created Tuple Mock by Source. \n\t Vals: {} \n\t SYS_COMP_ID: {}",
                    LogPrinter.toValString(vals),
                    componentID);
        return TupleMock.mockTuple(componentID, null, vals, KeyConfigFactory.BySource(), IKeyType.BY_SOURCE);
    }

    public static Tuple mockTupleByFields(Values vals, Fields fields)
    {
        LOGGER.debug(DETAILED, "Created Tuple Mock by Fields. \n\t Vals: {} \n\t Fields: {}",
                LogPrinter.toValString(vals),
                LogPrinter.toFieldsString(fields));

        String[] fieldsStr = new String[fields.size()];
        for(int n = 0 ; n < fields.size() ; n++){
            fieldsStr[n] = fields.get(n);
        }
        return TupleMock.mockTuple(null, null, vals, KeyConfigFactory.ByFields(fields), IKeyType.BY_FIELDS, fieldsStr);
    }

    /**
     * The most general Tuple Mockup. This Mock-Up is implementing the functionality,
     * while all other Mock-Up calls are just convenient methods that are re-directing to this method.
     * @param componentID
     * @param streamID
     * @param vals
     * @param keyConfig
     * @return The Mockup as a {@link Tuple}
     */
    public static Tuple mockTupleCompletely(String componentID, String streamID, Values vals, IKeyConfig keyConfig, String... keyFields)
    {
        LOGGER.debug(DETAILED, "Created Tuple Mock by complete assignment. \n\t Vals: {} \n\t SYS_COMP_ID: {} \n\t SYS_STREAM_ID: {}",
                LogPrinter.toValString(vals),
                componentID,
                streamID);
        return TupleMock.mockTuple(componentID, streamID, vals, keyConfig, IKeyType.UNDEFINED, keyFields);
    }



    private static Tuple mockTuple(final String componentID, final String streamID, final Values vals, final IKeyConfig keyConfig, final IKeyType keyType, final String... keyFields)
    {
        Tuple tuple = Mockito.mock(Tuple.class);

	//Argument Matchers:
	//------------------

        class IsAnyFieldsSelector extends ArgumentMatcher<Fields>
        {
            @Override
            public boolean matches(Object argument){
                if(argument.getClass().equals(Fields.class))
                    return true;
                else return false;
            }
        }

        class IsAnyString extends ArgumentMatcher<String>
        {
            @Override
            public boolean matches(Object argument){
                if(argument.getClass().equals(String.class))
                    return true;
                else return false;
            }
        }



	//Mocking of Source-Component & -StreamID:
	//----------------------------------------

        if(componentID == null){
            Mockito.when(tuple.getSourceComponent()).thenReturn("__null");
        }
        else
            Mockito.when(tuple.getSourceComponent()).thenReturn(componentID);

        if(streamID == null){
            Mockito.when(tuple.getSourceStreamId()).thenReturn("__null");
        }
        else
            Mockito.when(tuple.getSourceStreamId()).thenReturn(streamID);


	//All Tuple-Values related methods are mocked here:
	//-------------------------------------------------

        if(vals == null){
			Mockito.when(tuple.size()).thenReturn(0);
			Mockito.when(tuple.getValues()).thenThrow(new RuntimeException("Trying to call getValues() on a MockTuple, that has no values!"));

            Mockito.when(tuple.select(Matchers.argThat(new IsAnyFieldsSelector()))).thenThrow(new RuntimeException("Trying to call select() on a MockTuple, that has no values!"));
        }
        else{
			Mockito.when(tuple.size()).thenReturn(vals.size());
            Mockito.when(tuple.getValues()).thenReturn(vals);

			Mockito.when(tuple.getValue(anyInt())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					Integer i = (Integer) invocation.getArguments()[0];
					try {
						return vals.get(i);
					} catch (IndexOutOfBoundsException e) {
						LOGGER.error("getValue({}) and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					}
				}
			});
			Mockito.when(tuple.getValueByField(anyString())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					String fieldName = (String) invocation.getArguments()[0];
					int i = 0;
					try {
						for (i = 0; i < keyFields.length; i++) {
							if (keyFields[i].equals(fieldName)) {
								return vals.get(i);
							}
						}
						LOGGER.info("getValueByField({}) has no Value to return.", fieldName);
						return null;
					} catch (IndexOutOfBoundsException e) {
						LOGGER.error("getValueByField() called vals.get({}) and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					} catch (NullPointerException e){
						LOGGER.error("Tuple, that tried to handle getValueByField({}) seems to have no keyfields defined!", fieldName);
						throw e;
					}
				}
			});

			Mockito.when(tuple.getString(anyInt())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					Integer i = (Integer) invocation.getArguments()[0];
					try{
						return (String) vals.get(i);
					}catch(IndexOutOfBoundsException e){
						LOGGER.error("getString({}) and values.size()={} resulted in an IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					}catch(ClassCastException e){
						LOGGER.error("getString({}) and values.get(i)={} resulted in a ClassCastExcp..", i, vals.get(i).toString());
						throw e;
					}
				}
			});
			Mockito.when(tuple.getStringByField(anyString())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					String fieldName = (String) invocation.getArguments()[0];
					int i = 0;
					try {
						for (i = 0; i < keyFields.length; i++) {
							if (keyFields[i].equals(fieldName)) {
								return (String) vals.get(i);
							}
						}
						LOGGER.info("getStringByField({}) has no Value to return.", fieldName);
						return null;
					} catch (IndexOutOfBoundsException e) {
						LOGGER.error("getStringByField({}) called vals.get({}) and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					} catch (NullPointerException e){
						LOGGER.error("Tuple, that tried to handle getStringByField({}) seems to have no keyfields defined!", fieldName);
						throw e;
					}
				}
			});

			Mockito.when(tuple.getInteger(anyInt())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					Integer i = (Integer) invocation.getArguments()[0];
					try{
						return (Integer) vals.get(i);
					}catch(IndexOutOfBoundsException e){
						LOGGER.error("getInteger(i) with i={} and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					}catch(ClassCastException e){
						LOGGER.error("getInteger(i) with i={} and values.get(i)={} resulted in a ClassCastExcp..", i, vals.get(i).toString());
						throw e;
					}
				}
			});
			Mockito.when(tuple.getIntegerByField(anyString())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					String fieldName = (String) invocation.getArguments()[0];
					int i = 0;
					try {
						for (i = 0; i < keyFields.length; i++) {
							if (keyFields[i].equals(fieldName)) {
								return (Integer) vals.get(i);
							}
						}
						LOGGER.info("getIntegerByField({}) has no Value to return.", fieldName);
						return null;
					} catch (IndexOutOfBoundsException e) {
						LOGGER.error("getIntegerByField({}) called vals.get({}) and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					} catch (NullPointerException e){
						LOGGER.error("Tuple, that tried to handle getIntegerByField({}) seems to have no keyfields defined!", fieldName);
						throw e;
					}
				}
			});

			Mockito.when(tuple.getLong(anyInt())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					Integer i = (Integer) invocation.getArguments()[0];
					try{
						return (Long) vals.get(i);
					}catch(IndexOutOfBoundsException e){
						LOGGER.error("getLong(i) with i={} and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					}catch(ClassCastException e){
						LOGGER.error("getLong(i) with i={} and values.get(i)={} resulted in a ClassCastExcp..", i, vals.get(i).toString());
						throw e;
					}
				}
			});
			Mockito.when(tuple.getLongByField(anyString())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					String fieldName = (String) invocation.getArguments()[0];
					int i = 0;
					try {
						for (i = 0; i < keyFields.length; i++) {
							if (keyFields[i].equals(fieldName)) {
								return (Long) vals.get(i);
							}
						}
						LOGGER.info("getLongByField({}) has no Value to return.", fieldName);
						return null;
					} catch (IndexOutOfBoundsException e) {
						LOGGER.error("getLongByField({}) called vals.get({}) and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					} catch (NullPointerException e){
						LOGGER.error("Tuple, that tried to handle getLongByField({}) seems to have no keyfields defined!", fieldName);
						throw e;
					}
				}
			});

			Mockito.when(tuple.getBoolean(anyInt())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					Integer i = (Integer) invocation.getArguments()[0];
					try{
						return (Boolean) vals.get(i);
					}catch(IndexOutOfBoundsException e){
						LOGGER.error("getBoolean(i) with i={} and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					}catch(ClassCastException e){
						LOGGER.error("getBoolean(i) with i={} and values.get(i)={} resulted in a ClassCastExcp..", i, vals.get(i).toString());
						throw e;
					}
				}
			});
			Mockito.when(tuple.getBooleanByField(anyString())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					String fieldName = (String) invocation.getArguments()[0];
					int i = 0;
					try {
						for (i = 0; i < keyFields.length; i++) {
							if (keyFields[i].equals(fieldName)) {
								return (Boolean) vals.get(i);
							}
						}
						LOGGER.info("getBooleanByField({}) has no Value to return.", fieldName);
						return null;
					} catch (IndexOutOfBoundsException e) {
						LOGGER.error("getBooleanByField({}) called vals.get({}) and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					} catch (NullPointerException e){
						LOGGER.error("Tuple, that tried to handle getBooleanByField({}) seems to have no keyfields defined!", fieldName);
						throw e;
					}
				}
			});

			Mockito.when(tuple.getShort(anyInt())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					Integer i = (Integer) invocation.getArguments()[0];
					try{
						return (Short) vals.get(i);
					}catch(IndexOutOfBoundsException e){
						LOGGER.error("getShort(i) with i={} and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					}catch(ClassCastException e){
						LOGGER.error("getShort(i) with i={} and values.get(i)={} resulted in a ClassCastExcp..", i, vals.get(i).toString());
						throw e;
					}
				}
			});
			Mockito.when(tuple.getShortByField(anyString())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					String fieldName = (String) invocation.getArguments()[0];
					int i = 0;
					try {
						for (i = 0; i < keyFields.length; i++) {
							if (keyFields[i].equals(fieldName)) {
								return (Short) vals.get(i);
							}
						}
						LOGGER.info("getShortByField({}) has no Value to return.", fieldName);
						return null;
					} catch (IndexOutOfBoundsException e) {
						LOGGER.error("getShortByField({}) called vals.get({}) and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					} catch (NullPointerException e){
						LOGGER.error("Tuple, that tried to handle getShortByField({}) seems to have no keyfields defined!", fieldName);
						throw e;
					}
				}
			});

			Mockito.when(tuple.getByte(anyInt())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					Integer i = (Integer) invocation.getArguments()[0];
					try{
						return (Byte) vals.get(i);
					}catch(IndexOutOfBoundsException e){
						LOGGER.error("getByte(i) with i={} and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					}catch(ClassCastException e){
						LOGGER.error("getByte(i) with i={} and values.get(i)={} resulted in a ClassCastExcp..", i, vals.get(i).toString());
						throw e;
					}
				}
			});
			Mockito.when(tuple.getByteByField(anyString())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					String fieldName = (String) invocation.getArguments()[0];
					int i = 0;
					try {
						for (i = 0; i < keyFields.length; i++) {
							if (keyFields[i].equals(fieldName)) {
								return (Byte) vals.get(i);
							}
						}
						LOGGER.info("getByteByField({}) has no Value to return.", fieldName);
						return null;
					} catch (IndexOutOfBoundsException e) {
						LOGGER.error("getByteByField({}) called vals.get({}) and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					} catch (NullPointerException e){
						LOGGER.error("Tuple, that tried to handle getByteByField({}) seems to have no keyfields defined!", fieldName);
						throw e;
					}
				}
			});

			Mockito.when(tuple.getDouble(anyInt())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					Integer i = (Integer) invocation.getArguments()[0];
					try{
						return (Boolean) vals.get(i);
					}catch(IndexOutOfBoundsException e){
						LOGGER.error("getDouble(i) with i={} and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					}catch(ClassCastException e){
						LOGGER.error("getDouble(i) with i={} and values.get(i)={} resulted in a ClassCastExcp..", i, vals.get(i).toString());
						throw e;
					}
				}
			});
			Mockito.when(tuple.getDoubleByField(anyString())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					String fieldName = (String) invocation.getArguments()[0];
					int i = 0;
					try {
						for (i = 0; i < keyFields.length; i++) {
							if (keyFields[i].equals(fieldName)) {
								return (Double) vals.get(i);
							}
						}
						LOGGER.info("getDoubleByField({}) has no Value to return.", fieldName);
						return null;
					} catch (IndexOutOfBoundsException e) {
						LOGGER.error("getDoubleByField({}) called vals.get({}) and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					} catch (NullPointerException e){
						LOGGER.error("Tuple, that tried to handle getDoubleByField({}) seems to have no keyfields defined!", fieldName);
						throw e;
					}
				}
			});

			Mockito.when(tuple.getFloat(anyInt())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					Integer i = (Integer) invocation.getArguments()[0];
					try{
						return (Boolean) vals.get(i);
					}catch(IndexOutOfBoundsException e){
						LOGGER.error("getFloat(i) with i={} and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					}catch(ClassCastException e){
						LOGGER.error("getFloat(i) with i={} and values.get(i)={} resulted in a ClassCastExcp..", i, vals.get(i).toString());
						throw e;
					}
				}
			});
			Mockito.when(tuple.getFloatByField(anyString())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					String fieldName = (String) invocation.getArguments()[0];
					int i = 0;
					try {
						for (i = 0; i < keyFields.length; i++) {
							if (keyFields[i].equals(fieldName)) {
								return (Float) vals.get(i);
							}
						}
						LOGGER.info("getFloatByField({}) has no Value to return.", fieldName);
						return null;
					} catch (IndexOutOfBoundsException e) {
						LOGGER.error("getFloatByField({}) called vals.get({}) and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					} catch (NullPointerException e){
						LOGGER.error("Tuple, that tried to handle getFloatByField({}) seems to have no keyfields defined!", fieldName);
						throw e;
					}
				}
			});

			Mockito.when(tuple.getBinary(anyInt())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					Integer i = (Integer) invocation.getArguments()[0];
					try{
						return (byte[]) vals.get(i);
					}catch(IndexOutOfBoundsException e){
						LOGGER.error("getBinary(i) with i={} and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					}catch(ClassCastException e){
						LOGGER.error("getBinary(i) with i={} and values.get(i)={} resulted in a ClassCastExcp..", i, vals.get(i).toString());
						throw e;
					}
				}
			});
			Mockito.when(tuple.getBinaryByField(anyString())).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					String fieldName = (String) invocation.getArguments()[0];
					int i = 0;
					try {
						for (i = 0; i < keyFields.length; i++) {
							if (keyFields[i].equals(fieldName)) {
								return (byte[]) vals.get(i);
							}
						}
						LOGGER.info("getBinaryByField({}) has no Value to return.", fieldName);
						return null;
					} catch (IndexOutOfBoundsException e) {
						LOGGER.error("getBinaryByField({}) called vals.get({}) and values.size()={} resulted in IndexOutOfBoundsExcp.", i, vals.size());
						throw e;
					} catch (NullPointerException e){
						LOGGER.error("Tuple, that tried to handle getBinaryByField({}) seems to have no keyfields defined!", fieldName);
						throw e;
					}
				}
			});


			if(keyFields.length > 0){
				Mockito.when(tuple.select(Matchers.argThat(new IsAnyFieldsSelector()))).thenAnswer(new Answer<Object>() {
					@Override
					public Object answer(InvocationOnMock invocation) throws Throwable {
						if(vals.size() != keyFields.length){
							LOGGER.error("Value- and KeyField-length do not match for this tuple! Correct that in the Tuple generation.");
							return null;
						}
						Fields fields = (Fields) invocation.getArguments()[0];
						List<Object> retArr = new ArrayList<Object>(fields.size());


						for(String actField : fields){
							for (int n = 0; n < keyFields.length; n++) {
								String keyField = keyFields[n];
								if (actField.equals(keyField)) {
									retArr.add(vals.get(n));
								}
							}
						}
						return retArr;
					}
				});
			}
			else{
				Mockito.when(tuple.select(Matchers.argThat(new IsAnyFieldsSelector()))).thenReturn(vals);
			}
        }


	//All Tuple-Fields related methods are mocked here:
	//-------------------------------------------------

        if(keyType.equals(IKeyType.BY_FIELDS)){
			Mockito.when(tuple.getFields()).thenReturn(new Fields(keyFields));

			Mockito.when(tuple.fieldIndex(Matchers.argThat(new IsAnyString()))).thenAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation)
						throws Throwable {
					String keyField = (String) invocation.getArguments()[0];

					for (int n = 0; n < keyFields.length; n++) {
						String actField = keyFields[n];
						if (keyField.equals(actField)) {

							LOGGER.debug(DETAILED, "Found FieldIndex by Field. \n\t Key-Field: {}, index: {}",
									actField, n);
							return n;
						}

					}
					LOGGER.debug(DETAILED, "No FieldIndex was found for Field {}", keyField);
					return null;
				}
			});

            Mockito.when(tuple.getValueByField(Matchers.argThat(new IsAnyString()))).thenAnswer(new Answer<Object>() {
				@Override
                public Object answer(InvocationOnMock invocation)
                        throws Throwable {

					if(vals.size() != keyFields.length){
						LOGGER.error("Value- and KeyField-length do not match for this tuple! Correct that in the Tuple generation.");
						return null;
					}
                    String keyField = (String) invocation.getArguments()[0];

                    for (int n = 0; n < keyFields.length; n++) {
                        String actField = keyFields[n];
                        if (keyField.equals(actField)) {
							LOGGER.debug(DETAILED, "Found ValueByField. \n\t Value: {} \n\t Key-Field: {}",
								vals.get(n).toString(),
								actField);
							return vals.get(n);
                        }

                    }
					LOGGER.debug(DETAILED, "No ValueByField was found for Field {}", keyField);
                    return null;
                }
            });
        }
        else{
            Mockito.when(tuple.getFields()).thenThrow(new RuntimeException("Trying to call getFields() on a MockTuple that has no fields!"));
            Mockito.when(tuple.getValueByField(Matchers.argThat(new IsAnyString()))).thenThrow(new RuntimeException("Trying to call getValueByField(..) on a MockTuple that has no fields!"));
        }



	//Tuple toFieldsString Methods, dependent from the kind of tuple:
	//---------------------------------------------------------

        if((vals == null) && (componentID != null) && (streamID != null)){
            Mockito.when(tuple.toString()).thenAnswer(new Answer<String>(){

                public String answer(InvocationOnMock invocation)
                        throws Throwable {
                    String toString = TupleMock.toComponentTupleString((Tuple) invocation.getMock());
                    return toString;
                }
            });
        }
        else if(vals != null){
            Mockito.when(tuple.toString()).thenAnswer(new Answer<String>(){
                public String answer(InvocationOnMock invocation)
                        throws Throwable {
                    String toString = TupleMock.toValueTupleString((Tuple) invocation.getMock());
                    return toString;
                }
            });
        }

        return tuple;
    }

    private static String toComponentTupleString(Tuple mockTuple)
    {
        //If the mockTuple is a TickTuple, simply return this as a String-output:
        if(mockTuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)){
            return "TickTuple";
        }
        else{
            String tupleString = "Component-String "+
                                  "(Comp-ID: "+ mockTuple.getSourceComponent() +", "+
                                  "Stream-ID: "+ mockTuple.getSourceStreamId() +")";
            return tupleString;
        }

    }

    private static String toValueTupleString(Tuple mockTuple)
    {
        String tupleString = "Tuple <Vals: (";
        int valSize = mockTuple.getValues().size();
        for(int n = 0 ; n < valSize ; n++){
            String actVal = mockTuple.getValues().get(n).toString();
            tupleString += actVal;
            if(n+1 != valSize)
                tupleString += ", ";
        }
        tupleString += ")>";

        return tupleString;
    }
}
