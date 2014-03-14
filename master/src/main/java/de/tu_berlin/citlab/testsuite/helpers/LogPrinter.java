package de.tu_berlin.citlab.testsuite.helpers;


import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.List;


public final class LogPrinter
{

    private static final String NEWLINE = System.getProperty("line.separator");
    private static final int LINE_SEPARATOR_LENGTH = 40;


    /**
     * Just for clear arrangement, this method will create a stand-out text-header for console-printing. <br>
     * This method creates a header, that defines a text-block beginning, combining the char-parameter "lineSymbol" to a line-separator.
     * <p>
     *
     * @param headText : The {@link String}-Headline, that will be printed in the Console.
     * @param lineSymbol : A <b>char</b> that is repeatedly printed as a header-line. <em>Hint: Use '=', '-', '.' or something like that.</em>
     * @return The formatted header as a {@link String}, to further append this headline to a log-file.
     * @see {@link System#getProperty(String key)} - with <b>key = "line.separator"</b> for newlines.
     * @see {@link de.tu_berlin.citlab.testsuite.helpers.DebugLogger.LoD}
     */
    public static synchronized String printHeader(String headText, char lineSymbol)
    {
        //Line-Generators:
        String headerSeparator = fillSymbolLine(LINE_SEPARATOR_LENGTH, lineSymbol);
        String headTextLine = fillSymbolLine(headText.length(), lineSymbol);

        //Headline-arranging:
        String headline = NEWLINE + headerSeparator + NEWLINE + headText.toUpperCase() + NEWLINE + headTextLine + NEWLINE;

        return headline;
    }

    /**
     * Just for clear arrangement, this method will create a stand-out text-footer for console-printing.
     * This method creates a footer, that defines a text-block ending, combining the char-parameter "lineSymbol" to a line-separator.
     *
     * @param footerText : The {@link String}-Foot-Line, that will be printed in the Console.
     * @param lineSymbol : A <b>char</b> that is repeatedly printed as a header-line. <em>Hint: Use '=', '-', '.' or something like that.</em>
     * @return : The formatted header as a {@link String}, to further append this headline to a log-file.
     * @see {@link System#getProperty(String key)} - with <b>key = "line.separator"</b> for newlines.
     * @see {@link de.tu_berlin.citlab.testsuite.helpers.DebugLogger.LoD}
     */
    public static synchronized String printFooter(String footerText, char lineSymbol)
    {
        //Line-Generators:
        String footerSeparator = fillSymbolLine(LINE_SEPARATOR_LENGTH, lineSymbol);
        String footTextLine = fillSymbolLine(footerText.length(), lineSymbol);

        //Headline-arranging:
        String footLine = NEWLINE + footTextLine + NEWLINE + footerText.toUpperCase() + NEWLINE + footerSeparator + NEWLINE;

        return footLine;
    }


    private static synchronized String fillSymbolLine(int lineLength, char lineSymbol)
    {
        String symbolLine ="";
        for(int n = 0 ; n < lineLength; n++)
        {
            symbolLine = symbolLine.concat(String.valueOf(lineSymbol));
        }

        return symbolLine;
    }




/* List-Printing Methods: */
/* ====================== */
	
	public static String toValListString(List<Values> valList)
	{
        if(valList != null){
            String output = "List<Values>(";
            for(int n = 0 ; n < valList.size() ; n++){
                Values actVals = valList.get(n);
                output += LogPrinter.toValString(actVals);

                if(n+1 != valList.size())
                    output += ", ";
            }
            output += ")";

            return output;
        }
        else return "null";
	}
	
	public static String toTupleListString(List<Tuple> tupleList)
	{
        if(tupleList != null){
            String output = "List<Tuple>(";

            for(int n = 0 ; n < tupleList.size() ; n++){
                Tuple actTuple = tupleList.get(n);
                output += actTuple.toString(); //Assumed to use TupleMock, so toValString() is valid here.

                if(n+1 != tupleList.size())
                    output += ", ";
            }
            output += ")";

            return output;
        }
        else return "null";
	}
	
	public static String toObjectListString(List<Object> objList)
	{
        if(objList != null){
            String output = "List<Object>(";

            for(int n = 0 ; n < objList.size() ; n++){
                Object actObj = objList.get(n);
                if(actObj != null)
                    output += actObj.toString();

                if(n+1 != objList.size())
                    output += ", ";
            }
            output += ")";

            return output;
        }
        else return "null";
	}
	
	
	public static String toObjectWindowString(List<List<Object>> objWindow)
	{
        if(objWindow != null){
            String output = "List<List<Object>>((";
            for(int n = 0 ; n < objWindow.size(); n++){
                List<Object> actList = objWindow.get(n);
                output += LogPrinter.toObjectListString(actList);

                if(n+1 != objWindow.size())
                    output += ", ";
            }
            output += ")";

            return output;
        }
        else return "null";
	}
	
	
	
/* Storm Element-Printing Methods: */
/* =============================== */
	
	public static String toValString(Values vals)
	{
        if(vals != null){
            String output ="[";
            for(int i = 0 ; i < vals.size() ; i++){
                Object actObj = vals.get(i);
                output += actObj.toString();

                if(i+1 == vals.size())
                    output += "]";
                else
                    output +=", ";
            }

            return output;
        }
        else return "null";
	}
	
	
	public static String toString(Fields fields)
	{
        if(fields != null){
            String output ="[";

            for(int i = 0 ; i < fields.size() ; i++){
                String actField = fields.get(i);
                output += actField;

                if(i+1 != fields.size())
                    output += ", ";
                else
                    output +="]";
            }

            return output;
        }
        else return "null";
	}
}
