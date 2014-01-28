package de.tu_berlin.citlab.testsuite.helpers;


import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public final class DebugPrinter
{

    private static final String newline = System.getProperty("line.separator");
    private static final int lineSeparator_length = 40;


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
    public static synchronized String print_Header(String headText, char lineSymbol)
    {
        //Line-Generators:
        String headerSeparator = ""; //will have a constant line-length, defined in the class-constant lineSeparator_length.
        for(int n = 0 ; n < lineSeparator_length ; n++)
        {
            headerSeparator = headerSeparator.concat(String.valueOf(lineSymbol));
        }

        String headText_Line =""; //will have exactly the same length, as the char-count of the "headText"-String.
        for(int n = 0 ; n < headText.length() ; n++)
        {
            headText_Line = headText_Line.concat(String.valueOf(lineSymbol));
        }


        //Headline-arranging:
        String headline = newline + headerSeparator + newline + headText.toUpperCase() + newline + headText_Line + newline;

        return headline;
    }

    /**
     * Just for clear arrangement, this method will create a stand-out text-footer for console-printing.
     * This method creates a footer, that defines a text-block ending, combining the char-parameter "lineSymbol" to a line-separator.
     *
     * @param headText : The {@link String}-Foot-Line, that will be printed in the Console.
     * @param lineSymbol : A <b>char</b> that is repeatedly printed as a header-line. <em>Hint: Use '=', '-', '.' or something like that.</em>
     * @return : The formatted header as a {@link String}, to further append this headline to a log-file.
     * @see {@link System#getProperty(String key)} - with <b>key = "line.separator"</b> for newlines.
     * @see {@link de.tu_berlin.citlab.testsuite.helpers.DebugLogger.LoD}
     */
    public static synchronized String print_Footer(String headText, char lineSymbol)
    {
        //Line-Generators:
        String footerSeparator = ""; //will have a constant line-length, defined in the class-constant lineSeparator_length.
        for(int n = 0 ; n < lineSeparator_length ; n++)
        {
            footerSeparator = footerSeparator.concat(String.valueOf(lineSymbol));
        }

        String headText_Line =""; //will have exactly the same length, as the char-count of the "headText"-String.
        for(int n = 0 ; n < headText.length() ; n++)
        {
            headText_Line = headText_Line.concat(String.valueOf(lineSymbol));
        }


        //Headline-arranging:
        String footLine = newline + headText_Line + newline + headText.toUpperCase() + newline + footerSeparator + newline;

        return footLine;
    }




/* List-Printing Methods: */
/* ====================== */
	
	public static String toValListString(List<Values> valList)
	{
        if(valList != null){
            String output = "List<Values>(";
            for(int n = 0 ; n < valList.size() ; n++){
                Values actVals = valList.get(n);
                output += DebugPrinter.toString(actVals);

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
                output += actTuple.toString(); //Assumed to use TupleMock, so toString() is valid here.

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
                output += DebugPrinter.toObjectListString(actList);

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
	
	public static String toString(Values vals)
	{
        if(vals != null){
            String output ="[";
            for(int i = 0 ; i < vals.size() ; i++){
                Object actObj = vals.get(i);
                output += actObj.toString();

                if(i+1 != vals.size())
                    output += ", ";
                else
                    output +="]";
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
