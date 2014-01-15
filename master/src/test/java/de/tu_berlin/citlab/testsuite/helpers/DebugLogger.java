package de.tu_berlin.citlab.testsuite.helpers;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.util.AbstractMap.SimpleEntry;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.NoSuchElementException;


/**
 * TODO for DebugCon v.1.0: Write valid javadoc at every entry.
 * 
 * A Singleton-Class for Code-Debugging. <br>
 * Offers convenience-Methods for console-printing and enables the ability to log
 * debug-Messages in a log-file.
 * @version 0.8
 * @author Constantin Gaul - 315687
 *
 */
public final class DebugLogger
{

/* ------------ */
/* Class ENUMS: */
/* ------------ */
	
	/**
	 * The Level of Detail (in short "LoD") is set for each message and the DebugLogger itself.
	 * <p>
	 * The LoD, defined for the DebugLogger, reveals that only messages with the current-LoD, or lower, will be printed to the console.
	 */
	public enum LoD
	{
	/** Basic is the rawest LoD-Type. 
	 * If a message uses this type, it will always be printed. 
	 **/
		BASIC,
		
	/** Medium is the default assignment for the DebugLogger and all messages. 
	 * Every message for general purposes should be printed with that type. 
	 **/
		DEFAULT, 
		
	/** Detailed LoD should be used by every message, that gives a very special insight in the monitored functionality. 
	 * All messages, that will be often printed or could limit a clear view should be printed with that type. 
	 **/
		DETAILED;
	}
	

/* ---------------- */	
/* Class Constants: */
/* ---------------- */
	
	private static final String newline = System.getProperty("line.separator");
	private static final int lineSeparator_length = 40;
	

/* ----------------- */
/* Global Variables: */
/* ----------------- */	
	
	private static DebugLogger debugCon; //The singleton-Instance.
	
	private static File homeDir;
	
	private static boolean isEnabled = true;
	private static boolean hasConsoleOutput;
	private static boolean hasLoggingOutput;
	private static boolean hasErrorOutput;
	
	private static boolean appendTime;
	private static boolean appendCounter;
	private static long counter;
	
	private static LoD console_LoD;
	private static HashMap<String, SimpleEntry<LoD, File>> logFileMap;
	
	

/* ------------------------- */	
/* PRIVATE Constructor-Call: */
/* ------------------------- */
	
	private DebugLogger()
	{
		DebugLogger.homeDir = new File("Logs");
		
		DebugLogger.hasConsoleOutput = true;
		DebugLogger.hasLoggingOutput = true;
		DebugLogger.hasErrorOutput = true;
		
		DebugLogger.appendTime = false;
		DebugLogger.appendCounter = false;
		DebugLogger.counter = 0;
		
		DebugLogger.console_LoD = LoD.DEFAULT;
		logFileMap = new HashMap<String, SimpleEntry<LoD, File>>();
	}
	
	/**
	 * Manages the Singleton-Object construction of this class. If this singleton-Object is not already established,
	 * exactly one object of this class will be instantiated, which stays alive over the whole runtime. <br>
	 * 
	 * <em> <b>EVERY</b> static synchronized method in this class needs to call this method
	 * at the beginning of it, so that the singleton object is well defined over the whole runtime.<em>
	 */
	private static synchronized void constructionRequest()
	{
		if(isEnabled)
		{
			//Creates the singleton-Object, if it's not available yet:
			if(debugCon == null)
			{
				debugCon = new DebugLogger();
			}
		}
	}
	

	
/* -------------------- */	
/* Getters and Setters: */
/* -------------------- */
	
	public static synchronized File getHomeDir()
	{
		constructionRequest();
		return DebugLogger.homeDir;
	}
	
	public static synchronized void setHomeDir(String homeDir, String... subDirPath)
	{
		constructionRequest();
		
		String completePath = homeDir;
		for(String actSubDir : subDirPath)
		{
			completePath = completePath.concat(File.separatorChar + actSubDir);
		}
		DebugLogger.homeDir = new File(completePath);
	}
	
	
	public static synchronized boolean isEnabled()
	{
		constructionRequest();
		return DebugLogger.isEnabled;
	}
	public static synchronized void setEnabled(boolean enabled)
	{
		constructionRequest();
		DebugLogger.isEnabled = enabled;
	}	
	
	/**
	 * Checks, whether this DebugLogger's Singleton-Object has Console-Output enabled or not.
	 * @return a {@link boolean} value, that is true, when the DebugLogger is printing lines on the console and false otherwise.
	 */
	public static synchronized boolean hasConsoleOutput()
	{
		constructionRequest();
		return DebugLogger.hasConsoleOutput;
	}
	public static synchronized void setConsoleOutput(boolean consoleOutput)
	{
		constructionRequest();
		DebugLogger.hasConsoleOutput = consoleOutput;
	}
	public static synchronized void setConsoleOutput(LoD levelOfDetail, boolean consoleOutput)
	{
		constructionRequest();
		DebugLogger.hasConsoleOutput = consoleOutput;
		DebugLogger.console_LoD = levelOfDetail;
	}
	
	
	public static synchronized boolean hasErrorOutput()
	{
		constructionRequest();
		return DebugLogger.hasErrorOutput;
	}
	public static synchronized void setErrorOutput(boolean errorOutput)
	{
		constructionRequest();
		DebugLogger.hasErrorOutput = errorOutput;
	}
	
	
	public static synchronized boolean hasLoggingOutput()
	{
		constructionRequest();
		return DebugLogger.hasLoggingOutput;
	}
	public static synchronized void setLoggingOutput(boolean loggingOutput)
	{
		constructionRequest();
		DebugLogger.hasLoggingOutput = loggingOutput;
	}	
	
	
	public static synchronized void appendTimeToOutput(boolean appendTime)
	{
		constructionRequest();
		DebugLogger.appendTime = appendTime;
	}
	
	public static synchronized void appendCounterToOutput(boolean appendCounter)
	{
		constructionRequest();
		DebugLogger.appendCounter = appendCounter;
	}
	
	
	/**
	 * Enables the File-Logging functionality of this DebugLogger's Singleton-Object. <br>
	 * <em>This method needs to be called, <b>before</b> one of it's logging-method is called for this <b>logType</b>.</em>
	 * <p>
	 * Creates a file with the specified fileName in the debug-logFile-dir, which will contain every log-entry, that this DebugLogger will log with the given logType.
	 * Every logFile that is added with this method will have it's own loggingLevel and can be filled independent of other concurrent logFiles via the logType.
	 * 
	 * @param fileName : A {@link String} that represents the fileName of the new logFile. Each logFile will be created in a subDir in the Project-Folder.
	 * @param loggingLevel : The {@link LoD}, which sets up the level of Detail for the logs in this logFile.
	 * @param logType : A {@link String} that specifies, which kind of logs will be written in the logFile from {@link DebugLogger#log_Message(LoD, String, String, String...)}-calls.
	 * @return the logFile as a {@link File}-object.
	 * @see File
	 * @see HashMap
	 * @see SimpleEntry
	 */
	public static synchronized File addFileLogging(String fileName, LoD loggingLevel, String logType)
	{
		constructionRequest();
		if(isEnabled && DebugLogger.hasLoggingOutput)
		{
			
			File logFileDir = DebugLogger.homeDir;
			if(logFileDir.exists() == false) {
				try{
					logFileDir.mkdir();
				}
				catch (SecurityException e){
					System.err.println("DebugLogger ERROR: No permission to create a directory at "+ DebugLogger.homeDir.toString());
					e.printStackTrace();
				}
			}
			
			File logFile = new File(logFileDir.getAbsolutePath() + File.separatorChar + fileName);
			if(logFile.exists()) {
				try{
					logFile.delete();
				}
				catch (SecurityException e){
					System.err.println("DebugLogger ERROR: No permission to delete a file at "+ DebugLogger.homeDir.toString());
					e.printStackTrace();
				}
			}
			try
			{
				logFile.createNewFile();
				
				SimpleEntry<LoD, File> logFileEntry = new SimpleEntry<LoD, File>(loggingLevel, logFile);
				DebugLogger.logFileMap.put(logType, logFileEntry);
			}
			catch (IOException e)
			{
				System.err.println("DebugLogger ERROR: Log-File could not be created: "+ fileName);
				e.printStackTrace();
			}
			
			return logFile;
		}
		else return null;
	}
	
	/**
	 * Enables the File-Logging functionality of this DebugLogger's Singleton-Object. <br>
	 * <em>This method needs to be called, <b>before</b> one of it's logging-method is called for this <b>logType</b>.</em>
	 * <p>
	 * Creates a file with the specified fileName in a sub-directory, relative to the debug-logFile-dir, which will contain every log-entry, that this DebugLogger will log with the given logType.
	 * Every logFile that is added with this method will have it's own loggingLevel and can be filled independent of other concurrent logFiles via the logType.
	 * 
	 * @param fileName : A {@link String} that represents the fileName of the new logFile. Each logFile will be created in a subDir in the Project-Folder.
	 * @param loggingLevel : The {@link LoD}, which sets up the level of Detail for the logs in this logFile.
	 * @param logType : A {@link String} that specifies, which kind of logs will be written in the logFile from {@link DebugLogger#log_Message(LoD, String, String, String...)}-calls.
	 * @return the logFile as a {@link File}-object.
	 * @see File
	 * @see HashMap
	 * @see SimpleEntry
	 */
	public static synchronized File addFileLogging(String relFilePath, String fileName, LoD loggingLevel, String logType)
	{
		constructionRequest();
		if(isEnabled && DebugLogger.hasLoggingOutput)
		{
			File debugDir = DebugLogger.homeDir;
			if(debugDir.exists() == false) debugDir.mkdirs();
			
			File logFileDir = new File(debugDir.getPath() + File.separatorChar + relFilePath);
			if(logFileDir.exists() == false) logFileDir.mkdirs();
			
			File logFile = new File(logFileDir.getPath() + File.separatorChar + fileName);
			if(logFile.exists()) logFile.delete();
			try
			{
				logFile.createNewFile();
				
				SimpleEntry<LoD, File> logFileEntry = new SimpleEntry<LoD, File>(loggingLevel, logFile);
				DebugLogger.logFileMap.put(logType, logFileEntry);
			}
			catch (IOException e)
			{
				System.err.println("DebugLogger ERROR: Log-File could not be created: "+ fileName);
				e.printStackTrace();
			}
			
			return logFile;
		}
		else return null;
	}


	
/* --------------- */
/* Public Methods: */
/* --------------- */
	
	/**
	 * Just for clear arrangement, this method will create a stand-out text-header for console-printing. <br>
	 * This method creates a header, that defines a text-block beginning, combining the char-parameter "lineSymbol" to a line-separator.
	 * <p>
	 * <em>This is a convenience-method for {@link DebugLogger#print_Header(LOD, String, char)} with a {@link LoD#DEFAULT} Level-of-Detail.</em>
	 * 
	 * @param headText : The {@link String}-Headline, that will be printed in the Console.
	 * @param lineSymbol : A <b>char</b> that is repeatedly printed as a header-line. <em>Hint: Use '=', '-', '.' or something like that.</em>
	 * @return The formatted header as a {@link String}, to further append this headline to a log-file.
	 * @see {@link System#getProperty(String key)} - with <b>key = "line.separator"</b> for newlines.
	 * @see {@link LoD#DEFAULT}
	 */
	public static synchronized String print_Header(String headText, char lineSymbol)
	{
		constructionRequest();
		if(isEnabled)
		{
			String header = print_Header(LoD.DEFAULT, headText, lineSymbol);
			
			return header;
		}
		else return null;
	}
	
	/**
	 * Just for clear arrangement, this method will create a stand-out text-header for console-printing. <br>
	 * This method creates a header, that defines a text-block beginning, combining the char-parameter "lineSymbol" to a line-separator.
	 * <p>
	 * 
	 * @param levelOfDetail : The Logging {@link LoD}, which the DebugLogger needs to use at least, to log (or print) this message.
	 * @param headText : The {@link String}-Headline, that will be printed in the Console.
	 * @param lineSymbol : A <b>char</b> that is repeatedly printed as a header-line. <em>Hint: Use '=', '-', '.' or something like that.</em>
	 * @return The formatted header as a {@link String}, to further append this headline to a log-file.
	 * @see {@link System#getProperty(String key)} - with <b>key = "line.separator"</b> for newlines.
	 * @see {@link LoD}
	 */
	public static synchronized String print_Header(LoD levelOfDetail, String headText, char lineSymbol)
	{
		constructionRequest();
		if(isEnabled)
		{			
			if((DebugLogger.console_LoD.compareTo(levelOfDetail) >= 0) && DebugLogger.hasConsoleOutput)
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
				System.out.println(headline);
				
				return headline;
			}
			else return "";
		}
		else return null;
	}
	
	
	/**
	 * Just for clear arrangement, this method will create a stand-out text-footer for console-printing.
	 * This method creates a footer, that defines a text-block ending, combining the char-parameter "lineSymbol" to a line-separator.
	 * <p>
	 * <em>This is a convenience-method for {@link DebugLogger#print_Footer(LOD, String, char)} with a {@link LoD#DEFAULT} Level-of-Detail.</em>
	 * 
	 * @param headText : The {@link String}-Foot-Line, that will be printed in the Console.
	 * @param lineSymbol : A <b>char</b> that is repeatedly printed as a header-line. <em>Hint: Use '=', '-', '.' or something like that.</em>
	 * @return The formatted header as a {@link String}, to further append this headline to a log-file.
	 * @see {@link System#getProperty(String key)} - with <b>key = "line.separator"</b> for newlines.
	 * @see {@link LoD#DEFAULT}
	 */
	public static synchronized String print_Footer(String headText, char lineSymbol)
	{
		constructionRequest();
		if(isEnabled)
		{
			String footer = print_Footer(LoD.DEFAULT, headText, lineSymbol);
			return footer;
		}
		else return null;
	}
	
	/**
	 * Just for clear arrangement, this method will create a stand-out text-footer for console-printing.
	 * This method creates a footer, that defines a text-block ending, combining the char-parameter "lineSymbol" to a line-separator.
	 * 
	 * @param levelOfDetail : The Logging {@link LoD}, which the DebugLogger needs to use at least, to log (or print) this message.
	 * @param headText : The {@link String}-Foot-Line, that will be printed in the Console.
	 * @param lineSymbol : A <b>char</b> that is repeatedly printed as a header-line. <em>Hint: Use '=', '-', '.' or something like that.</em>
	 * @return : The formatted header as a {@link String}, to further append this headline to a log-file.
	 * @see {@link System#getProperty(String key)} - with <b>key = "line.separator"</b> for newlines.
	 * @see {@link LoD}
	 */
	public static synchronized String print_Footer(LoD levelOfDetail, String headText, char lineSymbol)
	{
		constructionRequest();
		if(isEnabled)
		{ 					
			if((DebugLogger.console_LoD.compareTo(levelOfDetail) >= 0) && DebugLogger.hasConsoleOutput)
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
				System.out.println(footLine);
				
				return footLine;
			}
			else return "";
		}
		else return null;
	}
	
	
	
	/**
	 * Prints an entered message out on the console.
	 * <p>
	 * The message is split in two parts: The message-body, which should define the error or give additional information,
	 * and the optional data-String input, which is printed on a comma-separated way in a new-line.
	 * <p>
	 * <em>This is a convenience-method for {@link DebugLogger#print_Message(LOD, String, char)} with a {@link LoD#DEFAULT} Level-of-Detail.</em>
	 * 
	 * @param msgBody : The debug-information as a <b>String</b>
	 * @param appendedStrings : The optional Data-String's, containing detailed debugging information on the objects, that are in touch with this debug-Message.
	 * @return : The complete log-Line as a <b>String</b>, to further append this message to a log-file.
	 * @see LoD#DEFAULT
	 */
	public static synchronized void print_Message(String msgBody, String...appendedStrings)
	{
		constructionRequest();
		if(isEnabled)
		{
			print_Message(LoD.DEFAULT, msgBody, appendedStrings);
		}
	}
	
	/**
	 * Prints an entered message out on the console.
	 * <p>
	 * The message is split in two parts: The message-body, which should define the error or give additional information,
	 * and the optional data-String input, which is printed on a comma-separated way in a new-line.
	 * 
	 * @param levelOfDetail : The Logging {@link LoD}, which the DebugLogger needs to use at least, to log (or print) this message.
	 * @param msgBody : The debug-information as a <b>String</b>
	 * @param appendedStrings : The optional Data-String's, containing detailed debugging information on the objects, that are in touch with this debug-Message.
	 * @return : The complete log-Line as a <b>String</b>, to further append this message to a log-file.
	 * @see LoD#DEFAULT
	 */
	public static synchronized void print_Message(LoD levelOfDetail, String msgBody, String...appendedStrings)
	{
		constructionRequest();
		if(isEnabled)
		{			
			if((DebugLogger.console_LoD.compareTo(levelOfDetail) >= 0) && DebugLogger.hasConsoleOutput)
			{
				//The following code writes the message-Body and the following input-Data-Strings to the console:
				String message = generate_Message(msgBody, appendedStrings);
				System.out.print(message);
			}
		}
	}
	
	
	public static synchronized void print_Error(String msgBody, String...appendedStrings)
	{
		constructionRequest();
		if(isEnabled)
		{			
			if(DebugLogger.hasErrorOutput)
			{
				String message = generate_Message(msgBody, appendedStrings);
				System.err.print(message);
			}
		}
	}
	
	
	/** 
	 * Logs an entered message in a log-file, which was previously defined via the {@link DebugLogger#addFileLogging(String, LoD, String)} - method. <br>
	 * <em>If the log-File was <b>not defined before</b>, every call of this method will result in an {@link NoSuchElementException}! </em>
	 * <p>
	 * The message is split in two parts: The message-body, which should define the error or give additional information,
	 * and the data-String input, which is printed on a comma-separated way in a new-line.
	 * 
	 * @param levelOfDetail : The {@link LoD}, that the file-logger needs to have set up at least, to print this message in the logfile.
	 * @param logType: The logType as a {@link String}, that will define the log-File, in which this message will be printed.
	 * @param msgBody : The debug-information as a {@link String}
	 * @param appendedStrings : The Data-String's, containing detailed debugging information on the objects, that are in touch with this debug-Message.
	 * @throws NoSuchElementException
	 * @see HashMap
	 */
	public static synchronized void log_Message(LoD levelOfDetail, String logType, String msgBody, String...appendedStrings) throws NoSuchElementException
	{
		constructionRequest();
		if(isEnabled && DebugLogger.hasLoggingOutput)
		{
			
			if(logFileMap.containsKey(logType))
			{
				SimpleEntry<LoD, File> fileEntry = DebugLogger.logFileMap.get(logType);
				if(fileEntry.getKey().compareTo(levelOfDetail) >= 0)
				{
					try
					{
						
						FileWriter fileWriter = new FileWriter(fileEntry.getValue(), true);
						
						String message = generate_Message(msgBody, appendedStrings);
						fileWriter.write(message);
						
						fileWriter.close();
					}
					catch (IOException e)
					{
						System.err.println("DebugLogger ERROR: FileWriter has thrown an IOException, while connecting to the File: "+ logFileMap.get(logType).getValue().getAbsolutePath());
						e.printStackTrace();
					}
				}
			}
			else
			{
				NoSuchElementException noElemExcept = new NoSuchElementException("No log-File exists for the logType \""+ logType +"\". Call DebugLogger.addFileLogging(...) for this logType, before this method-call is reached!");
				throw noElemExcept;
			}
		}
	}
	
	/** 
	 * Logs an entered message in <b>multiple</b> log-files, which were previously defined via the {@link DebugLogger#addFileLogging(String, LoD, String)} - method. <br>
	 * <em>If one of the log-Files were <b>not defined before</b>, every call of this method will result in an {@link NoSuchElementException}! </em>
	 * <p>
	 * The message is split in two parts: The message-body, which should define the error or give additional information,
	 * and the data-String input, which is printed on a comma-separated way in a new-line.
	 * 
	 * @param levelOfDetail : The {@link LoD}, that the file-logger needs to have set up at least, to print this message in the logfile.
	 * @param logTypes : All logTypes as a <b>{@link String}-Array</b>, that will define all log-Files, in which this message will be printed.
	 * @param msgBody : The debug-information as a {@link String}
	 * @param appendedStrings : The Data-String's, containing detailed debugging information on the objects, that are in touch with this debug-Message.
	 * @throws NoSuchElementException
	 * @see HashMap
	 */
	public static synchronized void multiLog_Message(LoD levelOfDetail, String[] logTypes, String msgBody, String...appendedStrings) throws NoSuchElementException
	{
		constructionRequest();
		if(isEnabled && DebugLogger.hasLoggingOutput)
		{
			for(String logType : logTypes)
			{
				if(logFileMap.containsKey(logType))
				{
					SimpleEntry<LoD, File> fileEntry = DebugLogger.logFileMap.get(logType);
					if(fileEntry.getKey().compareTo(levelOfDetail) >= 0)
					{
						try
						{
							
							FileWriter fileWriter = new FileWriter(fileEntry.getValue(), true);
							
							String message = generate_Message(msgBody, appendedStrings);
							fileWriter.write(message);
							
							fileWriter.close();
						}
						catch (IOException e)
						{
							System.err.println("DebugLogger ERROR: FileWriter has thrown an IOException, while connecting to the File: "+ logFileMap.get(logType).getValue().getAbsolutePath());
							e.printStackTrace();
						}
					}
				}
				else
				{
					NoSuchElementException noElemExcept = new NoSuchElementException("No log-File exists for the logType \""+ logType +"\". Call DebugLogger.addFileLogging(...) for this logType, before this method-call is reached!");
					throw noElemExcept;
				}
			}
		}
	}
	
	
	/**
	 * TODO: Write javadoc.
	 * @param levelOfDetail
	 * @param logType
	 * @param msgBody
	 * @param appendedStrings
	 * @throws NoSuchElementException
	 */
	public static synchronized void printAndLog_Message(LoD levelOfDetail, String logType, String msgBody, String...appendedStrings) throws NoSuchElementException
	{
		constructionRequest();
		if(isEnabled)
		{
			print_Message(levelOfDetail, msgBody, appendedStrings);
			log_Message(levelOfDetail, logType, msgBody, appendedStrings);
		}
	}
	
	/**
	 * TODO: Write javadoc.
	 * @param levelOfDetail
	 * @param logTypes
	 * @param msgBody
	 * @param appendedStrings
	 * @throws NoSuchElementException
	 */
	public static synchronized void printAndMultiLog_Message(LoD levelOfDetail, String[] logTypes, String msgBody, String...appendedStrings) throws NoSuchElementException
	{
		constructionRequest();
		if(isEnabled)
		{
			print_Message(levelOfDetail, msgBody, appendedStrings);
			multiLog_Message(levelOfDetail, logTypes, msgBody, appendedStrings);
		}
	}
	
	
	/**
	 * TODO: write javadoc.
	 * @param logType
	 * @param msgBody
	 * @param appendedStrings
	 * @throws NoSuchElementException
	 */
	public static synchronized String log_Error(String logType, String msgBody, String...appendedStrings) throws NoSuchElementException
	{
		constructionRequest();
		if(isEnabled && DebugLogger.hasErrorOutput)
		{
			if(logFileMap.containsKey(logType))
			{
				SimpleEntry<LoD, File> fileEntry = DebugLogger.logFileMap.get(logType);
				try
				{
					
					FileWriter fileWriter = new FileWriter(fileEntry.getValue(), true);
					
					String message = "ERROR: "+ generate_Message(msgBody, appendedStrings);
					fileWriter.write(message);
					
					fileWriter.close();
					
					return message;
				}
				catch (IOException e)
				{
					System.err.println("DebugLogger ERROR: FileWriter has thrown an IOException, while connecting to the File: "+ logFileMap.get(logType).getValue().getAbsolutePath());
					e.printStackTrace();
					return null;
				}
			}
			else
			{
				NoSuchElementException noElemExcept = new NoSuchElementException("No log-File exists for the logType \""+ logType +"\". Call DebugLogger.addFileLogging(...) for this logType, before this method-call is reached!");
				throw noElemExcept;
			}
		}
		else return null;
	}
	
	/**
	 * TODO: write javadoc.
	 * @param logTypes
	 * @param msgBody
	 * @param appendedStrings
	 * @throws NoSuchElementException
	 */
	public static synchronized void multiLog_Error(String[] logTypes, String msgBody, String...appendedStrings) throws NoSuchElementException
	{
		constructionRequest();
		if(isEnabled && DebugLogger.hasErrorOutput)
		{
			for(String logType : logTypes)
			{
				if(logFileMap.containsKey(logType))
				{
					SimpleEntry<LoD, File> fileEntry = DebugLogger.logFileMap.get(logType);
					try
					{
						
						FileWriter fileWriter = new FileWriter(fileEntry.getValue(), true);
						
						String message = "<ERROR: "+ generate_Message(msgBody, appendedStrings) +"ERROR/>";
						fileWriter.write(message);
						
						fileWriter.close();
					}
					catch (IOException e)
					{
						System.err.println("DebugLogger ERROR: FileWriter has thrown an IOException, while connecting to the File: "+ logFileMap.get(logType).getValue().getAbsolutePath());
						e.printStackTrace();
					}
				}
				else
				{
					NoSuchElementException noElemExcept = new NoSuchElementException("No log-File exists for the logType \""+ logType +"\". Call DebugLogger.addFileLogging(...) for this logType, before this method-call is reached!");
					throw noElemExcept;
				}
			}
		}
	}
	
	/**
	 * TODO: write javadoc.
	 * @param logType
	 * @param msgBody
	 * @param appendedStrings
	 */
	public static synchronized void printAndLog_Error(String logType, String msgBody, String...appendedStrings)
	{
		constructionRequest();
		if(isEnabled && DebugLogger.hasErrorOutput)
		{
			print_Error(msgBody, appendedStrings);
			log_Message(LoD.BASIC, logType, msgBody, appendedStrings);
		}
	}
	
	/**
	 * TODO: write javadoc.
	 * @param levelOfDetail
	 * @param logTypes
	 * @param msgBody
	 * @param appendedStrings
	 * @throws NoSuchElementException
	 */
	public static synchronized void printAndMultiLog_Error(String[] logTypes, String msgBody, String...appendedStrings) throws NoSuchElementException
	{
		constructionRequest();
		if(isEnabled && DebugLogger.hasErrorOutput)
		{
			print_Error(msgBody, appendedStrings);
			multiLog_Error(logTypes, msgBody, appendedStrings);
		}
	}

	
	public static synchronized PrintWriter get_LogFilePrinter(String logType)
	{
		constructionRequest();
		if(isEnabled)
		{
			if(logFileMap.containsKey(logType))
			{
				SimpleEntry<LoD, File> fileEntry = DebugLogger.logFileMap.get(logType);
				try
				{
					FileWriter fileWriter = new FileWriter(fileEntry.getValue(), true);
					
					PrintWriter printWriter = new PrintWriter(fileWriter);
					return printWriter;
				}
				catch (IOException e)
				{
					System.err.println("DebugLogger ERROR: FileWriter has thrown an IOException, while connecting to the File: "+ logFileMap.get(logType).getValue().getAbsolutePath());
					e.printStackTrace();
					return null;
				}
			}
			else
			{
				NoSuchElementException noElemExcept = new NoSuchElementException("No log-File exists for the logType \""+ logType +"\". Call DebugLogger.addFileLogging(...) for this logType, before this method-call is reached!");
				throw noElemExcept;
			}
		}
		else return null;
	}
	
	
	
/* ---------------- */
/* Private Methods: */
/* ---------------- */
	
	private static synchronized String generate_Message(String msgBody, String... appendedStrings)
	{	
		constructionRequest();
		if(isEnabled)
		{
			String appendix ="";
			
			if(appendedStrings.length > 0)
			{
				appendix = appendix + "(";
				for(int n = 0 ; n < appendedStrings.length ; n++)
				{
					appendix = appendix.concat(appendedStrings[n]);
					if((n+1) < appendedStrings.length) appendix = appendix.concat(", ");
				}
				appendix = appendix + ")" + newline;
			}
			
			int timeLen = 0;
			String timeStamp = "";
			if(DebugLogger.appendTime)
			{
				Date date = new Date();
				DateFormat dateForm = DateFormat.getTimeInstance(DateFormat.MEDIUM, Locale.GERMANY);
				
				timeStamp = "["+ dateForm.format(date) +"]: ";
				timeLen = timeStamp.length();
			}
			
			int counterLen = 0;
			String counter = "";
			if(DebugLogger.appendCounter)
			{
				counter = "("+ String.valueOf(DebugLogger.counter) +") ";
				counterLen = counter.length();
				DebugLogger.counter = DebugLogger.counter + 1;
			}
			
			String message = "";
			if(DebugLogger.appendTime || DebugLogger.appendCounter)
			{
				message = counter + timeStamp + "\t" + msgBody + newline;
				
				for(int n = 0 ; n < timeLen + counterLen ; n++)
				{
					message = message.concat(" ");
				}
				
				message = message.concat("\t" + appendix) + newline;
			}
			else 
			{
				message = msgBody + newline + appendix + newline;
			}
			
			return message;
		}
		else return null;
	}
}
