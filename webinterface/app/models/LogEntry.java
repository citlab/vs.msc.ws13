package models;

public class LogEntry {
  private int id;
  private String datetime;
  private String milliseconds;
  private String logger;
  private String level;
  private String message;
  private String exception;
  private String thread;
  private String marker;

  public LogEntry(int id) {
    this.id = id;
  }

  public int getId() {
    return this.id;
  }

  public String getDatetime() {
    return this.datetime;
  }

  public String getMilliseconds() {
    return this.milliseconds;
  }

  public String getLogger() {
    return this.logger;
  }

  public String getLevel() {
    return this.level;
  }

  public String getMessage() {
    return this.message;
  }

  public String getException() {
    return this.exception;
  }

  public String getThread() {
    return this.thread;
  }

  public String getMarker() {
    return this.marker;
  }

  public LogEntry datetime(String datetime) {
    this.datetime = datetime;
    return this;
  }

  public LogEntry milliseconds(String milliseconds) {
    this.milliseconds = milliseconds;
    return this;
  }

  public LogEntry logger(String logger) {
    this.logger = logger;
    return this;
  }

  public LogEntry level(String level) {
    this.level = level;
    return this;
  }

  public LogEntry message(String message) {
    this.message = message;
    return this;
  }

  public LogEntry exception(String exception) {
    this.exception = exception;
    return this;
  }

  public LogEntry thread(String thread) {
    this.thread = thread;
    return this;
  }

  public LogEntry marker(String marker) {
    this.marker = marker;
    return this;
  }
}