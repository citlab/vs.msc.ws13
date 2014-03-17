CIT 11 - Master Project "Cloud Computing"
=========================================

This Master Project from the CIT team of the Berlin Institute of Technology is is addressing new implementations of Streaming Operators, using the Twitter Storm Project. (https://github.com/nathanmarz/storm).  
[Link to CIT Website](http://www.cit.tu-berlin.de/menue/lehre/curriculum/ws_20132014/verteilte_systeme_pj_msc/)

**For a quick look:**  
*[GIT-Commands](https://confluence.atlassian.com/display/STASH/Basic+Git+commands)*  
*[GIT-Reference](http://gitref.org/)*  


Code Composition:
-----------------------

<table>
  <tr>
    <th>Code Path</th><th>Description</th>
  </tr>
  <tr>
    <td>Playground</td><td>First Code tries. Every Programmer has its own directory</td>
  </tr>
  <tr>
    <td>Testing</td><td>Test-Suite for Storm-Free Debugging (currently WIP!)</td>
  </tr>
  <tr>
    <td>Master</td><td>The Joint Master Directory of Code</td>
  </tr>
</table>

MySQL Logging:
-----------------------
The default log4j2 configuration (src/main/resources/log4j2.xml) is set to log to a MySQL database. A schema file including MySQL commands to create the necessary database, table and it's user is available at src/main/log4j2.citstorm.sql. At runtime the server's hostname, the database's name, it's user and password are read from a properties-config file at src/main/resources/log4j2_mysql.properties to establish a connection. **Note:** I didn't found a way to programmatically set the table's name. So the name of the table is set by the log4j2 configuration (src/main/resources/log4j2.xml) regardless from what is defined in the properties-config (src/main/resources/log4j2_mysql.properties)!
