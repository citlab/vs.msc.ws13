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
    <th>Path</th><th>Description</th>
  </tr>


  <tr>
  	<td>Infrastructure</td><td>Services and Deployment setups for the CIT-Storm cloud deployment.</td>
  </tr>
  <tr>
  	<td>Documentation</td><td>Project Documentation and JavaDoc. <em>(currently in Branch "Documentation")</em></td>
  </tr>
  <tr>
    <td>Master</td><td>The Master-Code directory of the Project.</td>
  </tr>
  <tr>
    <td>Playground</td><td>First Code tries. Every Programmer has its own directory. <em>(May be deleted soon.)</em></td>
  </tr>
  <tr>
    <td>Webinterface</td><td>Web-Handling for the Cloud-Deployment.</td>
  </tr>
</table>

MySQL Logging:
-----------------------
The default log4j2 configuration (src/main/resources/log4j2.xml) is set to log to a MySQL database. A schema file including MySQL commands to create the necessary database, table and it's user is available at src/main/log4j2.citstorm.sql. At runtime the server's hostname, the database's name, it's user and password are read from a properties-config file at src/main/resources/log4j2_mysql.properties to establish a connection. **Note:** I didn't found a way to programmatically set the table's name. So the name of the table is set by the log4j2 configuration (src/main/resources/log4j2.xml) regardless from what is defined in the properties-config (src/main/resources/log4j2_mysql.properties)!
