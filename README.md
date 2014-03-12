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

Cassandra Binding:
----------------------
1. Creaete new CassandraConfig instance
2. Use setParams-method of instance to provide Cassandra configuration parameters
  * String - keysapce name to use or create
  * String - table name to use or
  * new PrimaryKey( String... keys )  //will change in future implementation
  * new Fields() - pass tuple field names to store (or no arguments for storing whole tuple)
  * boolean - set true for a counting table  //to be implemented
3. Use CassandraOperator in UDF-Bolt and pass configuration instance
