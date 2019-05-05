package org.sh.db.config

import org.sh.db.h2.Util._

trait TraitDBConfig {
  val dbname:String // e.g. petstore
  val dbhost:String // e.g. localhost
  val dbms:String   // e.g. mysql or h2
  val dbuser:String // e.g. root
  val dbpass:String // e.g. somepass
  val connTimeOut:Int // e.g. 1000
  val usePool:Boolean // e.g. true (use db pool)
  val configSource:String // where is this read from? (which file etc)
  override def toString = s"DBMS:$dbms, HOST:$dbhost, DB_NAME:$dbname" //dbms+":"+dbhost+":"+dbname
  private def dbString = dbms match {
    case s if s.startsWith("h2") => getH2DBString(dbms, dbname)
    case "mysql" => "mysql://localhost:3306"
    case _ => dbms +"://"+dbhost
  }
  def url = "jdbc:"+dbString+"/"+dbname+(if (dbms=="h2mixed") ";AUTO_SERVER=TRUE" else "")  
    /*
      The connection url should be as follows
      jdbc:mysql://localhost/someDb                   // mysql mode 
      jdbc:h2:~/somedir/someDb                        // h2 mode 
      jdbc:h2:tcp://localhost/somedir/someDb          // h2 server mode 
     */
    
  def resolveClass = dbms match {
    case "h2" | "h2mixed" | "h2server" => Class.forName("org.h2.Driver")
    case x if x.startsWith("h2:mem:") => Class.forName("org.h2.Driver")
    case "mysql" => Class.forName("com.mysql.jdbc.Driver")
    case "postgresql" => Class.forName("org.postgresql.Driver")
	case any => throw new Exception("database not supported: "+any)
  }
  def init = {
    if (dbms == "h2server") startH2DBServer
    resolveClass
  }
}
