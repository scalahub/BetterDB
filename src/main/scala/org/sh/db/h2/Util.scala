
package org.sh.db.h2

import org.h2.tools.Server
import java.io.File
import java.util.Calendar

import org.sh.utils.common.file.TraitFilePropertyReader


object Util extends TraitFilePropertyReader{
  val propertyFile = "h2.properties"
  lazy val home = read("h2home", System.getProperty("user.home"))
  lazy val sepr = System.getProperty("file.separator")
  def h2dbDir(db:String) = home+sepr+".h2_"+db
  def getH2DBString(dbms:String, db:String) = dbms match {
    case "h2" | "h2mixed" => "h2:"+h2dbDir(db)
    case x if x.startsWith("h2:mem:") => x
    case "h2server" => "h2:tcp://localhost/"+h2dbDir(db)
  }
  def startH2DBServer = try {
    val server = Server.createTcpServer("")
    if (! server.isRunning(true)) server.start
  } catch { case any:Throwable => println ("could not start h2 db server") }
  
  /**
   * Cleans the H2 database. Works only if the database is in H2 embedded or server mode. The database should not be locked by another process.
   * Renames the db file to an timestamped one. 
   */
  def cleanH2DB(dbName:String) = {
    val file = new File(h2dbDir(dbName))
    val file2 = new File(h2dbDir(dbName)+Calendar.getInstance().getTimeInMillis());
    // Rename file (or directory)
    println (file+" ==> "+file2)
    println (file.renameTo(file2) match {case true => "success"; case _ => "error"});
  }
}
