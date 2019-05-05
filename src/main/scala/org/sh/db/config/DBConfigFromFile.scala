package org.sh.db.config

import org.sh.utils.common.file.TraitFilePropertyReader

object DefaultDBConfigFromFile extends DBConfigFromFile("db.properties")

class DBConfigFromFile(val propertyFile:String) extends TraitDBConfig with TraitFilePropertyReader {
  //  val propertyFile = "db.properties"
  val dbname = read ("dbname", "mydb")
  val dbhost = read ("dbhost", "localhost")
  val dbms = read ("dbms", "h2")
  val dbuser = read ("dbuser", "cs")
  val dbpass = read ("dbpass", "eipoxj4i4xm4lmw")
  val connTimeOut = read("dbConnTimeOut", 2000)
  val usePool = read("usePool", true)
  val configSource = "file:"+propertyFile
  init
  override def toString = super[TraitDBConfig].toString
}

//////////////////////////////

