package org.sh.db.core

import java.sql.DriverManager
import org.sh.db.config._
import java.sql.Connection
import snaq.db.ConnectionPool

case class DB(c:TraitDBConfig) {
  /**
   * uses connection pool
   */
  lazy val pool = new ConnectionPool("poolname", 1, 200, 300, 180000, c.url, c.dbuser, c.dbpass) // pool parameters hardcoded for now

  var connection:Connection = _
  def getConnection = {
    if (c.usePool) {
      pool.getConnection(3000) 
    } else {
      if (connection == null || !connection.isValid(3000)) {        
        connection = 
            DriverManager.getConnection(c.url, c.dbuser, c.dbpass)      
      }
      connection  
    }
  }
}


