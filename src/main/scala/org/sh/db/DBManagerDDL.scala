
package org.sh.db

import org.sh.db.core.FKDataStructures._
import org.sh.db.core._
import org.sh.db.core.Util._
import org.sh.db.config.TraitDBConfig
import org.sh.db.core.DataStructures._
import org.sh.utils.Util._

abstract class DBManagerDDL(table:Table, dbConfig:TraitDBConfig) extends DBManagerDML(table:Table, dbConfig:TraitDBConfig) {  
  import table._
  // FOREIGN KEY MANAGEMENT
  def removeIndexIfExists(cols:Col*) = {
    using(connection) {
      conn => using(conn.prepareStatement(dropIndexString(cols.toArray))){_.executeUpdate}
    }
  }
  @deprecated("Index reduces performance", "21 Nov 2017")
  def indexBy(cols:Col*) = {
    cols.foreach(c => assert(c.optTable.isEmpty, "indexCols should not refer to other tables"))
    cols.foreach(c => assert(!c.isComposite, "indexCols cannot be composite"))
    cols.foreach(assertColExists)  
    using(connection) { 
      conn => using(conn.prepareStatement(createIndexString(cols.toArray))){_.executeUpdate}
    } 
  }
  //  //  unused below (commented out)
  //  def isIndexExists(cols:Cols) = using(connection){ conn =>
  //    using(conn.prepareStatement(getSelectIndexStr(cols))) { st =>
  //      using(st.executeQuery) { _.next }
  //    }
  //  }
  
  private def isFKLinkExists(link:Link) = using(connection){ conn =>
    val sql = if (dbConfig.dbms == "postgresql") getFKConstraintStrPostgreSQL(link) else getFKConstraintStr(link)
    using(conn.prepareStatement(sql)) { st =>
      using(st.executeQuery) { _.next }
    }
  }
  // needs to override in SecureDBManager
  def addForeignKey(link:Link) = {
    link.fkCols foreach assertColExists
    if (table == link.pkTable && !link.pkCols.intersect(link.fkCols).isEmpty) // referring to same column in same table.. not allowed
      throw new DBException("Foreign Key: cols in table["+table+"]: cannot refer to itself.")
    if (!isFKLinkExists(link)) using(connection) { 
      conn => using(conn.prepareStatement(createFkLinkString(link))){_.executeUpdate}
    } else 0
  }
  def removeForeignKey(link:Link) = {
    link.fkCols foreach assertColExists
    if (table == link.pkTable && !link.pkCols.intersect(link.fkCols).isEmpty) // referring to same column in same table.. not allowed
      throw new DBException("Foreign Key: cols in table["+table+"]: cannot refer to itself.")
    if (isFKLinkExists(link)) using(connection) { 
      conn => using(conn.prepareStatement(dropFkLinkString(link))){_.executeUpdate}
    } else 0
  }
  
}
