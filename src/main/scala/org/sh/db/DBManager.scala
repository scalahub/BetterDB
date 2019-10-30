package org.sh.db

import org.sh.db.config._
import org.sh.db.core.Util._
import org.sh.db.core._
import org.sh.db.core.DataStructures._
import org.sh.utils.Util._
import org.sh.utils.json.JSONUtil.JsonFormatted
import java.io.File
import java.sql.ResultSet
import java.sql.{ResultSet => RS}
import org.sh.db.core.FKDataStructures._
import DBManagerMgmt._
import QueryProfiler._

case class ExistingDB(tableName:String)(implicit val dbConfig:TraitDBConfig = DefaultDBConfigFromFile) { 
  import Table._
  // for loading tables not created by this library. We will not have column defs here
  private val dropString = "DROP TABLE "+tableName
  private val countString = "SELECT COUNT(*) FROM "+tableName
  private lazy val db = new DB(dbConfig)
  def connection = db.getConnection
  def count = using(connection) {conn =>
    if (printSQL_?) println("Count query SQL [?]:\n  "+countString)
    usingProfiler(countString){
      using(conn.prepareStatement(countString)) {st => 
        using (st.executeQuery){rs =>
            rs.next
            rs.getLong(1)
        }        
      }
    }
  }
  def drop = {
    using(connection) {conn =>
      if (printSQL_?) println("Count query SQL [?]:\n  "+countString)
      usingProfiler(countString){
        using(conn.prepareStatement(dropString)) {st => 
          st.executeUpdate
        }
      }
    }
  }
  def dropIfEmpty = {
    if (count > 0) throw new DBException("table is not empty: "+tableName)
    drop
  }
  
  private lazy val schemaStr = if (dbConfig.dbms == "postgresql") getPostgreSQLSchemaStr(tableName) else getMySQLSchemaStr(tableName)

  def generateDB = using(connection){conn =>
    val (cols, keys) = using(conn.prepareStatement(schemaStr)){st => 
      using (st.executeQuery) {rs => extractTableColsAndKeys(getColList(rs))}      
    }
    val newCols = if (cols.isEmpty) List(Col("Dummy", BOOL)) else cols
    DBManager(tableName)(newCols: _*)(keys: _*)(dbConfig)
  }  

}

import Table._
private [db] object DBManager {
  
  val noCols = Array[Col]()  
  
  def apply(name:String)(cols:Col *)(priCols:Col *)(implicit config:TraitDBConfig = DefaultDBConfigFromFile) = new DBManager(Table(name, cols.toArray, priCols.toArray))(config)
  def apply(name:String, cols:Cols, priCols:Cols)(implicit config:TraitDBConfig):DBManager = apply(name)(cols: _ *)(priCols: _ *)(config)
  def apply(name:String, cols:Cols, priCol:Col)(implicit config:TraitDBConfig):DBManager = apply(name, cols, Array(priCol))(config)
  def apply(name:String, cols:Cols)(implicit config:TraitDBConfig):DBManager = apply(name, cols, noCols)(config)
  
  var loadedDBManagers = Map[String, DBManager]()
  
  private def addLoadedDBManager(dbm:DBManager, dbmID:String, trace:String) = {
    loadedDBManagers += (dbmID -> dbm)
    loadedDBManagersTrace += (dbmID -> trace)
  }
  
  def apply(table:Table)(implicit config:TraitDBConfig) = new DBManager(table)(config)

  var loadedDBManagersTrace = Map[String, String]() // stackTrace
   
}
/* Database functionality for making the following SQL queries: SELECT, DELETE, INSERT and UPDATE */
private [db] class DBManager(val table:Table)(implicit val dbConfig:TraitDBConfig = DefaultDBConfigFromFile)
extends DBManagerDDL(table:Table, dbConfig:TraitDBConfig) with JsonFormatted {
  implicit val dbDML:DBManagerDML = this
  import table._

  val tableID = table.tableName+"_"+shaSmall(table.tableName+"|"+dbConfig.dbname+"|"+dbConfig.dbhost+"|"+dbConfig.dbms)
  val keys = Array("tableName", "dbname", "host", "dbms", "tableID", "numCols")
  val vals = Array[Any](table.tableName, dbConfig.dbname, dbConfig.dbhost, dbConfig.dbms, tableID, tableCols.size)
  
  //  
  if (isPostGreSQL) {
    // https://stackoverflow.com/a/47638417/243233
    val sql1 = """
CREATE OR REPLACE FUNCTION _group_concat_finalize(anyarray)
RETURNS text AS $$
    SELECT array_to_string($1,',')
$$ IMMUTABLE LANGUAGE SQL;
"""
    val sql2 = """CREATE AGGREGATE group_concat(anyelement) (
   SFUNC=array_append,
   STYPE=anyarray,
   FFUNC=_group_concat_finalize,
   INITCOND='{}'
);"""    
    using(connection){  // old postgresql does not support blob. So we create an alias from blob to bytea
      //// POSTGRESQL TO DEBUG .. added tryIt
      conn => tryIt(using(conn.prepareStatement(sql1))(_.executeUpdate))
    }
    using(connection){  // old postgresql does not support blob. So we create an alias from blob to bytea
      //// POSTGRESQL TO DEBUG .. added tryIt
      conn => tryIt(using(conn.prepareStatement(sql2))(_.executeUpdate))
    }
    // // new version supports??
    //    using(connection){  // old postgresql does not support blob. So we create an alias from blob to bytea
    //      //// POSTGRESQL TO DEBUG .. added tryIt
    //      conn => using(conn.prepareStatement(Table.createPostGreBlob))(_.executeUpdate)
    //    }
  }

  def getTableID = DBManager.loadedDBManagers.find(_._2 eq this) match {
    case Some((id, _)) => id
    case _ => throw new DBException("DBManger not registered: "+this)
  }
  // private fields
  private lazy val db = new DB(dbConfig)
  def connection = db.getConnection
  ////////////////////////////////////////////////////////////////
  //// DML/DDL METHODS START
  ////////////////////////////////////////////////////////////////
   /**
     * Inserts an entry (row) into the table
     *    @param data is an array giving the values to insert in the Cols (columns). Every column must be filled.
     *    @return the number of rows affected by the query
     */ /* to override in SecureDBManager */
  // note rs below must be plaintext
  def insertRS[T](rs:ResultSet) = bmap(rs.next)(insertArray(getTable.tableCols.map(get(rs, _)))).size   
  def exists(wheres:Wheres) = countRows(wheres) >= 1
  def exists(wheres:Where*):Boolean = exists(wheres.toArray)
  def isNonEmpty = exists()
  def isEmpty = !isNonEmpty

  @deprecated("use aggregateBigDecimal as this may result in loss of precision", "22 Oct 2016") 
  def aggregateBigIntOld(aggregates:Aggregates, wheres:Wheres, havings:Havings = Array()) = 
    aggregate(aggregates, wheres, havings, (rs, s) => BigInt(rs.getString(s)))(BigDecimal(_).toBigInt) // why having BigDecimal?
  
  def aggregateLong(aggregates:Aggregates, wheres:Wheres, havings:Havings = Array()) = 
    aggregate(
      aggregates, wheres, havings, 
      (rs, s) => {
        rs.getLong(s)
      }
    )(_.toLong)
  
  @deprecated("use aggregateBigDecimal as this may result in loss of precision", "22 Oct 2016") 
  def aggregateBigInt(aggregates:Aggregates, wheres:Wheres, havings:Havings = Array()) = 
    aggregate(
      aggregates, wheres, havings, 
      (rs, s) => {
        val str = rs.getString(s)
        try {
          BigInt(str) 
        } catch {
          case a:Any => 
            val long = rs.getLong(s)
            println(Console.RED+" [DB AggregateBigInt] "+Console.RESET+"[Exception] rs.getString = $str. rs.getLong = $long. Error: "+a.getMessage)
            a.getStackTrace.map(_.toString).filterNot{x =>
              x.startsWith("java") || 
              x.startsWith("scala") || 
              x.startsWith("mux") || 
              x.startsWith("akka") || 
              x.startsWith("org") || 
              x.startsWith("sun") || 
              x.startsWith("com")
            }.foreach {tr =>
              println(Console.RED+"   [stacktrace] "+Console.RESET+tr.toString)
            }
            BigInt(long)
        }
      }
      
    )(
      BigDecimal(_).toBigInt //BigInt(_)
    )
  
  def aggregateDouble(select:Aggregates, wheres:Wheres, havings:Havings = Array()) = aggregate(select, wheres, havings, (rs, s) => rs.getDouble(s))(_.toDouble)
  def aggregateInt(select:Aggregates, wheres:Wheres, havings:Havings = Array()) = aggregate(select, wheres, havings, (rs, s) => rs.getInt(s))(_.toInt)

  def aggregateBigDecimal(aggregates:Aggregates, wheres:Wheres, havings:Havings = Array()) = aggregate(aggregates, wheres, havings, (rs, s) => BigDecimal(rs.getBigDecimal(s)))(BigDecimal(_))
  def aggregateString(select:Aggregates, wheres:Wheres, havings:Havings = Array()) = aggregate(select, wheres, havings, (rs, s) => rs.getString(s))(_.toString)
  
  // is deleteAll really necessary?
  def deleteAll:Int = delete(Array()) // deletes all rows from the table. returns the number of rows deleted (Int)

  def aggregateGroupBigInt(aggregates:Aggregates, wheres:Wheres, groups:GroupByIntervals=Array(), havings:Havings=Array()) = 
    aggregateGroupHaving(aggregates, wheres, groups, havings, (rs, s) => BigInt(rs.getString(s)))(BigInt(_))

  def aggregateGroupBigDecimal(aggregates:Aggregates, wheres:Wheres, groups:GroupByIntervals=Array(), havings:Havings=Array()) = 
    aggregateGroupHaving(aggregates, wheres, groups, havings, (rs, s) => BigDecimal(rs.getBigDecimal(s)))(BigDecimal(_))
  def aggregateGroupString(aggregates:Aggregates, wheres:Wheres, groups:GroupByIntervals=Array(), havings:Havings=Array()) = 
    aggregateGroupHaving(aggregates, wheres, groups, havings, (rs, s) => rs.getString(s))(s => s)
  
  def aggregateGroupLong(aggregates:Aggregates, wheres:Wheres, groups:GroupByIntervals=Array(), havings:Havings=Array()) = 
    aggregateGroupHaving(aggregates, wheres, groups, havings, (rs, s) => rs.getLong(s))(_.toLong)
  def aggregateGroupDouble(aggregates:Aggregates, wheres:Wheres, groups:GroupByIntervals=Array(), havings:Havings=Array()) = 
    aggregateGroupHaving(aggregates, wheres, groups, havings, (rs, s) => rs.getDouble(s))(_.toDouble)
  def aggregateGroupInt(aggregates:Aggregates, wheres:Wheres, groups:GroupByIntervals=Array(), havings:Havings=Array()) = 
    aggregateGroupHaving(aggregates, wheres, groups, havings, (rs, s) => rs.getInt(s))(_.toInt)  

    // following for single where
  def countRows(where:Where):Long = countRows(Array(where))
  
  def countAllRows = countRows(Array[Where]())
  
  // following for single where, single Increment
  @deprecated def incrementCol(where:Where, increment:Increment) = incrementCols(Array(where), Array(increment))
  def incrementColTx(where:Where, increment:Increment) = incrementColsTx(Array(where), Array(increment))

  // following for single where, single update
  def updateCol(where:Where, update:Update[Any]):Int = updateCols(Array(where), Array(update))

  def insertList(data:List[Any]):Long = insertArray(data.toArray)  
   
  // hack below. (why?)
  // an implementation of func2 needs to be provided to convert the result to the intended type (i.e. Long, Int, Double BigInt, etc
  // this is done in aggregateLong, aggregateInt, etc
  private def aggregate[T](select:Aggregates, wheres:Wheres, havings:Havings,func: (RS, String)=>T)(
    implicit func2:String => T = ???
  ) = {  // to provide implementation for converting to Scala Number types
    val data = aggregateGroupHaving(select, wheres, Array(), havings, func)(func2)
    if (data.size != 1) throw DBException("Aggregate query returned "+data.size+" rows. Require exactly one row for this operation")
    data(0)
  }
  
  //////////////////////////////////////////////////////////////////////////////////////////////////
  // DDL related stuff below
  //////////////////////////////////////////////////////////////////////////////////////////////////

  // initialize table
  createTableIfNotExists // create table if it does not exist
  validateTableSchema // see if schema of actual table matches schems here

  // methods start
  def dropTable:Int = using(connection) { conn => using(conn.prepareStatement(dropSQLString)){st => st.executeUpdate} }
  def getTable = table
  def getConfig = dbConfig
  
  lazy val createString = createSQLString(isPostGreSQL)
  
  private def createTableIfNotExists = using(connection){
    if (printSQL) 
      println("Create table SQL:\n "+createString)
    conn => using(conn.prepareStatement(createString))(_.executeUpdate)
  }
  // GENERAL TABLE VALIDATION
  private lazy val schemaStr = if (isPostGreSQL) postgreSQLSchemaStr else mySQLSchemaStr  

  private def validateTableSchema = using(connection){conn =>
    using(conn.prepareStatement(schemaStr)){st => 
      using (st.executeQuery) {rs => validateSchema(getColList(rs))(isPostGreSQL)}      
    }
  }  
  
  private def checkH2 = if (dbConfig.dbms != "h2") throw new DBException("Only H2 currently supported for this command")
  
  def updatePassword(newPass:String) = {
    checkH2
    using(connection){conn =>
      using(conn.prepareStatement(Table.getH2PassString(dbConfig.dbuser, newPass))){st => {
         st.executeUpdate
        }
      }
    }
    "Ok"
  }
  // inefficient way to copy
  /**
   * this is inefficient. It inserts one row at a time
   */
  @deprecated def copyTo(dest:DBManager) = copyFromTo(this, dest)
  //////////////////////////////////////////////////////////////////
  // does not support secure methods .. export/import will be encrypted in securedb
  def exportAllToCSV(file:String) = exportToCSV(file, Array())
  def exportToCSV(file:String, wheres:Wheres) = {
    checkH2
    if (new File(file).exists) throw new DBException("dest file should not exist")
    val debugStr = getH2ExportStr(file, wheres)
    using(connection){conn =>
      using(conn.prepareStatement(getH2ExportStr(file, wheres))) { st => 
        setData(getWheresData(wheres), 0, st, debugStr)
        st.executeUpdate
      } 
    }
  }
  def exportToCSV(file:String, cols:Cols, wheres:Wheres) = {
    checkH2
    if (new File(file).exists) throw new DBException("dest file should not exist")
    using(connection){conn =>
      val str = getH2ExportStr(file, cols, wheres)
      using(conn.prepareStatement(str)) { st => 
        val (setCtr, debugStr) = setData(
          getSelectData(cols, wheres, Array()), 0, st, str
        )
        st.executeUpdate
      } 
    }
  }
  def exportToCSV(file:String, aggregates:Aggregates, wheres:Wheres, groupByIntervals:GroupByIntervals, havings:Havings) = {
    checkH2
    if (new File(file).exists) throw new DBException("dest file should not exist")
    using(connection){conn =>
      val str = getH2ExportStr(file, aggregates, wheres, groupByIntervals, havings)
      using(conn.prepareStatement(str)) { st => 
        val (setCtr, debugStr) = setData(
          getAggregateData(aggregates, groupByIntervals, wheres, havings),
          0, st, str)
        st.executeUpdate
      } 
    }
  }
  def importFromCSV(file:String) = {
    checkH2
    using(connection){conn => 
      using(conn.prepareStatement(table.getH2ImportStr(file))) { st => 
        st.executeUpdate
      } 
    }
  }
  //////////////////////////////////////////////////////////////////
  def exportAllEncrypted(file:String, secretKey:String) = exportEncrypted(file, secretKey)
  def exportEncrypted(file:String, secretKey:String, wheres: Where *):Int = exportToEncryptedCSV(this, file, secretKey, wheres.toArray)
  def importEncrypted(file:String, secretKey:String) = importFromEncryptedCSV(this, file, secretKey)
  DBManager.addLoadedDBManager(this, tableID, Thread.currentThread.getStackTrace.toList.take(4).map(_.toString).reduceLeft(_+","+_))
  
  
  
}



   
   
   
   
   
   
   
   
   
   
   
   
   
   



