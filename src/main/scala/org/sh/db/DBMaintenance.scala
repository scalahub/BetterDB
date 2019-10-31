package org.sh.db

import org.sh.utils.file.TraitFilePropertyReader
import org.sh.utils.json.JSONUtil
import org.sh.utils.Util._
import java.io.File
import org.sh.db.config.DBConfigFromFile
import org.sh.db.core.DataStructures._
import org.sh.db.core.Util._
import org.sh.db.ScalaDB._
import org.getopt.util.hash.MurmurHash
import scala.collection.JavaConversions._


case class GroupedBy(dbHost:String, dbms:String, dbname:String) extends JSONUtil.JsonFormatted {
  val keys = Array("dbHost":String, "dbms":String, "dbname":String)
  val vals = Array[Any](dbHost:String, dbms:String, dbname:String)
}
case class DBMgrData(id:String, dbMgr:DBManager) extends JSONUtil.JsonFormatted {
  val tableName = dbMgr.table.tableName
  val keys = Array("id", "tableName", "numRows")
  val numRows = dbMgr.countAllRows
  val vals = Array[Any](id, tableName, numRows)
  val conf = dbMgr.dbConfig
  val getGroupBy = GroupedBy(conf.dbhost, conf.dbms, conf.dbname)
}
case class DBGroup(index:Int, groupedBy:GroupedBy, dbMgrs:Array[DBManager]) extends JSONUtil.JsonFormatted {
  val validateUserIDsPasswords = if (dbMgrs.size == 0) true else {
    val c0 = dbMgrs(0).dbConfig
    dbMgrs.map(_.dbConfig).forall(c => c.dbname == c0.dbname && c.dbpass == c0.dbpass)
  }
  val getGroupSummary = new JSONUtil.JsonFormatted {
    val keys = groupedBy.keys ++ Array("groupID", "validation_passed", "configSrc")
    val vals = groupedBy.vals ++ Array(index, validateUserIDsPasswords, dbMgrs.map(_.dbConfig.configSource).toSet.toArray)
  }
  val tableNames = dbMgrs.map(_.table.tableName)
  val keys = getGroupSummary.keys ++ Array("tables") 
  val vals = getGroupSummary.vals ++ Array[Any](JSONUtil.encodeJSONArray(tableNames)) 
  def updatePassword(oldPassword:String, newPassword:String, checkOldPassword:Boolean) = {    
    if (dbMgrs.size == 0) throw new Exception("Empty dbName group with groupID: "+index)
    if (!validateUserIDsPasswords) throw new Exception("Usrname/password different in group with groupID: "+index)
    if (checkOldPassword && dbMgrs(0).dbConfig.dbpass != oldPassword) throw new Exception("oldPassword does not match: "+index)
    dbMgrs(0).updatePassword(newPassword)
    val configsToUpdate = dbMgrs.map(_.dbConfig).filter(_.configSource.startsWith("file")).toSet
    configsToUpdate.foreach{c =>
      val cProps = c.asInstanceOf[DBConfigFromFile]
      cProps.write("dbpass", newPassword, "changed_by_DBMaintenance_tool", true)
      if (org.sh.utils.Util.debug) println("writing to "+c.configSource)
    }
    configsToUpdate.map(_.configSource).toArray
  }
}
class DBMaintenanceUtil(val objects:Seq[AnyRef]) extends TraitFilePropertyReader {
  val propertyFile = "dbMaintenance.properties"
  val masterPassword = read("masterPassword", "84059809ejdn37yfghfd3dkj87yhfr74fh7yfgnwuhn3y7d")
  val dbGroups = objects
  var autoBackupEnabled = read("autoBackup", false) // earlier was autoBackupEnabled.
  var autoBackupMillis = read("autoBackupTimeMillis", 72000000L) // earlier was autoBackupMillis 72000000 = 1500 mins = 24 hrs
  var autoBackupDirectory = read("autoBackupDirectory", "h2_backup_Doc_DB") // 1800000 = 30 mins 
  var lastBackup = 0L
  
  def getDBMgrs = {
    if (dbGroups.size < 1) throw new Exception("no db groups defined") // dummy access
    // dummy access above, to ensure all dbs are loaded      
    DBManager.loadedDBManagers.toArray.map{
      case (id, db)=> DBMgrData(id, db) 
    }
  }  
  
}
import QueryProfiler._
class DBMaintenance(dbu:DBMaintenanceUtil) { // objects will be accessed to load the DBManagers in them. They just need to be accessed as in the next line
  def this(objects:Seq[AnyRef]) = this(new DBMaintenanceUtil(objects)) // objects will be accessed to load the DBManagers in them. They just need to be accessed as in the next line
  import dbu._
  import DBManager._
  // allow for changing password, migrating db etc
  def getTableIDs = getDBMgrs//.getDBMgrs
  def getTableIDsSorted = getDBMgrs.sortWith{(l, r) =>
    l.dbMgr.getTable.tableName < r.dbMgr.getTable.tableName
  }
  def exportAllToCSV = {
    getDBNameGroupsDetails.flatMap{
      case DBGroup(index:Int, GroupedBy("localhost", "h2", db), dbMgrs:Array[DBManager]) => 
        dbMgrs.map{dbm =>
          val fileName = dbm.tableName+org.sh.utils.Util.randomAlphanumericString(10)
          dbm.exportAllToCSV(fileName)
          dbm.tableName + " exported to "+fileName
        }
      case DBGroup(index:Int, GroupedBy(host, dbms, db), dbMgrs:Array[DBManager]) =>
        throw new DBException(s"Operation not supported for DBMS $dbms and host $host.")      
    }
  }
  def getTableIDsSortedRows = getDBMgrs.map(db => (db, db.numRows)).sortWith{(l, r) =>
    l._2 > r._2
  }.map(_._1)
  
  def autoBackup_get = {
    Array(
      "Auto Backup enabled: "+autoBackupEnabled,
      "Auto Backup directory: "+autoBackupDirectory,
      "Auto Backup time (millis): "+autoBackupMillis
    )
  }
  def autoBackup_set(enabled:Boolean, backupDirectory:Option[String], backupTimeMillis:Option[String]) = {
    val $backupDirectory$ = "None"
    val $enabled$ = "false"
    val $backupTimeMillis$ = "None"
    autoBackupEnabled = enabled
    if (backupDirectory.isDefined) autoBackupDirectory = backupDirectory.get
    if (backupTimeMillis.isDefined) {
      val millis = backupTimeMillis.get.toLong
      if (millis < 300000) throw DBException(s"auto backup millis must be >= 300000 (5 mins)") // 1 min
      autoBackupMillis = millis
    }
    autoBackup_get
  }
  
  def archiveTable(tableID:String, timeColName:String, to:Long, masterPassword:String) = {
    // need to fix below. Takes too much time for larget tables, need to do in steps, 
    if (masterPassword != dbu.masterPassword) throw new Exception("Incorrect master password")    
    val $info$ = "archives table having a 'time' column of ULONG type such that entries with time <= beforeTime are archived (moved to table ending with _archive"
    loadedDBManagers.get(tableID) match {
      case Some(db) => 
        // define archive table
        val t = db.getTable
        t.tableCols.find(col => col.name.toUpperCase == timeColName.toUpperCase && col.colType == ULONG) match {
          case Some(col) =>
            val numRowsAffected = db.countWhere(col <= to)
            if (numRowsAffected > 20000) throw DBException(s"Num selected rows ($numRowsAffected) > 20000. Select lower time")
            val archiveDB = DBManager(t.tableName+"_archived")(t.tableCols:_*)()(db.dbConfig)
            val numSelectedRows = db.selectStar.where(col <= to).into(archiveDB)
            val numDeletedRows = db.deleteWhere(col <= to)
            if (numSelectedRows != numDeletedRows) throw DBException(s"Num selected rows ($numSelectedRows) != Num deleted rows ($numDeletedRows). Archive table untouched: $archiveDB")
            numSelectedRows
          case _ => throw DBException(s"No such col $timeColName of type ULONG")
        } 
      case _ => throw DBException("table not found for ID: "+tableID)      
    }  
  }
  
  def backupLoadedDBMgrs(dir:String):Unit = {
    val dirFile = new File(dir)
    if (dirFile.isFile) throw new DBException("[dir] is a file")
    dirFile.mkdir
    if (!dirFile.isDirectory) throw new DBException("[dir] not found")
    
    val time = getTime
    getDBNameGroupsDetails.map{
      case DBGroup(index:Int, GroupedBy("localhost", "h2", db), dbMgrs:Array[DBManager]) => 
        val randomTime = org.sh.utils.Util.rand.abs % OneHour
        // don't do immediately, do after some time
        doOnce(
          { // autoh2_<date time>_<name>_<epoch>
            val start = getTime
            val humanTime = toDateString(time).replace(':', '_').replace(' ', '_')
            val fileName = s"${dir}/autoh2_${humanTime}_${db}_${time}.zip" // dir+"/"+db+"_"+time+".zip"        
            val sqlString = "BACKUP TO ?"
            if (printSQL_?) println("BACKUP query SQL [?]:\n  "+sqlString)
            usingProfiler(sqlString){
              using(dbMgrs(0).connection.prepareStatement(sqlString)){st =>           
                val (_, str) = dbMgrs(0).setData(Array((fileName, VARCHAR(255))), 0, st, sqlString)            
                if (printSQL) println("BACKUP query SQL [R]:\n  "+str)
                st.executeUpdate
              }
            }
            val backTimeMillis = getTime-start
            new JSONUtil.JsonFormatted{
              val keys = Array("db", "backupTimeMillis", "numTables", "tables", "fileName", "dir", "startTime", "humanStartTime")
              val vals = Array(db, backTimeMillis, dbMgrs.size, dbMgrs.map(_.getTable.tableName), fileName, dir, time, toDateString(time))
            }
          }, 
          randomTime
        )
      case any => throw new DBException("unsupported backup config: "+any)
    }
  }

  def getDuplicateTableNames = getTableIDs.groupBy(_.tableName.toLowerCase).filter{_._2.size > 1}.values.toArray.map(_.toList)
  def getInitStackTrace(tableID:String) = loadedDBManagersTrace.get(tableID).getOrElse(throw DBException("no such tableID trace: "+tableID)).split(",")
  def getCreateStrings = getDBMgrs.map(_.dbMgr.createString)
  def getCreateString(tableID:String) = getTable(tableID).createSQLString(false)
  def getDBNameGroups = getDBNameGroupsDetails.map(_.getGroupSummary)
  def getDBNameGroupsDetails = {
    val dbMgrs = getDBMgrs
    dbMgrs.groupBy(_.getGroupBy).toArray.zipWithIndex.map{
      case ((groupBy, dbs), i) => DBGroup(i, groupBy, dbs.map(_.dbMgr))
    }
  }
  def changePassword(groupID:Int, oldPassword:String, newPassword:String) = {    
    val groups = getDBNameGroupsDetails
    if (groupID < groups.size && groupID >= 0) {    
      groups(groupID).updatePassword(oldPassword, newPassword, true)
    } else throw new Exception("Invalid groupID: "+groupID)
  }
  def resetPassword(groupID:Int, masterPassword:String, newPassword:String) = {    
    val groups = getDBNameGroupsDetails
    if (masterPassword != dbu.masterPassword) throw new Exception("Incorrect master password")    
    if (groupID < groups.size && groupID >= 0) {    
      groups(groupID).updatePassword("", newPassword, false)
    } else throw new Exception("Invalid groupID: "+groupID)
  }
  // also validate that userNames and passwords across same group are same
  def getDB(tableID:String) = loadedDBManagers.get(tableID) match {
    case Some(dbm) => dbm
    case _ => throw DBException("table not found for ID: "+tableID)      
  }  
  def getConfig(tableID:String) = {
    val db = getDB(tableID)
    import db.dbConfig._
    new JSONUtil.JsonFormatted {
      val keys = Array  ("host", "dbname", "dbms", "userID")
      val vals = Array[Any](dbhost, dbname, dbms, dbuser)
    }
  }

  def getTable(tableID:String) = getDB(tableID).table
  def countAllRows(tableID:String) = getDB(tableID).countAllRows
  def getAllRows(tableID:String) = {
    val db = getDB(tableID)
    val cols = db.table.tableCols
    val ordering = cols.find(_.name.toLowerCase == "time") match {
      case Some(col) => Array(Ordering(col, Decreasing))
      case _ => Array[Ordering]()
    }    
    getRows(
      cols, 
      db.select(cols).orderBy(ordering).asList
    )
  }  
  
  def getWhere(tableID:String, colName:String, operation:String, data:String) = {
    
    val $info$ = "This tests whether a where clause is valid or not. Does not return any data."
    val table = getTable(tableID)    
    table.tableCols.find(_.name.toLowerCase == colName.toLowerCase) match {
      case Some(col) => Where(col, getOp(operation), castData(col.colType, data))
      case None => throw DBException("column not found: "+colName)
    }
  }
  def getAggregate(tableID:String, colName:String, aggregate:String) = {
    val table = getTable(tableID)    
    table.tableCols.find(_.name.toLowerCase == colName.toLowerCase) match {
      case Some(col) => Aggregate(col, getAggr(aggregate))
      case None => throw DBException("column not found: "+colName)
    }
  }
  def getAvailableOps = allOps
  def getAvailableAggregates = allAggrs
  def getAggregatedColFiltered(tableID:String, aggregateCol:String, aggregate:String, whereColName:String, whereOp:String, whereData:String) = {
    val wheres = Array(getWhere(tableID:String, whereColName:String, whereOp:String, whereData:String))
    val aggr = Array(getAggregate(tableID, aggregateCol, aggregate))
    val db = getDB(tableID)
    db.aggregateLong(aggr, wheres)(0)
  }    
  def getAggregatedCol(tableID:String, aggregateCol:String, aggregate:String) = {
    val aggr = Array(getAggregate(tableID, aggregateCol, aggregate))
    val db = getDB(tableID)
    db.aggregateLong(aggr, Array())(0)
  }    
  @deprecated("use getRowsFor", "25 Sept 2017")
  def getRowsWhere(tableID:String, colName:String, operation:String, data:String) = {
    val wheres = Array(getWhere(tableID:String, colName:String, operation:String, data:String))    
    val db = getDB(tableID)
    getRows(db.table.tableCols, db.selectStar.where(wheres: _*).asList)      
  }    
  def getRowsFor(tableID:String, colName:String, operation:String, data:String, max:Int, offset:Long) = {
    val wheres = Array(getWhere(tableID:String, colName:String, operation:String, data:String))    
    val db = getDB(tableID)
    getRows(db.table.tableCols, db.selectStar.where(wheres: _*).max(max).offset(offset).asList)      
  }    
  def countRowsWhere(tableID:String, colName:String, operation:String, data:String) = getDB(tableID).countRows(getWhere(tableID:String, colName:String, operation:String, data:String))
  
  def deleteRowsWhere(tableID:String, colName:String, operation:String, data:String, masterPassword:String) = {
    if (masterPassword != dbu.masterPassword) throw new Exception("Incorrect master password")    
    val wheres = Array(getWhere(tableID:String, colName:String, operation:String, data:String))
    getDB(tableID).delete(wheres)
  }      
  doRegularly(
    {
      if (autoBackupEnabled) {
        val start = getTime
        if ((start - lastBackup) > autoBackupMillis) {
          if (debug) println (" [INFO DB_AUTO_BACKUP STARTED]")
          tryIt{backupLoadedDBMgrs(autoBackupDirectory)}
          val end = getTime
          //if (debug) 
          println (s" [INFO DB_AUTO_BACKUP ENDED] Time elapsed: ${(end - start)/1000d} s ")
          lastBackup = end
        }
      }
    },
    OneHour
  )
  def getTablesWithCol(colName:String, colType:String) = {
    getDBMgrs.filter{db =>
      db.dbMgr.tableCols.exists{col => 
        col.name.toUpperCase == colName.toUpperCase &&
        col.colType.getClass.getSimpleName.startsWith(colType.toUpperCase) // dodgy way to check
      }
    }.map(db => db.tableName+"  ("+db.dbMgr.colNames.reduceLeft(_+","+_)+")")
  }                            
  def getTablesWithoutCol(colName:String, colType:String) = {
    getDBMgrs.filterNot{db =>
      db.dbMgr.tableCols.exists{col =>
        col.name.toUpperCase == colName.toUpperCase &&
        col.colType.getClass.getSimpleName.startsWith(colType.toUpperCase) // dodgy way to check
      }
    }.map(db => db.tableName+ "  ("+db.dbMgr.colNames.reduceLeft(_+","+_)+")")
  }
  
  def queryProfiler_setOptions(enabled:Boolean, purgeData:Boolean) = {
    val $enabled$ = "true"
    val $purgeData$ = "false"
    profileSQL = enabled
    if (purgeData) queryLatency.synchronized{queryLatency.clear}     
  }
  def queryProfiler_isRunning = profileSQL

  def queryProfiler_getQueries(
    sortBySlowest:Boolean, 
    sortByCount:Boolean,
    max:Int, 
    showTrace:Boolean
  ):Array[String] = {
    val $info$ = """
If both are true, 'sortBySlowest' takes precedence over 'sortByCount'. 
If both are false then it will be sorted by recent"""
    val $sortBySlowest$ = "true"
    val $sortByCount$ = "false"
    val $showTrace$ = "false"
    queryLatency.toArray.sortBy{
      case (hash, (maxLatency, _, _, _, _)) if (sortBySlowest) => -maxLatency
      case (hash, (_, _, _, _, count)) if (sortByCount) => -count
      case (hash, (_, _, _, lastTime, _)) => -lastTime
    }.take(max).map{
      case (hash, (maxLatency, maxLatencyTime, lastLatency, lastTime, count)) =>
        queryProfiler_getQuery(hash) ++
        Array(
          s"Number of queries: $count",
          s"Max latency: $maxLatency ms",
          s"Time at max latency: ${toDateString(maxLatencyTime)} ($maxLatencyTime)",
          s"Last latency: $lastLatency ms",
          s"Time of last query: ${toDateString(lastTime)} ($lastTime)"
        ) ++ 
        (if (showTrace) queryProfiler_getLatestTrace(hash) else Array[String]()) ++ Array("","")
    }.flatten
  }
  
  def queryProfiler_getLatestTrace(hash:Hash):Array[String] = {
    queryTraces.get(hash).getOrElse(Array()).map(_.toString).filterNot{x =>
      x.startsWith("java") || 
      x.startsWith("akka") ||
      x.startsWith("scala") ||
      x.startsWith("play") ||
      x.startsWith("com") ||
      x.startsWith("mux") ||
      x.startsWith("org") ||
      x.startsWith("sun") 
    }
  }
}

object QueryProfiler {
  def queryProfiler_getQuery(hash:Hash) = hashMap.get(hash).toArray
  import scala.collection.mutable.{Map => MMap}
  
  type Hash = Int
  
  val hashMap = MMap[Hash, String]()  // hash -> SQL string
  
  type MaxLatency = Long
  type MaxLatencyTime = Long
  type LastLatency = Long
  type LastTime = Long
  type QueryCount = Int
  
  val queryLatency = MMap[Hash, (MaxLatency, MaxLatencyTime, LastLatency, LastTime, QueryCount)]().withDefaultValue((0, 0, 0, 0, 0))  // hash -> longestLatency, time_of_latency
  
  val queryTraces = MMap[Hash, Array[StackTraceElement]]() // hash -> trace

  def addHash(hash:Hash, sql:String) = hashMap += (hash -> sql)
  
  def updateLatency(hash:Hash, currentLatency:Long, currentStartTime:Long) = {    
    val (newMaxLatency, newMaxLatencyTime, newCount) = 
      queryLatency.get(hash) match {
      case Some((maxLatency, maxLatencyTime, _, _, oldCount)) =>
        val newCount = oldCount+1
        if (currentLatency < maxLatency)           
          (maxLatency, maxLatencyTime, newCount) 
        else 
          (currentLatency, currentStartTime, newCount)
      case _ =>
        (currentLatency, currentStartTime, 1)
    }
    queryLatency += (hash -> ((newMaxLatency, newMaxLatencyTime, currentLatency, currentStartTime, newCount)))    
  }
  def updateTrace(hash:Hash, trace:Array[StackTraceElement]) = queryTraces += (hash -> trace)


  val seed = rand
  def usingProfiler[T](sql:String)(f: => T):T = {    
    if (profileSQL) {
      val hash = MurmurHash.hash(sql.getBytes, seed)
      addHash(hash, sql)
      updateTrace(hash, Thread.currentThread.getStackTrace)
      val startTime = getTime
      val result = f
      val latency = getTime - startTime
      updateLatency(hash, latency, startTime)
      result
    } else f
  }
}
////////////////////////////////











