package org.sh.db.core

import DataStructures._
import Util._
import org.sh.db.core.FKDataStructures._
import org.sh.utils.common.Util._

case class Table(tableName:String, tableCols:Cols, priKey:Cols) {
  import Table._
  if (Col.reservedNames.contains(tableName.toUpperCase)) throw DBException(s"Table name '$tableName' is a reserved word")

  val colNames = tableCols.map(_.name.toUpperCase) 
  val priKeyColNames = priKey.map(_.name.toUpperCase) 
  if (colNames.distinct.size != colNames.size) throw new DBException(s"Columns contain duplicates in table $tableName. Rename some cols and retry.")
  if (priKeyColNames.distinct.size != priKeyColNames.size) throw new DBException("Primary keys contain duplicates.")

  if (!priKeyColNames.forall(colNames.contains)) throw new DBException(s"One or more primary keys not present in table $tableName.")
  val autoIncrCols = tableCols.filter(_.colType == ULONGAuto)
  if (autoIncrCols.size > 1) throw new DBException(s"At most one column can be Auto-Increment. Currently: ${autoIncrCols.size} in table $tableName")

  val autoIncrementCol = autoIncrCols.headOption
  
  def this(tableName:String, col:Col, priKey:Col) = this(tableName, Array(col), Array(priKey))

  override def toString = tableName

  def assertColExists(c:Col) = c.compositeCols.foreach{col =>
    if (!containsCol(col)) throw DBException("Table ["+col.optTable.getOrElse(tableName)+"] does not have column: "+col.name)
  }
  private def containsCol(anyCol:Col) = {
    //if (printSQL) println(" ----> "+anyCol+" ---> "+anyCol.colType+" ---> "+anyCol.isComposite)
    anyCol.colType match {
      case ULONGCOMPOSITE|CASTTYPE(_) => true
      case CONST(_, _) => true
      case _ => anyCol.optTable.getOrElse(this).tableCols.contains(anyCol.simpleCol)
    }    
  }
  
  // postGre needs special treatment. Later move this to DBManager
  protected [db] def createSQLString(isPostGreSQL:Boolean) = "CREATE TABLE IF NOT EXISTS "+tableName+
    " ("+tableCols.map(f => f.name+" "+colTypeString(f.colType)(isPostGreSQL)).reduceLeft(_+", "+_)+priKeyString+checkString+")"
  
  private def checkString = tableCols.filter(
    _.colType match {
      case ULONG(_) | UINT(_) | UBIGDEC(_, _) | UScalaBIGINT(_) => true
      case _ => false
    }
  ) match {
    case f if f.size > 0 => ","+f.map("CHECK("+_.name+" >= 0)").reduceLeft(_+","+_) 
    case _ => ""
  }
  private def priKeyString = if (priKey == null || priKey.size == 0) "" else ", PRIMARY KEY ("+priKey.map(_.name).reduceLeft(_+", "+_)+")"

  def insertSQLString(isPostGreSQL:Boolean) = {
    val (actualCols, qMark) = tableCols.collect{
      case Col(name, ULONGAuto, _) => None //(name, "?")
      case Col(name, _, _) => Some((name, "?"))
    }.flatten.unzip
    "INSERT INTO "+tableName+"("+actualCols.reduceLeft{_+","+_}+")"+
    " VALUES("+ qMark.reduceLeft(_+", "+_)+ ")" + (if (isPostGreSQL && autoIncrementCol.isDefined) " RETURNING "+autoIncrementCol.get else "")
  }
  @deprecated("Below was without auto increment/default", "22 Nov 2017")
  def insertSQLStringOrig = "INSERT INTO "+tableName+" VALUES("+ tableCols.map(_ => "?").reduceLeft(_+", "+_)+ ")"
  
  def dropSQLString = "DROP TABLE "+tableName
  def deleteSQLString(search:Wheres) = {
    // prevent "FROM" clause at root level:
    /*
      db.deleteWhere(sal in nested) // works
        DELETE FROM USERS WHERE USERS.SAL IN (SELECT USERS.SAL AS MsGnzNZXKV FROM USERS LIMIT 10)
     
      db.deleteWhere(sal === nested.max(1)) // works
        DELETE FROM USERS WHERE USERS.SAL = (SELECT USERS.SAL AS MsGnzNZXKV FROM USERS LIMIT 1)
        
      db.deleteWhere(sal === sel2)  // works
        DELETE FROM USERS WHERE USERS.SAL = (SELECT USERS.SAL AS MsGnzNZXKV FROM (SELECT USERS.SAL AS MsGnzNZXKV FROM (SELECT USERS.SAL AS MsGnzNZXKV FROM USERS WHERE USERS.SAL > ? and USERS.SAL <> ? ORDER BY USERS.SAL DESC LIMIT 3 OFFSET 4) AS oUTjxgSYUe,USERS WHERE USERS.SAL > ? and USERS.SAL = oUTjxgSYUe.MsGnzNZXKV ORDER BY USERS.SAL DESC LIMIT 6 OFFSET 7) AS uDgOzEpdRr,USERS WHERE USERS.SAL > ? and USERS.SAL = uDgOzEpdRr.MsGnzNZXKV ORDER BY USERS.SAL DESC LIMIT 9 OFFSET 10)

      db.deleteWhere(sal === db.select(sal).where(sal from nested))
        DELETE FROM USERS WHERE USERS.SAL = (SELECT USERS.SAL AS MsGnzNZXKV FROM (SELECT USERS.SAL AS MsGnzNZXKV FROM USERS LIMIT 10) AS bpwTZXqUTT,USERS WHERE USERS.SAL = bpwTZXqUTT.MsGnzNZXKV LIMIT 2147483647)
     
      db.deleteWhere(sal from nested) // not working ! 
        DELETE FROM USERS WHERE USERS.SAL = bpwTZXqUTT.MsGnzNZXKV [42122-191]  // ERROR!!!
        
NOTE: select below
      db.select(sal).where(sal from nested).execute
        SELECT USERS.SAL AS MsGnzNZXKV FROM (SELECT USERS.SAL AS MsGnzNZXKV FROM USERS LIMIT 10) AS bpwTZXqUTT,USERS WHERE USERS.SAL = bpwTZXqUTT.MsGnzNZXKV LIMIT 2147483647
    */
    search.foreach{ where => // see comment above
      where.op match {
        case From => throw DBException(
            s"""Delete clause cannot use a Nested query in 'FROM'. Use operators === or 'in'. For example:
Instead of: 
  db.deleteWhere(col from nested), 
use
  db.deleteWhere(col === nested) or
  db.deleteWhere(col === db.select(col from nested))            
"""
          )
        case _ => // ok
      }        
    }
       
    "DELETE FROM "+tableName+whereString(search, "WHERE", "and")
  }

  // selecting rows; 
  def selectSQLString(selectCols:Cols, wheres:Wheres)(implicit orderings:Orderings=Array(), limit:Int = 0, offset:Long = 0) = {
    "SELECT "+getSelectString(selectCols)+
    " FROM "+getTableNames(selectCols, wheres)+whereString(wheres, "WHERE", "and")+getOrderLimitSQLString(orderings,limit, offset)
  }
  def insertIntoSQLString(table:Table, selectCols:Cols, wheres:Wheres)(implicit orderings:Orderings=Array(), limit:Int = 0, offset:Long = 0) = {
    "INSERT INTO "+table.tableName+" "+selectSQLString(selectCols:Cols, wheres:Wheres)
  }
  def insertIntoSQLString(table:Table, aggregates:Aggregates, wheres:Wheres, groupByIntervals:GroupByIntervals, havings:Havings) = {
    "INSERT INTO "+table.tableName+" "+aggregateSQLString(aggregates, wheres, groupByIntervals, havings)
  }

  
  def getTableNames(selectCols:Cols, wheres:Wheres) = {
    val selectTables = selectCols.flatMap(_.compositeTableNames).toSet
    
    val whereTables = wheres.flatMap(_.col.compositeTableNames).toSet 
    val dataTables = wheres.collect{
      case Where(_, _, col:Col) => col.compositeTableNames
    }.flatten.toSet

    val allTables =   wheres.flatMap(_.nestedWhereTables) ++ (selectTables ++ whereTables ++ dataTables + tableName) // add this table name too (last item)
    allTables.reduceLeft(_+","+_)
  }
  // counting rows
  def countSQLString(wheres:Wheres) = "SELECT COUNT(*) FROM "+ getTableNames(Array(), wheres) + whereString(wheres, "WHERE", "and") 
  
  /**
   * in the following the toOp gives the method of update for the cols (to be applied on update:Cols below). 
   * For example in normal update 
   *  UPDATE Foo SET bar = "123" WHERE baz = "43"  
   * the other way this can be used is for increment (See incrementString below)   
   *  UPDATE Foo SET bar = bar + 123" WHERE baz = "34"
   * 
   * this op will be different in different cases
   */
  def updateSQLString[T](wheres:Wheres, updates:Updates[T])(implicit toWhere:Update[_] => Where = upd => Where(upd.col, Eq, upd.data)) = {  ///////// TO CHECK IF NOTIMPL IS EVER CALLED
    //    assert(updates.size > 0)
    //    assert(wheres.size >= 0)
    if (updates.size < 1) throw new Exception("Updates size must be >= 1")
    //assert(wheres.size >= 0)
    "UPDATE "+tableName+whereString(updates.map(toWhere), "SET", ",")+whereString(wheres, "WHERE", "and")
  }
  
  ////////////////////////////////////////
  // aggregates 
  ////////////////////////////////////////
  private def aggregateStringNew(aggregates: Aggregates) = {
    // skipping Top as its a syntactic sugar added by our library and unsupported natively
    aggregates.flatMap {
      case Aggregate(_, Top(_)|GroupBy) => None // GroupBy should not generate SQL query
      case x => Some(x.aggrSQLString+" as "+x.alias)
    }
  }  
  private def aggregateStringOld(aggregates: Aggregates) = {
    // skipping Top as its a syntactic sugar added by our library and unsupported natively
    assert(aggregates.size > 0, "aggregate ops must be on one or more cols")
    val aggrsToUse = aggregates.filter{
      case Aggregate(_, Top(_)|GroupBy) => false // GroupBy should not generate SQL query
      case _ => true 
    }.map{x => 
      x.aggrSQLString+" as "+x.alias
    }
    if (aggrsToUse.isEmpty) "" else aggrsToUse.reduceLeft(_+","+_)
  }  
  // following only works for MySql, (SUBSTRING_INDEX is not supported in h2)
  //  private def getAggrOpStr(aggr:Aggr, name:String) = {
  //    val str = aggr+"("+name+")"
  //    aggr match {
  //      case First => "SUBSTRING_INDEX("+str+"',',1)"
  //      case Last => "SUBSTRING_INDEX("+str+"',',-1)"
  //      case _ => str 
  //    }
  //  }
  
  
  ////////////////////////////////////////
  // aggregates with intervals (find avg(age) in each age_group (0-10, 10-20, 20-30, 30-40)... 
  // intervals need to be equal!
  // intervals can only be applied to Numeric type (INT, LONG, ULONG, UINT). (U)ScalaBIGINT is not supported.
  //  uses the idea from http://stackoverflow.com/a/197300/243233
  ////////////////////////////////////////
  
  def aggregateSQLString(aggregates:Aggregates, wheres:Wheres, groupByIntervals:GroupByIntervals, havings:Havings) = {
    //"SELECT "+getOpStr(select)+intervalString(groupByIntervals)+" FROM "+tableName+ 
    val tableNames = getTableNames(aggregates.map(_.col), wheres)

    //SELECT MAX(USERS.SAL) as LjkkudMSTzMAX,((USERS.AGE + USERS.SAL) + 4) as uOzFPSGTyfinterval0 FROM USERS WHERE USERS.SAL > 10000 GROUP BY uOzFPSGTyfinterval0
    
    // MAX(USERS.SAL) as LjkkudMSTzMAX    
    val selects = 
      aggregateStringNew(aggregates) ++ // MAX(USERS.SAL) as LjkkudMSTzMAX    
      intervalStringNew(groupByIntervals)  // ((USERS.AGE + USERS.SAL) + 4) as uOzFPSGTyfinterval0

    if (selects.isEmpty) throw DBException("At least one column must be selected")
    "SELECT "+
    selects.reduceLeft(_ +","+_)+ // MAX(USERS.SAL) as LjkkudMSTzMAX
    " FROM "+
    tableNames+ // USERS
    whereString(wheres, "WHERE", "and")+ // USERS.SAL > 10000
    groupByString(groupByIntervals)+ // GROUP BY uOzFPSGTyfinterval0
    havingString(havings)
  }
  private def intervalStringNew(groupByIntervals:GroupByIntervals) =  
    groupByIntervals.map{
      case g if g.interval != 0 => 
        g.interval+"*round("+g.col.colSQLString+"/"+g.interval+".0, 0) as "+g.alias
      case g => 
        g.col.colSQLString+" as "+g.alias
    }
  private def intervalStringOld(groupByIntervals:GroupByIntervals) =  
    groupByIntervals.map{g => 
      (
        if (g.interval != 0) 
          g.interval+"*round("+g.col.colSQLString+"/"+g.interval+".0, 0)"
        else g.col.colSQLString // ((USERS.AGE + USERS.SAL) + 4)
      )+" as "+g.alias //  as uOzFPSGTyfinterval0
    }.reduceLeft(_+","+_)
  private def groupByString(groupByIntervals:GroupByIntervals) = 
    if (groupByIntervals.size == 0) "" else " GROUP BY "+groupByIntervals.map(g => g.alias).reduceLeft(_+","+_)
  
  private def havingString(havings:Havings) = 
    if (havings.size == 0) "" else " HAVING "+havings.map(h => h.havingSQLString).reduceLeft(_+" AND "+_)
  
  ////////////////////////////////////////
  // following is used for incrementing a single col
  ////////////////////////////////////////
  def incrementColsString(wheres:Wheres, increments:Increments) = 
    updateSQLString(wheres, increments)(increment => Where(increment.col, IncrOp(increment), increment.data)) 

  lazy val postgreSQLSchemaStr = getPostgreSQLSchemaStr(tableName)
  lazy val mySQLSchemaStr = getMySQLSchemaStr(tableName)
  
  def validateSchema(colList:List[(String, List[String])])(isPostGreSQL:Boolean) = 
    validateTableSchema(colList, tableCols, priKey, tableName)(isPostGreSQL)
  /**
   * http://stackoverflow.com/a/2499396/243233
   * http://stackoverflow.com/a/16792904/243233
   * http://stackoverflow.com/a/14072931/243233
   */
  
  ////////////////////// Foreign Key constraints //////////////////////////
  def getFKConstraintStr(link:Link) = "SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS WHERE "+
    "CONSTRAINT_NAME = '"+getFKConstraintName(link)+"'"
  def getFKConstraintStrPostgreSQL(link:Link) = "SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE "+
    "CONSTRAINT_NAME = '"+getFKConstraintName(link)+"'"
  def getFKConstraintName(link:Link) = ("fk_"+shaSmall(tableName+"_"+
                                                       link.pkTable+"_"+
                                                       link.pkCols.map(_.colSQLString).reduceLeft(_+"_"+_)+"_"+
                                                       link.fkCols.map(_.colSQLString).reduceLeft(_+"_"+_))).toUpperCase
  def dropFkLinkString(link:Link) = 
    "ALTER TABLE "+tableName+
    " DROP CONSTRAINT "+getFKConstraintName(link)
  
  def createFkLinkString(link:Link) = 
    "ALTER TABLE "+tableName+
    " ADD CONSTRAINT "+getFKConstraintName(link)+
    " FOREIGN KEY ("+link.fkCols.map(_.colSQLString).reduceLeft(_+","+_)+") REFERENCES "+
    link.pkTable.tableName+"("+link.pkCols.map(_.colSQLString).reduceLeft(_+","+_)+") ON DELETE "+link.rule.onDelete+" ON UPDATE "+link.rule.onUpdate  
  ///////////////////////////////////////////////////////////////////
  
  
  
  ////////////////////// INDEX constraints //////////////////////////

  def createIndexString(cols:Cols) = {
    "CREATE INDEX IF NOT EXISTS "+getIndexName(cols:Cols)+" ON "+tableName+" ("+cols.map(_.colSQLString).reduceLeft(_+","+_)+")"
  }
  def dropIndexString(cols:Cols) = {
    "DROP INDEX IF EXISTS "+getIndexName(cols:Cols)
  }
  
  def getIndexName(cols:Cols) = {
    if (cols.isEmpty) throw DBException("At least one column is needed for index")
    val name = shaSmall(tableName+"_"+cols.map(_.alias).reduceLeft(_+_))
    name
  }
  
  def getH2ExportStr(file:String, aggregates:Aggregates, wheres:Wheres, groupByIntervals:GroupByIntervals, havings:Havings) = 
    "CALL CSVWRITE('"+file+"', '"+aggregateSQLString(aggregates, wheres, groupByIntervals, havings)+"')"
  def getH2ExportStr(file:String, cols:Cols, wheres:Wheres) = "CALL CSVWRITE('"+file+"', '"+selectSQLString(cols, wheres)+"')"
  def getH2ExportStr(file:String, wheres:Wheres):String = getH2ExportStr(file, Array(), wheres)
  def getH2ImportStr(file:String) = "INSERT INTO "+tableName+" SELECT * FROM CSVREAD('"+file+"')"
    
}
object Table {
  
  def apply(tableName:String, col:Col, priKey:Col) = new Table(tableName, Array(col), Array(priKey))
  def apply(tableName:String, tableCols:Cols) = new Table(tableName, tableCols, Array[Col]())
  def apply(tableName:String, tableCols:Col*):Table = apply(tableName, tableCols.toArray)

  def createPostGreBlob = "CREATE DOMAIN BLOB as BYTEA"
  private def getOrdering(isDescending:Boolean) = if (isDescending) " DESC" else ""
  private def getOrderStr(orderings:Orderings) = {
    assert(orderings.size > 0)      // "ORDER BY supplier_city DESC, supplier_state ASC;"
    " ORDER BY "+orderings.map{
      x => x.col.colSQLString+getOrdering(x.isDescending)
    }.reduceLeft(_+","+_)
  }
  private def getOrderLimitSQLString(orderings:Orderings, limit:Int, offset:Long) = {
    (if (orderings.size == 0) "" else getOrderStr(orderings)) + 
    (if (limit == 0) "" else " LIMIT "+limit) +
    (if (offset == 0) "" else " OFFSET "+offset)
  }
  private def whereString(wheres:Wheres, str:String, joinOp:String) = if (wheres.size == 0) "" else {
     " "+str+" "+wheres.map{_.whereSQLString}.reduceLeft(_+" "+joinOp+" "+_)
  }

  private def getSelectString(cols:Cols) = if (cols.size == 0) "*" else cols.map(col => col.colSQLString +" AS "+col.alias).reduceLeft(_+","+_)
  def getPostgreSQLSchemaStr(tableName:String) = 
    """SELECT c.COLUMN_NAME as COLUMN_NAME, c.DATA_TYPE as TYPE, 
          CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PRI' ELSE '' END AS KEY
          FROM INFORMATION_SCHEMA.COLUMNS c 
          LEFT JOIN (
                      SELECT ku.TABLE_CATALOG,ku.TABLE_SCHEMA,ku.TABLE_NAME,ku.COLUMN_NAME
                      FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                      INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                          ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                          AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                   )   pk 
          ON  c.TABLE_CATALOG = pk.TABLE_CATALOG
                      AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
                      AND c.TABLE_NAME = pk.TABLE_NAME
                      AND c.COLUMN_NAME = pk.COLUMN_NAME
          WHERE c.TABLE_NAME='"""+tableName.toLowerCase+"'";

  def getMySQLSchemaStr(tableName:String) = "SHOW COLUMNS FROM "+tableName.toLowerCase

  @deprecated("To create alternate using the method 'extractTableSchema' in 'Util' object", "14 Sep 2017")
  private def validateTableSchema(colMap:List[(String, List[String])], cols:Cols, priKeys:Cols, tableName:String)(isPostGreSQL:Boolean) = {
    def sameType(t:String, col:Col) = (t, colTypeString(col.colType)(isPostGreSQL)) match {
      case (t1, t2) if (t1 == t2) => true
      case (t1, t2) if (t1.startsWith("BOOLEAN") && t2 == "BOOLEAN") => true
      case (t1, t2) if (t1.startsWith("BIGINT") && t2.startsWith("BIGINT")) => true
      case (t1, t2) if (t1.startsWith("CHARACTER VARYING") && t2.startsWith("VARCHAR")) => true // for postgreSQL
      case (t1, t2) if (t1 == "VARCHAR(2147483647)" && t2 == "VARCHAR") => true // for h2 (possibly MySQL)
      case (t1, t2) if (t1.startsWith("INTEGER") && t2.startsWith("INT")) => true // for postgreSQL
      case (t1, t2) if (t1.startsWith("DECIMAL") && t2.startsWith("DECIMAL")) => true // for h2
      case (t1, t2) if (t1.startsWith("NUMERIC") && t2.startsWith("NUMERIC")) => true // for postgreSQL
      case (t1, t2) if (t1.startsWith("BIGINT") && t2.startsWith("BIGSERIAL")) => true // for postgreSQL
      case (t1, t2) if (t1.startsWith("TIMESTAMP") && t2.startsWith("TIMESTAMP")) => true // for h2
      case (t1, t2) if (t1.startsWith("BLOB") && t2.startsWith("BLOB")) => true 
      case (t1, t2) if (t1.startsWith("BYTEA") && t2.startsWith("BLOB")) => true 
      case (t1, t2) => println(" mismatch: existing["+t1+"], needed["+t2+"]"); false    
    }
    val indices = cols.indices
    val names = colMap.find(_._1 == "COLUMN_NAME").get._2
    val types = colMap.find(_._1 == "TYPE").get._2
    val keys = colMap.find(_._1 == "KEY").get._2 zip names filter (_._1 == "PRI") map (_._2)
    val keyMatch = keys.size == priKeys.size && priKeys.forall(x => keys.contains(x.name.toUpperCase))
    if (!keyMatch) throw DBException("Table schema mismatch: (primary key) for table: "+tableName+". Found ["+keys+"]. Required ["+priKeys.toList+"]")
    val tmpMap = (names zip types) map (x => (x._1, x._2, cols.find(_.name.toUpperCase == x._1)))
    val nameMatch = tmpMap.forall(_._3.isDefined)
    if (!nameMatch) throw DBException("Table schema mismatch: (name) for table: "+tableName+". Found ["+names+"]. Required ["+cols.toList+"]")
    val typeMatch = nameMatch && tmpMap.forall(x => sameType(x._2, x._3.get))
    if (!typeMatch) throw DBException("Table schema mismatch: (types) for table: "+tableName+". Found ["+types+"]. Required ["+cols.toList.map(x => colTypeString(x.colType)(isPostGreSQL))+"]") 
    val numberMatch =  tmpMap.size == cols.size
    if (!numberMatch) throw DBException("Table schema mismatch: (wrong No. of columns) for table: "+tableName+". Found["+tmpMap.size+"], ["+names+"]. Required["+cols.size+"]:["+cols.toList.map(_.name)+"]")
  }
  def getH2PassString(user:String, pass:String) = "ALTER USER "+user+" SET PASSWORD '"+pass+"'"
  
}
