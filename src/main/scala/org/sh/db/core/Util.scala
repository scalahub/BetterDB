package org.sh.db.core

import DataStructures._
import org.sh.utils.file.TraitFilePropertyReader
import org.sh.utils.json.JSONUtil.JsonFormatted
import java.sql.SQLException
import java.sql.Timestamp
import java.util.Date
import org.sh.db.DBManager
import org.bouncycastle.util.encoders.Base64
/** Util is used for translating between SQL data types and Scala data types. The following rules are used
 *
 *    SQL Col element type                        corresponding Scala subtype (of Any)
 *    ----------------------------------------------------------------------------------
 *    INT                                           Int
 *    BIGINT                                        Long
 *    BIGINT UNSIGNED                               ULong
 *    VARCHAR                                       String
 *    VARBINARY                                     Array[Byte]
 *    TIMESTAMP                                     String
 *    BLOB                                          Array[Byte]
 *
 */
object Util extends TraitFilePropertyReader {
  val propertyFile = "commonDB.properties" // common to all DBManagers ...
  var printSQL = read("printSQL", false)
  var printSQL_? = read("printSQL_?", false) // prints ? instead of variables (parametrized SQL)
  var profileSQL = read("profileSQL", false) 
  
  import java.sql.{ResultSet, PreparedStatement}
  protected [db] def getWheresData(cWheres:Wheres):Array[(Any, DataType)] = 
    cWheres.flatMap(_.nestedWhereData)++cWheres.flatMap(_.compositeWheresData)
  
  protected [db] def getHavingsData(cHavings:Havings) = cHavings.flatMap(_.nestedHavingData) ++ cHavings.flatMap(_.compositeHavingsData)
  
  protected [db] def getSelectData(cCols:Cols, cWheres:Wheres, cOrderings:Orderings):Array[(Any, DataType)] = {
    cCols.flatMap{a => a.compositeColData} ++ getWheresData(cWheres)++ cOrderings.flatMap{_.col.compositeColData}
  }
  
  protected [db] def getAggregateData(cAggregates:Aggregates, cGroupByIntervals:GroupByIntervals, cWheres:Wheres, cHavings:Havings):Array[(Any, DataType)] = 
    cAggregates.flatMap(_.compositeAggrData) ++ cGroupByIntervals.flatMap(_.col.compositeColData) ++ getWheresData(cWheres) ++ getHavingsData(cHavings) 
  
  
  def getTableIDs(tableInfo:Map[String, DBManager]):Seq[JsonFormatted] = getTableIDs(tableInfo.toArray: _*)
  def getTableIDs(tableInfo:(String, DBManager)*) = tableInfo.map{
    case (info, db) => new JsonFormatted{
      val keys = Array("info", "id", "tableName")
      val vals = Array[Any](info, db.getTableID, db.getTable.tableName)
    }
  }
  def getRows(cols:Cols, datas:List[List[Any]]) = {
    datas map {case d => getRow(cols, d)}
  }
  def castData(d:DataType, data:String): Any = {
    // for user-defined where (via UI) for searching and deleting
    d match{
      case TIMESTAMP => data.toLong
      case VARBINARY(_) | BLOB => Base64.decode(data)
      case BIGDEC(_, _) | UBIGDEC(_, _) => BigDecimal(data)
      case VARCHAR(_)|VARCHARANY => data
      case INT | UINT(_) => data.toInt
      case LONG | ULONG(_) | ULONGAuto => data.toLong
      //case BLOB => rs.getBlob
      case any => throw new DBException("unsupported type: "+any)
    }
  }
  def getRow(cols:Cols, data:Array[Any]):Row = getRow(cols, data.toList)
  def getRow(cols:Cols, data:List[Any]) = {
    val names = cols.map(_.name.toLowerCase)
    val newData = data.map { d =>
      d match {
        case a:Array[Byte] => new String(a, "UTF-8")
        case any => any
      }
    }
    Row(newData, names)
  }
  /**
   * This function maps Scala data types to a string description of the corresponding SQL data types.
   * This is used for generating SQL statements.
   */
  def colTypeString(t:DataType)(isPostGreSQL:Boolean) = t match {
    case VARCHAR(size) => "VARCHAR("+size+")"
    case VARCHARANY => "VARCHAR"
    case VARBINARY(size) => "VARBINARY("+size+")"
    case BIGDEC(size, precision) if isPostGreSQL => s"NUMERIC($size, $precision)"
    case UBIGDEC(size, precision) if isPostGreSQL => s"NUMERIC($size, $precision)"
    case BIGDEC(size, precision) => s"DECIMAL($size, $precision)"
    case UBIGDEC(size, precision) => s"DECIMAL($size, $precision)"
    case TIMESTAMP => "TIMESTAMP"
    case INT | UINT(_) => "INT"
    case BOOL => "BOOLEAN"
    case LONG | ULONG(_) => "BIGINT"
    case ULONGAuto if isPostGreSQL => "BIGSERIAL"
    case ULONGAuto => "BIGINT AUTO_INCREMENT"
    // case ULONG => "BIGINT UNSIGNED" // not supported in most dbs such as MySQL, H2, etc
    case BLOB => "BLOB"
    case ULONGCOMPOSITE | CASTTYPE(_) | CompositeCol(_, _, _)|CONST(_, _) => throw new DBException("Unsupported colType: "+t)
  }
  //  ID                BIGINT(19)	NO	PRI	NULL
  //TOKEN               VARCHAR(255)	YES	UNI	NULL
  //TARGET_USER_ID	BIGINT(19)	YES		NULL
  //TYPE                VARCHAR(2)	YES		NULL
  //CREATED             TIMESTAMP(23)	YES		NULL
  //EXPIRES             TIMESTAMP(23)	YES		NULL

  def strToColType(t:String) = t match {
    case s if s.startsWith("BOOLEAN") => 
      BOOL 
    case s if s.startsWith("VARCHAR") => 
      VARCHAR(s.drop("VARCHAR".size+1).init.toInt) // drop '(' too
    case s if s.startsWith("VARBINARY") => 
      VARBINARY(s.drop("VARBINARY".size+1).init.toInt) // drop '(' too
    case s if s.startsWith("DECIMAL") => 
      BIGDEC(s.drop("DECIMAL".size+1).init.toInt, 0) // drop '(' too
    case s if s.startsWith("INT") => 
      UINT(s.drop("INT".size+1).init.toInt) // drop '(' too
    case "BIGINT(19)" =>  // H2 only seems to have BIGINT(19)
      LONG
    case s if s.startsWith("BIGINT AUTO_INCREMENT") => 
      ULONGAuto
    case s if s.startsWith("BIGINT") => 
      ULONG(s.drop("BIGINT".size+1).init.toInt) // drop '(' too
    case "TIMESTAMP(23)" =>  // H2 only seems to have BIGINT(19)
      TIMESTAMP
    case "BLOB" => 
      BLOB 
    case any => throw DBException(s"Cannot convert $any of type ${any.getClass} to known DataType")
  }
  def extractTableColsAndKeys(colList:Seq[(String, List[String])]) = {
    val names = colList.find(_._1 == "COLUMN_NAME").get._2
    val colTypes = colList.find(_._1 == "TYPE").get._2
    val keys = colList.find(_._1 == "KEY").get._2 zip names filter (_._1 == "PRI") map (_._2)
    val cols = names zip colTypes map {
      case (name, colType) => Col(name, strToColType(colType))
    }
    val priKeyCols = cols.filter(col => keys.contains(col.name))
    (cols.reverse, priKeyCols)
  }
  import org.sh.utils.Util._

  def getColList(rs:ResultSet) = { // used for validating table schema
    val rsmd = rs.getMetaData
    val numberOfColumns = rsmd.getColumnCount
    val colNames = for (i <- 1 to numberOfColumns) yield rsmd.getColumnName(i).toUpperCase
    var colVals:List[Seq[String]] = Nil
    while (rs.next()) colVals ::= (for (i <- 1 to numberOfColumns) yield rs.getString(i).toUpperCase)         
    colNames.indices.map(i => colNames(i) -> colVals.map(_(i))).toList
  } 
  
  /**
   *
   * Used to extract a particular Scala Col from a SQL query result set.
   *
   * @return The data stored in ResultSet rs corresponding to Col col
   *
   * @param rs The ResultSet to use
   * @param col The Col to extract from rs. Returns () if col does not exist in rs.
   */
  def get(rs:ResultSet, col:Col, optAlias:Option[String]=None) = {
    /**
     *
     * A function that takes in a Scala DataType (say t) and returns another function f as follows:
     *  f takes in as input a string corresponding to a SQL Col name (say n) and extracts the value of Scala type corresponding to t for col d from 
     *  the result set, if the col with name n is indeed of type t, otherwise it outputs Unit.
     *
     *  So for instance if t is INT, then f takes any string corresponding to a col name (such as "AGE") and outputs the corresponding
     *  Scala type equivalent to INT from the result set (if "AGE" is indeed of type INT).
     *
     *  This method is used only internally by the other methods and should never need be used externally.
     *
     * @param d is a DataType (e.g., VARBINARY)
     * @return A function that takes in a string representing a col name (e.g., "MyColumn") and outputs a Any type
     * depending on d. For instance, if d is VARBINARY, the function will return rs.getBytes
     *
     */
    def getFunc(d:DataType):String=> Any = {
      d match{
        case TIMESTAMP => rs.getTimestamp 
        case VARBINARY(s) => rs.getBytes
        case BOOL => rs.getBoolean
        case BOOLEAN => 
          rs.getInt(_) match {
            case 0 => false 
            case _ => true
          }          
        case BIGDEC(size, scale) => s => BigDecimal(rs.getBigDecimal(s))
        case UBIGDEC(size, scale) => s => BigDecimal(rs.getBigDecimal(s))
        case VARCHAR(_)|VARCHARANY => rs.getString
        case INT | UINT(_) => rs.getInt
        case ULONGCOMPOSITE | LONG | ULONG(_) | ULONGAuto => rs.getLong
        case BLOB => rs.getBytes
        case CASTTYPE(cd) => getFunc(cd)
        case CompositeCol(lhs, _, _) => getFunc(lhs.colType)
        case CONST(_, t) => getFunc(t)
        //case BLOB => rs.getBlob
        case any => _ => throw new DBException("unsupported type: "+any)
      }
    }
    getFunc(col.colType)(if (optAlias.isDefined) optAlias.get else col.alias)
  }
  def anyException(src:Any, dest:Class[_]) = 
    throw new DBException("I don't know how to convert ["+src+"] of type ["+src.getClass.getCanonicalName+"] to ["+dest.getCanonicalName+"]")

  /**
     * inserts a value into a Prepared Statement in place of "?".
     *
     * @param ctr The "?" is specified by ctr. The value ctr = 1 inserts at the first occurance, ctr = 2 inserts at the second occurance, and so on.
     * @param st The Prepared Statement to insert into
     * @param data the value to insert
     * @param d the datatype of the value
     */
  /*  
    In the method "set" below:
    the "ignoreUnsigned" flag is given to indicate that we can ignore contraint violation on unsigned numbers
    for instance a UINT type is assigned -1
    We can ignore these when the set method is called as part of constructing a "Where" clause, since this is only
    used for searching and not modifying the data. This integrity cannot be violated

    on the other hand, if this method is called as part of a "SET" clause in an "UPDATE" statement, then we do need
    the constraints to be checked. This flag will be then set to false.
  */
 
  var customTypes :Seq[Any => Any] = Seq()
  def addType(f: Any => Any) ={
    customTypes :+= f
  }
  def processCustomTypes(a:Any) = {
    customTypes.foldLeft(a)((x, f) => f(x))
  }
  
  def set(ctr: Int, st:PreparedStatement, dataUnprocessed: Any, dataType:DataType)(implicit ignoreUnsigned:Boolean = true):Unit = {
    val data = processCustomTypes(dataUnprocessed)
    def signException(x:Number, d:DataType) = if (!ignoreUnsigned) throw new SQLException("unsigned constraint violation: ["+x+"] for ["+d+"]")
    def rangeException(n:Number, x:Any, d:DataType) = throw new SQLException("range constraint violation: ["+x+"] for ["+d+"] (limit: "+n+")")

    dataType match {
      case CASTTYPE(castedDataType) => 
        set(ctr, st, data, castedDataType)
      case TIMESTAMP  =>  
        data match {
          case x: java.sql.Timestamp => st.setTimestamp(ctr, x) 
          case x: String => 
            st.setString(ctr, x)  // convert timestamp to string 
          case x: Long => st.setLong(ctr, x)  // added as supplement to above
          case any => 
            anyException(any, classOf[java.sql.Timestamp])
        }
      case VARBINARY(s) => data match { case x: Array[Byte] => st.setBytes(ctr, x) }
      case VARCHAR(s)   =>  data match { 
          case x: String => 
            if (x.size > s) throw new SQLException("Varchar size > "+s+": "+x.take(10)+"...")
            st.setString(ctr, x) 
          case x: Int =>  // for having clause
            st.setInt(ctr, x) 
          case any => 
            println("any "+any.getClass.getCanonicalName)
            anyException(any, classOf[String])
        }
      case VARCHARANY => data match {
          case x:String => st.setString(ctr, x) 
          case any => anyException(any, classOf[String])
        }
      case BIGDEC(s, p) => data match {
          case x: BigDecimal => 
            if (x.scale > s) throw new SQLException("scala bigdec size > "+s+": "+x.toString.take(10)+"...") 
            st.setBigDecimal(ctr, x.bigDecimal)
          case x: BigInt => set(ctr, st, BigDecimal(x), dataType)
          case x: Long => set(ctr, st, BigDecimal(x), dataType)
          case x: Int => set(ctr, st, BigDecimal(x), dataType)
          case any => anyException(any, classOf[BigDecimal])
        }
      case INT => data match { 
          case x: Int => st.setInt(ctr, x) 
          case any => anyException(any, classOf[Int])
        }
      case LONG | ULONGCOMPOSITE => data match { 
          case x: Long => st.setLong(ctr, x) 
          case x: BigInt => if (x <= Long.MaxValue && x >= Long.MinValue) st.setLong(ctr, x.toLong) 
          case x: Int => st.setLong(ctr, x) 
          case x: java.sql.Timestamp => st.setLong(ctr, x.getTime) 
          case x: String => st.setString(ctr, x) 
          case any => anyException(any, classOf[Long])
        }
      case UBIGDEC(s, p) => data match {
          case x: BigDecimal => // ignoreUnsigned is used to avoid errors when data is in where clause. See top of method definition
            if (x < 0) signException(x, dataType)
            if (x.scale > s) rangeException(s, x.toString.take(10)+"...", dataType)
            st.setBigDecimal(ctr, x.bigDecimal) 
          case x: BigInt => set(ctr, st, BigDecimal(x), dataType)
          case x: Long => set(ctr, st, BigDecimal(x), dataType)
          case x: Int => set(ctr, st, BigDecimal(x), dataType)
          case any => anyException(any, classOf[BigInt])
        }
      case BOOL => data match {
          case x:Boolean => st.setBoolean(ctr, x)
          case any => anyException(any, classOf[Boolean])
        }
        
      case UINT(n) => data match { 
          case x: Int => 
            if (x < 0) signException(x, dataType)
            if (x > n) rangeException(n, x, dataType)
            st.setInt(ctr, x)
          case x:Boolean if n == 1 => st.setInt(ctr, if (x) 1 else 0)
          case any => anyException(any, classOf[Int])
        }
      case ULONGAuto => set(ctr, st, data, ULONG)
      //        throw new DBException(s"Not permitted setting ULONGAuto type. Counter: $ctr and data: $data")
      case ULONG(n) => data match { 
          case x: Long => 
            if (x < 0) signException(x, dataType)
            if (x > n) rangeException(n, x, dataType)
            st.setLong(ctr, x) 
          case x: BigInt => if (x <= Long.MaxValue && x >= Long.MinValue) st.setLong(ctr, x.toLong) 
          case x: Int => st.setLong(ctr, x) 
          case x: java.sql.Timestamp => st.setLong(ctr, x.getTime) 
          case any => anyException(any, classOf[Long])
        }
      // following code from http://objectmix.com/jdbc-java/41011-create-java-sql-blob-byte%5B%5D.html
      //case BLOB => data match { case x: Array[Byte] => st.setBinaryStream(ctr, new ByteArrayInputStream(x), x.length ) }
      //
      // following code from http://www.herongyang.com/JDBC/SQL-Server-CLOB-setBytes.html
      case BLOB => data match { case x: Array[Byte] => st.setBytes(ctr, x) }
      case CompositeCol(_, oper, _) => 
        (oper, data) match { // used for composite cols.. see method tableColType below
          case (Add| Sub| Mul| Div | Mod, a:Int) => st.setInt(ctr, a) // should we check for division by zero here and below?? I guess not
          case (Add| Sub| Mul| Div | Mod, a:Long) => st.setLong(ctr, a)
          case (Add| Sub| Mul| Div, a:Double) => st.setDouble(ctr, a)
          case (Add| Sub| Mul| Div, a:Float) => st.setFloat(ctr, a)
          case (Upper | Lower, a:String) => st.setString(ctr, a)
          case _ => throw new DBException("Data type not supported: "+data+" for oper: "+oper)
        }
      case CONST(_, constType) => 
        set(ctr, st, data, constType)
        //println(" [DB TEST CONST] "+st)
      case _ => 
        //println("ANY ==> "+ctr+"\n "+ st +"\n   DATA: "+data + "\n   TYPE: "+ dataType)
        println(" [DB SET STR] Ignoring data (ctr = "+ctr+", data: "+data + ", type: "+ dataType)
        
    }
  }
  def getSQLTimeStamp:java.sql.Timestamp = { new java.sql.Timestamp(new java.util.Date().getTime) }
  
  def canDoComparison(col:Col, op:Op) = (col.colType, op) match {
    case (colType, _) if colType.isSortable => true
    case (_, From | In | NotIn | IsNull | IsNotNull) => true
    case (CompositeCol(_, _, _), _) => true //  
    case (_, Eq | Ne) => true
    case (VARCHAR(_), Like | NotLike | RegExp | NotRegExp) => true
    case _ => false
  }
  
  // do double check below
  def canDoComparison(aggregate:Aggregate, op:Op) = (aggregate, op) match {
    case (Aggregate(_, Top | First | Last), _)  => false
    case (_, Eq | Ne | Le | Ge | Gt | Lt)  => true
    case (_, From | In | NotIn) => true
    case _ => false
  }
  def incrementIt(a:Array[Any], incrVals:Array[Any]):Array[Any] = (a zip incrVals) map {
    case (a:Int, i:Int) => a+i
    case (a:Long, i:Long) => a+i
    case (a:Long, i:Int) => a+i
    case (a:BigInt, i:BigInt) => a+i
    case (a:BigDecimal, i:BigInt) => a + BigDecimal(i)
    case (a:BigDecimal, i:Int) => a + i
    case (a:BigDecimal, i:BigDecimal) => a + i
    case (a:BigDecimal, i:Long) => a + i
    case (a:BigInt, i:Long) => a+i
    case (a:BigInt, i:Int) => a+i
    case (a, i) => throw new DBException("cannot do increment on ["+a+":"+a.getClass.getCanonicalName+"] with ["+i+":"+i.getClass.getCanonicalName+"]")
  }  
  def incrementItWithResult(a:Array[Any], incrVals:Array[Any]):Array[(Any, Any)] = (a zip incrVals).map {
    case (a:Int, i:Int) => a+i
    case (a:Long, i:Long) => a+i
    case (a:Long, i:Int) => a+i
    case (a:BigInt, i:BigInt) => a+i
    case (a:BigDecimal, i:BigInt) => a + BigDecimal(i)
    case (a:BigDecimal, i:Int) => a + i
    case (a:BigDecimal, i:BigDecimal) => a + i
    case (a:BigDecimal, i:Long) => a + i
    case (a:BigInt, i:Long) => a+i
    case (a:BigInt, i:Int) => a+i
    case (a, i) => throw new DBException("cannot do increment on ["+a+":"+a.getClass.getCanonicalName+"] with ["+i+":"+i.getClass.getCanonicalName+"]")
  } zip (a)
}
