package org.sh.db.core.DataStructures

import org.sh.db.ScalaDB.Sel
import org.sh.db.DBManager
import org.sh.db.core.Table
import org.sh.db.core.Util
  
/**
 * Scala type corresponding to a SQL Col type. A SQL Col has a name and a SQL data type
 */

object Col {
  def apply(colName:String, colType:DataType) = new Col(colName, colType, None)
  
  // following from http://www.h2database.com/html/advanced.html
  val reservedNames = Array("CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DISTINCT", "EXCEPT", "EXISTS", "FALSE", "FETCH", "FOR", "FROM", "FULL", "GROUP", "HAVING", "INNER", "INTERSECT", "IS", "JOIN", "LIKE", "LIMIT", "MINUS", "NATURAL", "NOT", "NULL", "OFFSET", "ON", "ORDER", "PRIMARY", "ROWNUM", "SELECT", "SYSDATE", "SYSTIME", "SYSTIMESTAMP", "TODAY", "TRUE", "UNION", "UNIQUE", "WHERE")
}
case class Col(name:String, colType:DataType, optTable:Option[Table]) {
  if (Col.reservedNames.contains(name.toUpperCase)) throw DBException(s"column name '$name' is a reserved word")
  lazy val compositeColData:List[(Any, DataType)] = { // gives data for a composite col // need to check ordring of data 
    ////////////////////////////////////////////////
    //////// CHECK ORDERING OF OUTPUT STRING (FOR DATA VALUES)
    ////////////////////////////////////////////////
    colType match {
      case CompositeCol(lhs, oper, rhs) => 
        lhs.compositeColData ++ (
          rhs match {
            case null => Nil // null used for unary operations like UPPER
            case d:DataType => Nil // DataType used for 'cast as' operation
            case c:Col => c.compositeColData 
            case data => 
              List((data, lhs.colType)) 
          }
        ) 
      case CONST(any, t) => 
        List((any, t))
      case _ => Nil        
    }
  }
  lazy val isComposite = colType match {
    case CompositeCol(lhs, oper, rhs) => true
    case _ => false
  }
  lazy val colSQLString:String = {
    ////////////////////////////////////////////////
    //////// CHECK ORDERING OF OUTPUT STRING (FOR DATA VALUES)
    ////////////////////////////////////////////////
    colType match {
      case CompositeCol(lhs, Cast, d:DataType) => 
        Cast+"("+lhs.colSQLString+" AS "+d.toCastString+")"
      case CompositeCol(lhs, DateDiffSecond, rhs) =>
        s"DATEDIFF('SECOND', ${lhs.colSQLString}, ${rhs.anySQLString})"
      case CompositeCol(lhs, DateDiffMillis, rhs) =>
        s"DATEDIFF('MILLISECOND', ${lhs.colSQLString}, ${rhs.anySQLString})"
      case CompositeCol(lhs, oper, null) =>
        // null implies unary operator such as upper(age)
        oper+"("+lhs.colSQLString+")"
      case CompositeCol(lhs, oper, rhs) => 
        "("+lhs.colSQLString + " "+
        oper + " "+
        rhs.anySQLString+")" // in Where, lhs and Eq are not required for computing the output..
        // (but given because Where needs them in constructor, however getDataString does not use these values, if you look at its code)
      case CONST(any, _) => 
        any.anySQLString
      case other => 
        (if (optTable.isDefined) optTable.get.tableName+"." else "")+name
    }
  }

  lazy val compositeTables:Set[Table] =  { // in the query Select A+B from T, (A+B) is a composite column
    ////////////////////////////////////////////////
    //////// ORDERING DOES NOT MATTER AS OUTPUT IS SET
    ////////////////////////////////////////////////
    colType match{
      case CompositeCol(lhs, oper, rhs:Col) => lhs.compositeTables ++ rhs.compositeTables
      case CompositeCol(lhs, _oper, _rhs) => lhs.compositeTables
      //case _ => if (optTable.isDefined) Set(optTable.get.tableName) else Set()
      case _ => if (optTable.isDefined) Set(optTable.get) else Set()
    }
  }

  lazy val compositeTableNames = {
    ////////////////////////////////////////////////
    //////// ORDERING DOES NOT MATTER AS OUTPUT IS SET
    ////////////////////////////////////////////////
    compositeTables.map(_.tableName)
  }

  lazy val compositeCols:Set[Col] = colType match { // get accessed cols
    ////////////////////////////////////////////////
    //////// ORDERING DOES NOT MATTER AS OUTPUT IS SET
    ////////////////////////////////////////////////
    case CompositeCol(lhs, oper, rhs:Col) => lhs.compositeCols ++ rhs.compositeCols
    case CompositeCol(lhs, oper, any) => lhs.compositeCols
    case any => Set(this)
  }

  lazy val simpleCol:Col = Col(name, colType match{case CompositeCol(lhs, _, _) => lhs.simpleCol.colType case any => colType}, None)

  lazy val colHash = compositeColData.foldLeft(hash(colSQLString))((x, y) => hash(x+y._1))
  lazy val alias = colHash // hash(tableColName)

  lazy val canDoInterval = colType match { // for group by interval // no big Int
    case ULONGCOMPOSITE | INT | LONG | UINT(_) | ULONG(_) | CompositeCol(_,_,_) | UBIGDEC(_, _) | BIGDEC(_, _) => true                  // should we allow interval for composite cols?
    case _ => false
  }
  @deprecated lazy val canDoIncrement = canDoInterval

  override def toString = compositeColData.foldLeft(colSQLString)((x, y) => x.replaceFirst("\\?", y._1.toString))

  // BELOW METHOD converts all optTable in this column to Some(table) (including those of composite cols contained within this col) if optTable is undefined, otherwise leaves unchanged
  def to(table:Table):Col = {
    val newColType = colType match {
      case CompositeCol(lhs, oper, rhs) => 
        val newLhs = lhs.to(table)
        val newRhs = rhs match {
          case c@Col(_, _, _) => c.to(table)
          case any => any
        }
        CompositeCol(newLhs, oper, newRhs)
      case any => any
    }
    Col(name, newColType, if (optTable.isDefined) optTable else Some(table))
  }  

  // changes the optTable of current col to Some(table). Also converts tables of composite cols to Some(table) IF those optTables are not defined (else leaves unchanged)
  // however, it DOES change for current col (irrespect of what optTable is). The unchanging behaviour is only for composite cols
  def of(table:Table):Col = Col(name, colType, Some(table)).to(table)
  def of(db:DBManager):Col = of(db.getTable) // for DBManager, we call getTable rather than table so that we can override it in SecureDBManager
        // why? because we need to get the plaintext table, not ciphertext table when defining of using SecureDBManager (super class of DBManager)

  // helper methods for composite cols
  
  def upper = Col(name, CompositeCol(this, Upper, null), optTable)
  def abs    = Col(name, CompositeCol(this, ABS, null), optTable)
  
  def castAs (rhs:DataType)    = Col(name, CompositeCol(Col(name, CASTTYPE(rhs), optTable), Cast, rhs), optTable) // keeping name and optTable same for now
  def dateDiffSecond (rhs:Any)    = Col(name, CompositeCol(Col(name, ULONGCOMPOSITE, optTable), DateDiffSecond, rhs), optTable) // keeping name and optTable same for now
  def dateDiffMillis (rhs:Any)    = Col(name, CompositeCol(Col(name, ULONGCOMPOSITE, optTable), DateDiffMillis, rhs), optTable) // keeping name and optTable same for now
  
  def +(rhs:Any)    = Col(name, CompositeCol(this, Add, rhs), optTable) // keeping name and optTable same for now
  def -(rhs:Any)    = Col(name, CompositeCol(this, Sub, rhs), optTable) // keeping name and optTable same for now
  def /(rhs:Any)    = Col(name, CompositeCol(this, Div, rhs), optTable) // keeping name and optTable same for now
  def *(rhs:Any)    = Col(name, CompositeCol(this, Mul, rhs), optTable) // keeping name and optTable same for now
  def %(rhs:Any)    = Col(name, CompositeCol(this, Mod, rhs), optTable) // keeping name and optTable same for now
  def \(interval:Long)    = GroupByInterval(this, interval)
  def withInterval(interval:Long) = \(interval)

  def decreasing = Ordering(this, Decreasing)
  def increasing = Ordering(this, Increasing)

  // helper methods for wheres

  // following three lines based on this answer 
  // http://stackoverflow.com/a/34809016/243233
  private var x0: Where = _
  def value = x0
  def value_=(data: Any) = Where(this, Eq, data)  

  def === (rhs:Any)    = Where(this, Eq, toNestedIfSel(rhs))
  def isNull    = Where(this, IsNull, null)
  def isNotNull    = Where(this, IsNotNull, null)
  @deprecated("Nested queries are slow", "15 Sept 2017")
  def from (rhs:Nested)    = Where(this, From, rhs)
  
  def in(rhs:Array[String]) = if (rhs.isEmpty) Where.alwaysFalse else Where(this, In, rhs)
  def in(rhs:Array[Long]) = if (rhs.isEmpty) Where.alwaysFalse else Where(this, In, rhs)
  def in(rhs:Array[Int]) = if (rhs.isEmpty) Where.alwaysFalse else Where(this, In, rhs)
  
  @deprecated("Nested queries are slow", "15 Sept 2017")
  def in (rhs:Nested)    = Where(this, In, rhs)
  
  @deprecated("Nested queries are slow", "15 Sept 2017")
  def notIn (rhs:Nested)    = Where(this, NotIn, rhs)
  
  //@deprecated("Need to test", "15 Sep 2017")
  def notIn (rhs:Array[String])    = if (rhs.isEmpty) Where.alwaysTrue else Where(this, NotIn, rhs)
  def notIn (rhs:Array[Long])    = if (rhs.isEmpty) Where.alwaysTrue else Where(this, NotIn, rhs)
  def notIn (rhs:Array[Int])    = if (rhs.isEmpty) Where.alwaysTrue else Where(this, NotIn, rhs)
  
  private def toNestedIfSel(a:Any) = {    
    a match {
      case s:Sel[_] => s.nested
      case any => any
    }
  }
  def <=(rhs:Any)    = Where(this, Le, toNestedIfSel(rhs))
  def >=(rhs:Any)    = Where(this, Ge, toNestedIfSel(rhs))
  def <(rhs:Any)    = Where(this, Lt, toNestedIfSel(rhs))
  def >(rhs:Any)    = Where(this, Gt, toNestedIfSel(rhs))
  def <>(rhs:Any)    = Where(this, Ne, toNestedIfSel(rhs))
  def like(rhs:Any)    = Where(this, Like, rhs)
  def notLike(rhs:Any)    = Where(this, NotLike, rhs)
  def ~(rhs:Any)    = Where(this, RegExp, rhs)
  def !~(rhs:Any)    = Where(this, NotRegExp, rhs)

  def sum = Aggregate(this, Sum)
  def max = Aggregate(this, Max)
  def min = Aggregate(this, Min)
  def avg = Aggregate(this, Avg)
  def first = Aggregate(this, First)
  def last = Aggregate(this, Last)
  def count = Aggregate(this, Count)
  def top = Aggregate(this, Top)
  def groupBy = Aggregate(this, GroupBy)
  def topWithInterval(interval:Long) = Aggregate(this, Top(Some(interval)))

  def <--(data:Any) = Update(this, data)
  def ++=(data:Number):Increment = Update(this, data)

  def -->(dataType:DataType) = castAs(dataType)

}
