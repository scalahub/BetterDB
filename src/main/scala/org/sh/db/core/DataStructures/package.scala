package org.sh.db.core

import java.util.Base64

import org.sh.utils.json.JSONUtil
import org.sh.db._
import org.sh.db.core.Util._

package object DataStructures{
  implicit def anyToAny(s: Any) = new BetterAny(s)
  class BetterAny(val a:Any) {
    def as[T] = a.asInstanceOf[T]
    // use above as 
    //  selectCols(col)(where)(a => (a(0).as[Int], a(1).as[String]))
    // instead of 
    //  selectCols(col)(where)(a => (a(0).asInstanceOf[Int], a(1).asInstanceOf[String]))

    def anySQLString:String = a match { // for using in "createStatement" (substitute with "?" for actual data)
      case t:Col => t.colSQLString // actual column name or compoite column name
      case n:Nested => n.nestedSQLString
      case a:Aggregate => a.aggrSQLString // NEW CODE 
        //following does not seem to work
        // case a:Array[Byte] => "?" // for blob
      case a:Array[String] => if (a.isEmpty) throw DBException("cannot deconstruct an empty array")
          "("+(a.map(x => new BetterAny(x).anySQLString).reduceLeft(_+","+_))+")"
      case a:Array[Long] => if (a.isEmpty) throw DBException("cannot deconstruct an empty array")
          "("+(a.map(_.anySQLString).reduceLeft(_+","+_))+")"
      case a:Array[Int] => if (a.isEmpty) throw DBException("cannot deconstruct an empty array")
          "("+(a.map(_.anySQLString).reduceLeft(_+","+_))+")"
      case _ => "?"
    }
    def to(table:Table) = a match { // make it canonical if it is a column
      case c:Col => c.to(table)
      case a:Aggregate => a.to(table)
      case w:Where => w.to(table)
      case null => null
      case any:Any => any
    }    
    def const = constCol(a)
  }

  abstract class Nested {
    val nestedSQLString:String
    val nestedData:Array[(Any, DataType)]
    private lazy val alias = nestedData.foldLeft(hash(nestedSQLString))((x, y) => hash(x + y))
    def alias(lhsAlias:String, op:Op):String = hash(lhsAlias + hash(op + alias))
    val nestedColAlias:String
    def debugExecute:List[Any]
    def count:Long
  }
  case class NestedSelect(db:DBManager, col:Col, wheres:Wheres, orderings:Orderings, limit:Int, offset:Long) extends Nested {
    // select a, b where x = (select col from db where u > v and u < w order by u desc max 10 offset 20)
    lazy val nestedSQLString = "("+db.getTable.selectSQLString(Array(col), wheres)(orderings, limit, offset)+")"
    lazy val nestedData:Array[(Any, DataType)] = getSelectData(Array(col), wheres, orderings)    
    def debugExecute = // execute to get data
      db.selectCols(wheres, Array(col), a => a(0))(orderings, limit, offset)
    lazy val nestedColAlias = col.alias
    def count = db.countRows(wheres)
  }
  
  case class NestedAggregate(db:DBManager, aggregate:Aggregate, wheres:Wheres, groupByIntervals:GroupByIntervals, havings:Havings) extends Nested {
    // select a, b where x = (select avg(col) from db where u > v and u < w)
    lazy val nestedSQLString = "("+db.getTable.aggregateSQLString(Array(aggregate), wheres, groupByIntervals, havings)+")"
    lazy val nestedData:Array[(Any, DataType)] = getAggregateData(Array(aggregate), groupByIntervals, wheres, havings)
    def debugExecute = // execute to get data
      db.aggregateGroupLong(Array(aggregate), wheres, groupByIntervals, havings).map(a => a(0))
    lazy val nestedColAlias = aggregate.alias
    def count = db.aggregateGroupLong(Array(aggregate), wheres, groupByIntervals, havings).size
  }

  def hash(s:String) = {
    val base64 = Base64.getEncoder
    val sha = java.security.MessageDigest.getInstance("SHA-1")
    try base64.encodeToString(
      sha.digest(
        s.getBytes
      )
    ).filter(x =>
      x.isLetter
    ).take(10)
    catch {
      case a:Any => 
        throw DBException(" [FATAL ERROR:DB] String: ("+s+") of size: "+s.size+". "+
                          (if (a.getMessage == null) a.getCause.getClass.getName+":"+a.getCause.getMessage else a.getMessage))
    }
  }
  
  case class Row(data:Seq[Any], names:Array[String]) extends JSONUtil.JsonFormatted {
    def this(data:Seq[Any], names:Seq[String]) = this(data, names.toArray)
    def this(data:Array[Any], names:Seq[String]) = this(data.toList, names.toArray)
    def this(data:Array[Any], names:Array[String]) = this(data.toList, names)
    
    val timeKeyIndex = names.indexWhere(_.toLowerCase == "time") 
    
    val newData = data.toArray
    if (timeKeyIndex >= 0 && data(timeKeyIndex).isInstanceOf[Long]) {
      val time = data(timeKeyIndex).asInstanceOf[Long]
      newData(timeKeyIndex) = org.sh.utils.Util.toDateString(time)+"("+time+")"
      
    }
    val keys = names 
    val vals = newData
  }
  case class DBException(m:String) extends Exception(m)
  /**
   * Abstract class defining a Scala data type corresponding to a SQL data type
   */
  sealed abstract class DataType {    
    def isSortable:Boolean // certain types can be sorted (needed for places where we can compare columns), for instance in MergedOB
    def toCastString:String // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  }
  private [db] case class CASTTYPE(dataType:DataType) extends DataType {
    def isSortable:Boolean = dataType.isSortable // certain types can be sorted (needed for places where we can compare columns), for instance in MergedOB
    def toCastString:String = dataType.toCastString // throw new DBException(s"Unsupported cast for column $this") // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  }
  
  // composite column. Nothing to do with composite integers
  object ULONGCOMPOSITE extends DataType {
    def isSortable:Boolean = true // certain types can be sorted (needed for places where we can compare columns), for instance in MergedOB
    def toCastString:String = throw new DBException(s"Unsupported cast for column $this") // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  } 
  /**
   * Scala data type corresponding to a VARCHAR SQL data type
   */
  //  @deprecated("use BIGDEC", "22 Oct 2016")
  //  case class ScalaBIGINT(size:Int) extends DataType { // size is number of decimal digits
  //    def isSortable = false // certain types can be sorted (needed for places where we can compare columns), for instance in MergedOB
  //    def toCastString:String = "VARCHAR" // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  //  }
  //  @deprecated("use UBIGDEC", "22 Oct 2016")
  //  case class UScalaBIGINT(size:Int) extends DataType { // size is number of decimal digits // U implies unsigned (positive)
  //    def isSortable = true // lexicographic ordering
  //    def toCastString:String = "VARCHAR" // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  //  }
  //
  case class VARCHAR(size:Int) extends DataType { 
    def isSortable = false 
    def toCastString:String = "VARCHAR" // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  }
  
  @deprecated("Use 'BOOL' instead. Kept for compatibility", "15 Sept 2017")
  object BOOLEAN extends UINT(1)
  object BOOL extends DataType { 
    def isSortable = true 
    def toCastString:String = "BOOLEAN" // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  }
  
  object VARCHARANY extends DataType {   // 'ANY' implies any size
    def isSortable = false 
    def toCastString:String = "VARCHAR" // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  } 
  /**
   * Scala data type corresponding to a VARBINARY SQL data type
   */
  case class VARBINARY(size:Int) extends DataType {
    def isSortable = false 
    def toCastString:String = "VARBINARY" // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  }
  /**
   * Scala data type corresponding to a TIMESTAMP SQL data type
   */
  case object TIMESTAMP extends DataType { 
    def isSortable = true 
    def toCastString:String = "TIMESTAMP" // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  }
  /**
   * Scala data type corresponding to a INT SQL data type
   */
  case object INT extends DataType  { 
    def isSortable = true 
    def toCastString:String = "INT" // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  }
  case class UINT(max:Int) extends DataType {
    def isSortable = true 
    def toCastString:String = "INT" // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  }
  object UINT extends UINT(Int.MaxValue)
  case object LONG extends DataType { 
    def isSortable = true 
    def toCastString:String = "BIGINT" // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  }
  case class ULONG(max:Long) extends DataType { 
    def isSortable = true 
    def toCastString:String = "BIGINT" // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
    // BIGINT in SQL is equivalent to Long in Scala and is DIFFERENT from BigInt in Scala.
    // So when we see BIGINT, we should thing of Long
    // There is no BigInt equivalent in SQL / H2
    // Instead there is DECIMAL, which is like BigDecimal
  }
  object ULONGAuto extends DataType { // for auto increment
    def isSortable = true 
    def toCastString:String = "BIGINT" // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  }
  //val AutoInc = ULONGAuto // alias
  object ULONG extends ULONG(Long.MaxValue)
  
  case class UBIGDEC (size:Int, precision:Int) extends DataType {
    def this(size:Int) = this(size, 0)
    def isSortable = true
    def toCastString:String = "VARCHAR" // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  }

  case class BIGDEC(size:Int, precision:Int) extends DataType {
    def this(size:Int) = this(size, 0)
    def isSortable = true 
    def toCastString:String = "VARCHAR" // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  }
  
  
  /**
   * Scala data type corresponding to a BLOB SQL data type
   */
  case object BLOB extends DataType { 
    def isSortable = false 
    def toCastString:String = "INT" // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  }
  
  
  case class CONST(a:Any, dataType:DataType) extends DataType {
    dataType match {
      case BIGDEC(_, _)|LONG|INT|VARCHAR(_)|BOOL =>
      case a => throw DBException(s"invalid data type for CONST col: "+a)
    }
    def isSortable = false
    def toCastString:String = throw new DBException(s"Unsupported cast for column $this") // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  }
  
  def constCol(data:Any) = { // special column to denote const types.. Example SELECT sal, 10 from Foo. Here 10 is const
    Col(data.getClass.getSimpleName, data match {
      case s:String => CONST(s, VARCHAR(255))
      case s:Int => CONST(s, INT)
      case s:Long => CONST(s, LONG)
      case s:Boolean => CONST(s, BOOL)
      case s:BigInt => CONST(s, BIGDEC(100, 0))
      case s:BigDecimal => CONST(s, BIGDEC(s.scale, s.precision))
      case _ => throw DBException(s"I don't know how to convert data of type ${data.getClass} to a CONST Col")
    }, None)
  }
  
  sealed abstract class ColOperation(symbol:String) {// select col1 + col2 from Table. Here + is the ColOperation
    override def toString = symbol
  }
  case object Add extends ColOperation("+")
  case object Mul extends ColOperation("*")
  case object Sub extends ColOperation("-")
  case object Div extends ColOperation("/")
  case object Mod extends ColOperation("%")
  case object DateDiffSecond extends ColOperation("dateDiffSecond")
  case object DateDiffMillis extends ColOperation("dateDiffMillis")
  
  //   added 3rd march
  case object Upper extends ColOperation("UPPER") // ???
  case object ABS extends ColOperation("ABS") // ???
  case object Lower extends ColOperation("LOWER") // ???
  
  //   added 17 Sep 2017
  case object Cast extends ColOperation("CAST")
  
  // Example of a query with composite cols: select (t1.c1*(t2.c2+(t3.c3+4))) from t1, t2, t3. 
  // For any col, we will store the composite-ness in the colType
  // In the above, (t1.c1*(t2.c2+(t3.c3+4))) is a composite column
  case class CompositeCol(left:Col, op:ColOperation, right:Any) extends DataType { // left can be another ordinary or composite column. (ordinary columns are non-composite ones) {
    def isSortable = left.colType.isSortable
    def toCastString:String = throw new DBException(s"Unsupported cast for column $this") // used for a 'cast as' operation example cast age as INTEGER. This should output 'INTEGER'
  }
  
  
  implicit def colToOrdering(col:Col) = Ordering(col, Increasing)
  implicit def groupByIntervalToTop(groupByInterval:GroupByInterval) = Aggregate(groupByInterval.col, Top(Some(groupByInterval.interval)))
  
  implicit def colToGroupBy(col:Col) = Aggregate(col, GroupBy)
  implicit def dbMgrToTable(dbm:DBManager) = dbm.getTable
  
  abstract class Op {override def toString:String} // Op is op used in where clauses
  object Eq extends Op {override def toString = "="}
  object Le extends Op {override def toString = "<="}
  object Ge extends Op {override def toString = ">="}
  object Gt extends Op {override def toString = ">"}
  object Lt extends Op {override def toString = "<"}
  object Ne extends Op {override def toString = "<>"}

  object From extends Op{override def toString = "="}
  
  object In extends Op{override def toString = "IN"}
  object NotIn extends Op{override def toString = "NOT IN"}

  object IsNull extends Op{override def toString = "IS NULL"}
  object IsNotNull extends Op{override def toString = "IS NOT NULL"}
  //  object In extends Op{override def toString = "= SOME"} // may not work
  //  object In extends Op{override def toString = "= ANY"} // may not work
  //  object In extends Op{override def toString = "IN ANY"} // may not work
  //  object NotIn extends Op{override def toString = "<> ALL"} // works equivalent to NOT IN ... but NOT IN had issue with H2 version.. NOT IN is better though
  //    see http://stackoverflow.com/q/35274539/243233
  
  object Like extends Op {override def toString = "LIKE"}
  object RegExp extends Op {override def toString = "REGEXP"}
  object NotRegExp extends Op {override def toString = "NOT REGEXP"}
  object NotLike extends Op {override def toString = "NOT LIKE"}
  //  object StartsWith extends Op {override def toString = ???}   // to implement if needed (using Like)
  //  object EndsWith extends Op {override def toString = ???}   // to implement if needed (using Like)
  //  object Contains extends Op {override def toString = ???}   // to implement if needed (using Like)
  
  
  val allOps = Seq(Eq, Le, Ge, Gt, Lt, Ne, Like, NotLike, RegExp, NotRegExp)
  def getOp(opString:String) = {
    val upper = opString.toUpperCase
    allOps.find(_.toString == upper) match {
      case Some(op) => op
      case _ => throw new DBException("operation not found: "+opString)
    }
  }
  // missing ops. From: https://en.wikipedia.org/wiki/SQL#Operators
  // Op                         meaning                                             example
  //BETWEEN                 Between an inclusive range                          Cost BETWEEN 100.00 AND 500.00
  //IN                      Equal to one of multiple possible values            DeptCode IN (101, 103, 209)
  //IS or IS NOT            Compare to null (missing data)                      Address IS NOT NULL
  //IS NOT DISTINCT FROM    Is equal to value or both are nulls (missing data) 	Debt IS NOT DISTINCT FROM - Receivables
  
  sealed abstract class Aggr {override def toString:String}
  case class Top(optWithInterval:Option[Long]) extends Aggr{
    optWithInterval match {
      case Some(0) => throw DBException(s"Top interval cannot be 0")
      case _ =>
    }
    override def toString = "TOP_WITH_INTERVAL_"+optWithInterval.getOrElse("None")
  }
  object Top extends Top(None)
  // NOTE THE TOP COL IS USED AS FOLLOWS: 
  // 
  // First note that 
  //  select(x.sum).groupBy(y \ 10) 
  //    is translated to
  //  SELECT SUM(X) as foo, 10*round(y/10.0, 0) as bar group by bar
  // 
  // In query like:  
  //  select(x.sum, y.top).groupBy(y \ 10) 
  //   which is again translated to (i.e., top is ignored)
  //  SELECT SUM(X) as foo, 10*round(y/10.0, 0) as bar group by bar
  // The Top(y) simply selects column with alias bar (i.e., the top interval used for column y)
  // 
  // Thus, whatever column is used in Top (in this case y) MUST be present in group by with some interval
  // 
  // Note that we can have multiple invervals for the same col. In this case we will have problems because there is no unique interval column for y.
  // To see this, observe that:
  // 
  //  select(x.sum).groupBy(y \ 10, y \ 20) 
  //    is translated to
  //  SELECT SUM(X) as foo, 10*round(y/10.0, 0) as bar1, 20*round(y/20.0, 0) as bar2 group by bar1, bar2
  // 
  // 
  object Max extends Aggr {override def toString = "MAX"}
  object Min extends Aggr {override def toString = "MIN"}
  object Avg extends Aggr {override def toString = "AVG"}
  object Sum extends Aggr {override def toString = "SUM"}
  object Count extends Aggr {override def toString = "COUNT"}
  
  object First extends Aggr {override def toString = "GROUP_CONCAT"}
  object Last extends Aggr {override def toString = "GROUP_CONCAT"}
  object GroupBy extends Aggr {
    // for columns to be used in groupBy e.g., 
    //    select users, max(sal) from T group by users. 
    // Here users just after the select keyword is to be represented as Aggregate(users, GroupBy)
    override def toString = "GROUP_BY" // setting to group by. Should not be used directly in query similarly to Top
  } 
  
  private val composableAggrs: Array[Aggr] = Array(Max, Min, Avg, Sum, Count) // can compose
  protected[db] val nativeAggrs = composableAggrs // can use in having clause, etc. Native support
  
  case class CompositeAggr(lhs:Aggregate, op:ColOperation, rhs:Any) extends Aggr{ // RHS can be Aggregate or number 
    def checkComposable(a:Aggregate) = {
      a.aggr match {
        case a if composableAggrs.contains(a) =>
        case CompositeAggr(_, _, _) =>
        case a => throw DBException(s"aggregate ${a} cannot be composed using operation ${op}")
      }
    }
    override def toString = "composite" 
    checkComposable(lhs)
    rhs match {
      case a:Aggregate => checkComposable(a)
      case _ =>  
    }
  }
  
  val allAggrs = Set(Top, Max, Min, Avg, Sum, Count, First, Last)
  
  def getAggr(aggrString:String) = {
    val upper = aggrString.toUpperCase
    allAggrs.find(_.toString == upper) match {
      case Some(aggr) => aggr
      case _ => throw new DBException("aggregate not found: "+aggrString)
    }
  }
  case class IncrOp(increment:Update[_]) extends Op {override def toString = Eq+" "+increment.col.colSQLString+" + "}

  case class GroupByInterval(col:Col, interval:Long) {
    lazy val alias = col.alias+"interval"+interval
    override def toString = s"GROUPBY ($col \\ $interval)"    
  }
  
  implicit def toWhere(a:(Col, Op, Any)) = Where(a)
  implicit def toColOpAny(w:Where) = (w.col, w.op, w.data):(Col, Op, Any)
  implicit def toWhere(a:Seq[(Col, Op, Any)]) = a.map(Where(_)) // .toArray
  implicit def toWhere(a:Array[(Col, Op, Any)]):Wheres = a.map(Where(_))
  
  // we need to add composite wheres to allow and / or.. currently we are taking a sequence of Where and using AND for them.
  // The composite one will allow us to do more fine grained by joining it with and and or.
  // 
  //   we will use op as the place to store this

  sealed abstract class WhereJoinOp(symbol:String) {// select where c1 = 3 or c3 = 4. Here or is the whereOperation
    override def toString = symbol
  }
  case object And extends WhereJoinOp("and")
  case object Or extends WhereJoinOp("or")
  case class WhereJoin(left:Where, whereJoinOp:WhereJoinOp, right:Where) extends Op {
    def to(table:Table):WhereJoin = {
      WhereJoin(left.to(table), whereJoinOp, right.to(table))
    }
  }
  
  case class Ordering(col:Col, isDescending:Boolean){
    lazy val order = if (isDescending) "DESC" else "ASC"
    override def toString = s"ORDERBY ($col, $order)"    
    def to(table:Table) = Ordering(col.to(table), isDescending)

  }
  
  implicit def toAggregate(a:(Col, Aggr)) = Aggregate(a)
  implicit def toAggregate(a:Seq[(Col, Aggr)]) = a.map(Aggregate(_)) // .toArray
  implicit def toAggregate(a:Array[(Col, Aggr)]):Aggregates = a.map(Aggregate(_))

  object Update{def apply[T](update:(Col, T)) = new Update[T](update._1, update._2)}
  implicit def toUpdate[T](a:(Col, T)) = Update(a)
  implicit def toUpdate[_](a:Seq[(Col, Any)]) = a.map(Update(_)) // .toArray
  implicit def toUpdate[_](a:Array[(Col, Any)]):Updates[_] = a.map(Update(_))

  object Ordering{def apply(ordering:(Col, Boolean)) = new Ordering(ordering._1, ordering._2)}
  implicit def toOrdering(a:(Col, Boolean)) = Ordering(a)
  implicit def toOrdering(a:Seq[(Col, Boolean)]) = a.map(Ordering(_)) // .toArray
  implicit def toOrdering(a:Array[(Col, Boolean)]):Orderings = a.map(Ordering(_))

  final val Decreasing = true
  final val Increasing = false

  case class Update[+T](col:Col, data:T) {
    override def toString = s"UPDATE ($col, $data)"
    def and (that:Update[Any]):Updates[Any] = Array(this, that)    
  }
  
  type Updates[T] = Array[Update[T]]
  type Increment = Update[Number]
  type Increments = Array[Increment]
  type Cols = Array[Col]
    
  type Ops = Array[Op]
  type GroupByIntervals = Array[GroupByInterval]
  type Havings = Array[Having]
  type Wheres = Array[Where]
  type Aggregates = Array[Aggregate]
  type Orderings = Array[Ordering]  
  type OptConn = Option[java.sql.Connection]
}
