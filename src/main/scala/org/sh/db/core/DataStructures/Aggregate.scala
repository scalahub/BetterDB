package org.sh.db.core.DataStructures

import org.sh.db.core.Table

object Aggregate{def apply(aggregate:(Col, Aggr)) = new Aggregate(aggregate._1, aggregate._2)}

case class Aggregate(col:Col, aggr:Aggr) {
  lazy val isComposite = aggr match {
    case CompositeAggr(_, _, _) => true
    case _ => false
  }
  lazy val aggrSQLString:String = aggr match {
    case Top | GroupBy => throw DBException("No native support for operation "+aggr)
    case CompositeAggr(left, aggrOp, right:Aggregate) => "("+left.aggrSQLString+" "+aggrOp+" "+right.aggrSQLString+")"        
    case CompositeAggr(left, aggrOp, right) => "("+left.aggrSQLString+" "+aggrOp+" "+right.anySQLString+")"        
    case _ => aggr+"("+col.colSQLString+")"
  }
  lazy val compositeAggrData:List[(Any, DataType)] = { // gives data for a composite col // need to check ordring of data 
    aggr match {
      case CompositeAggr(lhs, aggrOp, rhs) => 
        lhs.compositeAggrData ++ (
          rhs match {
            case c:Aggregate => c.compositeAggrData 
            case data => List((data, lhs.col.colType)) 
          }
        ) 
      case GroupBy => Nil // We will be handling groupBy similarly to Top (see below). It is syntactic sugar. 
        // The data will be read from the groupByInterval (special case of group-by-interval with interval 0)
      case Top(_) => Nil // top should not map to a real sql query. Instead a groupBy clause should have been used with that group-by-interval
        // we will then read the result of Top(_) using the alias of the column used for the group-by-interval
        // Example: (Refer to ScalaDB syntax)
        //   tab.aggregate(age.max).groupByInterval(sal \ 100) 
        // will be mapped to
        //   SELECT MAX(age) as foo, 100*round(sal/100, 0) as bar from tab GROUP BY bar
        // 
        // MAX(age) will be read using alias foo
        // 
        // Similarly, the following 
        //   tab.aggregate(age.max, sal.topWithInterval(100)).groupByInterval(sal \ 100)  ------------> (1)
        // will also be mapped to
        //   SELECT MAX(age) as foo, 100*round(sal/100, 0) as bar from tab GROUP BY bar
        // 
        // Thus, Top does not have an affect.
        // 
        // The sal.topWithInterval(100) will be read using bar. Thus, the data for sql query corresponding to the Top column will be populated by the group-by-interval information
        // (data will exist for a composite column such as: tab.aggregate(age.max, (sal+3).topWithInterval(100)).groupByInterval(sal+3 \ 100) 
        // 
        // Note:
        //   tab.aggregate(age.max, sal \ 100).groupByInterval(sal \ 100)  ------------> (2)
        //   tab.aggregate(age.max, sal.top).groupByInterval(sal \ 100)  ------------> (3)
        // 
        // are both shorthand for (1)
        // 
        // In (3), we automatically infer to use the group-by-interval (sal \ 100) because this is the only group-by-interval having sal
        // If there are more than one using sal then we need to use (1) or (2)
        //   
      case _ => col.compositeColData        
    }
  }
  lazy val aggrHash = compositeAggrData.map(_._1).foldLeft(hash(aggrSQLString+aggr.getClass.getSimpleName))((x, y) => hash(x+y))+aggr
  // adding aggr.getClass.getSimpleName because Aggregate(x, First) and Aggregate(x, Last) will have same hash otherwise, because both map to GROUP_CONCAT
  lazy val alias = aggrHash // hash(tableColName)

  lazy val canDoAggregate = (col.colType, aggr) match {
    case (INT | LONG | UINT(_) | ULONG(_) | BIGDEC(_, _)| UBIGDEC(_, _)| TIMESTAMP | CompositeCol(_,_,_), _) => true // should we allow aggregate for composite cols?
    case (_, Count) => true
    case (_, GroupBy) => true
    // case (BIGDEC(_, _), Max | Min) => true
    case _ => false
  }
  def +(rhs:Any)    = Aggregate(col, CompositeAggr(this, Add, rhs)) // keeping col for now
  def -(rhs:Any)    = Aggregate(col, CompositeAggr(this, Sub, rhs)) // keeping col for now
  def /(rhs:Any)    = Aggregate(col, CompositeAggr(this, Div, rhs)) // keeping col for now
  def *(rhs:Any)    = Aggregate(col, CompositeAggr(this, Mul, rhs)) // keeping col for now
  def %(rhs:Any)    = Aggregate(col, CompositeAggr(this, Mod, rhs)) // keeping col for now
  def \(interval:Long)    = {
    aggr match {
      case Top => col.topWithInterval(interval)
      case _ => throw DBException("cannot do interval for aggregate "+aggr)
    }
  }
  def withInterval(interval:Long) = \(interval)
  def to(table:Table):Aggregate = {
    aggr match {
      case CompositeAggr(lhs, oper, rhs) => Aggregate(col, CompositeAggr(lhs.to(table), oper, rhs.to(table)))
      case any => Aggregate(col.to(table), any) 
    }
  }  

  lazy val compositeAggregates:Array[Aggregate] = {
    aggr match {
      case Top => throw DBException("cannot obtain composite aggregates for: "+aggr)
      case CompositeAggr(left, aggrOp, right:Aggregate) => left.compositeAggregates ++ right.compositeAggregates
      case CompositeAggr(left, aggrOp, right) => left.compositeAggregates 
      case _ => Array(this)
    }
  }
  override def toString = aggr+"("+col+")"
  // helper methods for wheres

  // following three lines based on this answer 
  // http://stackoverflow.com/a/34809016/243233
  private var x0: Having = _
  def value = x0
  def value_=(data: Any) = Having(this, Eq, data)  

  def === (rhs:Any)    = Having(this, Eq, rhs)
  def from (rhs:Nested)    = Having(this, From, rhs)  
  def in (rhs:Nested)    = Having(this, In, rhs)
  def notIn (rhs:Nested)    = Having(this, NotIn, rhs)
  def <=(rhs:Any)    = Having(this, Le, rhs)
  def >=(rhs:Any)    = Having(this, Ge, rhs)
  def <(rhs:Any)    = Having(this, Lt, rhs)
  def >(rhs:Any)    = Having(this, Gt, rhs)
  def <>(rhs:Any)    = Having(this, Ne, rhs)
  
}
