
package org.sh.db.core.DataStructures

import org.sh.db.core.Table
import org.sh.db.core.Util

case class HavingJoin(left:Having, filterJoinOp:WhereJoinOp, right:Having) extends Op {
  def to(table:Table):HavingJoin = HavingJoin(left.to(table), filterJoinOp, right.to(table))
}
case class Having(aggregate:Aggregate, op:Op, data:Any) {
  if (!(aggregate.isComposite || nativeAggrs.contains(aggregate.aggr)))
    throw new DBException("invalid aggregate["+aggregate+"] for HAVING in table["+aggregate.col.optTable.getOrElse("none")+"]")
  def checkValidOp = if (!Util.canDoComparison(aggregate, op)) 
    throw new DBException("invalid op["+op+"] for aggregate["+aggregate+"] in table["+aggregate.col.optTable.getOrElse("none")+"]")
  def to(table:Table):Having = op match {
    case whereJoin:HavingJoin => Having(aggregate, whereJoin.to(table), data) // keeping (col, data) same instead of (col.to(table), data.to(table)
    case _ => Having(aggregate.to(table), op, data.to(table))
  }
  /////////////////// need to change below ////////////////////
  def isDataAnotherAggregate = data match {
    case _:Aggregate => true
    case _ => false
  }
  /////////////////// need to change below ////////////////////
  lazy val havingSQLString:String = op match {
    case HavingJoin(left, whereOp, right) => "("+left.havingSQLString+" "+whereOp+" "+right.havingSQLString+")"        
    case _ => 
      aggregate.aggrSQLString +" "+op+ " "+ (
        (op, data) match {
          case (From, n:Nested) => n.alias(aggregate.alias, op)+"."+n.nestedColAlias
          case (From, n) => throw DBException("from operation must map to NestedSelect. Found: "+n+" of type: "+n.getClass)
          case (_, any) => any.anySQLString
        }
      )
  }
  /////////////////// need to change below ////////////////////
  lazy val compositeHavings:Array[Having] = {
    op match {
      case HavingJoin(left, whereOp, right) => left.compositeHavings ++ right.compositeHavings
      case _ => Array(this)
    }
  }
  lazy val compositeHavingsData:Array[(Any, DataType)] = compositeHavings.flatMap(h => 
    // first block is for composite cols .. select * from T where a+4 = 5. The first block handles the data (i.e., 4) in (a+4)
    h.aggregate.compositeAggrData ++ (
      (h.op, h.data) match {
        case (_, a:Aggregate) => a.compositeAggrData
        case (From, n:Nested) => 
          Nil 
        case (op, n:Nested) => 
          n.nestedData
        case (_ ,d:Any) => List((d, h.aggregate.col.colType))
      }
    )
  )

  lazy val nestedHavingData: Array[(Any, DataType)] = compositeHavings.flatMap{ // List.. ordering needs to be preserved because we need to set data
    case Having(_, From, n:Nested) => n.nestedData
    case _ => Array[(Any, DataType)]()
  }
  lazy val nestedHavingTables:Array[String] = compositeHavings.flatMap{ // List.. ordering needs to be preserved because we need to set data
    case Having(aggregate, op@From, n:Nested) => List(n.nestedSQLString + " AS "+n.alias(aggregate.alias, op))
    case _ => Nil
  }
  def or (where:Having) = Having(aggregate, HavingJoin(this, Or, where), data) // keeping col and data same for now. These should never be accessed though, so can be set to null
  def and (where:Having) = Having(aggregate, HavingJoin(this, And, where), data) // keeping col and data same for now
  // validation for ops
  op match {
    case Like | NotLike | RegExp | NotRegExp => 
      data match {
        case s:String =>
        case any => throw new DBException("Operation: ["+op+"] accepts only strings as data. Found: "+any.getClass.getCanonicalName)
      }
    case HavingJoin(left, _ , right) if right == left => throw new DBException("HavingJoin: ["+this+"] has same left and right havings: "+left)
    case _ => 
  }
  data match {
    case a:Aggregate if aggregate == a && a.col.optTable == aggregate.col.optTable =>  throw new DBException("Having: ["+this+"] has same left and right aggregates: "+a)
    case _ =>
  }
  override def toString = s"HAVING $aggregate $op $data"
}
