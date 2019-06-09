package org.sh.db

import org.sh.db.core.DataStructures._
import org.sh.utils.Util._

import org.sh.db.BetterDB._

/**
 * purpose of this code:
 * 
 * Sometimes we have two tables (say INR withdraws and BTC withdraws) as below
 * 
 * Table INR withdraws                                                            Table BTC withdraws
-------------------------------------------                                       --------------------------------------------
| time | withdrawID | Amount | userID | ...                                       | time | withdrawID |  Amount | userID | ... 
|------|------------|--------|--------|----                                       -------|------------|-----------------------
| 23   | wjdejduejd | 34544  | alice  | ...                                       | 20   | ecjerjcruc |  146    | alice  | ...
| 45   | gtfcnmecnv | 4434   | bob    | ...                                       | 29   | roijfoirjf |  444    | carol  | ...
| 54   | 4jto4rkmkc | 3444   | alice  | ...                                       | 34   | i4jf4jifjj |  3944   | carol  | ...

 * 
 * Sometimes we may need a combined table for both INR and BTC withdraws sorted by time and search by max/offset
 * This is not possible with two separate tables
 * 
 * The MergedIndex object below takes care of this
 * 
 * It provides a "merged table" M with the above two tables as "left-half" and "right-half" of that table
 * 
 * The HalfTable encapsulates each half table
 * 
 * case class HalfTable(db:DBManager, indexCol:Col, priKeyCol:Col, filterCol:Col, wheres:Where*){    
 * 
 * If sorting by time, then indexCol will be time.
 * Each table should have a primary key column that can be uniqely used to reference a row
 * Finally the filterCol is the one we will use to filter the results by (say userID).
 * Each half table is declared using the above rule.
 * 
 * The resulying MergedTable M will appear like this
--------------------------------------
| indexCol |  priKeyCol  | filterCol |
|----------|-------------|-----------|
| 23       | wjdejduejd  |  alice    |
| 45       | gtfcnmecnv  |  bob      |
| 54       | 4jto4rkmkc  |  alice    |
| 20       | ecjerjcruc  |  alice    |
| 29       | roijfoirjf  |  carol    |
| 34       | i4jf4jifjj  |  carol    |

 When giving a from-to search query (possibly with a filter on userID), the resulting output will be a (nested query) representing the 
 set of ids from the priKeyCol that can be used to search the original tables 
 
 so suppose if we search for time =< 30 and time >= 20 in decreasing order, the the getPriKeys will return a nested query that returns the following keys
 
 ecjerjcruc (corresponding to indexCol 20)
 wjdejduejd (corresponding to indexCol 23)
 roijfoirjf (corresponding to indexCol 29)
 
 similarly if we search for time =< 30 and time >= 20 in decreasing order with filter == "alice", the the getPriKeys will return a nested query that returns the following keys
 
 ecjerjcruc (corresponding to indexCol 20)
 wjdejduejd (corresponding to indexCol 23)
 
 *** A Nested query is an "lazy query", i.e., a query exactly like a normal query except that it has not yet been run. However, when it will run, it will return the above data.
 * Examples of nested queries:
 * 
 * SELECT amount FROM T1, (SELECT ID FROM .. AS T2) where T1.ID = T2.ID
 * 
 *  OR 
 *  
 * SELECT amount FROM T1 where T1.ID IN (SELECT ID FROM T2)
 * 
 * In both examples the value inside ( ) is a nested query
 * 
 *
 
 Now suppose M is going to used as a half table in another merged table T (say, transactions), then we can use connectToLeftOf to connect to left of T
 (we can use connectToRightOf to connect to right)
 
 Then whenever any entry is added to INR withdraws, it will first cause an entry to be added to M and then (through the chaning) to T
 (Note that we could have done the same thing by manually creating a MergedTable using M and accessing the values (indexCol, priKeyCol, filterCol) of that)
 
 * 
 */
object MergedIndex {
  val LeftHalf = "L"
  val RightHalf = "R"
  val whichHalfCol = Col("whichHalf", VARCHAR(1))
  case class HalfTable(db:DBManager, indexCol:Col, priKeyCol:Col, filterCol:Col, wheres:Where*){    
    // example: 
    // indexCol = time (for doing >=, <=, <, >, etc)
    // priKeyCol = orderID
    // filterCol = userID  (for === match ONLY) 
    val table = db.getTable
    Array(indexCol, priKeyCol, filterCol).foreach{c => 
      val tab = if (c.optTable.isDefined) c.optTable.get else table
      tab.assertColExists(c)
    }
    def where(wheres:Where*) = HalfTable(db:DBManager, indexCol:Col, priKeyCol:Col, filterCol:Col,this.wheres ++ wheres : _*)
    def hash = shaSmall(db.table.tableName + indexCol.alias + priKeyCol.alias)    
  }
  private def getIndexTableName(left:HalfTable, right:HalfTable) = {    
    shaSmall(left.hash + right.hash)+"INDX_v13"
  }
  private val indexColName = "indexCol"
  private val priKeyColName = "priKeyCol"
  private val filterColName = "filterCol"
  
  case class MergedTable(left:HalfTable, right:HalfTable) { // maxRows are number of rows in this
    Array((left.indexCol, right.indexCol), 
          (left.priKeyCol, right.priKeyCol), 
          (left.filterCol, right.filterCol)) foreach{
      case (Col(lhsJavaType, CONST(_, lhsColType), _), Col(rhsJavaType, CONST(_, rhsColType), _)) if lhsJavaType == rhsJavaType && lhsColType == rhsColType => 
      case (localCol, remoteCol) => 
        if (localCol.colType != remoteCol.colType) throw new DBException("local remote index mismatch. local: "+localCol.colType+", remote: "+remoteCol.colType)
    }
    if (!left.indexCol.colType.isSortable) throw new DBException("index col type must be sortable: "+left.indexCol.colType) 
    
    val indexCol = Col(indexColName, left.indexCol.colType)
    val priKeyCol = Col(priKeyColName, left.priKeyCol.colType)
    val filterCol = Col(filterColName, left.filterCol.colType match {
        case CONST(_, dataType) => dataType
        case any => any
      }
    )

    val indexTableName = getIndexTableName(left:HalfTable, right:HalfTable)
    val mergedDB = DBManager(indexTableName)(indexCol, priKeyCol, filterCol, whichHalfCol)(priKeyCol)    
    mergedDB.indexBy(indexCol) // comment out the merged index (to see if any perf difference.. earlier was hanging)
    mergedDB.indexBy(filterCol) // comment out the merged index (to see if any perf difference.. earlier was hanging)

    // populates from existing tables    

    // This complete table can be the sub-table (either left or right) of another table.
    // This can be used to internally chain many sub-tables.
    // Following defines those tables
    // Int is given to ensure no duplicates (sometimes Play initializes an object more than once)
    private var connectedToLeftOf:Map[Int, MergedTable] = Map() // defines MergedTable(s) where this MergedTable's mergedDB maps to left of that MergedTable    
    private var connectedToRightOf:Map[Int, MergedTable] = Map() // defines MergedTable(s) where this MergedTable's mergedDB maps to right of that MergedTable    
    def connectToLeftOf(int:Int, mergedTable:MergedTable) = connectedToLeftOf += (int -> mergedTable)
    def connectToRightOf(int:Int, mergedTable:MergedTable) = connectedToRightOf += (int -> mergedTable)
    private def insertIntoSuperTables(indexData:Any, priKeyData:Any, filterData:Any):Unit = {
      connectedToLeftOf.foreach{case (_, superTable) => superTable.addLeft(indexData, priKeyData, filterData)}
      connectedToRightOf.foreach{case (_, superTable) => superTable.addRight(indexData, priKeyData, filterData)}
    }
    def addLeft(indexData:Any, priKeyData:Any, filterData:Any):Unit = {
      mergedDB.insert(indexData, priKeyData, filterData, LeftHalf)
      insertIntoSuperTables(indexData:Any, priKeyData:Any, filterData)
    }
    def addRight(indexData:Any, priKeyData:Any, filterData:Any):Unit = {
      mergedDB.insert(indexData, priKeyData, filterData, RightHalf)
      insertIntoSuperTables(indexData:Any, priKeyData:Any, filterData)
    }
    
    @deprecated("Nested queries take long time if the merged index table has > 10k entries. Use getPriKeyTagged instead")
    def getPriKeysNested(from:Any, to:Any, mergedMax:Int, mergedOffset:Long, isDecreasingOrder:Boolean) = {
      mergedDB.select(priKeyCol).where(
        indexCol >= from, 
        indexCol <= to
      ).orderBy(indexCol.decreasing).max(mergedMax).offset(mergedOffset).nested
    }
    // below filter will be userID
    @deprecated("Nested queries take long time if the merged index table has > 10k entries. Use getPriKeyTaggedWithFilter instead")
    def getPriKeysNestedWithFilter(filter:Any, from:Any, to:Any, mergedMax:Int, mergedOffset:Long, isDecreasingOrder:Boolean) = {
      mergedDB.select(priKeyCol).where(
        indexCol >= from, 
        indexCol <= to,
        filterCol === filter
      ).orderBy(indexCol.decreasing).max(mergedMax).offset(mergedOffset).nested
    }
    def getPriKeysTagged(from:Any, to:Any, mergedMax:Int, mergedOffset:Long, isDecreasingOrder:Boolean) = {
      // tagged implies that it returns also whether the key belongs to left or right
      // we use a boolean variable isLeft to indicate if this is left. If true, then left, else right
      mergedDB.select(priKeyCol, whichHalfCol).where(
        indexCol >= from, 
        indexCol <= to
      ).orderBy(indexCol.decreasing).max(mergedMax).offset(mergedOffset).as(ar => (ar(0), ar(1).as[String] == LeftHalf))
    }
    // below filter will be userID
    def getPriKeysTaggedWithFilter(filter:Any, from:Any, to:Any, mergedMax:Int, mergedOffset:Long, isDecreasingOrder:Boolean) = {
      mergedDB.select(priKeyCol, whichHalfCol).where(
        indexCol >= from, 
        indexCol <= to,
        filterCol === filter
      ).orderBy(indexCol.decreasing).max(mergedMax).offset(mergedOffset).as(ar => (ar(0), ar(1).as[String] == LeftHalf))
    }
    def getPriKeyCount(from:Any, to:Any) = {
      mergedDB.aggregate(priKeyCol.count).where(
        indexCol >= from, 
        indexCol <= to
      )
    }.asLong(0).as[Long]
    // below filter will be userID
    def getPriKeyCountWithFilter(filter:Any, from:Any, to:Any) = {
      mergedDB.aggregate(priKeyCol.count).where(
        indexCol >= from, 
        indexCol <= to,
        filterCol === filter
      ).firstAsLong
    }
    def populate = { 
      if (mergedDB.isNonEmpty) throw new DBException("merged table is non-empty. Cannot populate")
      left.db.select(left.indexCol.to(left.db.getTable), left.priKeyCol.to(left.db.getTable), left.filterCol.to(left.db.getTable), LeftHalf.const).where(
        left.wheres.map(_.to(left.db.getTable)): _*
      ).into(mergedDB)
      right.db.select(right.indexCol.to(right.db.getTable), right.priKeyCol.to(right.db.getTable), right.filterCol.to(right.db.getTable), RightHalf.const).where(
        right.wheres.map(_.to(right.db.getTable)): _*
      ).into(mergedDB)
      getInfo
    }
    def getInfo = {
      Array(
        "Populated "+mergedDB.countAllRows+" rows",
        mergedDB.countWhere(whichHalfCol === LeftHalf)+" LEFT", 
        mergedDB.countWhere(whichHalfCol === RightHalf)+" RIGHT"
      )      
    }
    def repopulate = {
      mergedDB.deleteAll
      populate
    }
    if (!mergedDB.isNonEmpty) {
      populate
    }
  }  

}
