
package org.sh.db

import org.sh.db.core.Util
import org.sh.db.{DBManager => DBM}
import org.sh.db.ScalaDB._
import org.sh.db.core.DataStructures._

object Test extends App{
  Util.printSQL = true;
  Util.printSQL_? = true;
  val STR = VARCHAR(255)
  val a = Col("a", INT)
  val b = Col("b", STR)
  val d = DBM("db")(a, b)(a) // shorthand for val d = Tab.withName("db").withCols(a, b).withPriKey(a)
  try {
    d.deleteAll
    d.insert(1, "a")
    d.insert(2, "b")
    d.insert(3, "c")
    d.insert(4, "d")
    d.insert(5, "e")
    d.insert(6, "f")
    println(" 1: "+Where(a, In, Array[Long](1L, 2L, 3L)).whereSQLString)
    println(" 2: "+(a in Array[Long](1L, 2L, 3L)).whereSQLString) // same as above
    println(" 3: "+Where(a, In, Array[Int](1, 2, 3)).whereSQLString)
    println(" 4: "+(a in Array[Int](1, 2, 3)).whereSQLString) // same as above
    println(" 5: "+Where(a, In, Array("1", "2", "3")).whereSQLString)
    println(" 6: "+(a in Array("1", "2", "3")).whereSQLString) // same as above
    d.select(a).where(b in Array("a", "d", "c")).asList foreach println
  } catch {
    case a:Any => a.printStackTrace 
  } finally {
    System.exit(0)
  }
}
