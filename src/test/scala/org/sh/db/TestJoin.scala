
package org.sh.db

import org.sh.db._
import org.sh.db.core.DataStructures._
import org.sh.db.core._
import ScalaDB._

object TestJoin extends App {
  import Main._
  case class Person(uid:String, email:String, value:BigDecimal)
  def arrayToPerson(a:Array[Any]) = {
    Person(a(0).asInstanceOf[String], a(1).asInstanceOf[String], a(2).asInstanceOf[BigDecimal])
  }
  val (uid, email, value, num) = (Col("uid", VARCHAR(255)), Col("email", VARCHAR(255)), Col("value", UBIGDEC(100, 0)), Col("num", ULONG))
  implicit val config = MyDBConfig
  val dbm1 = Tab.withName("user1").withCols(uid, email, value, num).withPriKey(uid)
  val dbm2 = Tab.withName("user2").withCols(uid, value).withPriKey(uid)
  dbm1.deleteAll
  dbm2.deleteAll
  try {
    Util.printSQL = true
    dbm1.insert("ajit", "hello", BigInt(922337203675806L), 34)
    //    dbm1.insert("ajit", "hello", BigInt(9223372036854775806L), 34)
    dbm1.insert("alice", "hi", BigInt(92036854775806L), 349494)
    Util.printSQL = false
    dbm1.increment(num ++= 2444).where(uid === "ajit")
    dbm1.selectStar.asList.foreach(println)
    println( "Aggregate num.sum = "+dbm1.aggregate(num.sum).firstAsBigInt)
    dbm1.insert("ravi", "hi", 51222, 2)
    dbm2.insert("ravi", 1)
    // dbm2.insert("ravi", 2) // should throw exception; duplicate primary key
    dbm1.select(uid, email, value).where(uid like "ami%").as(arrayToPerson) foreach println
    dbm1.select(uid, email, value.of(dbm2)).where(
      value >= 5 
      and 
      (value >= value.of(dbm2) or value === value.of(dbm2)) 
      or 
      value.of(dbm1) <> value.of(dbm2) * (value.of(dbm1) + 5)
      or 
      value <> value.of(dbm2) + value.of(dbm1)
    ).as(arrayToPerson) foreach println 
  } catch {
    case e:Throwable => 
      e.printStackTrace
      println(e.getMessage)
  } finally { 
    System.exit(1)
  }
}















