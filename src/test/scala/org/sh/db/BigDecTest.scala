package org.sh.db

import org.sh.db.BetterDB._
import org.sh.db.config.TraitDBConfig
import org.sh.db.core.DataStructures._
import org.sh.utils.common.Util.randomAlphanumericString

object BigDecTest {
  object MyDBConfig extends TraitDBConfig {
    val dbname:String = "mydb_bigDecTest"
    val dbhost:String = "localhost"
    val dbms:String = "h2"  // e.g. mysql or h2 or postgresql
    val dbuser:String = "alice"
    val dbpass:String = "abcdefg"
    val connTimeOut:Int = 1000 // seconds
    val usePool:Boolean = true // e.g. true (use db pool)
    val configSource = "test"
  }
  def main(args:Array[String]):Unit = {}

  val STR = VARCHAR(255)
  val name = Col("name", STR)
  val age = Col("age", UINT) // unsigned int
  val sal = Col("sal", UBIGDEC(100, 10)) // unsigned BigDecimal with size 100 and precision 10 decimal digits
  val key = Col("key", ULONGAuto) // auto incremented key

  try {
    val tab1 = Tab.withName("tab1"+randomAlphanumericString(10)).withConfig(MyDBConfig).withCols(name, key, age, sal).withPriKey(key)
    val tab2 = Tab.withName("tab2"+randomAlphanumericString(10)).withConfig(MyDBConfig).withCols(name, age, sal).withPriKey()

    tab1.insert("alice", 20, 25000)
    tab1.insert("bob", 20, 10)
    tab1.insert("arun", 25, 20000)
    tab1.insert("ajit", 30, 30000)
    tab1.insert("ajit", 19, 10000)
    tab1.insert("max", 59, 50000)

    tab2.insert("alice", 20, 25000)
    tab2.insert("bob", 20, 10)
    tab2.insert("arun", 25, 20000)
    tab2.insert("ajit", 30, 30000)
    tab2.insert("ajit", 19, 10000)
    tab2.insert("max", 59, 50000)

    assert(tab1.insert("alice", 28, BigDecimal("1800000000000000000000")) == 7) // will return auto incremebted key
    assert(tab2.insert("alice", 28, BigDecimal("1800000000000000000000")) == 1) // without auto incrmented key. Will return num rows affected.

    assert(tab1.insert("bob2", 43, BigDecimal("480099999999999999999990")) == 8)
    assert(tab1.insert("bob1", 44, BigDecimal("450099999999999999999990")) == 9)
    assert(tab1.increment(sal ++= 22222).where(name === "bob2") == 1)

    val (numRows, lastValues) = tab1.incrementNew(sal ++= 22222).where(name === "bob2")
    assert(numRows == 1)
    assert(lastValues(0).as[BigDecimal] == BigDecimal("480100000000000000022212"))

    tab1.selectStar.asList foreach println // prints all rows
    tab1.select(key, name).where(key > 5).as{a =>
      (a(0).as[Long], a(1).as[String])
    }.foreach(println)

  } catch {
    case t:Throwable => t.printStackTrace
  } finally System.exit(1)
}
