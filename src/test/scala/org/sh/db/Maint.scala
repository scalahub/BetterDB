
package org.sh.db

import org.sh.db.config.TraitDBConfig
import org.sh.db.core.DataStructures._
import org.sh.db.core._

object Maint extends App{
  case class Person(uid:String, email:String, value:BigInt, scan:Array[Byte])
  def arrayToPerson(a:Array[Any]) = {
    Person(a(0).asInstanceOf[String], a(1).asInstanceOf[String], a(2).asInstanceOf[BigInt], a(3).asInstanceOf[Array[Byte]])
  }
  object OldConfig extends TraitDBConfig {
    val dbname:String = "mydbOld"
    val dbhost:String = "localhost"
    val dbms:String = "h2"  // e.g. mysql or h2 or postgresql
    val dbuser:String = "aliceOld"
    val dbpass:String = "abcdef"
    val connTimeOut:Int = 1000 // seconds
    val usePool:Boolean = true // e.g. true (use db pool)
    val configSource = "test"
  }
  object NewConfig extends TraitDBConfig {
    val dbname:String = "mydbNew"
    val dbhost:String = "localhost"
    val dbms:String = "h2"  // e.g. mysql or h2 or postgresql
    val dbuser:String = "aliceNew"
    val dbpass:String = "xyzw"
    val connTimeOut:Int = 1000 // seconds
    val usePool:Boolean = true // e.g. true (use db pool)
    val configSource = "test"
  }
  foo
  import ScalaDB._
  def foo = try {
    val (uid, email, value, scan) = (Col("uid", VARCHAR(255)), Col("email", VARCHAR(255)), Col("value", new UBIGDEC(100)),
                                     Col("scan", BLOB))
    val dbmOld = Tab.withName("blobsOldTable").withConfig(NewConfig).withCols(uid, email, value, scan).withPriKey(uid)
    val dbmNew = Tab.withName("blobsNewTable").withConfig(NewConfig).withCols(uid, email, value, scan).withPriKey(uid)
    import org.sh.utils.Util._
    dbmOld.insertArray(Array("hey"+randomAlphanumericString(10), "hello", BigInt(9223372036854775806L), Array[Byte](33, 43)))
    println (" dmbOld : read following ")
    dbmOld.select(uid, email, value, scan).as(arrayToPerson) foreach println
    dbmNew.deleteAll
    println (" dmbNew : read following before import")
    dbmNew.select(uid, email, value, scan).as(arrayToPerson) foreach println
    val testFile = "test12345"+randomAlphanumericString(5)+".txt"
    org.sh.utils.file.Util.deleteFile(testFile)
    dbmOld.exportToCSV(testFile, Array())
    //    dbmNew.updatePassword("xyzw")
    dbmNew.importFromCSV(testFile)
    dbmNew.select(uid, email, value, scan).as(arrayToPerson) foreach println
  } catch {
    case e:Throwable => 
      e.printStackTrace
      println(e.getMessage)
  } finally { 
    println("finally")
    System.exit(1)
  }
}
