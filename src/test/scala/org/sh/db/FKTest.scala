
package org.sh.db

import org.sh.db.ScalaDB._
import org.sh.db.config.TraitDBConfig
import org.sh.db.core.DataStructures._
import org.sh.db.core._

//import org.sh.db.core.fk._
import org.sh.db.core.FKDataStructures._
import org.sh.db.{DBManager => DBMgr}
import org.h2.jdbc.JdbcSQLException

object FKTest {
  implicit object MyDBConfig extends TraitDBConfig {
    val dbname:String = "fktest"
    val dbhost:String = "localhost"
    val dbms:String = "h2"  // e.g. mysql or h2 or postgresql
    val dbuser:String = "alice"
    val dbpass:String = "testpass"
    val connTimeOut:Int = 1000 // seconds
    val usePool:Boolean = true // e.g. true (use db pool)
    val configSource = "test"
  }
  Class.forName("org.h2.Driver")

  def main(args:Array[String]):Unit = {}
  case class Person(uid:String, email:String, bal:BigInt)
  def arrayToPerson(a:Array[Any]) = Person(a(0).as[String], a(1).as[String], a(2).as[BigInt])
  try {
    val (userid, email, bal) = (Col("userid", VARCHAR(255)), Col("email", VARCHAR(20)), Col("value", UScalaBIGINT(100))) 
    val (itemid, desc) = (Col("itemid", VARCHAR(255)), Col("desc", VARCHAR(255))) 
    val (orderid, amt) = (Col("orderid", VARCHAR(255)), Col("amt", UINT)) 

    val userTab = Tab.withName("users").withCols(userid, email, bal).withPriKey(userid) // implicit config from above
    val orderTab = Tab.withName("orders").withConfig(MyDBConfig).withCols(userid, orderid, itemid, amt).withPriKey(userid) // explicit config
    val itemTab = Tab.withName("items").withCols(itemid, desc).withPriKey(itemid) // implicit config

    orderTab.deleteAll
    itemTab.deleteAll
    userTab.deleteAll
    
    orderTab.addForeignKey(userid).toPriKeyOf(userTab).onDeleteRestrict.onUpdateCascade // added to table containing foreign key, not primary key
    orderTab.addForeignKey(itemid).toPriKeyOf(itemTab).onDeleteRestrict.onUpdateCascade
    
    userTab.insert("user1", "user1@email.com", BigInt(922337)) // can insert array. Use below for most cases
    userTab.insert("user2", "user2@email.com", 34344) // better syntax for insert
    itemTab.insert("item1", "item1Desc")
    orderTab.insert("user1", "order1", "item1", 20)
    
    val orders = orderTab.selectStar.asList 
    
    assert(orders.size == 1)
    assert(orders(0) == List("user1", "order1", "item1", 20))
    
    assertException(classOf[JdbcSQLException])(userTab.deleteAll) // should prevent due to FK constraint (Delete Restrict)
    
    userTab.update(userid <-- "user1New").where(userid === "user1") // should update both users and orders table due to Update Cascade
    itemTab.update(itemid <-- "item1New").where(itemid === "item1") // should update both items and orders table due to Update Cascade
    
    val ordersNew = orderTab.selectStar.asList // should have updated userID, itemID
    assert(ordersNew(0) == List("user1New", "order1", "item1New", 20))
    
    userTab.select(userid, email, bal).as(arrayToPerson) foreach println // prints all users
    
    println("Foreign Key: all tests passed")
  } catch {
    case e:Throwable => 
      e.printStackTrace
  } finally { 
    System.exit(1)
  }
  def assertException[T <: Exception, S](e:Class[T])(f: => S) = 
    try {f; assert(false) } catch { case a:Any => assert(a.getClass == e) }

}
