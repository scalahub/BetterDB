
package org.sh.db

import org.sh.db.config.TraitDBConfig
import org.sh.db.core.DataStructures._
import org.sh.db.core._
import org.sh.db.{DBManager => DB}
import ScalaDB._
import ScalaDB.Implicits._

object MyDBConfig2 extends TraitDBConfig {
  val dbname:String = "dbtest2"
  val dbhost:String = "localhost"
  val dbms:String = "h2"  // e.g. mysql or h2 or postgresql
  val dbuser:String = "alice"
  val dbpass:String = "pass"
  val connTimeOut:Int = 1000 // seconds
  val usePool:Boolean = true // e.g. true (use db pool)
  val configSource = "test"
}
object MyDBConfig3 extends TraitDBConfig {
  val dbname:String = "dbtest3"
  val dbhost:String = "localhost"
  val dbms:String = "h2"  // e.g. mysql or h2 or postgresql
  val dbuser:String = "alice"
  val dbpass:String = "pass"
  val connTimeOut:Int = 1000 // seconds
  val usePool:Boolean = true // e.g. true (use db pool)
  val configSource = "test"
}

object MainBasic {
  val STR = VARCHAR(255)
  val name = Col("NAME", STR)
  val age = Col("AGE", UINT)
  val sal = Col("SAL", UINT)
  val isMale = Col("isMale", BOOLEAN)

  // Alternate   val db = DB("USERS")(name, age, sal, isMale)(name, age)
  //                          NAME          COLUMNS            PRI-KEYS

  val db = Tab.withName("USERS").withCols(name, age, sal, isMale).withPriKey(name, age)

  case class User(name:String, age:Int, sal:Int, isMale:Boolean)
  object User{def apply(a:Array[Any]) = new User(a(0).as[String], a(1).as[Int], a(2).as[Int], a(3).as[Boolean])}
  def asUser(a:Array[Any]) = User(a)
  
  def main(a:Array[String]) {
    try {    
      db.deleteAll
      insert
      println (" Aggregate => "+db.aggregate(sal.max).where(name === "alice").firstAsLong)
    } catch {
      case t:Throwable => t.printStackTrace
    } finally System.exit(1)
  }
  var ctr = 0
  def insert = {    
    db.insert("alice", 20, 25000, 1)
    db.insert(Array("bob", 20, 25000, true))
    db.insertArray(Array("carol", 25, 20000, false))
  }
  def colTest {
    Col("2", INT) === "3"
    Col("2", INT) <> "3"
    Col("3", INT) like "4"
    val a = Col("a", INT)
    val b = Col("b", INT)
    a + (b + 1)
  }
  def sel = db.select(sal).where(sal > 4000).orderBy(sal.decreasing).max(10).offset(2)

  def select0 = { 

    //    val u = (name:Aggregate) <> "hello"
    //    val v = name.groupBy <> "hello"
    //    val a = (name <> "hello"):Having
    //    val b = name <> "hello"
    //    val c:Aggregate = name

    val d = sal from sel // implicit conversion from Sel[_] to Nested
    val g = sel.nested
    g.debugExecute foreach println
    db.update(name <-- "hello")
    db.select(age).where(sal from g).execute foreach println
    db.select(age).where(sal in g).execute foreach println
    db.select(age).where(sal notIn g).execute foreach println
    db.select(age).where(sal <= g).execute foreach println
    db.select(age).execute foreach println
  }
  def select0Bak = { 
    db.select(
      (age.of(db) -10) + (sal.of(db) - 60000) * age.of(db), 
      age,constCol("he"),
      34 / 5,
      34 / age,
      sal
    ).where( 
      sal / 1000 >= age - 10 or // and // <--------------------------- (AND)
      age < 100
    ).orderBy(
      (age+sal+2, Decreasing),
      age.decreasing,
      sal
    ).max(
      10
    ).offset(
      0 
    ).as(
      _.toList
    ).foreach(println)
  }
  def aggregate1 = {    

    db.aggregate(sal.last, (sal / 2).top).where(age <= 100 and age >= 10).groupByInterval((sal / 2) \ 40, age \ 40).asLong.map(_.toList).foreach(println)
    
    db.aggregate(sal.last, age.avg, sal \ 10).groupByInterval(sal \ 10).asLong.map(_.toList).foreach(println)

    db.aggregate(
      (sal/2).sum / (age*sal).avg, 
      sal.avg, 
      sal.sum / sal.avg, 
      sal.count, 
      (sal / 2) \ 40
    ).where(
      age <= 100 or age >= 10
    ).groupByInterval(
      (sal / 2) \ 40, // with interval 40, will be used as a group-by-interval
      age \ 3 // with interval 0, will be used as a grou-by
    ).asLong.map(
      _.toList
    ).foreach(println)

    db.aggregate(
      (sal/2).sum / (age*sal).avg, 
      sal.avg, 
      sal.sum / sal.avg, 
      sal.count,
      age \ 3,
      (sal / 2)
    ).where(
      age <= 100 and age >= 10
    ).groupByInterval(
      (sal / 2) \ 40, 
      age \ 3
    ).groupBy(
      (sal / 2),
      name
    ).asLong.map(
      _.toList
    ).foreach(println)

    db.aggregate(
      (sal/2).sum * 1000 / (age*sal).avg, 
      sal.avg, 
      sal.sum / sal.avg, 
      sal.count
    ).where(
      age <= 100 and age >= 10
    ).asLong.foreach(println)
  }

  def select1 { 
    println("CTR => "+ctr); ctr += 1

    db.select(age).as(_.toList) foreach println
  }
  def select2 { 
    println("CTR => "+ctr); ctr += 1

    db.select((age.of(db) + sal.of(db)) * age.of(db)).where(sal + 2 >= 10).as(_.toList) foreach println
  }
  def select3 { 
    println("CTR => "+ctr); ctr += 1

    db.select(age.of(db) + sal.of(db) * age.of(db)).where(sal + 2 >= 10).as(_.toList) foreach println
  }
  def select4 { 
    println("CTR => "+ctr); ctr += 1

    db.select(
      (age.of(db) -10) + (sal.of(db) - 60000) * age.of(db), 
      age, 
      sal
    ).where( 
      sal / 1000 >= age - 10 or // and // <--------------------------- (AND)
      age < 100
    ).orderBy(
      (age+sal+2, Decreasing)
    ).max(
      10
    ).offset(
      0
    ).as(
      _.toList
    ).foreach(println)

  }
  def select5 { 
    println("CTR => "+ctr); ctr += 1

    db.select(
      (age.of(db) -10) + (sal.of(db) - 60000) * age.of(db), 
      age, 
      sal
    ).where( 
      sal / 1234 >= age - 56 or (sal * 78 <= age and age - (sal + 91) <> age), // and // <--------------------------- (AND)
      age < 3334
    ).orderBy(
      (age, Decreasing)
    ).max(
      10
    ).offset(
      0
    ).as(
      _.toList
    ).foreach(println)

  }
  def select6 { 
    println("CTR => "+ctr); ctr += 1
    db.select(
      (age.of(db) -10) + (sal.of(db) - 60000) * age.of(db), 
      age, 
      sal
    ).select(
      age
    ).where( 
      sal / 1000 >= age - 10, // and // <--------------------------- (AND)
      age < 100
    ).where(
      age <> 20
    ).orderBy( // should join with AND
      (age, Decreasing)
    ).orderBy(
      (sal, Increasing)
    ).max(
      10
    ).offset(
      0
    ).as(
      _.toList
    ).foreach(println)
  } 

  def select7 { 
    println("CTR => "+ctr); ctr += 1
    db.select(
      (age.of(db) -10) + (sal.of(db) - 60000) * age.of(db), 
      age, 
      sal
    ).where( 
      sal / 1000 >= age - 10 and // <--------------------------- (AND)
      age < 100
    ).orderBy(
      (age, Decreasing)
    ).max(
      1
    ).offset(
      3
    ).as(
      _.toList
      // asUser
    ).foreach(println)

  }
  def select8 { 
    println("CTR => "+ctr); ctr += 1
    db.select(age.of(db) + sal.of(db)).as(_.toList) foreach println
  }
  def aggregateG1 { 
    println("CTR => "+ctr); ctr += 1
    db.aggregateGroupInt(Array(Aggregate(age * ((age + 5)*(age * 5)), Sum),     
                               Aggregate(sal, Avg)), Array[Where](), Array[GroupByInterval]()).foreach(println)
  }
  def aggregateG2 { 
    println("CTR => "+ctr); ctr += 1
    db.aggregateGroupInt(Array(Aggregate((age + sal) * 2, Avg),     
                               Aggregate(sal, Avg)), Array[Where](), Array[GroupByInterval]()).foreach(println)
  }
  def aggregateG3 { 
    println("CTR => "+ctr); ctr += 1
    db.aggregateGroupInt(Array(Aggregate((age + sal) * age, Avg),     
                               Aggregate(sal, Avg)), Array[Where](), Array[GroupByInterval]()).foreach(println)
  }
  def aggregateG4 { 
    println("CTR => "+ctr); ctr += 1
    db.aggregateGroupInt(Array(Aggregate(age * sal + age + 5, Sum),     
                               Aggregate(sal, Avg)), Array(), Array()).foreach(println)
  }
  def aggregateG5 { 
    println("CTR => "+ctr); ctr += 1
    db.aggregateGroupInt(Array(Aggregate(age * (sal + age + 5), Sum),     
                               Aggregate(sal, Avg)), Array(), Array()).foreach(println)
  }
  def aggregateG6 { 
    println("CTR => "+ctr); ctr += 1
    db.aggregateGroupInt(Array(Aggregate(age, Min), 
                               Aggregate(sal, Avg)), Array(), Array()).foreach(println)
  }
  def aggregateG7 { 
    println("CTR => "+ctr); ctr += 1
    db.aggregateGroupInt(Array(Aggregate(age * ((age + 5)*(age * 5)), Sum),     
                               Aggregate(age, First), 
                               Aggregate(age, Last), 
                               Aggregate(age, Max), 
                               Aggregate(sal, Avg)), Array(), Array()).foreach(println)
  }
  def aggregateG8 { 
    println("CTR => "+ctr); ctr += 1
    db.aggregateGroupInt(Array(Aggregate(age * ((age + 5)*(age * 5)), Sum),     
                               Aggregate(sal, Avg)), Array(age <= 10000, Where(age, Ge, 0)), Array(GroupByInterval(age, 3), GroupByInterval(sal, 2))).foreach(println)
  }
  def aggregateG9 { 
    println("CTR => "+ctr); ctr += 1
    db.aggregateGroupInt(Array(Aggregate(age * ((age + 5)*(age * 5)), Sum),     
                               Aggregate(age, First), 
                               Aggregate(age, Last), 
                               Aggregate(age, Max), 
                               Aggregate(sal, Avg)), Array(Where(age, Le, 10000), Where(age, Ge, 0)), Array(GroupByInterval(age, 3), GroupByInterval(sal, 2))).foreach(println)
  }
  def aggregateGOrig { 
    println("CTR => "+ctr); ctr += 1
    db.aggregateGroupInt(Array(Aggregate(age * ((age + 5)*(age * 5)), Sum),
                         Aggregate(sal, Avg)), Array(), Array()).foreach(println)
  }
  def aggregate10 { 
    println("CTR => "+ctr); ctr += 1
    val x = db.aggregateInt(Array(Aggregate(age, Min)), 
                            Array(Where(age, Le, 100), Where(age, Ge, 10)))(0)
    println(x)
  }
  def aggregateG10 { 
    println("CTR => "+ctr); ctr += 1
    val x = db.aggregateGroupInt(Array(Aggregate(age, Min),Aggregate(age, Last),Aggregate(age, First)), 
                       Array(Where(age, Le, 100), Where(age, Ge, 10)))(0)
    x.foreach(println)
  }
  def aggregateG11 { 
    println("CTR => "+ctr); ctr += 1
    db.aggregateGroupInt(Array(Aggregate(age, Min),Aggregate(age, Last)), 
                         Array(Where(age, Le, 100), Where(age, Ge, 10)))(0)
  }
  def aggregateG12 { 
    println("CTR => "+ctr); ctr += 1
    db.aggregateGroupInt(Array(Aggregate(age, Min),Aggregate(age, Last)),
                         Array(Where(age, Le, 100), Where(age, Ge, 10))).foreach(println)
  }
}
object Main 
{
  case class Person(uid:String, email:String, value:BigInt, scan:Array[Byte])
  def arrayToPerson(a:Array[Any]) = {
    Person(a(0).asInstanceOf[String], a(1).asInstanceOf[String], a(2).asInstanceOf[BigInt], a(3).asInstanceOf[Array[Byte]])
  }
  foo
  def foo = try {
    println("here0")
    
    val (uid, email, value, scan) = (Col("uid", VARCHAR(255)), Col("email", VARCHAR(255)), Col("value", new UBIGDEC(100)),
                                     Col("scan", BLOB))
    val table = Tab.withName("blobs").withCols(uid, email, value, scan).withPriKey(uid)
    val dbm = new DB(table)(MyDBConfig)
    dbm.deleteAll
    dbm.insertArray(Array("hi", "hello", BigInt(9223372036854775806L), Array[Byte](33, 43)))
    dbm.insertArray(Array("hey", "there", 1l, Array[Byte](11, 21)))
    dbm.insert("howdy", "man", 51222, Array[Byte](16, 32))
    println (" initial: read following ")
    dbm.select(uid,email, value, scan).as(arrayToPerson) foreach println
    dbm.incrementColTx(Where(uid, Eq, "hi"), Update(value, -3372058))
    dbm.select(uid, email, value, scan).as(arrayToPerson) foreach println
    dbm.incrementColsTx(Array(Where(uid, Eq, "hi")), Array(Update(value, 33)))
    dbm.select(uid,email, value, scan).as(arrayToPerson) foreach println
    dbm.incrementColsTx(Array(Where(uid, Eq, "hi")), Array(Update(value, BigInt("-32020029223372058"))))
    dbm.select(uid, email, value, scan).as(arrayToPerson) foreach println
    println (" update ho + 1: read following ")
    dbm.select(uid, email, value, scan).as(arrayToPerson) foreach println
  } catch {
    case e:Throwable => 
      e.printStackTrace
      println(e.getMessage)
  } finally { 
    System.exit(1)
  }
}

