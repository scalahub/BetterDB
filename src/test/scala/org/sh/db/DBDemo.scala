
package org.sh.db

//import org.sh.db.{DBManager => DBMgr}
import org.sh.db.config.TraitDBConfig
import org.sh.db.core.DataStructures.{VARCHAR => STR, _}
import org.sh.utils.encoding.Hex
import org.sh.utils.json.JSONUtil.JsonFormatted

object MyDBConfig extends TraitDBConfig {
  val dbname:String = "demoDB"
  val dbhost:String = "localhost"
  val dbms:String = "h2"  // e.g. mysql or h2 or postgresql
  val dbuser:String = "alice"
  val dbpass:String = "demopass"
  val connTimeOut:Int = 1000 // seconds
  val usePool:Boolean = true // e.g. true (use db pool)
  val configSource = "test"
}
import org.sh.db.ScalaDB._
object SameTableKey extends App {
  val key1 = Col("ID1", STR(255))
  val key2 = Col("ID2", STR(255))
  val db = Tab.withName("samekey_test").withConfig(MyDBConfig).withCols(key1, key2).withPriKey(key1, key2)
  db.deleteAll
  db.insert("hello", "hi")
  db.insert("hello", "hello")
  db.insert("hi", "hi")
  db.insert("hi", "hello")
  db.select(key1.of(db), key2.of(db)).where(key1.of(db) === key2.of(db)).execute foreach println
  db.select(key1, key2).where(key1 === key2).execute foreach println // same as above. "of" is not needed for same table where select is called
  System.exit(1)
}

object DBDemo extends App
{
  Class.forName("org.h2.Driver")

  val userIDCol = Col("userID", STR(255))
  val ageCol = Col("age", INT)
  val salaryCol = Col("salary", LONG)
  val passportScanCol = Col("passport", BLOB)
  
  val cols = Array(userIDCol, ageCol, salaryCol, passportScanCol)
  val priKeys = Array(userIDCol)
  
  implicit val config = MyDBConfig
  val userTab = Tab.withName("userTable").withCols(cols :_*).withPriKey(priKeys: _*) // using implicit config
  // table will be created automatically if not exists
  
  userTab.deleteAll // delete all rows for fresh test
  
  // define data to be inserted
  val userID = "Ajit"
  val age = 20
  val salary = 1000000000L
  val passportScan:Array[Byte] = Array(1, 2, 3)
  try {    

    // insert data into table
    userTab.insert(userID, age, salary, passportScan)

    // define a case class to hold user details
    case class User(userID:String, salary:Long, passportScan:Array[Byte]) extends JsonFormatted {
      val keys = Array("userID", "salary", "passportScan")
      val vals = Array(userID, salary, passportScan.toList)
    }

    // define a method to convert Array[Any] to User
    def toUser(a:Array[Any]) = User(a(0).as[String], a(1).as[Long], a(2).as[Array[Byte]]) // as is shorthand for asInstanceOf
    // search db 
    val users:List[User] = userTab select(userIDCol, salaryCol, passportScanCol) where(userIDCol === "Ajit" or ageCol < 25) as(toUser)
    assert(users.size == 1)    
    assert(users(0).userID == "Ajit")
    assert(users(0).salary == 1000000000L)
    assert(Hex.encodeBytes(users(0).passportScan) == "010203")
    
    userTab.select(userIDCol, salaryCol, passportScanCol).where(userIDCol === "Ajit" and ageCol < 25).as(toUser) foreach println // returns List[User]
    userTab.select(userIDCol, salaryCol, passportScanCol).where(userIDCol === "Ajit" and ageCol < 25).execute foreach println // returns List[List[Any]]
  } catch {case e:Throwable => e.printStackTrace }
  finally System.exit(0)
}
