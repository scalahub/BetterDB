package org.sh.db

import org.sh.db.ScalaDB._
import org.sh.db.core.DataStructures._
import org.sh.db.core._
import MainBasic._

object NestedNew 
{
  def main(a:Array[String]) {
    try {    
      MainBasic.db.deleteAll
      MainBasic.insert
      //      Util.printSQL = true;     // prints the SQL generated with actual data
      //      Util.printSQL_? = true    // prints the SQL generated with ? for parameters
      db.selectStar.where(name notIn Array[String]()).asList foreach println
      db.selectStar.where(name in Array[String]()).asList foreach println

      val db1 = DBManager("foo1")(name, age)()
      val db2 = DBManager("foo2")(age, name)()
      db1.deleteAll
      //db.aggregate(name, age.sum).groupBy(name).into(db1)
      db.aggregate(age.sum, name).groupBy(name).into(db2)
    } catch {
      case t:Throwable => t.printStackTrace
    } finally System.exit(1)
  }
}

object Nested {
  def main(a:Array[String]) {
    try {    
      Util.printSQL = false;
      Util.printSQL_? = false;
      val sel0 = db.select(sal).where(sal > 1, sal <> 2).orderBy(sal.decreasing).max(3).offset(4)
      val sel1 = db.select(sal).where(sal > 5, sal from sel0).orderBy(sal.decreasing).max(6).offset(7) 
      val sel2 = db.select(sal).where(sal > 8, sal from sel1).orderBy(sal.decreasing).max(9).offset(10) 
      Util.printSQL = true;
      val sel4 = db.select(sal).where(sal < sel0.nested).execute
      val a = db.aggregate(sal.min).where(
        age >= 203
      ).groupBy(age).firstAsLong
      
      db.selectStar.asList foreach println
      //Select query SQL [?]:
      //  SELECT USERS.SAL AS MsGnzNZXKV FROM USERS WHERE USERS.SAL IN (SELECT USERS.SAL AS MsGnzNZXKV FROM USERS WHERE USERS.SAL > ? and USERS.SAL <> ? ORDER BY USERS.SAL DESC LIMIT 3 OFFSET 4) LIMIT 2147483647
      //Select query SQL [R]:
      //  SELECT USERS.SAL AS MsGnzNZXKV FROM USERS WHERE USERS.SAL IN (SELECT USERS.SAL AS MsGnzNZXKV FROM USERS WHERE USERS.SAL > 1 and USERS.SAL <> 2 ORDER BY USERS.SAL DESC LIMIT 3 OFFSET 4) LIMIT 2147483647

    } catch {
      case t:Throwable => t.printStackTrace
    } finally System.exit(1)
  }
}