# BetterDB 

[![Build Status](https://travis-ci.org/scalahub/BetterDB.svg?branch=master)](https://travis-ci.org/scalahub/BetterDB)

BetterDB is a Scala library for connecting to and querying relational databases. It is created on top of JDBC.

## Supported Databases

BetterDB is extensively tested on H2. It should work with MySQL and PostGreSQL as well, although not heavily tested on those databases.

## Examples

Columns can be defined as:
  
>      val STR = VARCHAR(255)
>      val name = Col("NAME", STR)
>      val age = Col("AGE", INT)
>      val sal = Col("SAL", LONG)

Tables can be defined as:  
>      val users = Tab.withName("USERS").withCols(name, age, sal).withPriKey(name, age) 
  
Tables can be queried as:
>      users.select(name, age).where(sal >= 2000).as(User(_))  
and

>      users.aggregate(age.avg).where(sal >= 200).asLong 
  
More examples [here](https://github.com/scalahub/BetterDB/tree/master/src/test/scala/org/sh/db).

