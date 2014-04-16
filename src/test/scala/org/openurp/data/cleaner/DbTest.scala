package org.openurp.data.cleaner

import java.sql.Types
import org.beangle.commons.io.IOs
import org.beangle.commons.lang.ClassLoaders
import org.beangle.data.jdbc.query.JdbcExecutor
import org.beangle.data.jdbc.util.PoolingDataSourceFactory
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DbTest extends FunSpec with Matchers {
  val url = ClassLoaders.getResource("db.properties", getClass)
  val db = IOs.readJavaProperties(url)
  println(url, db)
  describe("DbTest") {
    it("Create Table") {
      val factory = new PoolingDataSourceFactory(db("h2.driverClassName"), db("h2.url"),
        db("h2.username"), db("h2.password"), new java.util.Properties())
      val datasource = factory.getObject

      val executor = new JdbcExecutor(datasource)
      executor.update("create table tmp(id bigint,name varchar(20))");
      executor.update("insert into tmp(id,name) values(1,'zhanshan')");
      executor.batch("insert into tmp(id,name) values(?,?)", List(List(2L, "lisi"), List(3L, "xiaoming")), List(Types.BIGINT, Types.VARCHAR))
      val rs = executor.query("select id,name from tmp")
      println("id", "name")
      rs foreach (r => println(r(0), r(1)))
    }
    
  }
}
