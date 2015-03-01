package com.kleggett.db

import java.sql.{Connection, DriverManager}
import javax.sql.DataSource

import org.apache.derby.jdbc.BasicEmbeddedDataSource40

/**
 * A utility for using the Apache Derby database during testing.
 *
 * @author K. Leggett
 * @since 1.0 (1/3/15 3:42 PM)
 */
object DerbyDB
{
  val driver = "org.apache.derby.jdbc.EmbeddedDriver"
  val dbName = "runtime/TestDB"
  val connectionURL = "jdbc:derby:" + dbName + ";create=true"

  try {
    Class.forName(driver)
  }
  catch {
    case ex: ClassNotFoundException =>
      println(s"Error loading Derby driver: ${ex.getMessage}")
      throw ex
  }

  def connection: Connection = DriverManager.getConnection(connectionURL)

  def dataSource: DataSource = {
    val ds = new BasicEmbeddedDataSource40()
    ds.setDatabaseName(s"$dbName;create=true")
    ds
  }
}
