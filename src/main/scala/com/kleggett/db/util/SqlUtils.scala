package com.kleggett.db.util

import java.sql._
import javax.sql.DataSource

import org.apache.commons.lang3.time.DateFormatUtils

/**
 * Utilities for making simple JDBC calls in Scala easier.
 * It includes an implicit conversion for PreparedStatements to enhance their functionality.
 * Feel free to use, enhance, etc...
 *
 * @author K. Leggett
 * @since 1.0 (1/3/15 3:38 PM)
 */
object SqlUtils
{
  val PREPARE_NO_OP = (ps: PreparedStatement) => {}
  val SINGLE_STRING_RS = (rs: ResultSet) => { rs.getString(1) }
  val SINGLE_INT_RS = (rs: ResultSet) => { rs.getInt(1) }

  implicit class EnhancedPreparedStatement(ps: PreparedStatement)
  {
    def setIntOrNull(index: Int, num: java.lang.Integer) = {
      if (num != null) ps.setInt(index, num) else ps.setNull(index, Types.NUMERIC)
    }

    def setIntOrNull(index: Int, num: Option[Int]) = {
      num match {
        case Some(n) => ps.setInt(index, n)
        case None => ps.setNull(index, Types.NUMERIC)
      }
    }

    def setLongOrNull(index: Int, num: java.lang.Long) = {
      if (num != null) ps.setLong(index, num) else ps.setNull(index, Types.NUMERIC)
    }

    def setLongOrNull(index: Int, num: Option[Long]) = {
      num match {
        case Some(n) => ps.setLong(index, n)
        case None => ps.setNull(index, Types.NUMERIC)
      }
    }

    def setDateOrNull(index: Int, date: java.util.Date) = {
      if (date != null) ps.setDate(index, new java.sql.Date(date.getTime)) else ps.setNull(index, Types.DATE)
    }

    def setDateOrNull(index: Int, date: Option[java.util.Date]) = {
      date match {
        case Some(d) => ps.setDate(index, new java.sql.Date(d.getTime))
        case None => ps.setNull(index, Types.DATE)
      }
    }

    def setTimestampOrNull(index: Int, ts: Timestamp) = {
      if (ts != null) ps.setTimestamp(index, ts) else ps.setNull(index, Types.TIMESTAMP)
    }

    def setTimestampOrNull(index: Int, ts: Option[Timestamp]) = {
      ts match {
        case Some(t) => ps.setTimestamp(index, t)
        case None => ps.setNull(index, Types.TIMESTAMP)
      }
    }
  }

  implicit class EnhancedResultSet(rs: ResultSet)
  {
    def getStringOption(index: Int): Option[String] = Option(rs.getString(index))

    def getStringOption(column: String): Option[String] = Option(rs.getString(column))

    def getIntOption(index: Int): Option[Int] = Option(rs.getInt(index))

    def getIntOption(column: String): Option[Int] = Option(rs.getInt(column))

    def getLongOption(index: Int): Option[Long] = Option(rs.getLong(index))

    def getLongOption(column: String): Option[Long] = Option(rs.getLong(column))

    def getDateOption(index: Int): Option[java.util.Date] = Option(rs.getDate(index))

    def getDateOption(column: String): Option[java.util.Date] = Option(rs.getDate(column))

    def getTimestampOption(index: Int): Option[Timestamp] = Option(rs.getTimestamp(index))

    def getTimestampOption(column: String): Option[Timestamp] = Option(rs.getTimestamp(column))
  }

  def prepSingleString(s: String): (PreparedStatement) => Unit = {
    (ps: PreparedStatement) => { ps.setString(1, s) }
  }

  def prepSingleLong(l: Long): (PreparedStatement) => Unit = {
    (ps: PreparedStatement) => { ps.setLong(1, l) }
  }

  def prepSingleDate(d: java.util.Date): (PreparedStatement) => Unit = {
    (ps: PreparedStatement) => { ps.setDate(1, new java.sql.Date(d.getTime)) }
  }

  def prepSingleTimestamp(t: Timestamp): (PreparedStatement) => Unit = {
    (ps: PreparedStatement) => { ps.setTimestamp(1, t) }
  }

  def executeUpdate(ds: DataSource, sql: String, prepare: (PreparedStatement) => Unit): Int = {
    var conOpt: Option[Connection] = None

    try {
      conOpt = Some(ds.getConnection)
      executeUpdate(conOpt.get, sql, prepare)
    }
    finally {
      conOpt.foreach(_.close())
    }
  }

  def executeUpdate(con: Connection, sql: String, prepare: (PreparedStatement) => Unit): Int = {
    var psOpt: Option[PreparedStatement] = None

    try {
      psOpt = Some(con.prepareStatement(sql))
      psOpt.foreach(p => prepare(p))
      psOpt.get.executeUpdate()
    }
    finally {
      psOpt.foreach(_.close())
    }
  }

  def executeQuery[A](ds: DataSource, sql: String, prepare: (PreparedStatement) => Unit, mapResult: (ResultSet) => A): List[A] = {
    var conOpt: Option[Connection] = None

    try {
      conOpt = Some(ds.getConnection)
      executeQuery(conOpt.get, sql, prepare, mapResult)
    }
    finally {
      conOpt.foreach(_.close())
    }
  }

  def executeQuery[A](con: Connection, sql: String, prepare: (PreparedStatement) => Unit, mapResult: (ResultSet) => A): List[A] = {
    var psOpt: Option[PreparedStatement] = None
    var rsOpt: Option[ResultSet] = None
    var results: List[A] = Nil

    try {
      psOpt = Some(con.prepareStatement(sql))
      psOpt.foreach(p => prepare(p))
      rsOpt = Some(psOpt.get.executeQuery())
      rsOpt.foreach(rs => while (rs.next()) results = results :+ mapResult(rs))
      results
    }
    finally {
      rsOpt.foreach(_.close())
      psOpt.foreach(_.close())
    }
  }

  def resetAndClose(conOpt: Option[Connection]): Unit = {
    conOpt.foreach(c => {
      c.setAutoCommit(true)
      c.close()
    })
  }

  def to_date(d: java.util.Date) = {
    s"to_date('${DateFormatUtils.format(d, "dd-MMM-yyyy HH:mm:ss") }', 'DD-MON-YYYY HH24:MI:SS')"
  }

  def to_timestamp(t: java.util.Date) = {
    s"to_timestamp('${DateFormatUtils.format(t, "dd-MMM-yyyy HH:mm:ss.SSS") }', 'DD-MON-YYYY HH24:MI:SS.FF3')"
  }

  /**
   * Returns true if the given exception is a SQL unique constraint exception, and false otherwise.
   * @param t the Throwable to check
   * @return true if the exception is a unique constraint error
   */
  def uniqueConstraint(t: Throwable): Boolean = {
    if (t == null) {
      false
    }
    else {
      t match {
        case ex: SQLException if ex.getErrorCode == 1 => true
        case _ => false
      }
    }
  }
}