package com.kleggett.db.util

import java.sql.{PreparedStatement, ResultSet}

import com.kleggett.db.Book
import com.kleggett.db.DerbyDB._
import com.kleggett.db.util.SqlUtils._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec, Matchers}

/**
 * This test class shows some simple examples of how to use the [[SqlUtils]] class.
 *
 * @author K. Leggett
 * @since 1.0 (2/28/15 2:54 PM)
 */
class SqlUtilsTest extends FunSpec with BeforeAndAfterEach with BeforeAndAfterAll with Matchers
{
  override protected def beforeAll(): Unit = {
    createTable()
    super.beforeAll()
  }

  val createTableSql = "create table books (title varchar(50), author varchar(50), published int)"

  def createTable(): Unit = executeUpdate(dataSource, createTableSql, PREPARE_NO_OP)

  override protected def afterEach(): Unit = {
    try super.afterEach()
    finally clearTestData()
  }

  val clearTestDataSql = "delete from books"

  def clearTestData(): Unit = executeUpdate(dataSource, clearTestDataSql, PREPARE_NO_OP)

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally dropTable()
  }

  val dropTableSql = "drop table books"

  def dropTable(): Unit = executeUpdate(dataSource, dropTableSql, PREPARE_NO_OP)

  def books: List[Book]= {
    List(Book("Dune", "Frank Herbert", Some(1965)),
         Book("Dune Messiah", "Frank Herbert", Some(1969)),
         Book("A Feast for Crows", "George R. Martin", Some(2005)),
         Book("Mistborn", "Brandon Sanderson", Some(2006)),
         Book("The Well of Ascension", "Brandon Sanderson", Some(2007)),
         Book("Children of Dune", "Frank Herbert", Some(1976)),
         Book("Warlock", "Wilbur Smith", Some(2001)),
         Book("The Pursuit of Happyness", "Chris Gardner", Some(2006)),
         Book("The Way of Shadows", "Brent Weeks", Some(2008)),
         Book("Hsun Tzu", null, None),
         Book("Assassin's Apprentice", "Robin Hobb", Some(1995)))
  }

  val insertBookSql = "insert into books values (?, ?, ?)"

  def insertBook(book: Book): Unit = executeUpdate(dataSource, insertBookSql, prepareBookPS(book))

  def prepareBookPS(book: Book): (PreparedStatement) => Unit = (ps) => {
    ps.setString(1, book.title)
    ps.setString(2, book.author)
    ps.setIntOrNull(3, book.published) // This works because SqlUtils "pimps" the PreparedStatement
  }

  val countBooksSql = "select count(1) from books"

  def countBooks(): Int = {
    executeQuery(dataSource, countBooksSql, PREPARE_NO_OP, SINGLE_INT_RS).head
  }

  val booksByAuthorSql =
    """select title, author, published
      |from books
      |where author = ?
    """.stripMargin

  def booksByAuthor(author: String): List[Book] = {
    executeQuery(dataSource, booksByAuthorSql, prepSingleString(author), (rs: ResultSet) => {
      // Take advantage of the "pimped" ResultSet
      Book(rs.getString("title"), rs.getString("author"), rs.getIntOption("published"))
    })
  }

  describe("SqlUtils")
  {
    it ("should be able to insert data")
    {
      books.foreach(insertBook)
      countBooks() === books.size
    }

    it ("should be able to update data")
    {
      val updateSql = "update books set published = ? where author = ?"
      val author = "Frank Herbert"
      val year = 3000
      books.foreach(insertBook)

      val result = executeUpdate(dataSource, updateSql, (ps: PreparedStatement) => {
        ps.setInt(1, year)
        ps.setString(2, author)
      })
      result === 3

      val actuals = booksByAuthor(author)
      val years = actuals.flatMap(_.published)
      years should have size 3
      years.forall(_ == year) shouldBe true
    }

    it ("should be able to delete data")
    {
      val deleteSql = "delete from books where published < 2000"
      books.foreach(insertBook)

      val result = executeUpdate(dataSource, deleteSql, PREPARE_NO_OP)
      result === books.size
      countBooks() === 7
    }

    it ("should be able to do queries")
    {
      books.foreach(insertBook)
      val actuals = booksByAuthor("Brandon Sanderson")
      actuals should have size 2

      val titles = actuals.map(_.title)
      titles.contains("Mistborn") shouldBe true
      titles.contains("The Well of Ascension") shouldBe true
    }
  }
}
