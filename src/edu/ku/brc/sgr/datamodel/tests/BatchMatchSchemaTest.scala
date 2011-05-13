/**
 * 
 */
package edu.ku.brc.sgr.datamodel.tests

import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, FunSuite}

import org.squeryl.{Session, SessionFactory}
import org.squeryl.PrimitiveTypeMode._

import edu.ku.brc.sgr.datamodel._

/**
 * @author ben
 *
 * @code_status Alpha
 *
 * Created Date: May 4, 2011
 *
 */
object TestSchema extends BatchMatchSchemaBase with tests.DroppableSchema 

class BatchMatchSchemaTest extends SchemaTester with RunTestsInsideTransaction {
  def schema = TestSchema
  
  override def connectToDb() = Some(() => {
    val session = Session.create(
            java.sql.DriverManager.getConnection(
              "jdbc:mysql://localhost/test", "root", "root"),
            new org.squeryl.adapters.MySQLInnoDBAdapter)
    //session.setLogger(Console.println(_))
    session
  })


  
  test("insert result set") {
      val bmrs = new BatchMatchResultSet("test", "testq", Some(0))
      val inserted = schema.resultSets.insert(bmrs)
      val c : Long = from(schema.resultSets)(rs => compute(count));
      assert(c == 1)
  }
  
  test("foreign key test") {
    val rSet = schema.resultSets.insert(new BatchMatchResultSet("test", "testq", Some(0)))
    
    try {
        val item = schema.items.insert(new BatchMatchResultItem(rSet.id + 1, "foo", 0, 0.0F))
        fail
    } catch {
      case e : RuntimeException => assert(
          "foreign key constraint fails".r.findFirstMatchIn(e.getMessage).isDefined
          )
    }
    
    val c : Long = rSet.items.Count 
    assert(c == 0)
    
    val item = schema.items.insert(new BatchMatchResultItem(rSet.id, "foo", 0, 0.0F))
    val c2 : Long = rSet.items.Count 
    assert(c2 == 1)    
  }
}