/**
 * 
 */
package edu.ku.brc.sgr.datamodel.tests

import org.squeryl.{Session, SessionFactory, Schema}
import org.squeryl.PrimitiveTypeMode._

import org.scalatest._

/**
 * @author ben
 *
 * @code_status Alpha
 *
 * Created Date: May 4, 2011
 *
 */

trait DroppableSchema extends Schema {
  override def drop : Unit = super.drop
}

abstract class SchemaTester extends FunSuite with BeforeAndAfterAll {
  
  def schema : DroppableSchema
  
  def prePopulate() = {}
  
  def connectToDb : Option[() => Session]
  
  override def beforeAll() {
    super.beforeAll
    SessionFactory.concreteFactory = connectToDb
    
    transaction {
      schema.drop
      schema.create
      prePopulate
    }
  }
  
  override def afterAll() {
    super.afterAll
    transaction {
      schema.drop
    }
  }
}

trait RunTestsInsideTransaction extends SchemaTester with BeforeAndAfterEach {
  
  case object TransactionAbortException extends Exception
  
  override def runTest(testName: String,
                       reporter: Reporter,
                       stopper: Stopper,
                       configMap: Map[String, Any],
                       tracker: Tracker): Unit = {
    try {
      transaction {
        super.runTest(testName, reporter, stopper, configMap, tracker)
        throw TransactionAbortException
      }
    }
    catch {
        case TransactionAbortException =>
    }
  }
}