package edu.ku.brc.sgr.datamodel

import scala.collection.JavaConversions._

import java.sql.Connection

import org.squeryl.PrimitiveTypeMode._
import org.squeryl.{Schema, KeyedEntity, Session, SessionFactory, ForeignKeyDeclaration} 
import org.squeryl.dsl.{OneToMany, ManyToOne}
import org.squeryl.dsl.ast.TypedExpressionNode
import org.squeryl.adapters.MySQLAdapter

import com.google.common.collect.ImmutableSet
import ImmutableSet.{Builder => ImmutableSetBuilder}
import com.google.common.base.Function

import edu.ku.brc.sgr
import sgr.BatchMatchResultAccumulator
import sgr.MatchResults
import sgr.SGRMatcher


class BatchMatchResultSet(val name: String,
                          val query: String,
                          val recordSetID: Option[Long],
                          val dbTableId: Option[Int]) 
    extends KeyedEntity[Long] {
  
    def this() = this("", "", Some(0), Some(0))
    
    val id : Long = 0
  
    lazy val items: OneToMany[BatchMatchResultItem] = BatchMatchSchema.setToItems.left(this)
    
    def nItems() : Long = transaction { items.Count }
    
    def delete() : Unit = transaction { 
      BatchMatchSchema.resultSets.delete(this.id)
    }
    
    def getValues() : Array[Double] = transaction {
      from(items)(i => select(i.maxScore)) map(_.asInstanceOf[Double]) toArray
    }
    
    def getMax() : Double = transaction {
      val maxScore : Option[Float] = from(items)(i => compute(max(i.maxScore)))
      maxScore match {
        case None => 0.0
        case Some(f) => f.asInstanceOf[Double]
      }
    }
    
    def getAllItems() : java.util.List[BatchMatchResultItem] = transaction {
      from(items)(i => select(i)).toList
    }
}
                          
class BatchMatchResultItem(val batchMatchResultSetId: Long,
                           val matchedId: String,
                           val qTime: Int,
                           val maxScore: Float) 
    extends KeyedEntity[Long] {
  
    val id : Long = 0
    
    lazy val set: ManyToOne[BatchMatchResultSet] = BatchMatchSchema.setToItems.right(this)
}
                           
                           
class BatchMatchSchemaBase extends Schema {
    val resultSets = table[BatchMatchResultSet]
    val items = table[BatchMatchResultItem]
    
    val setToItems = 
      oneToManyRelation(resultSets, items).
      via((s, i) => s.id === i.batchMatchResultSetId)
    
    override def applyDefaultForeignKeyPolicy(foreignKeyDeclaration: ForeignKeyDeclaration) =
      foreignKeyDeclaration.constrainReference
      
    setToItems.foreignKeyDeclaration.constrainReference(onDelete cascade)
    
    on(resultSets)(s => declare(
        s.id is(autoIncremented)
    ))

    on(items)(i => declare(
        i.id is(autoIncremented)
    ))
}

object BatchMatchSchema extends BatchMatchSchemaBase

object DataModel {
  def startDbSession(conn: Function[AnyRef, java.sql.Connection]) : Unit = {
    SessionFactory.concreteFactory =  Some(() => {
      val session = Session.create(conn.apply(null),  new org.squeryl.adapters.MySQLInnoDBAdapter)
      session.setLogger(Console.println(_))
      session
    })
  }
  
  def createBatchMatchResultSet(name: String, matcher: SGRMatcher, 
                                recordSetID: java.lang.Long, dbTableId: java.lang.Integer) 
      : BatchMatchResultSet = transaction {
          val rsid = if (recordSetID.eq(null)) None else Some(recordSetID : Long)
          val dbTbId = if (dbTableId.eq(null)) None else Some(dbTableId : Int)
          val rs = new BatchMatchResultSet(name, matcher.getBaseQuery.toString, rsid, dbTbId)
          BatchMatchSchema.resultSets.insert(rs)
      }
  
  def getBatchMatchResultSets() : java.util.List[BatchMatchResultSet] = transaction {
    from(BatchMatchSchema.resultSets)(select(_)).toList
  }
  
  def getBatchMatchResultSetsFor(recordSetId: java.lang.Long, dbTableId : java.lang.Integer) 
      : java.util.List[BatchMatchResultSet] = transaction {
    
    if (recordSetId == null) List()
    else {
        val rsid = Some(recordSetId : Long)
        val dbTbId = if (dbTableId.eq(null)) None else Some(dbTableId : Int)
        
        from(BatchMatchSchema.resultSets)( 
            s => where( 
                s.recordSetID === rsid and s.dbTableId === dbTbId.?
                ) 
            select(s)  
            ).toList
    }
  }
}

class AccumulateResults(val matcher : SGRMatcher,
                        val resultSet: BatchMatchResultSet) 
                        extends BatchMatchResultAccumulator {
  
  if (!matcher.sameQueryAs(resultSet.query))
    throw new IllegalArgumentException("cannot resume batchmatch with inconsistent query");
  
  override def addResult(result : MatchResults) : Unit = transaction {
    val item = new BatchMatchResultItem(resultSet.id, result.matchedId, result.qTime, result.maxScore)
    resultSet.items.associate(item)
  }
  
  override def getCompletedIds() : ImmutableSet[String] = transaction {
    val completedIds = from(resultSet.items)(item => select(item.matchedId))
    ImmutableSet.copyOf(completedIds.toIterator)
  }
  
  override def nCompleted() : Int = {
    val c : Long = resultSet.nItems
    if (c > Int.MaxValue) 
      throw new IllegalStateException("result set contains too many items")
    c.asInstanceOf[Int]
  }
  
  override def getMatcher() = matcher
}


object CreateSchema {
  def main(args : Array[String]) : Unit = {
    DataModel.startDbSession(new Function[AnyRef, java.sql.Connection] {
      def apply(foo: AnyRef) = java.sql.DriverManager.getConnection(
              "jdbc:mysql://localhost/kuplant", "root", "root")
    })
    
    transaction {
      BatchMatchSchema.create
    }
  }
}