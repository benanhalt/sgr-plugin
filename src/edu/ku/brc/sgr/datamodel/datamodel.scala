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

class MatchConfiguration(var name: String,
                         var remarks: String,
                         val serverUrl: String,
                         val nRows: Int,
                         val boostInterestingTerms: Boolean,
                         val similarityFields: String,
                         val queryFields: String,
                         val filterQuery: String)
    extends KeyedEntity[Long] {
  
    val id : Long = 0
    
    lazy val resultSets : OneToMany[BatchMatchResultSet] =
      BatchMatchSchema.matchConfigurationToResultSets.left(this)
      
    def createMatcherFactory() : SGRMatcher.Factory = {
      val factory = SGRMatcher.getFactory
      factory.serverUrl = serverUrl
      factory.nRows = nRows
      factory.boostInterestingTerms = boostInterestingTerms
      factory.similarityFields = similarityFields
      factory.queryFields = queryFields
      factory.filterQuery = filterQuery
      factory
    }
    
    def delete() : Unit = transaction {
      BatchMatchSchema.matchConfigurations.delete(this.id)
    }
    
    def updateProperties(newName: String, newRemarks: String) : Unit = transaction {
      name = newName
      remarks = newRemarks
      BatchMatchSchema.matchConfigurations.update(this)
    }
    
    override def toString = name
}


class BatchMatchResultSet(var name: String,
                          var remarks: String,
                          val query: String,
                          val recordSetID: Option[Long],
                          val dbTableId: Option[Int],
                          val matchConfigurationId: Long) 
    extends KeyedEntity[Long] {
  
    def this() = this("", "", "", Some(0), Some(0), 0)
    
    val id : Long = 0
  
    lazy val items: OneToMany[BatchMatchResultItem] = BatchMatchSchema.setToItems.left(this)
    
    lazy val matchConfiguration: ManyToOne[MatchConfiguration] = 
      BatchMatchSchema.matchConfigurationToResultSets.right(this)  
    
    def nItems() : Long = transaction { items.Count }
    
    def delete() : Unit = transaction { 
      BatchMatchSchema.resultSets.delete(this.id)
    }
    
    def getValues(multiplier: Double = 1.0) : Array[Double] = transaction {
      from(items)(i => select(i.maxScore)) map(_.asInstanceOf[Double] * multiplier) toArray
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
    
    def getMatchConfiguration() : MatchConfiguration =  transaction(matchConfiguration.single)
    
    def updateProperties(newName: String, newRemarks: String) : Unit = transaction {
      name = newName
      remarks = newRemarks
      BatchMatchSchema.resultSets.update(this)
    }
    
    override def toString = name
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
    val matchConfigurations = table[MatchConfiguration]
    val resultSets = table[BatchMatchResultSet]
    val items = table[BatchMatchResultItem]
    
    override def applyDefaultForeignKeyPolicy(foreignKeyDeclaration: ForeignKeyDeclaration) =
      foreignKeyDeclaration.constrainReference
      
    val setToItems = 
      oneToManyRelation(resultSets, items).
      via((s, i) => s.id === i.batchMatchResultSetId)
    
    setToItems.foreignKeyDeclaration.constrainReference(onDelete cascade)
    
    val matchConfigurationToResultSets =
      oneToManyRelation(matchConfigurations, resultSets).
      via((mc, rs) => mc.id === rs.matchConfigurationId)
      
    on(matchConfigurations)(mc => declare(
        mc.remarks is(dbType("text")),
        mc.queryFields is(dbType("text")),
        mc.serverUrl is(dbType("text")),
        mc.similarityFields is(dbType("text"))
    ))
      
    on(resultSets)(s => declare(
        s.query is(dbType("text")),
        s.remarks is(dbType("text"))
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
  
  def persistMatchConfiguration(name: String, matcherFactory: SGRMatcher.Factory) : 
      MatchConfiguration = transaction {
    val matcherConfig = new MatchConfiguration(name, "",
                                               matcherFactory.serverUrl, 
                                               matcherFactory.nRows,
                                               matcherFactory.boostInterestingTerms,
                                               matcherFactory.similarityFields,
                                               matcherFactory.queryFields,
                                               matcherFactory.filterQuery)
    BatchMatchSchema.matchConfigurations.insert(matcherConfig);
  }
  
  def getMatcherConfigurations() : java.util.List[MatchConfiguration] = transaction {
    from(BatchMatchSchema.matchConfigurations)(select(_)).toList
  }
  
  def createBatchMatchResultSet(name: String, matcher: SGRMatcher, 
                                recordSetID: java.lang.Long, 
                                dbTableId: java.lang.Integer,
                                matchConfigId: Long
                                ) 
      : BatchMatchResultSet = transaction {
          val rsid = if (recordSetID.eq(null)) None else Some(recordSetID : Long)
          val dbTbId = if (dbTableId.eq(null)) None else Some(dbTableId : Int)
          val rs = new BatchMatchResultSet(name, "", matcher.getBaseQuery.toString, rsid, 
              dbTbId, matchConfigId)
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