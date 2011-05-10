/**
 * 
 */
package edu.ku.brc.sgr.datamodel

import org.squeryl.PrimitiveTypeMode._
/**
 * @author ben
 *
 * @code_status Alpha
 *
 * Created Date: May 9, 2011
 *
 */
class Histogram (val results: BatchMatchResultSet, val binSize: Float) {
  
  private val histo =  new collection.mutable.HashMap[Int, Int]
  
  transaction {
      results.items foreach (item => {
        val bin : Int = Math.floor(item.maxScore / binSize).asInstanceOf[Int]
        histo.get(bin) match {
          case Some(n) => histo += ((bin, n+1))
          case None => histo += ((bin, 1))
        }
      })
  }
  def bins() : Int = histo.keySet.max + 1
  
  def count(bin: Int) : Int = histo.getOrElse(bin, 0)
}