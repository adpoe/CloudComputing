package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._

/**
 * Adapted from equations at:
 * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
 */
// Must extend Serializable to avoid this error:
//    17/04/16 16:25:16 ERROR Executor: Exception in task 0.0 in stage 0.0 (TID 0)
//    java.io.NotSerializableException
class VarianceAssignment extends Serializable { 
  
  // declare all necessary variables
  var n: Double = 0.0      // total count of values we have seen
  var mean: Double = 0.0   // running mean
  var delta: Double = 0.0  // change for this iteration
  var delta2: Double = 0.0 // change for this iteration squared
  var M2: Double = 0.0     // a running delta*delta2
 
  // Compute initial variance for numbers 
 def this(data: Iterator[Double]) { 
    this  // must add a 'this' to avoid error:  --> 'this' expected but identifier found.
    data.foreach((this.add(_)))
 } 
  
 // Update variance for a single value
 def add(value: Double) { 
   // value = current number we are iterating over
   n += 1
   delta = value - mean
   mean += delta/n
   delta2 = value - mean
   M2 += delta*delta2
 }
 
 // Merge another variance object, and update the variance
 def merge(other: VarianceAssignment): VarianceAssignment = {
   // merge values with another instance of this class...
   other.n += n
   other.mean += mean
   other.delta += delta
   other.delta2 += delta2
   other.M2 += M2
   return other : VarianceAssignment
 }
 
 // a 'Get' method. Use this to return the running variance at any given time.
def getVariance(): Double = {
  // calculate the variance --> M2/n
  // and return it
  return M2/n
}
}

// object to use as an entry point when running from Scala IDE
object VarianceExample {
  
  // main method, required in order to run the program from IDE
  def main(args: Array[String]) {
    
    // create a new spark context and use local machine, all cores
    val sc = new SparkContext("local[*]", "RunningVariance")
    
    // update code from last assignment to create 100 doubles,
    // in range [0 --> 100]
    val r = scala.util.Random
    val idx = 1 to 100
    
    // and ensure that these values are parallelized in an RDD
    val myRdd = sc.parallelize(idx.map(i => r.nextDouble()*100))

    // use my new class to calculate the variance
    // all done by merging, in one pass
    val myRunningVar = myRdd
      .mapPartitions(v=>Iterator(new VarianceAssignment(v))) 
      .reduce((a,b) => a.merge(b))
    
    // use my get method to return the final variance
    val myVariance = myRunningVar.getVariance()
    
    // for comparison, use Scala-Spark's built in variance method
    val scalaVar = myRdd.variance() 
    
    // and print both results to compare the values
    println(s"My Result: $myVariance ;; Scala's Result: $scalaVar")
 
    }
}
