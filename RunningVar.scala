package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._

/**
 * Adapted from algorithms at:
 * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
 */
// compiler said to extend Serializable...
class VarianceAssignment extends Serializable { 
  
  // declare the variables necessary
  var n: Double = 0.0         // total count of values we have seen
  var mean: Double = 0.0      // running mean
  var delta: Double = 0.0  // change for this iteration
  var delta2: Double = 0.0 // change for this iteration squared
  var M2: Double = 0.0     // a running delta*delta2
 
  // Compute initial variance for numbers 
 def this(numbers: Iterator[Double]) { 
    this()  // doesn't compile unless we add an extra reference to this(), on my system
    numbers.foreach((this.add(_)))
 } 
  
 def add(value: Double) { 
   // value = current number we are iterating over
   n += 1
   delta = value - mean
   mean += delta/n
   delta2 = value - mean
   M2 += delta*delta2
 }
 
 def merge(other: VarianceAssignment): VarianceAssignment = {
   // merge values with another instance of this class...
   other.n += n
   other.mean += mean
   other.delta += delta
   other.delta2 += delta2
   other.M2 += M2
   return other : VarianceAssignment
 }
   
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
    // with code provided in assignment prompt
    val tonyRunningVar = myRdd
      .mapPartitions(v=>Iterator(new VarianceAssignment(v))) 
      .reduce((a,b) => a.merge(b))
      
    val myVariance = tonyRunningVar.getVariance()
    // finally, use built-in method to calculate the variance
    val scalaVar = myRdd.variance() 
    
    // and print to compare the values
    println(s"My Result: $myVariance ;; Scala's Result: $scalaVar")
 
    }
}
