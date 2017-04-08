/* @author Anthony (Tony) Poerio
   @email adp59@pitt.edu

   CS1699 - Assignemnt #07
   SCALA SPARK  */

/* exercise 1 & 2 */
val r = scala.util.Random
val idx = 1 to 100
val myrdd = sc.parallelize(idx.map(i => (i, r.nextInt(100))))
myrdd.sortBy(_._2, false).take(10)


/* exercise 3 */
// Model the problem
val friends =
Array((100, List(200, 300, 400, 500, 600)),
      (200, List(100, 300, 400)),
      (300, List(100, 200, 400, 500)),
      (400, List(100, 200, 300)),
      (500, List(100, 300)),
      (600, List(100)))

val friendsRDD = sc.parallelize(friends)


/* My Solution */
val flatPairsRDD = friendsRDD.flatMap {
    case (name, friendList) => friendList.map(friend => ((name, friend), friendList))
  }

// adapted from: https://issues.scala-lang.org/browse/SI-9064
val solution = flatPairsRDD.groupBy(_._1).map(l => (l._1, l._2.map(_._2)))

solution.collect()
