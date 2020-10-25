import org.apache.log4j._
import org.apache.spark._

/** Compute the average number of friends by age in a social network. */
object FriendsByAge {

  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String) : (Int, Int) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the age and numFriends fields, and convert to integers
    val age = fields(2).toInt
    val name = fields(1).toString
    val numFriends = fields(3).toInt
    // Create a tuple that is our result.
    (age, numFriends)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")

    // Load each line of the source data into an RDD
    val lines = sc.textFile("data/fakefriends-noheader.csv")

    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)
    println("rdd")
    rdd.take(5).foreach(println)
    println("\n")


    println("groupByKey")
    rdd.groupByKey().sortByKey().take(10).foreach(println)
    println("\n")



    // Lots going on here...
    // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
    // adding together all the numFriends values and 1's respectively.
    val totalsByAge1 = rdd.mapValues(v => (v, 1))
    println("totalsByAge1")
    totalsByAge1.take(5).foreach(println)
    println("\n")



    val totalsByAge2 = totalsByAge1.reduceByKey( (a,c) => (a._1 + c._1, a._2 + c._2))
    println("totalsByAge2")
    totalsByAge2.take(5).foreach(println)
    println("\n")

    // So now we have tuples of (age, (totalFriends, totalInstances))
    // To compute the average we divide totalFriends / totalInstances for each age.
    val averagesByAge = totalsByAge2.mapValues(v => v._1 / v._2)
    println("averagesByAge")
    //    println(totalsByAge2.countByKey())
    println("\n")


    // Collect the results from the
    // RDD (This kicks off computing the DAG and actually executes the job)
    val results = averagesByAge.collect()

    System.in.read();

    // Sort and print the final results.
    results.sorted.foreach(println)
  }

}