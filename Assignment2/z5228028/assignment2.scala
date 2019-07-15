// Use the named values (val) below whenever your need to
// read/write inputs and outputs in your program. 
val inputFilePath  = "sample_input.txt"
val outputDirPath = "output"

// Write your solution here

// conversion constant
val MB_to_B = 1048576
val KB_to_B = 1024

// create an RDD from input file
val file = sc.textFile(inputFilePath)

// split each line by comma(,)
val lines = file.map(_.split(","))

// transform to a pair of base URL and size of payload RDD
val pairs = lines.map(x => (x.head, x.last))

// transform to a pair of base URL and size of payload RDD in bytes RDD
val bytes_pairs = pairs.map(x => if (x._2.takeRight(2) == "MB") (x._1,x._2.dropRight(2).toInt*MB_to_B) else if (x._2.takeRight(2) == "KB") (x._1,x._2.dropRight(2).toInt*KB_to_B)  else (x._1,x._2.dropRight(1).toInt))

// create a case class for calculating min, max, mean and variance
case class Stats(var total: Int, var min: Int, var max: Int, var count: Int, var all_sizes: List[Int]) {
  def agg(other: Stats): Stats = Stats(
    total + other.total,
    math.min(min, other.min),
    math.max(max, other.max),
    count + other.count,
    all_sizes:+other.total
  )
  def gen_output(): String = {
    val mean = total.toDouble / count
    val variance = all_sizes.map(x=>math.pow(x.toDouble-mean,2)).sum / count
    return min+"B,"+max+"B,"+"%.0fB,%.0fB".format(math.floor(mean),math.floor(variance))
  }
}

// Map size of payload to Stats Object and Reduce with aggregation function in case class
val result  = bytes_pairs.mapValues(f=>Stats(f,f,f,1,List(f))).reduceByKey(_ agg _)

// Generate final output and return new RDD with exactly one partition
val final_result  = result.map(x=>x._1+","+x._2.gen_output).repartition(1)

// Write to File
final_result.saveAsTextFile(outputDirPath)