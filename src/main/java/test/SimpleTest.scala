package test

object SimpleTest {
  def main(args: Array[String]): Unit = {
    val a = 0 to 10
    println(a)
    val b = 0 until 10
    println(b)
    println(a.filter(_!=3))

    val c = Array(("a", 2), ("b", 1), ("c", 4))
    val d = c.sortWith((x, y) => x._2 > y._2)
    for (x <- c) {
      print(x)
    }
    println()
    for (x <- d) {
      print(x)
    }
  }
}
