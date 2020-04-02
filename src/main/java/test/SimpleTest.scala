package test

object SimpleTest {
  def main(args: Array[String]): Unit = {
    val a = 0 to 10
    println(a)
    val b = 0 until 10
    println(b)
    println(a.filter(_ != 3))
    println("============================")
    val c = Array(("a", 2), ("b", 1), ("c", 4))
    val d = c.sortWith((x, y) => x._2 > y._2)
    for (x <- c) {
      print(x)
    }
    println()
    for (x <- d) {
      print(x)
    }
    println("\n============================")
    val s = """285_3, 462_5, 475_5, 222_3, 56_5"""
    val s_seq = s.split(",").toSeq
    val s_list = s_seq.map(_.split("_"))

    for (x <- s_list) {
      println(x(0).trim)
    }
   val s_list1 = s_seq.map{x=>
      val l =x.split("_")
      (l(0).trim,l(1).trim)
    }
    println(s_list1)
    println(s_list1.toMap)
    println("============================")
  }
}

