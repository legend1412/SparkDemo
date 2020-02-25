package test

object SimpleTest {
  def main(args: Array[String]): Unit = {
    val a = 0 to 10;
    println(a);
    val b = 0 until 10;
    println(b)
    println(a.filter(_!=3))
  }
}
