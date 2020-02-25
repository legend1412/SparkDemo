package streaming

import com.alibaba.fastjson.JSON

object JsonTest {
  def main(args: Array[String]): Unit = {
    val s = """
              |{"order_id": 2539329, "user_id": 1, "eval_set": "prior", "order_number": 1, "order_dow": 2, "hour": 8, "day": 0.0}
            """.stripMargin
    val mess = JSON.parseObject(s,classOf[Orders])
    println(mess.getOrder_id,mess.getOrder_dow)
  }
}
