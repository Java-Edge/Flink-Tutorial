package com.javaedge.scala.chapter3

import scala.util.Random

/**
  * @author JavaEdge
  * @date 2019-07-17
  *
  */
object DBUtils {

  def getConnection() = {
    new Random().nextInt(10) + ""
  }

  def returnConnection(connection: String): Unit = {

  }

}
