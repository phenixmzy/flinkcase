package com.flink.example.usecase.funcation

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object CaseFuncation {

  class MyAverageAggregate extends AggregateFunction[(String, Long), (Long, Long), Double] {
    override def createAccumulator(): (Long, Long) = (0L, 0L)

    override def add(in: (String, Long), acc: (Long, Long)): (Long, Long) = (acc._1 + in._2, acc._2 + 1L)

    override def getResult(acc: (Long, Long)): Double = acc._1 / acc._2

    override def merge(acc: (Long, Long), acc1: (Long, Long)): (Long, Long) = (acc._1 + acc1._1, acc._2 + acc1._2)
  }

  class GameAvgTime extends AggregateFunction[(String, Int), (Int, Int), Float] {
    override def createAccumulator(): (Long, Long) = (0L, 0L)

    override def add(in: (String, Int), acc: (Int, Int)): (Int, Int) = (acc._1 + in._2, acc._2 + 1)

    override def getResult(acc: (Int, Int)): Float = acc._1 / acc._2

    override def merge(acc: (Int, Int), acc1: (Int, Int)): (Int, Int) = (acc._1 + acc1._1, acc._2 + acc1._2)
  }

  class GameSummary extends ProcessWindowFunction[(String,Int),(String, Int, Int, Int, Int, Long), String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int, Int, Int, Int, Long)]): Unit = {

      val sum = elements.map(_._2).sum
      val max = elements.map(_._2).max
      val min = elements.map(_._2).min
      var avg = sum / elements.size
      val windowEnd = context.window.getEnd
      out.collect((key,min,max,sum,avg, windowEnd))
    }
  }
}
