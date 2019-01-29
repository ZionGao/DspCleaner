package com.moxiu.bigdata

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable

object DspRealTimeCleanerV2 {
  //读配置文件
  val prop = new Properties()
  //val inputStream = DspRealTimeCleanerV2.getClass.getClassLoader.getResourceAsStream("conf/settings.properties")
  val inputStream = DspRealTimeCleanerV2.getClass.getClassLoader.getResourceAsStream("c.properties")
  prop.load(inputStream)
  //设置hadoop用户
  System.setProperty("HADOOP_USER_NAME", prop.getProperty("HADOOP_USER_NAME"))
  //批次间隔
  val batchDuration = prop.getProperty("batchDuration").toInt
  //输出配置
  val separator = "\001"
  val topicForOutPut = prop.getProperty("topicForOutPut")
  val writeFileWindowLength = prop.getProperty("writeFileWindowLength").toInt
  val ErrorDataDir = prop.getProperty("ErrorDataDir")
  val outputBrokerList = prop.getProperty("outputBrokerList")
  //装载Kafka配置
  val zkQuorum = prop.getProperty("zkQuorum")
  val group = prop.getProperty("group")
  val topics = prop.getProperty("topics")
  val numThreads = prop.getProperty("numThreads")
  //保存点
  val CheckpointDir = prop.getProperty("CheckpointDir")
  val ConfFileLocation = prop.getProperty("ConfFileLocation")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DSPRealTimeCleanerV2")
    //.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val ssc = new StreamingContext(sc, Seconds(batchDuration))
    sc.setCheckpointDir(CheckpointDir)
    val broadOutParaForKafka = ssc.sparkContext.broadcast(Map("topic" -> topicForOutPut, "brokerList" -> outputBrokerList))
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", broadOutParaForKafka.value.getOrElse("brokerList", "10.1.0.168:9092,10.1.0.152:9092,10.1.0.156:9092,10.1.0.155:9092,10.1.0.170:9092"))
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p.setProperty("zk.connectiontimeout.ms", "15000")
        p
      }
      println("kafka producer init done!")
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
    /**
      * ==============================================  Kafka配置  =========================================================
      **/
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaDstream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    /**
      * ==============================================  日志清洗  ==========================================================
      **/
    val cleanData = kafkaDstream
      .map { x => //将json压平转成map
        val obj = JSON.parseObject(x._2)
        import scala.collection.JavaConverters._
        val paraIn = obj.getJSONObject("uri_args").entrySet().asScala.map(x => (x.getKey -> x.getValue.toString))
        obj.remove("uri_args")
        val paraOut = obj.entrySet().asScala.map(x => (x.getKey -> x.getValue.toString))
        paraIn ++= paraOut
        val outMap = paraIn.toMap
        (outMap.get("do"), outMap)
      }
    /**
      * ==============================================  动态更新Scheme  ====================================================
      **/
    val schemeDs = cleanData.updateStateByKey(updateFunction, new HashPartitioner(sc.defaultParallelism), true)
    schemeDs
      .filter(_._2._2) //scheme字段更新此处为true
      .map(line => line._1.getOrElse("NoType") + " Scheme Update:" + (line._2._1.filter(x => x != "do").mkString(",")))
      .repartition(1)
      .foreachRDD { rdd =>
        if (!rdd.isEmpty())
          rdd.saveAsTextFile(ErrorDataDir + "/" + System.currentTimeMillis())
      }
    /**
      * ===========================================  清洗结果同步写入Kafka  ================================================
      **/
    val outData = cleanData.join(schemeDs)
      .map { line =>
        val value = for (col <- line._2._2._1) yield line._2._1.getOrElse(col, "")
        val outMap = line._2._2._1.zip(value)
        val whichType = line._1.getOrElse("NoType")
        (whichType, outMap.toMap)
      }
      .mapPartitions { partitions =>
        val res = partitions.map { record =>
          kafkaProducer.value.send(broadOutParaForKafka.value.getOrElse("topic", "dsp"), record._2.mkString(separator))
          val map = record._2
          val value = (map - "do").values
          (record._1.replace(".", "_"), value)
        }
        res
      }
    outData.count() //触发批次内producer实例化并发送数据
    /**
      * ================================================= 文件落地  ========================================================
      **/
    val oneBatch = outData.window(Seconds(writeFileWindowLength), Seconds(writeFileWindowLength)).foreachRDD(rdd => {
      val dataRdd = rdd.persist
      val keys = dataRdd.map(_._1).aggregate(mutable.Set[String]())((set, key) => set += key, (set1, set2) => set1 ++= set2).toList
      val rdds = for (key <- keys) yield (key, dataRdd.filter(_._1 == key).map(_._2))
      dataRdd.unpersist()
      val dateFormat = new SimpleDateFormat("yy-MM-dd-HH")
      for (onetype <- rdds) {
        val when = dateFormat.format(new Date(System.currentTimeMillis()))
        onetype._2
          .map(row => row.mkString(separator))
          .repartition(1)
          //549c6462ba4d9b4d098b4567-base-active_service/15-07-21-19-1439935150000
          .saveAsTextFile("/sources/raw/streaming/" + "dsp00002ba4d9b4d098b4567-userlog-" + onetype._1 + "/" + when + "-" + System.currentTimeMillis())
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction = (iter: Iterator[(Option[String], Seq[Map[String, String]], Option[(List[String], Boolean)])]) => {
    iter.map { oneType =>
      val thisBatch = oneType._2.flatMap(x => x.keySet).distinct
      val historyBatch = oneType._3.getOrElse((List[String](), false))._1
      val updateScheme = (thisBatch.toSet -- historyBatch).toList.sortBy(x => x)(Ordering.String).filter(_.matches("[0-9a-zA-Z_-]+"))
      val newScheme =
        if (updateScheme.size > 0) {
          val after = historyBatch ::: updateScheme
          after
        } else {
          historyBatch
        }
      (oneType._1, (newScheme, updateScheme.size > 0))
    }
  }
}



