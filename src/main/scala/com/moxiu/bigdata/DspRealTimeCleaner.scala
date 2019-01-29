package com.moxiu.bigdata

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable

object DspRealTimeCleaner {
  //读配置文件
  val prop = new Properties()
  val inputStream = DspRealTimeCleaner.getClass.getClassLoader.getResourceAsStream("conf/settings.properties")
  //val inputStream = DspRealTimeCleaner.getClass.getClassLoader.getResourceAsStream("c.properties")
  prop.load(inputStream)
  //val inputStream =Thread.currentThread().getContextClassLoader.getResourceAsStream("settings.properties")
  // prop.load(new FileInputStream("conf/settings.properties"))
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
    val conf = new SparkConf().setAppName("DSPRealTimeCleaner")
    //.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val ssc = new StreamingContext(sc, Seconds(batchDuration))
    sc.setCheckpointDir(CheckpointDir)
    val broadOutParaForKafka = sc.broadcast(Map("topic" -> topicForOutPut, "brokerList" -> outputBrokerList))

    val schemeMap = Map(
      "App.Ad" -> DspScheme.AppAd,
      "App.Custom" -> DspScheme.AppCustom,
      "App.Essential" -> DspScheme.AppEssential,
      "App.Show" -> DspScheme.AppShow,
      "Collect.Click" -> DspScheme.CollectClick,
      "Collect.Download" -> DspScheme.CollectDownload,
      "Collect.Install" -> DspScheme.CollectInstall,
      "Collect.Show" -> DspScheme.CollectShow,
      "Conf.Cache" -> DspScheme.ConfCache,
      "Conf.Get" -> DspScheme.ConfGet,
      "Conf.Multi.Get" -> DspScheme.ConfMultiGet,
      "Conf.Pre.Get" -> DspScheme.ConfPreGet,
      "null" -> DspScheme.Other
    )
    val broadMap = sc.broadcast(schemeMap)
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
      * =================================================  日志清洗  =======================================================
      **/
    val cleanData = kafkaDstream
      .map { x => //将json压平转成map
        val obj = JSON.parseObject(x._2)
        import scala.collection.JavaConverters._
        val paraIn = obj.getJSONObject("uri_args").entrySet().asScala.map(x => (x.getKey -> x.getValue.toString))
        obj.remove("uri_args")
        val paraOut = obj.entrySet().asScala.map(x => (x.getKey -> x.getValue.toString))
        paraIn ++= paraOut
        paraIn.toMap
      }
      .map { record => //将json压平转成map
        val scheme = record.get("do") match {
          case Some("App.Ad") => DspScheme.AppAd
          case Some("App.Custom") => DspScheme.AppCustom
          case Some("App.Essential") => DspScheme.AppEssential
          case Some("App.Show") => DspScheme.AppShow
          case Some("Collect.Click") => DspScheme.CollectClick
          case Some("Collect.Download") => DspScheme.CollectDownload
          case Some("Collect.Install") => DspScheme.CollectInstall
          case Some("Collect.Show") => DspScheme.CollectShow
          case Some("Conf.Cache") => DspScheme.ConfCache
          case Some("Conf.Get") => DspScheme.ConfGet
          case Some("Conf.Multi.Get") => DspScheme.ConfMultiGet
          case Some("Conf.Pre.Get") => DspScheme.ConfPreGet
          case None => DspScheme.Other
          case _ => DspScheme.Error
        }
        val lackScheme = scheme.fieldNames.toSet -- record.keySet
        val lackData = lackScheme.zip(Array.fill(lackScheme.size)("null")).toMap
        val data = record.map(kv => (kv._1, if (kv._2.nonEmpty) kv._2 else "null"))
        val outPut = data ++ lackData - "do"
        val fitness = outPut.keySet -- scheme.fieldNames
        (data.getOrElse("do", "null"), outPut, fitness)
      }
      .mapPartitions { partitions =>
        val res = partitions.map { record =>
          val output = record._2 + ("do" -> record._1)
          kafkaProducer.value.send(broadOutParaForKafka.value.getOrElse("topic", "dsp"), output.mkString(","))
          record
        }
        res
      }
    cleanData.print(1)

    val oneBatch = cleanData.window(Seconds(writeFileWindowLength), Seconds(writeFileWindowLength)).foreachRDD(rdd => {
      // 保存未知数据
      val errorDataRdd = rdd.filter(_._3.size != 0)
      if (!errorDataRdd.isEmpty) errorDataRdd.repartition(1).saveAsTextFile(ErrorDataDir + "/" + System.currentTimeMillis())
      // 保存已知数据
      val dataRdd = rdd.filter(_._3.size == 0).map(one => (one._1, one._2.toList.sortBy(_._1).map(_._2))).persist
      val keys = schemeMap.keySet
      val rdds = for (key <- keys) yield (key, dataRdd.filter(_._1 == key).map(_._2))
      dataRdd.unpersist(false)
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      for (rdd <- rdds) {
        val when = dateFormat.format(new Date(System.currentTimeMillis()))
        rdd._2
          .map(row => row.mkString(","))
          .repartition(1)
          .saveAsTextFile("/sources/raw/streaming/" + rdd._1.replace(".", "_") + "/" + when + "_" + System.currentTimeMillis())
      }
    })

    //  val oneBatch = cleanData.window(Seconds(writeFileWindowLength), Seconds(writeFileWindowLength)).foreachRDD(rdd => {
    //    if (!rdd.isEmpty()) {
    //      // 保存未知数据
    //      val errorDataRdd = rdd.filter(_._3.size != 0)
    //      if (!errorDataRdd.isEmpty) errorDataRdd.saveAsTextFile(ErrorDataDir + "/" + System.currentTimeMillis())
    //      // 保存已知数据
    //      val dataRdd = rdd.filter(_._3.size == 0).map(one => (one._1, Row.fromSeq(one._2.toList.sortBy(_._1).map(_._2)))).persist
    //      val keys = dataRdd.map(_._1).aggregate(mutable.Set[String]())((set, key) => set += key, (set1, set2) => set1 ++= set2).toList
    //      val rdds = for (key <- keys) yield (key, dataRdd.filter(_._1 == key).map(_._2))
    //      dataRdd.unpersist(false)
    //      val dfs = for (oneType <- rdds) yield (oneType._1, sqlContext.createDataFrame(oneType._2, schemeMap(oneType._1)))
    //      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    //      for (df <- dfs) {
    //        val bigDate = df._2.agg(Map(("ctime", "max"))).head().getString(0).replace(".", "")
    //        val when = dateFormat.format(new Date(bigDate.toLong))
    //        df._2.rdd
    //          .map(row => row.mkString(","))
    //          .saveAsTextFile("/sources/raw/streaming/" + df._1.replace(".", "_") + "/" + when + "_" + System.currentTimeMillis())
    //      }
    //    }
    //  })
    ssc.start()
    ssc.awaitTermination()
  }
}



