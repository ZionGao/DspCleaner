package com.moxiu.bigdata

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object DspScheme {
  val AppAd = StructType(
    List(
      StructField("ads_id", StringType, true),
      StructField("ctime", StringType, false),
      StructField("ip", StringType, false)))


  val AppCustom = StructType(
    List(
      StructField("ctime", StringType, false),
      StructField("ip", StringType, false),
      StructField("is_application", StringType, true)))

  val AppEssential = StructType(
    List(
      StructField("ctime", StringType, false),
      StructField("ip", StringType, false)))

  val AppShow = StructType(
    List(
      StructField("channel", StringType, true),
      StructField("ctime", StringType, false),
      StructField("ip", StringType, false))
  )

  val CollectClick = StructType(
    List(
      StructField("adid", StringType, true),
      StructField("adsid", StringType, true),
      StructField("androidID", StringType, true),
      StructField("child", StringType, true),
      StructField("click", StringType, true),
      StructField("clk1", StringType, true),
      StructField("ctime", StringType, false),
      StructField("f", StringType, true),
      StructField("imei", StringType, true),
      StructField("ip", StringType, false),
      StructField("k", StringType, true),
      StructField("lat", StringType, true),
      StructField("lng", StringType, true),
      StructField("loc", StringType, true),
      StructField("model", StringType, true),
      StructField("os", StringType, true),
      StructField("p", StringType, true),
      StructField("package", StringType, true),
      StructField("pluginvcode", StringType, true),
      StructField("release", StringType, true),
      StructField("source", StringType, true),
      StructField("subtype", StringType, true),
      StructField("summ", StringType, true),
      StructField("summary", StringType, true),
      StructField("title", StringType, true),
      StructField("type", StringType, true),
      StructField("ua", StringType, true),
      StructField("uuid", StringType, true),
      StructField("vcode", StringType, true),
      StructField("vendor", StringType, true),
      StructField("ver", StringType, true),
      StructField("w", StringType, true)))

  val CollectDownload = StructType(
    List(
      StructField("adid", StringType, true),
      StructField("adsid", StringType, true),
      StructField("androidID", StringType, true),
      StructField("child", StringType, true),
      StructField("click", StringType, true),
      StructField("ctime", StringType, false),
      StructField("imei", StringType, true),
      StructField("ip", StringType, false),
      StructField("lat", StringType, true),
      StructField("lng", StringType, true),
      StructField("loc", StringType, true),
      StructField("model", StringType, true),
      StructField("package", StringType, true),
      StructField("pluginvcode", StringType, true),
      StructField("release", StringType, true),
      StructField("source", StringType, true),
      StructField("subtype", StringType, true),
      StructField("summary", StringType, true),
      StructField("title", StringType, true),
      StructField("type", StringType, true),
      StructField("ua", StringType, true),
      StructField("uuid", StringType, true),
      StructField("vcode", StringType, true),
      StructField("vendor", StringType, true),
      StructField("ver", StringType, true)))

  val CollectInstall = StructType(
    List(
      StructField("adid", StringType, true),
      StructField("adsid", StringType, true),
      StructField("androidID", StringType, true),
      StructField("child", StringType, true),
      StructField("click", StringType, true),
      StructField("ctime", StringType, false),
      StructField("imei", StringType, true),
      StructField("ip", StringType, false),
      StructField("lat", StringType, true),
      StructField("lng", StringType, true),
      StructField("loc", StringType, true),
      StructField("model", StringType, true),
      StructField("package", StringType, true),
      StructField("pluginvcode", StringType, true),
      StructField("release", StringType, true),
      StructField("source", StringType, true),
      StructField("subtype", StringType, true),
      StructField("summary", StringType, true),
      StructField("title", StringType, true),
      StructField("type", StringType, true),
      StructField("ua", StringType, true),
      StructField("uuid", StringType, true),
      StructField("vcode", StringType, true),
      StructField("vendor", StringType, true),
      StructField("ver", StringType, true)
    ))

  val CollectShow = StructType(
    List(
      StructField("adid", StringType, true),
      StructField("adsid", StringType, true),
      StructField("androidID", StringType, true),
      StructField("child", StringType, true),
      StructField("click", StringType, true),
      StructField("ctime", StringType, false),
      StructField("im", StringType, true),
      StructField("imei", StringType, true),
      StructField("ip", StringType, false),
      StructField("lat", StringType, true),
      StructField("lng", StringType, true),
      StructField("loc", StringType, true),
      StructField("model", StringType, true),
      StructField("pack", StringType, true),
      StructField("package", StringType, true),
      StructField("pluginvcode", StringType, true),
      StructField("pluginversion", StringType, true),
      StructField("release", StringType, true),
      StructField("source", StringType, true),
      StructField("su", StringType, true),
      StructField("subtype", StringType, true),
      StructField("summary", StringType, true),
      StructField("title", StringType, true),
      StructField("type", StringType, true),
      StructField("ua", StringType, true),
      StructField("uuid", StringType, true),
      StructField("vcode", StringType, true),
      StructField("vendor", StringType, true),
      StructField("ver", StringType, true)
    ))

  val ConfCache = StructType(
    List(
      StructField("androidID", StringType, true),
      StructField("carrier", StringType, true),
      StructField("child", StringType, true),
      StructField("conn", StringType, true),
      StructField("ctime", StringType, false),
      StructField("dpi", StringType, true),
      StructField("id", StringType, true),
      StructField("imei", StringType, true),
      StructField("ip", StringType, false),
      StructField("mac", StringType, true),
      StructField("model", StringType, true),
      StructField("release", StringType, true),
      StructField("skips", StringType, true),
      StructField("ua", StringType, true),
      StructField("vcode", StringType, true),
      StructField("vendor", StringType, true),
      StructField("ver", StringType, true)
    ))

  val ConfGet = StructType(
    List(
      StructField("adcount", StringType, true),
      StructField("androidID", StringType, true),
      StructField("car", StringType, true),
      StructField("carrier", StringType, true),
      StructField("child", StringType, true),
      StructField("conn", StringType, true),
      StructField("ctime", StringType, false),
      StructField("dpi", StringType, true),
      StructField("extra", StringType, true),
      StructField("id", StringType, true),
      StructField("imei", StringType, true),
      StructField("index", StringType, true),
      StructField("ip", StringType, false),
      StructField("keyword", StringType, true),
      StructField("lat", StringType, true),
      StructField("lng", StringType, true),
      StructField("loc", StringType, true),
      StructField("mac", StringType, true),
      StructField("model", StringType, true),
      StructField("pluginvcode", StringType, true),
      StructField("pluginversion", StringType, true),
      StructField("position", StringType, true),
      StructField("release", StringType, true),
      StructField("skips", StringType, true),
      StructField("ua", StringType, true),
      StructField("vcode", StringType, true),
      StructField("ve", StringType, true),
      StructField("vendor", StringType, true),
      StructField("ver", StringType, true),
      StructField("yikikata", StringType, true)
    ))

  val ConfMultiGet = StructType(
    List(
      StructField("androidID", StringType, true),
      StructField("carrier", StringType, true),
      StructField("child", StringType, true),
      StructField("conn", StringType, true),
      StructField("ctime", StringType, false),
      StructField("dpi", StringType, true),
      StructField("ids", StringType, true),
      StructField("imei", StringType, true),
      StructField("ip", StringType, false),
      StructField("mac", StringType, true),
      StructField("model", StringType, true),
      StructField("pluginvcode", StringType, true),
      StructField("release", StringType, true),
      StructField("skips", StringType, true),
      StructField("ua", StringType, true),
      StructField("vcode", StringType, true),
      StructField("vendor", StringType, true),
      StructField("ver", StringType, true)
    ))

  val ConfPreGet = StructType(
    List(
      StructField("androidID", StringType, true),
      StructField("carrier", StringType, true),
      StructField("child", StringType, true),
      StructField("conn", StringType, true),
      StructField("ctime", StringType, false),
      StructField("dpi", StringType, true),
      StructField("extra", StringType, true),
      StructField("ids", StringType, true),
      StructField("imei", StringType, true),
      StructField("ip", StringType, false),
      StructField("mac", StringType, true),
      StructField("model", StringType, true),
      StructField("release", StringType, true),
      StructField("skips", StringType, true),
      StructField("ua", StringType, true),
      StructField("vcode", StringType, true),
      StructField("vendor", StringType, true),
      StructField("ver", StringType, true)
    ))

  val Other = StructType(
    List(
      StructField("ctime", StringType, false),
      StructField("ip", StringType, false),
      StructField("ua", StringType, true)
    ))

  val Error = StructType(
    List(
      StructField("data", StringType, false)
    ))

}
