package sparkstreaming.kafka.operation

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaOperation extends App {
  val sparkConf = new SparkConf()
    .setAppName("kafkaOperation")
    .setMaster("local[2]")
    .set("spark.local.dir", "./tmp")
    .set("spark.streaming.kafka.maxRatePerPartition", "10")
  val ssc = new StreamingContext(sparkConf, Seconds(2))
  val kafkaParams = Map[String, Object] (
    "bootstrap.servers"->"172.25.6.23:9092,172.25.6.22:9092,172.25.6.21:9092",
    "key.deserializer"->classOf[StringDeserializer],
    "value.deserializer"->classOf[StringDeserializer],
    "group.id"->"kafkaOperationGroup",
    "auto.offset.reset"->"latest",
    "enable.auto.commit"->(false: java.lang.Boolean)
  )
  val kafkaDirectStream = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String,String](List("kafkaOperation"), kafkaParams)
  )
  val nameAddrStream = kafkaDirectStream.map(_.value()).filter(record=>{
    val tokens = record.split("\t")
    tokens(2).toInt == 0
  }).map(record=>{
    val tokens = record.split("\t")
    (tokens(0), tokens(1))
  })
  val namePhoneStream = kafkaDirectStream.map(_.value()).filter(record=>{
    val tokens = record.split("\t")
    tokens(2).toInt == 1
  }).map(record=>{
    val tokens = record.split("\t")
    (tokens(0), tokens(1))
  })
  val nameAddrPhoneStream = nameAddrStream.join(namePhoneStream).map(record=>{
    s"姓名：${record._1}，地址：${record._2._1}，电话：${record._2._2}"
  })
  nameAddrPhoneStream.print()
  ssc.start()
  ssc.awaitTermination()

}
