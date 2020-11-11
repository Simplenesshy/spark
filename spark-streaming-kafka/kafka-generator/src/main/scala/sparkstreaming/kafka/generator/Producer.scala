package sparkstreaming.kafka.generator

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object Producer extends App {

  val topic = "kafkaOperation"
  val brokers = "172.25.6.23:9092,172.25.6.22:9092,172.25.6.21:9092"
  println(topic)
  println(brokers)
  // 设置随机数
  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "kafkaGenerator")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()
  // 模拟用户名地址数据
  val nameAddrs = Map("bob"->"shanghai#200000", "any"->"bejing#1000000","alice"->"shanghai#200000","tom"->"bejing#100000","lulu"->"hangzhou#3100000","nick"->"shanghai#2000000")
  val namePhones = Map("bob"->"15876746123","any"->"135128867848","alice"->"13687847632","tom"->"13987467321","lulu"->"13087467381","nick"->"13687483213")
  for (nameAddr <- nameAddrs) {
    val data = new ProducerRecord[String,String](topic, nameAddr._1,s"${nameAddr._1}\t${nameAddr._2}\t0")
    println("send data")
    producer.send(data)
    if (rnd.nextInt(100) < 50) Thread.sleep(rnd.nextInt(10))
  }
  for (namePhone <- namePhones) {
    val data = new ProducerRecord[String, String](topic, namePhone._1,s"${namePhone._1}\t${namePhone._2}\t1")
    producer.send(data)
    if (rnd.nextInt(100) < 50) Thread.sleep(rnd.nextInt(10))
  }
  System.out.println("sent per second: "+ (nameAddrs.size + namePhones.size)*1000/(System.currentTimeMillis() - t))
  producer.close()
}
