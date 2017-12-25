package com.phx.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

/**
  * Created by uszanr8 on 12/25/2017.
  */
object Simple_Producer extends App{



  val props = new Properties()
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  import org.apache.kafka.clients.producer.ProducerConfig

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2181")

  props.put(ProducerConfig.CLIENT_ID_CONFIG, "SimpleProducer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val TOPIC = "Weather_Topic"
  val time = System.currentTimeMillis
  val producer = new KafkaProducer[String, String](props)
  try {
    for (i <- 1 to 10000) {

      val rand = new Random(1000)
      val producerRecord = new ProducerRecord(TOPIC, "key", s"" + rand + "_" + new java.util.Date)
      val metadata = producer.send(producerRecord).get()
      System.out.println("-----sent record(key=%s value=%s) " + producerRecord.key() + producerRecord.value(), metadata.partition(),
        metadata.offset())
      Thread.sleep(100)
    }
    val record = new ProducerRecord(TOPIC, "key", "the end " + new java.util.Date)
    producer.send(record)
  } finally {
    producer.flush();
    producer.close();
  }
  producer.close()
  System.out.println("-----DONE")


}
