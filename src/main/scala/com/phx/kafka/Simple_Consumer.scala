package com.phx.kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords}


/**
  * Created by uszanr8 on 12/25/2017.
  */
object Simple_Consumer extends App {

  val props = new Properties()
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  import org.apache.kafka.clients.producer.ProducerConfig

  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2181")

  props.put(ConsumerConfig.CLIENT_ID_CONFIG, "SimpleProducer")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

  val TOPIC = "Weather_Topic"
  import org.apache.kafka.clients.consumer.Consumer
  import org.apache.kafka.clients.consumer.KafkaConsumer

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(util.Collections.singletonList(TOPIC))

    while(true){
    val  consumerRecords = consumer.poll(100)
      consumerRecords.forEach( record => {
        System.out.println("---received record(key=%s value=%s)) "+ record.key()+ record.value(),
          record.partition(), record.offset());
      })

  }
  consumer.commitAsync
  consumer.close()

}
