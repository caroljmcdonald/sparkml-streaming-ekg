/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.sparkkafka.ekg

import org.apache.kafka.clients.producer.KafkaProducer
import scala.collection.mutable

object KafkaProducerFactory {

  import scala.collection.JavaConversions._

  private val producers = mutable.Map[Map[String, String], KafkaProducer[String, String]]()

  def getOrCreateProducer(config: Map[String, String]): KafkaProducer[String, String] = {

    val defaultConfig = Map(
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )

    val finalConfig = defaultConfig ++ config
    val brokers = "maprdemo:9092"
    // set up producer properties

    producers.getOrElseUpdate(finalConfig, {

      val producer = new KafkaProducer[String, String](finalConfig)
      sys.addShutdownHook {
        producer.close()
      }

      producer
    })
  }
}
