/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.sparkml.ekg

import org.apache.spark._

import org.apache.spark.sql.types._

import org.apache.spark.mllib.clustering.KMeans

import org.apache.spark.mllib.linalg.Vectors

object ClusterEkg {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkDFebay")
    val sc = new SparkContext(conf)

    /// 1. read in data from stream and convert to vector of Doubles...
    val rdd = sc.textFile("/user/user01/data/raw.tsv")
    val vrdd = rdd.map(line => Vectors.dense(line.split('\t').map(_.toDouble)))

    /// 2. window and normalize each record....
    val N = 32
    var window = for (i <- 0 to N - 1) yield math.pow(math.sin(math.Pi * i / (N - 1)), 2)

    def normalize(v: Array[Double]): Double = {
      var norm = 0.0
      for (i <- 0 to v.size - 1) { norm = norm + math.pow(v(i), 2) }
      math.sqrt(norm)
    }

    val processed = vrdd.map { v =>
      //for (i<-0 to v.size-1) {println( v(i))}
      val w = Array.fill[Double](N)(0.0)
      for (i <- 0 to v.size - 1) { w(i) = v(i) * window(i) }
      val wnorm = normalize(w)
      Vectors.dense(w.map(_ / wnorm))
    }
    processed.cache
    /// 3. create the clusters...
    val model = KMeans.train(processed, 300, 20)
    model.clusterCenters.foreach(println)
    model.save(sc, "/user/user01/data/anomaly-detection-master")

  }
}