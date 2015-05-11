/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow
package collectors
package scalastream
package sinks

// Java
import java.nio.ByteBuffer

// Thrift
import org.apache.thrift.TSerializer

// Logging
import org.slf4j.LoggerFactory

// Snowplow
import CollectorPayload.thrift.model1.CollectorPayload

// Define an interface for all sinks to use to store events.
trait AbstractSink {

  lazy val log = LoggerFactory.getLogger(getClass())

  def storeRawEvent(event: CollectorPayload, key: String): Array[Byte]

  // Serialize Thrift CollectorPayload objects
  private val thriftSerializer = new ThreadLocal[TSerializer] {
    override def initialValue = new TSerializer()
  }

  def serializeEvent(event: CollectorPayload): Array[Byte] = {
    splitAndSerialize(event)
    val serializer = thriftSerializer.get()
    serializer.serialize(event)
  }

  def splitAndSerialize(event: CollectorPayload): List[Array[Byte]] = {

    // json4s
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    import org.json4s._

    val MaxBytes = 51200 // TODO: make this vary based on the sink type

    val serializer = thriftSerializer.get()

    val everythingSerialized = serializer.serialize(event)

    val wholeEventBytes = ByteBuffer.wrap(everythingSerialized).capacity

    if (wholeEventBytes < MaxBytes && false) {
      List(everythingSerialized)
    } else {

      val oldBody = event.getBody

      val initialBodyBytes = ByteBuffer.wrap(oldBody.getBytes).capacity

      if (wholeEventBytes - initialBodyBytes >= MaxBytes && false) {
        log.error("Even without the body, the serialized event is too large")
        Nil
      } else {

        val bodySchemaData = parse(oldBody) \ "schema"

        val bodyDataArray = parse(oldBody) \ "data" match {
          case JNothing => None
          case data => Some(data)
        }

        val individualEvents: Option[List[String]] = for {
          d <- bodyDataArray
        } yield d.children.map(x => compact(x))

        val batchedIndividualEvents = individualEvents.map(SplitBatch.split(_, MaxBytes - wholeEventBytes + initialBodyBytes))

        batchedIndividualEvents match {
          case None => {
            log.error("Bad record with no data field")
            Nil
          }
          case Some(batches) => {
            batches.failedBigEvents foreach {f => log.error(s"Failed event with body $f for being too large")}
            batches.goodBatches.map(batch => {

              // Copy all data from the original event into the smaller events
              val payload = new CollectorPayload()
              payload.setSchema(event.getSchema)
              payload.setIpAddress(event.getIpAddress)
              payload.setTimestamp(event.getTimestamp)
              payload.setEncoding(event.getEncoding)
              payload.setCollector(event.getCollector)
              payload.setUserAgent(event.getUserAgent)
              payload.setRefererUri(event.getRefererUri)
              payload.setPath(event.getPath)
              payload.setQuerystring(event.getQuerystring)
              payload.setHeaders(event.getHeaders)
              payload.setContentType(event.getContentType)
              payload.setHostname(event.getHostname)
              payload.setNetworkUserId(event.getNetworkUserId)

              payload.setBody(compact(
                ("schema" -> bodySchemaData) ~
                ("data" -> batch.map(evt => parse(evt)))))

              serializer.serialize(payload)
            })
          }
        }
      }
    }

  }
}
