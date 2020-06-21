package com.akafka.influx

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, HttpResponse, Uri}

import scala.concurrent.Future

class Writer(host: String = "http://localhost:8086", database: String = "sf_covid") {
  def write(points: List[String])(implicit system: ActorSystem): Future[HttpResponse] = {
    val body = HttpEntity(points.mkString("\n"))
    val url = s"$host/write?db=$database&precision=ms"
    val request = HttpRequest(method = HttpMethods.POST, uri = Uri(url), entity = body)

    Http().singleRequest(request)
  }
}
