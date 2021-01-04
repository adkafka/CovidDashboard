package com.akafka

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.stream.ActorMaterializer
import cats.data._
import cats.implicits._
import com.akafka.data.{CovidRecord, VaccinationRecord}
import com.akafka.source.owid.vaccinations.{Reader => VaccinationReader}
import com.akafka.source.sf.{Reader => SfReader}
import com.akafka.transformation.{SfCovid, Vaccination}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object DataImporter extends App {
  def sfCovidImporter(url: String): EitherT[Future, String, (List[CovidRecord], List[String], HttpResponse)] = {
    val client = new influx.Writer(database = "sf_covid")
    for {
      data <- EitherT(SfReader.getData(url = url)).leftMap(_.toString)
      transformed <- EitherT.fromEither[Future](SfCovid.toInfluxProtocol(data))
      write <- EitherT.right[String](client.write(transformed))
    } yield (data, transformed, write)
  }

  def vaccinationImporter(url: String): EitherT[Future, String, (List[VaccinationRecord], List[String], HttpResponse)] = {
    val client = new influx.Writer(database = "vaccination")
    for {
      data <- EitherT(VaccinationReader.getData(url = url))
      transformed <- EitherT.fromEither[Future](Vaccination.toInfluxProtocol(data))
      write <- EitherT.right[String](client.write(transformed))
    } yield (data, transformed, write)
  }

  def logResult[A](result: EitherT[Future, String, A])(happyPath: A => Unit): Unit = result.value.onComplete {
    case Success(Right(a)) => happyPath(a)
    case Success(Left(err)) =>
      println(s"ERROR : $err")
      system.terminate().onComplete(_ => sys.exit(1))
    case Failure(exception) =>
      println(s"EXCEPTION : $exception")
      system.terminate().onComplete(_ => sys.exit(2))
  }

  implicit val system: ActorSystem = ActorSystem("sf-data-importer")
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val mat: ActorMaterializer = ActorMaterializer()

  val sfResult = sfCovidImporter("https://data.sfgov.org/resource/tvq9-ec9w.json")
  val vaccinationResult = vaccinationImporter("https://covid.ourworldindata.org/data/vaccinations/vaccinations.csv")

  logResult(sfResult){
    case (data, _, write) => println(s"SF Result : Done! Wrote ${data.length} points. Got $write.")
  }
  logResult(vaccinationResult){
    case (data, _, write) => println(s"Vaccination Result : Done! Wrote ${data.length} points. Got $write.")
  }

  for {
    _ <- sfResult
    _ <- vaccinationResult
  } yield {
    println("Complete")
    system.terminate().onComplete(_ => sys.exit(0))
  }
}
