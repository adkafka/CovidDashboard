package com.akafka.data

import akka.http.scaladsl.model.DateTime

import scala.util.Try

object VaccinationRecord {
  /*
   * Example:
   * $ curl "https://covid.ourworldindata.org/data/vaccinations/vaccinations.csv"
   *   location,iso_code,date,total_vaccinations,daily_vaccinations,total_vaccinations_per_hundred,daily_vaccinations_per_million
   *   Argentina,ARG,2020-12-29,700,,0,
   *   Argentina,ARG,2020-12-30,,15656.5,,346.42
   *   Argentina,ARG,2020-12-31,32013,15656.5,0.07,346.42
   */
  def fromSingleRow(locationIndex: Int, dateIndex: Int, tIndex: Int, dIndex: Int, tphIndex: Int, dphIndex: Int)(row: String): Either[String, VaccinationRecord] = {
    val fields = row.split(',').toList

    for {
      location <- fields.lift(locationIndex).toRight("Missing location")
      epochMs <- fields.lift(dateIndex).toRight("Missing date").flatMap(dateToEpoch)
      totalVaccinations <- fields.lift(tIndex).toRight("Missing total").map(parseLong)
    } yield VaccinationRecord(
      location = location,
      epochMs = epochMs,
      totalVaccinations = totalVaccinations,
      dailyVaccinations = fields.lift(dIndex).flatMap(_.toDoubleOption),
      totalVaccinationsPerHundred = fields.lift(tphIndex).flatMap(_.toDoubleOption),
      dailyVaccinationsPerMillion = fields.lift(dphIndex).flatMap(_.toDoubleOption)
    )
  }

  def fromCsvRows(rows: String): Either[String, List[VaccinationRecord]] = {
    def getIdx(cols: List[String], column: String): Either[String, Int] = {
      val idx = cols.indexOf(column)
      if (idx >= 0) Right(idx)
      else Left(s"$column not found in header")
    }

    import cats.implicits._

    rows.split('\n').toList match {
      case header :: data =>
        val cols = header.split(',').toList
        for {
          locationColumn <- getIdx(cols, "location")
          dateIndex <- getIdx(cols, "date")
          tIndex <- getIdx(cols, "total_vaccinations")
          dIndex <- getIdx(cols, "daily_vaccinations")
          tphIndex <- getIdx(cols, "total_vaccinations_per_hundred")
          dphIndex <- getIdx(cols, "daily_vaccinations_per_million")

          parser = fromSingleRow(
            locationIndex = locationColumn,
            dateIndex = dateIndex,
            tIndex = tIndex,
            dIndex = dIndex,
            tphIndex = tphIndex,
            dphIndex = dphIndex
          )(_)

          parsed <- data.traverse(parser)
        } yield parsed
      case Nil =>
        Left("Header row not found")
    }
  }

  def dateToEpoch(date: String): Either[String, Long] = {
    DateTime.fromIsoDateTimeString(date + "T00:00:00.000Z").map(_.clicks).toRight(s"Invalid date : $date")
  }

  def parseLong(s: String): Option[Long] = Try(s.toLong).toOption
}

final case class VaccinationRecord(
  location: String,
  epochMs: Long,
  totalVaccinations: Option[Long],
  dailyVaccinations: Option[Double],
  totalVaccinationsPerHundred: Option[Double],
  dailyVaccinationsPerMillion: Option[Double]
)
