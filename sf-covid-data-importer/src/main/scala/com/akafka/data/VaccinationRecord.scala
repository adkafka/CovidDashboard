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
  def fromSingleRow(row: String): Either[String, VaccinationRecord] = {
    val fields = row.split(',').toList

    for {
      location <- fields.headOption.toRight("Missing location")
      epochMs <- fields.lift(2).toRight("Missing date").flatMap(dateToEpoch)
      totalVaccinations <- fields.lift(3).toRight("Missing total").map(parseLong)
    } yield VaccinationRecord(
      location = location,
      epochMs = epochMs,
      totalVaccinations = totalVaccinations,
      dailyVaccinations = fields.lift(4).flatMap(_.toDoubleOption),
      totalVaccinationsPerHundred = fields.lift(5).flatMap(_.toDoubleOption),
      dailyVaccinationsPerMillion = fields.lift(6).flatMap(_.toDoubleOption)
    )
  }

  def fromCsvRows(rows: String): Either[String, List[VaccinationRecord]] = {
    import cats.implicits._

    rows
      .split('\n')
      .toList
      .drop(1) // drop header
      .traverse(r => fromSingleRow(r).leftMap(e => s"Invalid Row. Error : $e\nRow : $r"))
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
