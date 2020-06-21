package com.akafka.data

import io.circe.generic.extras.{Configuration, ConfiguredJsonCodec}

object CovidRecord {
  implicit val config: Configuration = Configuration.default.withSnakeCaseMemberNames

}

@ConfiguredJsonCodec  final case class CovidRecord(
  specimenCollectionDate: String,
  caseDisposition: String,
  transmissionCategory: String,
  caseCount: String,
  lastUpdatedAt: String
)
