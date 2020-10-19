package com.creanga.sparktest

import org.slf4j.{Logger, LoggerFactory}

trait LazyLogging  {
  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}
