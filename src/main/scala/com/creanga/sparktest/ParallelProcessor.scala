package com.creanga.sparktest

class ParallelProcessor extends LazyLogging {

    while(true){
      logger.debug("debug")
      logger.info("info")
      Thread.sleep(1000)
    }

}
