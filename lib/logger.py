class Log4j:
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        root_class = "spark.anaconda.basic"
        conf = spark.sparkContext.getConf()
        spark_name = conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(root_class + "." + spark_name)#using the logger name plus the application name
# creating a log info message
    def info(self, msg):
        self.logger.info(msg)
#creating a log warning message
    def warn(self, msg):
        self.logger.warn(msg)
#creating a log error message
    def error(self, msg):
        self.logger.error(msg)
#creating a log debug message
    def debug(self,msg):
        self.logger.debug(msg)