#' Apache Spark DBI resource connector for S3
#'
#' Makes a Apache Spark connection object, that is also a DBI connection object, from a S3 resource description.
#'
#' @docType class
#' @format A R6 object of class SparkResourceConnector
#' @import R6
#' @import httr
#' @import sparklyr
#' @export
S3SparkResourceConnector <- R6::R6Class(
  "S3SparkResourceConnector",
  inherit = SparkResourceConnector,
  public = list(
    
    #' @description Create a SparkResourceConnector instance.
    #' @return A SparkResourceConnector object.
    initialize = function() {},
    
    #' @description Check if the provided resource applies to a Apache Spark server.
    #'   The resource URL scheme must be one of "s3+spark", "s3+spark+http" or "s3+spark+https".
    #' @param resource The resource object to validate.
    #' @return A logical.
    isFor = function(resource) {
      "resource" %in% class(resource) && super$parseURL(resource)$scheme %in% c("s3+spark", "s3+spark+http", "s3+spark+https")
    },
    
    #' @description Creates a DBI connection object from a Apache Spark resource.
    #' @param resource A valid resource object.
    #' @return A DBI connection object.
    createDBIConnection = function(resource) {
      if (self$isFor(resource)) {
        super$loadDBI()
        private$loadSparklyr()
        # Extract base URL without s3config section for parsing
        baseUrl <- strsplit(resource$url, "//s3config::")[[1]][1]
        resource$url <- baseUrl
        url <- super$parseURL(resource)
        
        conf <- spark_config()
        
        # FIXME
        #Sys.setenv("AWS_ACCESS_KEY_ID" = resource$identity, "AWS_SECRET_ACCESS_KEY" = resource$secret)
        conf$`spark.hadoop.fs.s3a.access.key` <- resource$identity
        conf$`spark.hadoop.fs.s3a.secret.key` <- resource$secret
        
        conf$`spark.hadoop.fs.s3a.impl` <- "org.apache.hadoop.fs.s3a.S3AFileSystem"
        conf$`spark.hadoop.fs.s3a.aws.credentials.provider` <- "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        conf$`spark.hadoop.fs.s3a.path.style.access` <- "true"
        conf$`spark.hadoop.fs.s3a.connection.ssl.enabled` <- "true"
        master <- "local"
        version <- NULL
        region <- private$extractRegion(resource)
        for (n in names(url$query)) {
          if (n == "master") {
            master <- url$query$master
          } else if (n == "version") {
            version <- url$query$version
          } else if (n == "read") {
            if (url$query$read == "delta") {
              conf$`spark.sql.extensions` <- "io.delta.sql.DeltaSparkSessionExtension"
              conf$`spark.sql.catalog.spark_catalog` <- "org.apache.spark.sql.delta.catalog.DeltaCatalog"
              conf$`spark.databricks.delta.retentionDurationCheck.enabled` <- "false"  
            }
          } else {
            conf[n] <- url$query[[n]]
          }
        }
        
        if (identical(url$scheme, "s3+spark")) {
          # Only set AWS S3 endpoint if region is specified
          if (!is.null(region) && nchar(region) > 0) {
            if (region == "us-east-1") {
              conf$`spark.hadoop.fs.s3a.endpoint` <- "s3.amazonaws.com"
            } else {
              conf$`spark.hadoop.fs.s3a.endpoint` <- paste0("s3.", region, ".amazonaws.com")
            }
          }
          # If no region specified, let Spark/AWS use default region resolution
          conn <- sparklyr::spark_connect(master = master, version = version, config = conf, spark_home = spark_home_dir())
        } else {
          protocol <- "http"
          if (identical(url$scheme, "s3+spark+https")) {
            protocol <- "https"
          }
          conf$`spark.hadoop.fs.s3a.endpoint` <- paste0(protocol, "://", url$hostname, ifelse(is.null(url$port), "", paste0(":", url$port)))
          if (isTRUE(getOption("verbose"))) {
            print(paste0("master=", master))
            print(paste0("version=", version))
            print(paste0("spark_home=", spark_home_dir()))
            print(conf)
          }
          conn <- sparklyr::spark_connect(master = master, version = version, config = conf, spark_home = spark_home_dir())
        }
      } else {
        stop("Resource is not located in Apache Spark")
      }
    },
    
    #' @description Get the SQL table name from the resource URL.
    #' @param resource A valid resource object.
    #' @return The SQL table name.
    getTableName = function(resource) {
      # Extract base URL without s3config section
      baseUrl <- strsplit(resource$url, "//s3config::")[[1]][1]
      url <- httr::parse_url(baseUrl)
      if (is.null(url$path)) {
        stop("No database table name")
      } else {
        # Construct proper s3a:// URL with bucket and object path
        if (identical(url$scheme, "s3+spark")) {
          paste0("s3a://", url$host, "/", url$path)
        } else {
          # For HTTP S3 (Minio), use the full path
          paste0("s3a://", url$path)
        }
      }
    },
    
    #' @description Read a table as a vanilla tibble using DBI connection object.
    #' @param conn A DBI connection object.
    #' @param resource A valid resource object.
    readDBTable = function(conn, resource) {
      tibble::as_tibble(self$readDBTibble(conn, resource))
    },
    
    #' @description Read a table as a SQL tibble using DBI connection object.
    #' @param conn A DBI connection object.
    #' @param resource A valid resource object.
    readDBTibble = function(conn, resource) {
      table <- self$getTableName(resource)
      if (private$getReadParam(resource) == "delta") {
        sparklyr::spark_read_delta(conn, table)
      } else {
        sparklyr::spark_read_parquet(conn, table)
      }
    },
    
    #' @description Close the DBI connection to Apache Spark.
    #' @param conn A DBI connection object.
    closeDBIConnection = function(conn) {
      if ("spark_connection" %in% class(conn)) {
        sparklyr::spark_disconnect(conn)
      }
    }
    
  ),
  private = list(
    extractRegion = function(resource) {
      # Extract region from URL structure like s3+spark://bucket/object//s3config::/region:us-east-1
      parts <- strsplit(resource$url, "//s3config::")[[1]]
      if (length(parts) > 1) {
        config_part <- parts[2]
        region_match <- regexec("/region:([^/]+)", config_part)
        if (region_match[[1]][1] != -1) {
          return(regmatches(config_part, region_match)[[1]][2])
        }
      }
      return(NULL)
    },
    
    getReadParam = function(resource) {
      url <- httr::parse_url(resource$url)
      if (!is.null(url$query) && !is.null(url$query$read)) {
        url$query$read
      } else {
        "parquet"
      }
    },
    loadSparklyr = function() {
      if (!require("sparklyr")) {
        install.packages("sparklyr", repos = "https://cloud.r-project.org")
        library(sparklyr)
      }
      if (is.null(spark_home_dir())) {
        message("Installing Apache Spark")
        # spark package
        spark_install(version="3.2.1", hadoop_version = "3.2")
      }
      if (!is.null(spark_home_dir())) {
        # additional jars
        jars <- c("https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar",
                  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar",
                  "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/1.1.0/delta-core_2.12-1.1.0.jar")
        lapply(jars, function(jar) {
          if (!file.exists(file.path(spark_home_dir(), "jars", basename(jar)))) {
            httr::GET(jar, write_disk(file.path(spark_home_dir(), "jars", basename(jar)), overwrite = TRUE))
          }
        })
      }
    }
  )
)
