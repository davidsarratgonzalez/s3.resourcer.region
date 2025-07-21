#' AWS S3 file resource getter
#'
#' Access a file that is in the Amazon Web Services S3 file store or in a HTTP S3 compatible file store such as Minio. 
#' For AWS S3 the host name is the bucket name. For Minio, the url will include http or https base protocol. 
#' Credentials may apply.
#'
#' @docType class
#' @format A R6 object of class S3FileResourceGetter
#' @import R6 resourcer aws.s3
#' @export
S3FileResourceGetter <- R6::R6Class(
  "S3FileResourceGetter",
  inherit = FileResourceGetter,
  public = list(

    #' @description Creates a new S3FileResourceGetter instance.
    #' @return A S3FileResourceGetter object.
    initialize = function() {},

    #' @description Check that the provided resource has a URL that locates a file accessible through "s3" protocol or 
    #' "s3+http" or "s3+https" protocol (i.e. using Minio implementation of the AWS S3 file store API over HTTP).
    #' @param resource The resource object to validate.
    #' @return A logical.
    isFor = function(resource) {
      if (super$isFor(resource)) {
        super$parseURL(resource)$scheme %in% c("s3", "s3+http", "s3+https")
      } else {
        FALSE
      }
    },

    #' @description Download the file from the remote address in a temporary location. Applies authentication if credentials are provided in the resource.
    #' @param resource A valid resource object.
    #' @param ... Unused additional parameters.
    #' @return The "resource.file" object.
    downloadFile = function(resource, ...) {
      if (self$isFor(resource)) {
        fileName <- super$extractFileName(resource)
        downloadDir <- super$makeDownloadDir()
        path <- file.path(downloadDir, fileName)
        # Extract base URL without s3config section for S3 operations
        baseUrl <- strsplit(resource$url, "//s3config::")[[1]][1]
        url <- httr::parse_url(baseUrl)

        private$loadS3()
        if (url$scheme == "s3") {
          # Extract region from URL if specified
          region <- private$extractRegion(resource)
          if (!is.null(region) && nchar(region) > 0) {
            aws.s3::save_object(object = url$path, bucket = url$host, 
                                file = path, overwrite = TRUE,
                                region = region,
                                key = resource$identity, secret = resource$secret)
          } else {
            aws.s3::save_object(object = url$path, bucket = url$host, 
                                file = path, overwrite = TRUE,
                                key = resource$identity, secret = resource$secret)
          }
          
        } else {
          bucket <- dirname(url$path)
          base_url <- url$host
          if (!is.null(url$port)) {
            base_url <- paste0(url$host, ":", url$port)
          }
          # Extract region from URL if specified
          region <- private$extractRegion(resource)
          if (!is.null(region) && nchar(region) > 0) {
            aws.s3::save_object(object = fileName, bucket = bucket, base_url = base_url, 
                                use_https = (url$scheme == "s3+https"), region = region, 
                                file = path, overwrite = TRUE,
                                key = resource$identity, secret = resource$secret)
          } else {
            aws.s3::save_object(object = fileName, bucket = bucket, base_url = base_url, 
                                use_https = (url$scheme == "s3+https"), 
                                file = path, overwrite = TRUE,
                                key = resource$identity, secret = resource$secret)
          }
        }
        super$newFileObject(path, temp = TRUE)
      } else {
        NULL
      }
    }

  ),
  private = list(
    loadS3 = function() {
      if (!require("aws.s3")) {
        install.packages("aws.s3", repos = "https://cloud.r-project.org", dependencies = TRUE)
      }
    },
    
    extractRegion = function(resource) {
      # Extract region from URL structure like s3://bucket/object//s3config::/region:us-east-1
      parts <- strsplit(resource$url, "//s3config::")[[1]]
      if (length(parts) > 1) {
        config_part <- parts[2]
        region_match <- regexec("/region:([^/]+)", config_part)
        if (region_match[[1]][1] != -1) {
          return(regmatches(config_part, region_match)[[1]][2])
        }
      }
      return(NULL)
    }
  )
)
