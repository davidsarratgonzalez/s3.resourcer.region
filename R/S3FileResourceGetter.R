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
        url <- httr::parse_url(resource$url)

        private$loadS3()
        if (url$scheme == "s3") {
          # Extract region and bucket from URL
          region <- private$extractRegion(resource)
          bucket <- private$extractBucket(resource)
          
          if (!is.null(region) && nchar(region) > 0) {
            aws.s3::save_object(object = url$path, bucket = bucket, 
                                file = path, overwrite = TRUE,
                                region = region,
                                key = resource$identity, secret = resource$secret)
          } else {
            aws.s3::save_object(object = url$path, bucket = bucket, 
                                file = path, overwrite = TRUE,
                                key = resource$identity, secret = resource$secret)
          }
          
        } else {
          bucket <- dirname(url$path)
          # Extract the actual host (without region) for base_url
          hostname <- private$extractBucket(resource)
          base_url <- hostname
          if (!is.null(url$port)) {
            base_url <- paste0(hostname, ":", url$port)
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
      # Extract region from URL structure like s3://bucket@region/object
      # Parse manually since httr::parse_url treats @ as user@host separator
      if (grepl("@", resource$url)) {
        url_part <- sub("^s3(\\+http|\\+https)?://", "", resource$url)  # Remove scheme
        host_part <- strsplit(url_part, "/")[[1]][1]  # Get host part before first /
        if (grepl("@", host_part)) {
          parts <- strsplit(host_part, "@")[[1]]
          if (length(parts) == 2) {
            return(parts[2])  # Return the region part
          }
        }
      }
      return(NULL)
    },
    
    extractBucket = function(resource) {
      # Extract bucket name from URL structure like s3://bucket@region/object
      # Parse manually since httr::parse_url treats @ as user@host separator
      if (grepl("@", resource$url)) {
        url_part <- sub("^s3(\\+http|\\+https)?://", "", resource$url)  # Remove scheme
        host_part <- strsplit(url_part, "/")[[1]][1]  # Get host part before first /
        if (grepl("@", host_part)) {
          parts <- strsplit(host_part, "@")[[1]]
          if (length(parts) == 2) {
            return(parts[1])  # Return the bucket part
          }
        }
      }
      # Fallback to normal URL parsing if no @ present
      url <- httr::parse_url(resource$url)
      return(url$hostname)
    }
  )
)
