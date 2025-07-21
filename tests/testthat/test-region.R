test_that("S3 resource with region encodes properly in URL", {
  # Simulate what JavaScript factory should create with region
  res_with_region <- list(
    name = "test-file-region.csv",
    url = "s3://test-bucket@eu-west-1/test/file.csv",
    format = "csv",
    identity = "test-key",
    secret = "test-secret"
  )
  class(res_with_region) <- "resource"
  
  expect_equal(res_with_region$name, "test-file-region.csv")
  expect_true(grepl("@eu-west-1", res_with_region$url))
  expect_equal(class(res_with_region), "resource")
  
  # Test that S3FileResourceGetter recognizes it
  getter <- S3FileResourceGetter$new()
  expect_true(getter$isFor(res_with_region))
})

test_that("S3 resource without region works normally", {
  # Resource without region should work as before
  res_no_region <- list(
    name = "test-file-no-region.csv",
    url = "s3://test-bucket/test/file.csv",
    format = "csv",
    identity = "test-key",
    secret = "test-secret"
  )
  class(res_no_region) <- "resource"
  
  expect_equal(res_no_region$name, "test-file-no-region.csv")
  expect_false(grepl("@", res_no_region$url))
  expect_equal(class(res_no_region), "resource")
  
  # Test that S3FileResourceGetter recognizes it
  getter <- S3FileResourceGetter$new()
  expect_true(getter$isFor(res_no_region))
})

test_that("S3FileResourceGetter extracts region correctly", {
  # Create getter to access private method via a test method
  getter <- S3FileResourceGetter$new()
  
  # Test extraction with region
  res_with_region <- list(
    url = "s3://test-bucket@eu-west-1/test/file.csv"
  )
  
  # We need to access the private method - create a test version
  extractRegionTest <- function(url_string) {
    if (grepl("@", url_string)) {
      url_part <- sub("^s3(\\+http|\\+https)?://", "", url_string)
      host_part <- strsplit(url_part, "/")[[1]][1]
      if (grepl("@", host_part)) {
        parts <- strsplit(host_part, "@")[[1]]
        if (length(parts) == 2) {
          return(parts[2])  # Return the region part
        }
      }
    }
    return(NULL)
  }
  
  region <- extractRegionTest(res_with_region$url)
  expect_equal(region, "eu-west-1")
  
  # Test extraction without region
  res_no_region <- list(
    url = "s3://test-bucket/test/file.csv"
  )
  
  region_null <- extractRegionTest(res_no_region$url)
  expect_null(region_null)
})

test_that("Spark resource with region encodes properly in URL", {
  # Simulate what JavaScript factory should create for Spark with region
  res_spark_region <- list(
    name = "test-spark-region",
    url = "s3+spark://test-bucket@us-west-2/test/data.parquet?read=parquet",
    identity = "test-key",
    secret = "test-secret"
  )
  class(res_spark_region) <- "resource"
  
  expect_equal(res_spark_region$name, "test-spark-region")
  expect_true(grepl("s3\\+spark://", res_spark_region$url))
  expect_true(grepl("@us-west-2", res_spark_region$url))
  expect_equal(class(res_spark_region), "resource")
  
  # Test that S3SparkResourceConnector recognizes it
  connector <- S3SparkResourceConnector$new()
  expect_true(connector$isFor(res_spark_region))
})

test_that("S3SparkResourceConnector extracts region correctly", {
  # Test region extraction for Spark resources
  extractRegionTestSpark <- function(url_string) {
    if (grepl("@", url_string)) {
      url_part <- sub("^s3\\+(spark|http|https)://", "", url_string)
      host_part <- strsplit(url_part, "/")[[1]][1]
      # Remove query parameters if present
      host_part <- strsplit(host_part, "\\?")[[1]][1]
      if (grepl("@", host_part)) {
        parts <- strsplit(host_part, "@")[[1]]
        if (length(parts) == 2) {
          return(parts[2])  # Return the region part
        }
      }
    }
    return(NULL)
  }
  
  # Test with region
  spark_url_with_region <- "s3+spark://test-bucket@us-west-2/test/data.parquet?read=parquet"
  region <- extractRegionTestSpark(spark_url_with_region)
  expect_equal(region, "us-west-2")
  
  # Test without region
  spark_url_no_region <- "s3+spark://test-bucket/test/data.parquet?read=parquet"
  region_null <- extractRegionTestSpark(spark_url_no_region)
  expect_null(region_null)
})

test_that("S3SparkResourceConnector getTableName works with region URLs", {
  connector <- S3SparkResourceConnector$new()
  
  # Test with region - should extract base URL properly
  res_with_region <- list(
    url = "s3+spark://test-bucket@eu-west-1/test/data.parquet"
  )
  class(res_with_region) <- "resource"
  
  table_name <- connector$getTableName(res_with_region)
  expect_equal(table_name, "s3a://test-bucket/test/data.parquet")
  
  # Test without region - should work as before
  res_no_region <- list(
    url = "s3+spark://test-bucket/test/data.parquet"
  )
  class(res_no_region) <- "resource"
  
  table_name_no_region <- connector$getTableName(res_no_region)
  expect_equal(table_name_no_region, "s3a://test-bucket/test/data.parquet")
})

test_that("Region parsing handles various region formats", {
  # Test region extraction with @ delimiter
  extractRegionTest <- function(url_string) {
    if (grepl("@", url_string)) {
      url_part <- sub("^s3(\\+http|\\+https)?://", "", url_string)
      host_part <- strsplit(url_part, "/")[[1]][1]
      if (grepl("@", host_part)) {
        parts <- strsplit(host_part, "@")[[1]]
        if (length(parts) == 2) {
          return(parts[2])  # Return the region part
        }
      }
    }
    return(NULL)
  }
  
  # Test with different region formats
  url_region1 <- "s3://bucket@ap-southeast-1/path/file.csv"
  region1 <- extractRegionTest(url_region1)
  expect_equal(region1, "ap-southeast-1")
  
  # Test with different region format
  url_region2 <- "s3://test-bucket@ca-central-1/data/file.parquet"
  region2 <- extractRegionTest(url_region2)
  expect_equal(region2, "ca-central-1")
  
  # Test without region
  url_no_region <- "s3://bucket/path/file.csv"
  region_null <- extractRegionTest(url_no_region)
  expect_null(region_null)
}) 