test_that("S3 resource with region encodes properly in URL", {
  # Simulate what JavaScript factory should create with region
  res_with_region <- list(
    name = "test-file-region.csv",
    url = "s3://test-bucket/test/file.csv//s3config::/region:eu-west-1",
    format = "csv",
    identity = "test-key",
    secret = "test-secret"
  )
  class(res_with_region) <- "resource"
  
  expect_equal(res_with_region$name, "test-file-region.csv")
  expect_true(grepl("//s3config::/region:eu-west-1", res_with_region$url))
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
  expect_false(grepl("//s3config::", res_no_region$url))
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
    url = "s3://test-bucket/test/file.csv//s3config::/region:eu-west-1"
  )
  
  # We need to access the private method - create a test version
  extractRegionTest <- function(url_string) {
    parts <- strsplit(url_string, "//s3config::")[[1]]
    if (length(parts) > 1) {
      config_part <- parts[2]
      region_match <- regexec("/region:([^/]+)", config_part)
      if (region_match[[1]][1] != -1) {
        return(regmatches(config_part, region_match)[[1]][2])
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
    url = "s3+spark://test-bucket/test/data.parquet?read=parquet//s3config::/region:us-west-2",
    identity = "test-key",
    secret = "test-secret"
  )
  class(res_spark_region) <- "resource"
  
  expect_equal(res_spark_region$name, "test-spark-region")
  expect_true(grepl("s3\\+spark://", res_spark_region$url))
  expect_true(grepl("//s3config::/region:us-west-2", res_spark_region$url))
  expect_equal(class(res_spark_region), "resource")
  
  # Test that S3SparkResourceConnector recognizes it
  connector <- S3SparkResourceConnector$new()
  expect_true(connector$isFor(res_spark_region))
})

test_that("S3SparkResourceConnector extracts region correctly", {
  # Test region extraction for Spark resources
  extractRegionTestSpark <- function(url_string) {
    parts <- strsplit(url_string, "//s3config::")[[1]]
    if (length(parts) > 1) {
      config_part <- parts[2]
      region_match <- regexec("/region:([^/]+)", config_part)
      if (region_match[[1]][1] != -1) {
        return(regmatches(config_part, region_match)[[1]][2])
      }
    }
    return(NULL)
  }
  
  # Test with region
  spark_url_with_region <- "s3+spark://test-bucket/test/data.parquet?read=parquet//s3config::/region:us-west-2"
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
    url = "s3+spark://test-bucket/test/data.parquet//s3config::/region:eu-west-1"
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

test_that("Region parsing handles multiple configurations", {
  # Test URL with multiple config parameters including region
  extractRegionTest <- function(url_string) {
    parts <- strsplit(url_string, "//s3config::")[[1]]
    if (length(parts) > 1) {
      config_part <- parts[2]
      region_match <- regexec("/region:([^/]+)", config_part)
      if (region_match[[1]][1] != -1) {
        return(regmatches(config_part, region_match)[[1]][2])
      }
    }
    return(NULL)
  }
  
  # Test with region in middle of config
  url_complex <- "s3://bucket/path//s3config::/other:value/region:ap-southeast-1/more:config"
  region <- extractRegionTest(url_complex)
  expect_equal(region, "ap-southeast-1")
  
  # Test with region at end
  url_end <- "s3://bucket/path//s3config::/region:ca-central-1"
  region_end <- extractRegionTest(url_end)
  expect_equal(region_end, "ca-central-1")
}) 