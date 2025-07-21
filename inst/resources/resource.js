var s3_resourcer = {
  settings: {
    "title": "S3 Resources",
    "description": "File resources that can be retrieved from an Amazon Web Services S3 or HTTP S3 (ex: Minio) file stores.",
    "web": "https://github.com/obiba/s3.resourcer",
    "categories": [
      {
        "name": "s3",
        "title": "AWS S3 / HTTP S3",
        "description": "The resource is in an [Amazon Web Services S3](https://aws.amazon.com/s3/) or a HTTP S3 (ex: [Minio](https://min.io/)) file store. The file can be downloaded or accessed through Apache Spark."
      }
    ],
    "types": [
      {
        "name": "s3-rdata-file",
        "title": "R data file - AWS S3",
        "description": "File resource in R data format. The file will be downloaded from an AWS S3 file store.",
        "tags": ["s3", "data-file", "rdata-format"],
        "parameters": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "bucket",
              "type": "string",
              "title": "Bucket",
              "description": "The S3 bucket name."
            },
            {
              "key": "obj",
              "type": "string",
              "title": "Object path",
              "description": "The S3 object path."
            },
            {
              "key": "region",
              "type": "string",
              "title": "AWS Region",
              "description": "The AWS region where the bucket is located (e.g., 'us-east-1', 'eu-west-1'). Optional - if not provided, AWS will use its default region resolution."
            },
            {
              "key": "format",
              "type": "string",
              "title": "R object class",
              "description": "The primary class of the R object that is being loaded from the R data file. When there are several objects of this class, the one with the symbol with same name as the resource is chosen, otherwise the first one is selected."
            }
          ],
          "required": [
            "bucket", "obj", "format"
          ]
        },
        "credentials": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "description": "Credentials are optional. If not provided, it will default to AWS environment variables (if they exist).",
          "items": [
            {
              "key": "awskey",
              "type": "string",
              "title": "AWS key",
              "description": "The AWS Access Key ID. If missing, defaults to value stored in environment variable `AWS_ACCESS_KEY_ID`."
            },
            {
              "key": "awssecret",
              "type": "string",
              "title": "AWS secret",
              "format": "password",
              "description": "The AWS Secret Access Key. If missing, defaults to value stored in environment variable `AWS_SECRET_ACCESS_KEY`."
            }
          ]
        }
      },
      {
        "name": "s3-rds-file",
        "title": "RDS file - AWS S3",
        "description": "File resource in RDS format (serialized single R object). The file will be downloaded from an AWS S3 file store.",
        "tags": ["s3", "data-file", "rdata-format"],
        "parameters": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "bucket",
              "type": "string",
              "title": "Bucket",
              "description": "The S3 bucket name."
            },
            {
              "key": "obj",
              "type": "string",
              "title": "Object path",
              "description": "The S3 object path."
            },
            {
              "key": "region",
              "type": "string",
              "title": "AWS Region",
              "description": "The AWS region where the bucket is located (e.g., 'us-east-1', 'eu-west-1'). Optional - if not provided, AWS will use its default region resolution."
            },
            {
              "key": "format",
              "type": "string",
              "title": "R object class",
              "description": "The primary class of the R object that is being loaded from the RDS file."
            }
          ],
          "required": [
            "bucket", "obj", "format"
          ]
        },
        "credentials": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "description": "Credentials are optional. If not provided, it will default to AWS environment variables (if they exist).",
          "items": [
            {
              "key": "awskey",
              "type": "string",
              "title": "AWS key",
              "description": "The AWS Access Key ID. If missing, defaults to value stored in environment variable `AWS_ACCESS_KEY_ID`."
            },
            {
              "key": "awssecret",
              "type": "string",
              "title": "AWS secret",
              "format": "password",
              "description": "The AWS Secret Access Key. If missing, defaults to value stored in environment variable `AWS_SECRET_ACCESS_KEY`."
            }
          ]
        }
      },
      {
        "name": "s3-tidy-file",
        "title": "Tidy data file - AWS S3",
        "description": "File resource in tidy format, having a reader in the [tidyverse](https://www.tidyverse.org) ecosystem. The file will be downloaded from an Amazon Web Services S3 file store.",
        "tags": ["s3", "data-file", "tidy-format"],
        "parameters": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "bucket",
              "type": "string",
              "title": "Bucket",
              "description": "The S3 bucket name."
            },
            {
              "key": "obj",
              "type": "string",
              "title": "Object path",
              "description": "The S3 object path."
            },
            {
              "key": "region",
              "type": "string",
              "title": "AWS Region",
              "description": "The AWS region where the bucket is located (e.g., 'us-east-1', 'eu-west-1'). Optional - if not provided, AWS will use its default region resolution."
            },
            {
              "key": "format",
              "type": "string",
              "title": "Format",
              "description": "Data format that can help when trying to coerce the file content to a data.frame.",
              "enum": [
                {
                  "key": "csv",
                  "title": "CSV (comma delimiter)"
                },
                {
                  "key": "csv2",
                  "title": "CSV2 (semicolon delimiter)"
                },
                {
                  "key": "ssv",
                  "title": "SSV (space delimiter)"
                },
                {
                  "key": "tsv",
                  "title": "TSV (tab delimiter)"
                },
                {
                  "key": "spss",
                  "title": "SPSS"
                },
                {
                  "key": "sav",
                  "title": "SAV"
                },
                {
                  "key": "por",
                  "title": "POR"
                },
                {
                  "key": "stata",
                  "title": "STATA"
                },
                {
                  "key": "dta",
                  "title": "DTA"
                },
                {
                  "key": "sas",
                  "title": "SAS"
                },
                {
                  "key": "xpt",
                  "title": "XPT"
                },
                {
                  "key": "excel",
                  "title": "EXCEL"
                },
                {
                  "key": "xls",
                  "title": "XLS"
                },
                {
                  "key": "xlsx",
                  "title": "XLSX"
                }
              ]
            }
          ],
          "required": [
            "bucket", "obj", "format"
          ]
        },
        "credentials": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "description": "Credentials are optional. If not provided, it will default to AWS environment variables (if they exist).",
          "items": [
            {
              "key": "awskey",
              "type": "string",
              "title": "AWS key",
              "description": "The AWS Access Key ID. If missing, defaults to value stored in environment variable `AWS_ACCESS_KEY_ID`."
            },
            {
              "key": "awssecret",
              "type": "string",
              "title": "AWS secret",
              "format": "password",
              "description": "The AWS Secret Access Key. If missing, defaults to value stored in environment variable `AWS_SECRET_ACCESS_KEY`."
            }
          ]
        }
      },
      {
        "name": "s3-http-rdata-file",
        "title": "R data file - HTTP S3",
        "description": "File resource in R data format. The file will be downloaded from a HTTP S3 (ex: Minio) file store.",
        "tags": ["s3", "data-file", "rdata-format"],
        "parameters": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "url",
              "type": "string",
              "title": "URL",
              "description": "The HTTP S3 server base URL."
            },
            {
              "key": "obj",
              "type": "string",
              "title": "Object path",
              "description": "The S3 object path."
            },
            {
              "key": "format",
              "type": "string",
              "title": "R object class",
              "description": "The primary class of the R object that is being loaded from the R data file. When there are several objects of this class, the one with the symbol with same name as the resource is chosen, otherwise the first one is selected."
            }
          ],
          "required": [
            "url", "obj", "format"
          ]
        },
        "credentials": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "description": "Credentials are optional. If not provided, it will default to AWS environment variables (if they exist).",
          "items": [
            {
              "key": "awskey",
              "type": "string",
              "title": "Access key",
              "description": "The Access Key. If missing, defaults to value stored in environment variable `AWS_ACCESS_KEY_ID`."
            },
            {
              "key": "awssecret",
              "type": "string",
              "title": "Secret key",
              "format": "password",
              "description": "The Secret Key. If missing, defaults to value stored in environment variable `AWS_SECRET_ACCESS_KEY`."
            }
          ]
        }
      },
      {
        "name": "s3-http-rds-file",
        "title": "RDS file - HTTP S3",
        "description": "File resource in RDS format (serialized single R object). The file will be downloaded from a HTTP S3 (ex: Minio) file store.",
        "tags": ["s3", "data-file", "rdata-format"],
        "parameters": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "url",
              "type": "string",
              "title": "URL",
              "description": "The HTTP S3 server base URL."
            },
            {
              "key": "obj",
              "type": "string",
              "title": "Object path",
              "description": "The S3 object path."
            },
            {
              "key": "format",
              "type": "string",
              "title": "R object class",
              "description": "The primary class of the R object that is being loaded from the RDS file."
            }
          ],
          "required": [
            "url", "obj", "format"
          ]
        },
        "credentials": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "description": "Credentials are optional. If not provided, it will default to AWS environment variables (if they exist).",
          "items": [
            {
              "key": "awskey",
              "type": "string",
              "title": "Access key",
              "description": "The Access Key. If missing, defaults to value stored in environment variable `AWS_ACCESS_KEY_ID`."
            },
            {
              "key": "awssecret",
              "type": "string",
              "title": "Secret key",
              "format": "password",
              "description": "The Secret Key. If missing, defaults to value stored in environment variable `AWS_SECRET_ACCESS_KEY`."
            }
          ]
        }
      },
      {
        "name": "s3-http-tidy-file",
        "title": "Tidy data file - HTTP S3",
        "description": "File resource in tidy format, having a reader in the [tidyverse](https://www.tidyverse.org) ecosystem. The file will be downloaded from a HTTP S3 (ex: Minio) file store.",
        "tags": ["s3", "data-file", "tidy-format"],
        "parameters": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "url",
              "type": "string",
              "title": "URL",
              "description": "The HTTP S3 server base URL."
            },
            {
              "key": "obj",
              "type": "string",
              "title": "Object path",
              "description": "The S3 object path."
            },
            {
              "key": "format",
              "type": "string",
              "title": "Format",
              "description": "Data format that can help when trying to coerce the file content to a data.frame.",
              "enum": [
                {
                  "key": "csv",
                  "title": "CSV (comma delimiter)"
                },
                {
                  "key": "csv2",
                  "title": "CSV2 (semicolon delimiter)"
                },
                {
                  "key": "ssv",
                  "title": "SSV (space delimiter)"
                },
                {
                  "key": "tsv",
                  "title": "TSV (tab delimiter)"
                },
                {
                  "key": "spss",
                  "title": "SPSS"
                },
                {
                  "key": "sav",
                  "title": "SAV"
                },
                {
                  "key": "por",
                  "title": "POR"
                },
                {
                  "key": "stata",
                  "title": "STATA"
                },
                {
                  "key": "dta",
                  "title": "DTA"
                },
                {
                  "key": "sas",
                  "title": "SAS"
                },
                {
                  "key": "xpt",
                  "title": "XPT"
                },
                {
                  "key": "excel",
                  "title": "EXCEL"
                },
                {
                  "key": "xls",
                  "title": "XLS"
                },
                {
                  "key": "xlsx",
                  "title": "XLSX"
                }
              ]
            }
          ],
          "required": [
            "url", "obj", "format"
          ]
        },
        "credentials": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "description": "Credentials are optional. If not provided, it will default to AWS environment variables (if they exist).",
          "items": [
            {
              "key": "awskey",
              "type": "string",
              "title": "Access key",
              "description": "The Access Key. If missing, defaults to value stored in environment variable `AWS_ACCESS_KEY_ID`."
            },
            {
              "key": "awssecret",
              "type": "string",
              "title": "Secret key",
              "format": "password",
              "description": "The Secret Key. If missing, defaults to value stored in environment variable `AWS_SECRET_ACCESS_KEY`."
            }
          ]
        }
      },
      {
        "name": "s3-spark",
        "title": "Spark - AWS S3",
        "description": "File resource in Parquet format stored in a AWS S3 file store. The file will be accessed using Spark.",
        "tags": ["s3", "database", "analytics"],
        "parameters": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "bucket",
              "type": "string",
              "title": "Bucket",
              "description": "The S3 bucket name."
            },
            {
              "key": "obj",
              "type": "string",
              "title": "Object path",
              "description": "The S3 object path."
            },
            {
              "key": "region",
              "type": "string",
              "title": "AWS Region",
              "description": "The AWS region where the bucket is located (e.g., 'us-east-1', 'eu-west-1'). Optional - if not provided, AWS will use its default region resolution."
            },
            {
              "key": "master",
              "type": "string",
              "title": "Spark cluster name/url",
              "description": "The Spark cluster url to connect to. Use 'local' (default) to connect to a local instance of Spark."
            },
            {
              "key": "read",
              "type": "string",
              "title": "File read strategy",
              "description": "How Spark should read the file.",
              "enum": [
                {
                  "key": "parquet",
                  "title":"Parquet read"
                },
                {
                  "key": "delta",
                  "title":"Delta Lake read"
                }
              ]
            },
            {
              "key": "config",
              "type": "string",
              "format": "textarea",
              "title": "Spark configuration",
              "description": "Spark configuration options. One option per line, in the format 'key=value'."
            }
          ],
          "required": [
            "bucket", "obj", "read"
          ]
        },
        "credentials": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "description": "Credentials are optional. If not provided, it will default to AWS environment variables (if they exist).",
          "items": [
            {
              "key": "awskey",
              "type": "string",
              "title": "Access key",
              "description": "The Access Key. If missing, defaults to value stored in environment variable `AWS_ACCESS_KEY_ID`."
            },
            {
              "key": "awssecret",
              "type": "string",
              "title": "Secret key",
              "format": "password",
              "description": "The Secret Key. If missing, defaults to value stored in environment variable `AWS_SECRET_ACCESS_KEY`."
            }
          ]
        }
      },
      {
        "name": "s3-http-spark",
        "title": "Spark - HTTP S3",
        "description": "File resource in Parquet format stored in a HTTP S3 (ex: Minio) file store. The file will be accessed using Spark.",
        "tags": ["s3", "database", "analytics"],
        "parameters": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "url",
              "type": "string",
              "title": "URL",
              "description": "The HTTP S3 server base URL."
            },
            {
              "key": "obj",
              "type": "string",
              "title": "Object path",
              "description": "The S3 object path."
            },
            {
              "key": "master",
              "type": "string",
              "title": "Spark cluster name/url",
              "description": "The Spark cluster url to connect to. Use 'local' (default) to connect to a local instance of Spark."
            },
            {
              "key": "read",
              "type": "string",
              "title": "File read strategy",
              "description": "How Spark should read the file.",
              "enum": [
                {
                  "key": "parquet",
                  "title":"Parquet read"
                },
                {
                  "key": "delta",
                  "title":"Delta Lake read"
                }
              ]
            },
            {
              "key": "config",
              "type": "string",
              "format": "textarea",
              "title": "Spark configuration",
              "description": "Spark configuration options. One option per line, in the format 'key=value'."
            }
          ],
          "required": [
            "url", "obj", "read"
          ]
        },
        "credentials": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "description": "Credentials are optional. If not provided, it will default to AWS environment variables (if they exist).",
          "items": [
            {
              "key": "awskey",
              "type": "string",
              "title": "Access key",
              "description": "The Access Key. If missing, defaults to value stored in environment variable `AWS_ACCESS_KEY_ID`."
            },
            {
              "key": "awssecret",
              "type": "string",
              "title": "Secret key",
              "format": "password",
              "description": "The Secret Key. If missing, defaults to value stored in environment variable `AWS_SECRET_ACCESS_KEY`."
            }
          ]
        }
      }
    ]
  },
  asResource: function(type, name, params, credentials) {

    //
    // Resource factory functions to be reused
    //
    var toS3Resource = function(name, params, credentials) {
        var url = "s3://" + params.bucket + "/" + params.obj;
        
        // Encode region into URL structure if provided
        if (params.region) {
            url += "//s3config::/region:" + params.region;
        }
        
        return {
            name: name,
            url: url,
            format: params.format,
            identity: credentials.awskey,
            secret: credentials.awssecret
        };
    };

    var toHttpS3Resource = function(name, params, credentials) {
        return {
            name: name,
            url: "s3+" + params.url + "/" + params.obj,
            format: params.format,
            identity: credentials.awskey,
            secret: credentials.awssecret
        };
    };
    
    var toRdataFormat = function(resource) {
      if (resource.format && !resource.url.toLowerCase().endsWith(".rda") && !resource.url.toLowerCase().endsWith(".rdata")) {
        resource.format = "R:" + resource.format;
      }
      return resource;
    };
    
    var toRDSFormat = function(resource) {
      if (resource.format && !resource.url.toLowerCase().endsWith(".rds")) {
        resource.format = "RDS:" + resource.format;
      }
      return resource;
    };
    
    var makeSparkQuery = function(params) {
      var query = [];
      if (params.master) {
        query.push("master=" + params.master);
      }
      if (params.read) {
        query.push("read=" + params.read);
      }
      if (params.config) {
        var configs = params.config.split("\n");
        for (var i = 0; i < configs.length; i++) {
        	query.push(configs[i]);
        }
      }
      return query;
    }
    
    var toSparkResource = function(name, params, credentials) {
      var query = makeSparkQuery(params);
      var url = "s3+spark://" + params.bucket + "/" + params.obj + (query.length > 0 ? "?" + query.join("&") : "");
      
      // Encode region into URL structure if provided
      if (params.region) {
          url += "//s3config::/region:" + params.region;
      }
      
      return {
          name: name,
          url: url,
          identity: credentials.awskey,
          secret: credentials.awssecret
      };
    };
    
    var toHttpS3SparkResource = function(name, params, credentials) {
      var query = makeSparkQuery(params);
      return {
          name: name,
          url: "s3+spark+" + params.url + "/" + params.obj + (query.length > 0 ? "?" + query.join("&") : ""),
          identity: credentials.awskey,
          secret: credentials.awssecret
      };
    };

    //
    // Resource factory functions by resource form type
    //
    var toResourceFactories = {
      "s3-rdata-file": function(name, params, credentials) {
          return toRdataFormat(toS3Resource(name, params, credentials));
      },
      "s3-rds-file": function(name, params, credentials) {
          return toRDSFormat(toS3Resource(name, params, credentials));
      },
      "s3-tidy-file": toS3Resource,
      "s3-http-rdata-file": function(name, params, credentials) {
          return toRdataFormat(toHttpS3Resource(name, params, credentials));
      },
      "s3-http-rds-file": function(name, params, credentials) {
          return toRDSFormat(toHttpS3Resource(name, params, credentials));
      },
      "s3-http-tidy-file": toHttpS3Resource,
      "s3-spark": toSparkResource,
      "s3-http-spark": toHttpS3SparkResource
    };

    // Check if there is a resource factory function for the requested resource form type
    if (toResourceFactories[type]) {
        return toResourceFactories[type](name, params, credentials);
    }
    return undefined;
  }
}
