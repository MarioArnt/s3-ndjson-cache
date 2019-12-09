# s3-ndjson-cache

This package allows you to:

* Serialize large javascript object to ndjson (newline-delimited JSON) and upload to S3.

* Download large ndjson file from S3 and deserialize it in a javascript object.

Using only streams, it is optimized for huge object/ndjson file.

This is very useful when need to share large object between multiple AWS Lambda functions or step functions.

Indeed, when invoking a function from another, Payload is limited to 6MB.

This dead-simple abstraction layer allows you to cache data in the first lambda and read this cached-data
in the second. 

You can share any amount of data between lambdas (as long as it fit in its memory)

## Usage

```javascript
const cache = new CacheS3('my-cache-bucket' /* target bucket name */, {
    region: 'eu-west-1', // Any partial object of config supported by S3 constructor
});

// The following object/array could be typically the result of a heavy computation and larger than 6MB.
const data = [/*...some huge object/array */];

// Write a large object to S3
await cache.write('cached_for_later.ndjson', data);

// in another place with the same setup for CacheS3 instance
const data = await cache.read('cached_for_later.ndjson');
```

## Log levels

You can increase verbosity of S3 caching logs by exporting `NDJSON_CACHE_VERBOSITY`.
By default only errors are displayed.

Following levels can be set: ` 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR' | 'SILENT'`

By default, all logs are prefixed by `[S3 cache]` but you can you change this by exporting another `NDJSON_CACHE_LOG_PREFIX`.

