import {CacheS3} from "../src";
import { expect } from "chai";
import {fail} from "assert";
import {S3} from "aws-sdk";

const testConfig = {
  region: process.env.TEST_AWS_REGION,
  accessKeyId: process.env.TEST_AWS_ACCESS_KEY,
  secretAccessKey: process.env.TEST_AWS_SECRET_KEY,
};

const testBucket = process.env.TEST_BUCKET || 'S3-NDJSON-CACHE-TEST';
const testFileSize = 100000;
const testArrayKey = 'test-array.ndjson';
const testObjectKey = 'test-object.ndjson';

describe('[Class CacheS3]', () => {
  before(() => {
    if (testConfig.region) {
      console.error('Missing test AWS region');
      process.exit(1);
    }
    if (testConfig.accessKeyId) {
      console.error('Missing test AWS Access Key ID');
      process.exit(1);
    }
    if (testConfig.secretAccessKey) {
      console.error('Missing test AWS Secret Key');
      process.exit(1);
    }
  });
  describe('[Constructor]', () => {
    it('should be instantiated with default options', () => {
      const bucket = 'test_bucket';
      process.env.NDJSON_CACHE_BUCKET = bucket;
      process.env.AWS_REGION = 'us-east-2';
      const cache = new CacheS3();
      const options = cache.getOptions();
      expect(options.bucket).to.eq(bucket);
      expect(options.config.region).to.eq(process.env.AWS_REGION);
      expect(options.verbosity).to.eq('ERROR');
      delete process.env.NDJSON_CACHE_BUCKET;
      delete process.env.AWS_REGION;
    });
    it('should be instantiated with custom options', () => {
      process.env.NDJSON_CACHE_BUCKET = 'some-bucket';
      process.env.AWS_REGION = 'eu-west-3';
      const cache = new CacheS3('other-bucket', {
        region: 'eu-west-1',
        convertResponseTypes: false,
      });
      const options = cache.getOptions();
      expect(options.bucket).to.eq('other-bucket');
      expect(options.config.region).to.eq('eu-west-1');
      expect(options.verbosity).to.eq('ERROR');
      delete process.env.NDJSON_CACHE_BUCKET;
      delete process.env.AWS_REGION;
    });
    it('should throw if no bucket is specified', () => {
      delete process.env.NDJSON_CACHE_BUCKET;
      try {
        const cache = new CacheS3();
        fail('should throw')
      } catch (e) {
        expect(e.message).to.eq('No bucket specified');
      }
    })
  });
  describe('[Method toCache]', () => {
    it('should serialize and upload large array to cache', async () => {
      const cache = new CacheS3(testBucket, testConfig);
      const data = [];
      for (let i = 0; i < testFileSize; ++i) {
        data.push({foo: 'bar'});
      }
      await cache.toCache(testArrayKey, data);
      const s3 = new S3(testConfig);
      const metadata = await s3.headObject({
        Bucket: testBucket,
        Key: testArrayKey,
      });
      expect(!!metadata).to.be.true;
    });
    it('should serialize and upload large object to cache', async () => {
      const cache = new CacheS3(testBucket, testConfig);
      const data = {};
      for (let i = 0; i < testFileSize; ++i) {
        data['foo-' + i] ='bar-' + i;
      }
      await cache.toCache(testObjectKey, data);
      const s3 = new S3(testConfig);
      const metadata = await s3.headObject({
        Bucket: testBucket,
        Key: testObjectKey,
      });
      expect(!!metadata).to.be.true;
    });
  });
  describe('[Method fromCache]', () => {
    it('should download and deserialize large array from cache', async () => {
      const cache = new CacheS3(testBucket, testConfig);
      const data = await cache.fromCache(testArrayKey);
      expect(data.length).to.eq(testFileSize);
      expect(data.every(item => item.foo === 'bar')).to.be.true;
    });
    it('should download and deserialize large object from cache', async () => {
      const cache = new CacheS3(testBucket, testConfig);
      const data = await cache.fromCache(testArrayKey);
      expect(Object.keys(data).length).to.eq(testFileSize);
    });
  });
  describe('[Method flush]', () => {
    it('should flush cache', async () => {
      const cache = new CacheS3(testBucket, testConfig);
      await cache.flush(testObjectKey);
      await cache.flush(testArrayKey);
      try {
        const s3 = new S3(testConfig);
        await s3.headObject({
          Bucket: testBucket,
          Key: testObjectKey,
        });
        fail('should throw ');
      } catch (e) {
        expect(e.code).to.eq('NoSuchKey');
      }
      try {
        const s3 = new S3(testConfig);
        await s3.headObject({
          Bucket: testBucket,
          Key: testArrayKey,
        });
        fail('should throw ');
      } catch (e) {
        expect(e.code).to.eq('NoSuchKey');
      }
    });
  });
});
