import { S3 } from 'aws-sdk';
import { Readable } from 'stream';
// tslint:disable-next-line:no-var-requires
const ndjson: any = require('ndjson');

type Verbosity = 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR' | 'SILENT';
interface ILogger {
  debug: (...args: any) => void,
  info: (...args: any) => void,
  warn: (...args: any) => void,
  error: (...args: any) => void,
}

export class CacheS3 {

  private readonly s3: S3;
  private readonly config: S3.ClientConfiguration;
  private readonly bucket: string;
  private readonly log: ILogger;
  private readonly verbosity: Verbosity;

  constructor(bucket?: string, config?: Partial<S3.ClientConfiguration>) {
    this.verbosity = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'SILENT'].includes(process.env.NDJSON_CACHE_VERBOSITY)
      ? process.env.NDJSON_CACHE_VERBOSITY as Verbosity
      : 'ERROR';
    const logPrefix = process.env.NDJSON_CACHE_LOG_PREFIX || '[S3 cache]';
    this.log = {
      debug: (...args) => {
        if (['DEBUG'].includes(this.verbosity)) {
          console.debug(logPrefix, ...args);
        }
      },
      info: (...args) => {
        if (['DEBUG', 'INFO'].includes(this.verbosity)) {
          console.info(logPrefix, ...args);
        }
      },
      warn: (...args) => {
        if (['DEBUG', 'INFO', 'WARNING'].includes(this.verbosity)) {
          console.warn(logPrefix, ...args);
        }
      },
      error: (...args) => {
        if (['DEBUG', 'INFO', 'WARNING', 'ERROR'].includes(this.verbosity)) {
          console.error(logPrefix, ...args);
        }
      },
    };
    this.log.debug('Instantiating cache');
    this.config = {
      region: process.env.AWS_REGION || 'eu-west-1',
      ...config,
    };
    this.log.debug('Using config', this.config)
    this.s3 = new S3(this.config);
    this.bucket = bucket || process.env.NDJSON_CACHE_BUCKET;
    if (!this.bucket) {
      this.log.error('No bucket specified');
      throw Error('No bucket specified');
    }
    this.log.debug('Using bucket', this.bucket);
  }

  public getOptions() {
    return {
      bucket: this.bucket,
      config: this.config,
      verbosity: this.verbosity,
    }
  }

  public async toCache(key: string, data: any): Promise<void> {
    this.log.info('Caching data in S3 bucket', {
      bucket: this.bucket,
      key,
    });
    const stream = new Readable();
    // tslint:disable-next-line:no-empty
    stream._read = () => {};
    for (const item of data.items) {
      await stream.push(JSON.stringify(item) + '\n', 'utf-8');
    }
    const promise = this.upload(key, stream);
    stream.push(null);
    await promise;
  }

  public async fromCache(key: string): Promise<any> {
    this.log.debug('Read cached data in S3 bucket', {
      bucket: this.bucket,
      key,
    });
    const items: any[] = [];
    const downloadStream = this.download(key);
    const ndjsonStream = ndjson.parse();
    downloadStream.pipe(ndjsonStream);
    ndjsonStream.on('data', (item: any) => {
      items.push(item);
    });
    return new Promise<any>((resolve, reject) => {
      ndjsonStream.on('error', (err: Error) => {
        this.log.error('Error happened when deserializing ndjson');
        reject(err);
      });
      ndjsonStream.on('finish', () => {
        this.log.debug('Every item successfully deserialized');
        resolve(items);
      });
    });
  }

  public async flush(key: string): Promise<void> {
    this.log.info('Removing object cached at', {
      bucket: this.bucket,
      key,
    });
    await this.s3.deleteObject({
      Bucket: this.bucket,
      Key: key,
    });
  }

  private async upload(key: string, stream: Readable): Promise<void> {
    const params = { Bucket: this.bucket, Key: key, Body: stream };
    const manager = this.s3.upload(params);
    manager.on('httpUploadProgress', (progress) => {
      this.log.info(JSON.stringify(progress));
    });
    await manager.promise();
  }

  private download(key: string): Readable {
    return this.s3
      .getObject({
        Bucket: this.bucket,
        Key: key,
      })
      .createReadStream();
  }
}
