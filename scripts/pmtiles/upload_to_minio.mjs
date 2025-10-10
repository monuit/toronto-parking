#!/usr/bin/env node

import { createReadStream, promises as fs } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import process from 'node:process';

import { S3Client, HeadObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(__dirname, '../..');

function usage() {
  console.log('Usage: node scripts/pmtiles/upload_to_minio.mjs [--dir <path>] [--bucket <name>] [--prefix <key-prefix>]');
  process.exit(1);
}

function parseArgs(argv) {
  const args = { dir: path.join(PROJECT_ROOT, 'pmtiles'), bucket: process.env.PMTILES_BUCKET || 'pmtiles', prefix: process.env.PMTILES_PREFIX || 'pmtiles/' };
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === '--dir' && argv[i + 1]) {
      args.dir = path.resolve(argv[i + 1]);
      i += 1;
    } else if (token === '--bucket' && argv[i + 1]) {
      args.bucket = argv[i + 1];
      i += 1;
    } else if (token === '--prefix' && argv[i + 1]) {
      args.prefix = argv[i + 1];
      i += 1;
    } else if (token === '--help' || token === '-h') {
      usage();
    }
  }
  if (!args.prefix.endsWith('/')) {
    args.prefix = `${args.prefix}/`;
  }
  return args;
}

function resolveEndpoint() {
  const privateEndpoint = process.env.MINIO_PRIVATE_ENDPOINT || process.env.MINIO_ENDPOINT;
  const publicEndpoint = process.env.MINIO_PUBLIC_ENDPOINT;
  return privateEndpoint || publicEndpoint || null;
}

async function collectPmtilesFiles(directory) {
  const results = [];
  const entries = await fs.readdir(directory, { withFileTypes: true });
  for (const entry of entries) {
    if (entry.isDirectory()) {
      continue;
    }
    if (entry.name.endsWith('.pmtiles')) {
      results.push(path.join(directory, entry.name));
    }
  }
  return results;
}

async function fileSize(filePath) {
  const stats = await fs.stat(filePath);
  return stats.size;
}

async function uploadFile(client, bucket, key, filePath) {
  const stream = createReadStream(filePath);
  const size = await fileSize(filePath);
  await client.send(new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    Body: stream,
    ContentType: 'application/octet-stream',
    ContentLength: size,
    ACL: 'public-read',
  }));
}

async function ensureBucketAccess(client, bucket) {
  try {
    await client.send(new HeadObjectCommand({ Bucket: bucket, Key: '__pmtiles_probe__' }));
  } catch (error) {
    if (error?.$metadata?.httpStatusCode === 404) {
      return;
    }
    if (error?.$metadata?.httpStatusCode === 403) {
      throw new Error(`Access denied to bucket "${bucket}" – check credentials`);
    }
  }
}

async function main() {
  const { dir, bucket, prefix } = parseArgs(process.argv.slice(2));

  const resolvedDir = path.resolve(dir);
  try {
    const stats = await fs.stat(resolvedDir);
    if (!stats.isDirectory()) {
      throw new Error(`${resolvedDir} is not a directory`);
    }
  } catch (error) {
    throw new Error(`PMTiles directory not accessible: ${error.message}`);
  }

  const accessKeyId = process.env.MINIO_ROOT_USER;
  const secretAccessKey = process.env.MINIO_ROOT_PASSWORD;
  if (!accessKeyId || !secretAccessKey) {
    throw new Error('MINIO_ROOT_USER and MINIO_ROOT_PASSWORD must be set');
  }

  const endpoint = resolveEndpoint();
  if (!endpoint) {
    throw new Error('MINIO endpoint not configured (set MINIO_PRIVATE_ENDPOINT or MINIO_PUBLIC_ENDPOINT)');
  }

  const s3 = new S3Client({
    endpoint,
    region: process.env.MINIO_REGION || 'us-east-1',
    forcePathStyle: true,
    credentials: { accessKeyId, secretAccessKey },
  });

  await ensureBucketAccess(s3, bucket);

  const files = await collectPmtilesFiles(resolvedDir);
  if (files.length === 0) {
    console.log(`No .pmtiles files found in ${resolvedDir}`);
    return;
  }

  for (const file of files) {
    const key = `${prefix}${path.basename(file)}`;
    console.log(`Uploading ${file} → s3://${bucket}/${key}`);
    await uploadFile(s3, bucket, key, file);
  }

  console.log('Upload complete.');
}

main().catch((error) => {
  console.error('PMTiles upload failed:', error.message || error);
  process.exitCode = 1;
});
