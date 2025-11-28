#!/usr/bin/env node
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ROOT = path.resolve(__dirname, '..');
const MAP_APP_DIR = path.join(ROOT, 'map-app');
const WORKER_DIR = path.join(ROOT, 'tools', 'pmtiles');

// Debug: Log paths and environment
console.log('[start] ROOT:', ROOT);
console.log('[start] MAP_APP_DIR:', MAP_APP_DIR);
console.log('[start] WORKER_DIR:', WORKER_DIR);
console.log('[start] PATH:', process.env.PATH);
console.log('[start] NODE_ENV:', process.env.NODE_ENV);

let mainAppChild = null;
let workerChild = null;
let shuttingDown = false;

function spawnMainApp() {
  console.log(`[start] Spawning map-app: npm start in ${MAP_APP_DIR}`);
  const child = spawn('npm', ['start'], {
    cwd: MAP_APP_DIR,
    stdio: 'inherit',
    env: process.env,
    shell: true,
  });

  child.on('error', (err) => {
    console.error('[start] map-app spawn error:', err.message);
    process.exit(1);
  });

  child.on('exit', (code, signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    const reason = signal ? `signal ${signal}` : `exit code ${code}`;
    console.error(`[start] map-app exited (${reason}). Shutting down.`);
    if (workerChild && !workerChild.killed) {
      workerChild.kill('SIGTERM');
    }
    process.exitCode = code ?? 1;
  });

  return child;
}

function spawnWorker() {
  console.log(`[start] Spawning pmtiles-worker: npm run start:worker in ${WORKER_DIR}`);
  const child = spawn('npm', ['run', 'start:worker'], {
    cwd: WORKER_DIR,
    stdio: 'inherit',
    env: process.env,
    shell: true,
  });

  child.on('error', (err) => {
    console.warn('[start] pmtiles-worker spawn error (non-fatal):', err.message);
  });

  child.on('exit', (code, signal) => {
    if (shuttingDown) return;
    const reason = signal ? `signal ${signal}` : `exit code ${code}`;
    // Worker exit is non-fatal - main app continues
    console.warn(`[start] pmtiles-worker exited (${reason}). Main app continues.`);
  });

  return child;
}

function terminateAll() {
  shuttingDown = true;
  if (mainAppChild && !mainAppChild.killed) {
    console.log('[start] Sending SIGTERM to map-app');
    mainAppChild.kill('SIGTERM');
  }
  if (workerChild && !workerChild.killed) {
    console.log('[start] Sending SIGTERM to pmtiles-worker');
    workerChild.kill('SIGTERM');
  }
}

process.on('SIGINT', () => {
  console.log('[start] Caught SIGINT – terminating processes...');
  terminateAll();
});

process.on('SIGTERM', () => {
  console.log('[start] Caught SIGTERM – terminating processes...');
  terminateAll();
});

// Start main app first (critical), then worker (optional)
mainAppChild = spawnMainApp();
workerChild = spawnWorker();

console.log('[start] SSR and PMTiles worker started. Logs streaming above.');
