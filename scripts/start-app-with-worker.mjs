#!/usr/bin/env node
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ROOT = path.resolve(__dirname, '..');
const MAP_APP_DIR = path.join(ROOT, 'map-app');
const WORKER_DIR = path.join(ROOT, 'tools', 'pmtiles');

const children = [];
let shuttingDown = false;

function spawnProcess(command, args, cwd, name) {
  const child = spawn(command, args, {
    cwd,
    stdio: 'inherit',
    env: process.env,
    shell: false,
  });

  child.on('exit', (code, signal) => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    const reason = signal ? `signal ${signal}` : `exit code ${code}`;
    console.error(`[start] ${name} exited (${reason}). Shutting down remaining processes.`);
    terminateAll();
    process.exitCode = code ?? 1;
  });

  children.push({ child, name });
  return child;
}

function terminateAll() {
  shuttingDown = true;
  for (const { child, name } of children) {
    if (!child.killed) {
      console.log(`[start] Sending SIGTERM to ${name}`);
      child.kill('SIGTERM');
    }
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

spawnProcess('npm', ['start'], MAP_APP_DIR, 'map-app');
spawnProcess('npm', ['run', 'start:worker'], WORKER_DIR, 'pmtiles-worker');

console.log('[start] SSR and PMTiles worker started. Logs streaming above.');
