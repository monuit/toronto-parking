#!/usr/bin/env node
import { spawn } from 'node:child_process';
import path from 'node:path';
import process from 'node:process';

// Memory: Use 0 to let Node.js use all available container memory
// This prevents artificial OOM crashes from heap limits
const DEFAULT_HEAP_MB = 0;

function resolveHeapLimit() {
  const raw = process.env.MAP_APP_MAX_HEAP_MB;
  if (!raw) {
    return DEFAULT_HEAP_MB;
  }
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : DEFAULT_HEAP_MB;
}

function resolveTarget([entry, ...rest]) {
  if (!entry) {
    console.error('Usage: node scripts/run-with-heap.mjs <script> [args...]');
    process.exitCode = 1;
    return null;
  }

  const resolvedEntry = path.isAbsolute(entry)
    ? entry
    : path.resolve(process.cwd(), entry);

  return { entry: resolvedEntry, args: rest };
}

async function main() {
  const target = resolveTarget(process.argv.slice(2));
  if (!target) {
    return;
  }

  const heapLimit = resolveHeapLimit();
  // Memory optimization flags:
  // --gc-interval=100: More frequent garbage collection
  // When heapLimit is 0, skip --max-old-space-size to use all available memory
  const nodeArgs = [
    ...(heapLimit > 0 ? [`--max-old-space-size=${heapLimit}`] : []),
    '--gc-interval=100',
    target.entry,
    ...target.args,
  ];

  const child = spawn(process.execPath, nodeArgs, {
    stdio: 'inherit',
    env: process.env,
  });

  child.on('exit', (code, signal) => {
    if (signal) {
      process.kill(process.pid, signal);
    } else {
      process.exit(code ?? 0);
    }
  });
}

main().catch((error) => {
  console.error('run-with-heap failed:', error);
  process.exit(1);
});
