#!/usr/bin/env node
import { spawn } from 'node:child_process';
import path from 'node:path';
import process from 'node:process';

// Memory optimization: Reduced from 12GB to 3GB to lower Railway costs
// Railway was charging $55+/month for 241GB RAM due to unbounded heap
const DEFAULT_HEAP_MB = 3072;

function resolveHeapLimit() {
  const raw = process.env.MAP_APP_MAX_HEAP_MB;
  if (!raw) {
    return DEFAULT_HEAP_MB;
  }
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_HEAP_MB;
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
  // --optimize-for-size: Prioritize memory over speed
  // --gc-interval=100: More frequent garbage collection
  // --expose-gc: Allow manual GC calls if needed
  const nodeArgs = [
    `--max-old-space-size=${heapLimit}`,
    '--optimize-for-size',
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
