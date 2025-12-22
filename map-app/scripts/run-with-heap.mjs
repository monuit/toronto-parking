#!/usr/bin/env node
import { spawn } from 'node:child_process';
import path from 'node:path';
import process from 'node:process';

// Memory: Default to 1536MB to leave headroom for Node.js native allocations
// Container typically needs 20-30% overhead beyond heap for buffers, GC, etc.
const DEFAULT_HEAP_MB = 1536;

// Memory watchdog interval (ms) - log memory usage periodically
const MEMORY_LOG_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
const MEMORY_THRESHOLD_PERCENT = 85; // Trigger warning at 85% of heap limit

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

function startMemoryWatchdog(heapLimitMB) {
  if (heapLimitMB <= 0) return null;
  
  const thresholdBytes = heapLimitMB * 1024 * 1024 * (MEMORY_THRESHOLD_PERCENT / 100);
  
  const timer = setInterval(() => {
    const usage = process.memoryUsage();
    const heapUsedMB = Math.round(usage.heapUsed / 1024 / 1024);
    const rssMB = Math.round(usage.rss / 1024 / 1024);
    
    if (usage.heapUsed > thresholdBytes) {
      console.warn(
        `[memory-watchdog] ⚠️ High memory: heap ${heapUsedMB}MB/${heapLimitMB}MB ` +
        `(${Math.round(usage.heapUsed / (heapLimitMB * 1024 * 1024) * 100)}%), rss ${rssMB}MB`
      );
    }
  }, MEMORY_LOG_INTERVAL_MS);
  
  timer.unref();
  return timer;
}

async function main() {
  const target = resolveTarget(process.argv.slice(2));
  if (!target) {
    return;
  }

  const heapLimit = resolveHeapLimit();
  
  // Log memory configuration at startup
  console.log(`[run-with-heap] Heap limit: ${heapLimit > 0 ? `${heapLimit}MB` : 'unlimited'}`);
  
  // Memory optimization flags:
  // --gc-interval=100: More frequent garbage collection
  // --expose-gc: Allow manual GC triggering (combined with NODE_OPTIONS)
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
  
  // Start memory watchdog in parent process
  startMemoryWatchdog(heapLimit);

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
