/**
 * Memory Guard Module
 * Central memory pressure monitoring and GC management for Railway deployment.
 *
 * This module provides utilities to detect memory pressure and trigger garbage
 * collection to prevent OOM crashes on memory-constrained containers.
 */
import process from 'node:process';

// Memory thresholds (as percentage of heap limit)
const MEMORY_WARNING_THRESHOLD = Number.parseFloat(
  process.env.MEMORY_WARNING_THRESHOLD || '0.70',
);
const MEMORY_CRITICAL_THRESHOLD = Number.parseFloat(
  process.env.MEMORY_CRITICAL_THRESHOLD || '0.80',
);
const MEMORY_EMERGENCY_THRESHOLD = Number.parseFloat(
  process.env.MEMORY_EMERGENCY_THRESHOLD || '0.90',
);

// How often to check memory (ms)
const MEMORY_CHECK_INTERVAL_MS = Number.parseInt(
  process.env.MEMORY_CHECK_INTERVAL_MS || '',
  10,
) || 30_000; // 30 seconds

// Heap limit from environment or default
const HEAP_LIMIT_MB = Number.parseInt(
  process.env.MAP_APP_MAX_HEAP_MB || '',
  10,
) || 1200;

const HEAP_LIMIT_BYTES = HEAP_LIMIT_MB * 1024 * 1024;

// Track state
let lastWarningTime = 0;
let consecutivePressureCount = 0;
let monitorTimer = null;

/**
 * Get current memory usage statistics
 */
export function getMemoryStats() {
  const usage = process.memoryUsage();
  const heapUsedMB = Math.round(usage.heapUsed / 1024 / 1024);
  const heapTotalMB = Math.round(usage.heapTotal / 1024 / 1024);
  const rssMB = Math.round(usage.rss / 1024 / 1024);
  const externalMB = Math.round(usage.external / 1024 / 1024);
  const heapUsedPercent = usage.heapUsed / HEAP_LIMIT_BYTES;

  return {
    heapUsedMB,
    heapTotalMB,
    rssMB,
    externalMB,
    heapUsedBytes: usage.heapUsed,
    heapLimitMB: HEAP_LIMIT_MB,
    heapUsedPercent,
    isWarning: heapUsedPercent >= MEMORY_WARNING_THRESHOLD,
    isCritical: heapUsedPercent >= MEMORY_CRITICAL_THRESHOLD,
    isEmergency: heapUsedPercent >= MEMORY_EMERGENCY_THRESHOLD,
  };
}

/**
 * Check if the process is under memory pressure.
 * Returns true if heap usage exceeds the warning threshold.
 */
export function isUnderMemoryPressure() {
  const stats = getMemoryStats();
  return stats.isWarning;
}

/**
 * Check if memory is in critical state (should shed load)
 */
export function isMemoryCritical() {
  const stats = getMemoryStats();
  return stats.isCritical;
}

/**
 * Check if memory is in emergency state (imminent OOM)
 */
export function isMemoryEmergency() {
  const stats = getMemoryStats();
  return stats.isEmergency;
}

/**
 * Force garbage collection if available.
 * Requires --expose-gc flag.
 */
export function forceGC() {
  if (typeof globalThis.gc === 'function') {
    try {
      globalThis.gc();
      return true;
    } catch (error) {
      console.warn('[memory-guard] GC trigger failed:', error?.message || error);
      return false;
    }
  }
  return false;
}

/**
 * Force garbage collection and return memory freed.
 */
export function forceGCWithStats() {
  const before = getMemoryStats();
  const gcRan = forceGC();
  if (!gcRan) {
    return { gcRan: false, freedMB: 0, before, after: before };
  }
  const after = getMemoryStats();
  const freedMB = before.heapUsedMB - after.heapUsedMB;
  return { gcRan: true, freedMB, before, after };
}

/**
 * Emergency memory cleanup - try to free memory aggressively.
 * Call this when nearing OOM.
 */
export function emergencyCleanup() {
  console.warn('[memory-guard] 🚨 Emergency memory cleanup triggered');

  // Force GC multiple times
  for (let i = 0; i < 3; i++) {
    forceGC();
  }

  const stats = getMemoryStats();
  console.log(`[memory-guard] Post-emergency: heap ${stats.heapUsedMB}MB / ${stats.heapLimitMB}MB (${Math.round(stats.heapUsedPercent * 100)}%)`);

  return stats;
}

/**
 * Check memory and take action if needed.
 * This is the main monitoring function called periodically.
 */
function checkMemoryAndAct() {
  const stats = getMemoryStats();
  const now = Date.now();

  if (stats.isEmergency) {
    consecutivePressureCount++;
    console.error(
      `[memory-guard] 🚨 EMERGENCY: heap ${stats.heapUsedMB}MB / ${stats.heapLimitMB}MB ` +
      `(${Math.round(stats.heapUsedPercent * 100)}%) - consecutive pressure: ${consecutivePressureCount}`,
    );
    emergencyCleanup();
  } else if (stats.isCritical) {
    consecutivePressureCount++;
    // Rate limit warnings to once per minute
    if (now - lastWarningTime > 60_000) {
      console.warn(
        `[memory-guard] ⚠️ CRITICAL: heap ${stats.heapUsedMB}MB / ${stats.heapLimitMB}MB ` +
        `(${Math.round(stats.heapUsedPercent * 100)}%) - triggering GC`,
      );
      lastWarningTime = now;
    }
    forceGC();
  } else if (stats.isWarning) {
    consecutivePressureCount++;
    // Rate limit warnings to once per 2 minutes
    if (now - lastWarningTime > 120_000) {
      console.log(
        `[memory-guard] ⚡ Warning: heap ${stats.heapUsedMB}MB / ${stats.heapLimitMB}MB ` +
        `(${Math.round(stats.heapUsedPercent * 100)}%)`,
      );
      lastWarningTime = now;
    }
  } else {
    // Reset consecutive counter when memory is healthy
    if (consecutivePressureCount > 0) {
      console.log(
        `[memory-guard] ✓ Memory recovered: heap ${stats.heapUsedMB}MB / ${stats.heapLimitMB}MB ` +
        `(${Math.round(stats.heapUsedPercent * 100)}%)`,
      );
    }
    consecutivePressureCount = 0;
  }

  return stats;
}

/**
 * Start periodic memory monitoring.
 */
export function startMemoryMonitor() {
  if (monitorTimer) {
    return; // Already running
  }

  console.log(
    `[memory-guard] Starting memory monitor (limit: ${HEAP_LIMIT_MB}MB, ` +
    `warning: ${Math.round(MEMORY_WARNING_THRESHOLD * 100)}%, ` +
    `critical: ${Math.round(MEMORY_CRITICAL_THRESHOLD * 100)}%)`,
  );

  // Initial check
  checkMemoryAndAct();

  // Periodic checks
  monitorTimer = setInterval(checkMemoryAndAct, MEMORY_CHECK_INTERVAL_MS);

  // Don't prevent process exit
  if (typeof monitorTimer.unref === 'function') {
    monitorTimer.unref();
  }
}

/**
 * Stop the memory monitor.
 */
export function stopMemoryMonitor() {
  if (monitorTimer) {
    clearInterval(monitorTimer);
    monitorTimer = null;
  }
}

/**
 * Decorator/wrapper for functions that should be skipped under memory pressure.
 * Returns a wrapped function that checks memory before executing.
 */
export function withMemoryCheck(fn, options = {}) {
  const {
    skipOnWarning = false,
    skipOnCritical = true,
    fallbackValue = null,
    onSkip = null,
  } = options;

  return async function memoryCheckedFn(...args) {
    const stats = getMemoryStats();

    if (skipOnCritical && stats.isCritical) {
      if (typeof onSkip === 'function') {
        onSkip('critical', stats);
      }
      return fallbackValue;
    }

    if (skipOnWarning && stats.isWarning) {
      if (typeof onSkip === 'function') {
        onSkip('warning', stats);
      }
      return fallbackValue;
    }

    return fn.apply(this, args);
  };
}

// Auto-start monitor in production
if (process.env.NODE_ENV === 'production') {
  // Delay start slightly to let app initialize
  setTimeout(startMemoryMonitor, 5000);
}

export default {
  getMemoryStats,
  isUnderMemoryPressure,
  isMemoryCritical,
  isMemoryEmergency,
  forceGC,
  forceGCWithStats,
  emergencyCleanup,
  startMemoryMonitor,
  stopMemoryMonitor,
  withMemoryCheck,
};
