import process from 'node:process';
import {
  getMemoryStats,
  forceGC,
} from '../server/memoryGuard.js';

const DEFAULT_INTERVAL_SECONDS = Number.parseInt(process.env.APP_DATA_REFRESH_SECONDS || '900', 10);

// Skip refresh if memory usage is above this threshold (percentage)
const SKIP_REFRESH_MEMORY_THRESHOLD = Number.parseFloat(
  process.env.SKIP_REFRESH_MEMORY_THRESHOLD || '0.75',
);

// Memory monitoring helper
function formatMemoryUsage() {
  const usage = process.memoryUsage();
  const toMB = (bytes) => Math.round(bytes / 1024 / 1024);
  return {
    heapUsed: toMB(usage.heapUsed),
    heapTotal: toMB(usage.heapTotal),
    rss: toMB(usage.rss),
    external: toMB(usage.external),
  };
}

export function startBackgroundAppDataRefresh({
  intervalSeconds = DEFAULT_INTERVAL_SECONDS,
  createSnapshot,
  onAfterRefresh = null,
} = {}) {
  if (typeof createSnapshot !== 'function') {
    throw new Error('startBackgroundAppDataRefresh requires a createSnapshot function');
  }
  if (!Number.isFinite(intervalSeconds) || intervalSeconds <= 0) {
    return null;
  }

  let isRunning = false;
  let consecutiveSkips = 0;

  const runRefresh = async () => {
    if (isRunning) {
      return;
    }

    // Check memory pressure before starting refresh
    const memStats = getMemoryStats();
    if (memStats.heapUsedPercent >= SKIP_REFRESH_MEMORY_THRESHOLD) {
      consecutiveSkips++;
      console.warn(
        `[app-data] Skipping refresh due to memory pressure: ` +
        `${memStats.heapUsedMB}MB / ${memStats.heapLimitMB}MB ` +
        `(${Math.round(memStats.heapUsedPercent * 100)}%) - skips: ${consecutiveSkips}`,
      );
      // Force GC and try again next interval
      forceGC();
      return;
    }

    // Reset skip counter on successful start
    if (consecutiveSkips > 0) {
      console.log(`[app-data] Memory recovered after ${consecutiveSkips} skipped refreshes`);
      consecutiveSkips = 0;
    }

    isRunning = true;
    const memBefore = formatMemoryUsage();

    // Pre-refresh GC to start with clean slate
    forceGC();

    try {
      const startTime = Date.now();
      const snapshot = await createSnapshot();
      if (typeof onAfterRefresh === 'function') {
        await onAfterRefresh(snapshot);
      }
      const duration = Date.now() - startTime;
      const memAfter = formatMemoryUsage();
      console.log(
        `Background app-data refresh completed in ${duration}ms | ` +
        `Memory: heap ${memBefore.heapUsed}→${memAfter.heapUsed}MB, rss ${memBefore.rss}→${memAfter.rss}MB`
      );
      // Trigger GC if available and heap grew significantly
      if (memAfter.heapUsed > memBefore.heapUsed + 30) {
        forceGC();
        const memPostGc = formatMemoryUsage();
        console.log(`[gc] Post-refresh GC: heap ${memAfter.heapUsed}→${memPostGc.heapUsed}MB`);
      }
    } catch (error) {
      console.warn('Background app-data refresh failed:', error.message);
    } finally {
      isRunning = false;
    }
  };

  const timer = setInterval(runRefresh, intervalSeconds * 1000);
  if (typeof timer.unref === 'function') {
    timer.unref();
  }

  return {
    stop() {
      clearInterval(timer);
    },
    triggerNow() {
      runRefresh().catch(() => {
        /* handled above */
      });
    },
  };
}

