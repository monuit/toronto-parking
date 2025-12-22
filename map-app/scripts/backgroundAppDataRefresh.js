import process from 'node:process';

const DEFAULT_INTERVAL_SECONDS = Number.parseInt(process.env.APP_DATA_REFRESH_SECONDS || '900', 10);

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

  const runRefresh = async () => {
    if (isRunning) {
      return;
    }
    isRunning = true;
    const memBefore = formatMemoryUsage();
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
      if (global.gc && memAfter.heapUsed > memBefore.heapUsed + 50) {
        global.gc();
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
