import process from 'node:process';

const DEFAULT_INTERVAL_SECONDS = Number.parseInt(process.env.APP_DATA_REFRESH_SECONDS || '900', 10);

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
    try {
      const startTime = Date.now();
      const snapshot = await createSnapshot();
      if (typeof onAfterRefresh === 'function') {
        await onAfterRefresh(snapshot);
      }
      const duration = Date.now() - startTime;
      console.log(`Background app-data refresh completed in ${duration}ms`);
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
