const metricsState = {
  navigationStart: typeof performance !== 'undefined' ? performance.now() : 0,
  mapReadyAt: null,
  ticketsPaintAt: null,
  fpsSamples: [],
  panSamples: [],
  tileRequests: 0,
  flushScheduled: false,
  panFrameHandle: null,
  panStartTime: null,
  panFrameCount: 0,
  sent: false,
};

function now() {
  return typeof performance !== 'undefined' ? performance.now() : Date.now();
}

function collectBaselineFps(durationMs = 2000) {
  if (metricsState.baselineFrameHandle) {
    return;
  }
  const start = now();
  let frames = 0;
  const frame = () => {
    frames += 1;
    if (now() - start >= durationMs) {
      const elapsed = Math.max(now() - start, 1);
      metricsState.fpsSamples.push((frames / elapsed) * 1000);
      metricsState.baselineFrameHandle = null;
      return;
    }
    metricsState.baselineFrameHandle = requestAnimationFrame(frame);
  };
  metricsState.baselineFrameHandle = requestAnimationFrame(frame);
}

function stopPanMeasurement() {
  if (metricsState.panFrameHandle !== null && typeof cancelAnimationFrame === 'function') {
    cancelAnimationFrame(metricsState.panFrameHandle);
  }
  if (metricsState.panStartTime !== null) {
    const duration = Math.max(now() - metricsState.panStartTime, 1);
    const fps = (metricsState.panFrameCount / duration) * 1000;
    if (Number.isFinite(fps)) {
      metricsState.panSamples.push(fps);
    }
  }
  metricsState.panFrameHandle = null;
  metricsState.panStartTime = null;
  metricsState.panFrameCount = 0;
}

function scheduleFlush() {
  if (metricsState.flushScheduled || metricsState.sent) {
    return;
  }
  metricsState.flushScheduled = true;
  setTimeout(() => {
    metricsState.flushScheduled = false;
    sendMetrics();
  }, 12_000);
}

function sendMetrics() {
  if (metricsState.sent) {
    return;
  }
  metricsState.sent = true;
  stopPanMeasurement();
  const payload = {
    navigationStart: metricsState.navigationStart,
    mapReadyAt: metricsState.mapReadyAt,
    ticketsPaintAt: metricsState.ticketsPaintAt,
    fpsSamples: metricsState.fpsSamples.slice(0, 8),
    panFps: metricsState.panSamples.slice(0, 8),
    tileRequests: metricsState.tileRequests,
    generatedAt: new Date().toISOString(),
  };
  if (typeof navigator !== 'undefined' && typeof navigator.sendBeacon === 'function') {
    try {
      navigator.sendBeacon('/api/client-metrics', JSON.stringify(payload));
      return;
    } catch {
      /* ignore beacon errors */
    }
  }
  if (typeof fetch === 'function') {
    fetch('/api/client-metrics', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      keepalive: true,
    }).catch(() => {
      /* non-fatal */
    });
  }
}

function trackPanFrame() {
  metricsState.panFrameCount += 1;
  if (metricsState.panStartTime !== null) {
    metricsState.panFrameHandle = requestAnimationFrame(trackPanFrame);
  }
}

export function registerMapMetrics(map) {
  if (typeof window === 'undefined' || !map) {
    return;
  }
  collectBaselineFps();
  map.on('load', () => {
    if (!metricsState.mapReadyAt) {
      metricsState.mapReadyAt = now();
      scheduleFlush();
    }
  });
  map.on('dataloading', (event) => {
    if (event?.dataType === 'tile') {
      metricsState.tileRequests += 1;
    }
  });
  map.on('movestart', () => {
    stopPanMeasurement();
    metricsState.panStartTime = now();
    metricsState.panFrameCount = 0;
    metricsState.panFrameHandle = requestAnimationFrame(trackPanFrame);
  });
  map.on('moveend', () => {
    stopPanMeasurement();
  });
}

export function recordTicketsPaint() {
  if (!metricsState.ticketsPaintAt) {
    metricsState.ticketsPaintAt = now();
    scheduleFlush();
  }
}

export function incrementTileRequestCount() {
  metricsState.tileRequests += 1;
}
