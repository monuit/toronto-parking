const TILE_REQUEST_WINDOW_MS = 10_000;

const metricsState = {
  navigationStart: typeof performance !== 'undefined' ? performance.now() : 0,
  mapReadyAt: null,
  ticketsPaintAt: null,
  fpsSamples: [],
  panSamples: [],
  tileRequests: 0,
  tileWindowRequests: 0,
  tileWindowStart: null,
  tileTtfbSamples: [],
  tileCompleted: 0,
  tileAborted: 0,
  fcp: null,
  firstPaint: null,
  firstInputDelay: null,
  jsBytes: 0,
  flushScheduled: false,
  panFrameHandle: null,
  panStartTime: null,
  panFrameCount: 0,
  baselineFrameHandle: null,
  observersReady: false,
  sent: false,
};

const jsResourceSizes = new Map();

function now() {
  return typeof performance !== 'undefined' ? performance.now() : Date.now();
}

function ensurePerformanceObservers() {
  if (metricsState.observersReady || typeof window === 'undefined' || typeof PerformanceObserver === 'undefined') {
    return;
  }
  metricsState.observersReady = true;

  try {
    const paintObserver = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        if (entry.name === 'first-contentful-paint') {
          metricsState.fcp = entry.startTime;
          scheduleFlush();
        } else if (entry.name === 'first-paint') {
          metricsState.firstPaint = entry.startTime;
          scheduleFlush();
        }
      }
    });
    paintObserver.observe({ type: 'paint', buffered: true });
  } catch {
    /* ignore unsupported paint observer */
  }

  try {
    const fidObserver = new PerformanceObserver((list) => {
      const entry = list.getEntries()[0];
      if (!entry) {
        return;
      }
      const delay = entry.processingEnd - entry.startTime;
      if (Number.isFinite(delay) && delay >= 0) {
        metricsState.firstInputDelay = delay;
        scheduleFlush();
      }
    });
    fidObserver.observe({ type: 'first-input', buffered: true });
  } catch {
    /* ignore unsupported first-input observer */
  }

  try {
    const resourceObserver = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        if (!entry || typeof entry.name !== 'string') {
          continue;
        }
        const isScript = entry.initiatorType === 'script' || /\.js(\?|$)/.test(entry.name);
        if (isScript) {
          const size = Number.isFinite(entry.encodedBodySize) && entry.encodedBodySize > 0
            ? entry.encodedBodySize
            : (Number.isFinite(entry.transferSize) ? entry.transferSize : null);
          if (Number.isFinite(size) && size >= 0) {
            jsResourceSizes.set(entry.name, size);
            let total = 0;
            for (const value of jsResourceSizes.values()) {
              total += value;
            }
            metricsState.jsBytes = total;
            scheduleFlush();
          }
        }
      }
    });
    resourceObserver.observe({ type: 'resource', buffered: true });
  } catch {
    /* ignore unsupported resource observer */
  }
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
      scheduleFlush();
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

function tileKeyFromEvent(event) {
  const tile = event?.tile || event?.sourceTile;
  if (!tile) {
    return null;
  }
  if (tile.uid !== undefined) {
    return tile.uid;
  }
  if (tile.key) {
    return tile.key;
  }
  const tileId = tile.tileID || tile.coord || tile.canonicalID || tile.state || {};
  const getNumber = (value) => {
    if (Number.isFinite(value)) {
      return Number(value);
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  };
  const z = getNumber(tile.z ?? tile.zoom ?? tileId.z);
  const x = getNumber(tile.x ?? tileId.x);
  const y = getNumber(tile.y ?? tileId.y);
  if (z === null || x === null || y === null) {
    return null;
  }
  const source = event?.sourceId || tile.sourceId || tile.source || 'default';
  return `${source}:${z}:${x}:${y}`;
}

function sendMetrics() {
  if (metricsState.sent) {
    return;
  }
  metricsState.sent = true;
  stopPanMeasurement();
  const tileAttempts = metricsState.tileCompleted + metricsState.tileAborted;
  const payload = {
    navigationStart: metricsState.navigationStart,
    mapReadyAt: metricsState.mapReadyAt,
    ticketsPaintAt: metricsState.ticketsPaintAt,
    fpsSamples: metricsState.fpsSamples.slice(0, 8),
    panFps: metricsState.panSamples.slice(0, 8),
    tileRequests: metricsState.tileRequests,
    tileWindow: {
      count: metricsState.tileWindowRequests,
      windowMs: TILE_REQUEST_WINDOW_MS,
    },
    tileCompleted: metricsState.tileCompleted,
    tileAborted: metricsState.tileAborted,
    tileAbortRatio: tileAttempts > 0 ? metricsState.tileAborted / tileAttempts : 0,
    tileTtfb: metricsState.tileTtfbSamples.slice(0, 16),
    firstContentfulPaint: metricsState.fcp,
    firstPaint: metricsState.firstPaint,
    firstInputDelay: metricsState.firstInputDelay,
    jsBytes: metricsState.jsBytes,
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
  ensurePerformanceObservers();
  collectBaselineFps();
  const tileStartTimes = new Map();

  map.on('load', () => {
    if (!metricsState.mapReadyAt) {
      metricsState.mapReadyAt = now();
    }
    metricsState.tileWindowStart = metricsState.mapReadyAt;
    metricsState.tileWindowRequests = 0;
    metricsState.tileCompleted = 0;
    metricsState.tileAborted = 0;
    metricsState.tileTtfbSamples = [];
    tileStartTimes.clear();
    scheduleFlush();
  });

  map.on('dataloading', (event) => {
    if (event?.dataType !== 'tile') {
      return;
    }
    metricsState.tileRequests += 1;
    const startedAt = now();
    if (metricsState.tileWindowStart === null) {
      metricsState.tileWindowStart = startedAt;
    }
    if (startedAt - metricsState.tileWindowStart <= TILE_REQUEST_WINDOW_MS) {
      metricsState.tileWindowRequests += 1;
    }
    const key = tileKeyFromEvent(event);
    if (key !== null) {
      tileStartTimes.set(key, startedAt);
    }
  });

  map.on('data', (event) => {
    if (event?.dataType !== 'tile') {
      return;
    }
    const key = tileKeyFromEvent(event);
    if (key === null || !tileStartTimes.has(key)) {
      return;
    }
    const startedAt = tileStartTimes.get(key);
    tileStartTimes.delete(key);
    if (Number.isFinite(startedAt)) {
      const elapsed = Math.max(now() - startedAt, 0);
      metricsState.tileTtfbSamples.push(elapsed);
      if (metricsState.tileTtfbSamples.length > 32) {
        metricsState.tileTtfbSamples.shift();
      }
      metricsState.tileCompleted += 1;
      scheduleFlush();
    }
  });

  map.on('error', (event) => {
    if (!event) {
      return;
    }
    const key = tileKeyFromEvent(event);
    if (key !== null && tileStartTimes.has(key)) {
      tileStartTimes.delete(key);
    }
    const message = String(event.error?.message || '').toLowerCase();
    if (message.includes('abort') || message.includes('cancel')) {
      metricsState.tileAborted += 1;
      scheduleFlush();
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
