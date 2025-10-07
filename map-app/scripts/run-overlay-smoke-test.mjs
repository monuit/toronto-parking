import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';
import { chromium } from 'playwright';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..');
const PREVIEW_PORT = process.env.SMOKE_PORT ? Number(process.env.SMOKE_PORT) : 4173;
const PREVIEW_HOST = '127.0.0.1';
const PREVIEW_URL = `http://${PREVIEW_HOST}:${PREVIEW_PORT}`;
const VIEWPORTS = [
  { width: 1440, height: 900, label: 'desktop' },
  { width: 1024, height: 768, label: 'tablet' },
  { width: 390, height: 844, label: 'mobile' },
];

function startPreviewServer() {
  return new Promise((resolve, reject) => {
    let resolved = false;
    const previewCommand = `npm run preview -- --host ${PREVIEW_HOST} --port ${PREVIEW_PORT}`;
    const child = spawn(previewCommand, {
      cwd: projectRoot,
      stdio: ['ignore', 'pipe', 'pipe'],
      shell: true,
      env: { ...process.env, BROWSER: 'none' },
    });

    const readyTimeout = setTimeout(() => {
      if (!resolved) {
        child.kill();
        reject(new Error('Timed out waiting for Vite preview server to start'));
      }
    }, 15000);

    const handleStdout = (data) => {
      const raw = data.toString();
      const message = raw.replace(/\u001b\[[0-9;]*m/g, '');
      process.stdout.write(`[preview] ${message}`);
      if (!resolved && message.includes('Local:')) {
        resolved = true;
        clearTimeout(readyTimeout);
        child.stdout.off('data', handleStdout);
        resolve(child);
      }
    };

    child.stdout.on('data', handleStdout);
    child.stderr.on('data', (data) => {
      process.stdout.write(`[preview:err] ${data}`);
    });

    child.on('error', (error) => {
      if (!resolved) {
        clearTimeout(readyTimeout);
        reject(error);
      }
    });

    child.on('exit', (code) => {
      if (!resolved && code !== 0) {
        clearTimeout(readyTimeout);
        reject(new Error(`Preview server exited prematurely with code ${code}`));
      }
    });
  });
}

async function runSmokeTest() {
  const previewProcess = await startPreviewServer();
  const browser = await chromium.launch();
  const report = [];

  try {
    for (const viewport of VIEWPORTS) {
      const page = await browser.newPage({ viewport: { width: viewport.width, height: viewport.height } });
      await page.goto(PREVIEW_URL, { waitUntil: 'networkidle' });

      const overlayLocator = page.locator('.right-overlay');
      await overlayLocator.waitFor({ state: 'visible', timeout: 10000 });
      const boundingBox = await overlayLocator.boundingBox();
      if (!boundingBox) {
        throw new Error(`Overlay not found for viewport ${viewport.label}`);
      }

      const withinHorizontalBounds = boundingBox.x >= -1 && boundingBox.x + boundingBox.width <= viewport.width + 1;
      const withinVerticalBounds = boundingBox.y >= -1 && boundingBox.y + boundingBox.height <= viewport.height + 1;

      if (!withinHorizontalBounds || !withinVerticalBounds) {
        throw new Error(`Overlay out of viewport bounds for ${viewport.label}: ${JSON.stringify(boundingBox)}`);
      }

      report.push({ viewport: viewport.label, boundingBox });
      await page.close();
    }
  } finally {
    await browser.close();
    previewProcess.kill();
    await new Promise((resolve) => previewProcess.on('close', resolve));
  }

  return report;
}

(async () => {
  try {
    const results = await runSmokeTest();
    console.log('Overlay smoke test results:', results);
  } catch (error) {
    console.error('Overlay smoke test failed:', error);
    process.exitCode = 1;
  }
})();
