import { spawn } from 'child_process';
import { Client } from 'pg';

type LogStatus = 'running' | 'success' | 'skipped' | 'failed';

const LOG_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS etl_run_log (
    id BIGSERIAL PRIMARY KEY,
    task TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    notes TEXT,
    details JSONB DEFAULT '{}'::JSONB
  );
`;

function parseYearSpec(spec: string | null | undefined): number[] {
  if (!spec) {
    return Array.from({ length: 17 }, (_, index) => 2008 + index);
  }
  const result = new Set<number>();
  for (const token of spec.split(',')) {
    const trimmed = token.trim();
    if (!trimmed) {
      continue;
    }
    if (trimmed.includes('-')) {
      const [startRaw, endRaw] = trimmed.split('-', 2);
      const start = Number.parseInt(startRaw, 10);
      const end = Number.parseInt(endRaw, 10);
      if (Number.isFinite(start) && Number.isFinite(end)) {
        const [minYear, maxYear] = start <= end ? [start, end] : [end, start];
        for (let year = minYear; year <= maxYear; year += 1) {
          result.add(year);
        }
      }
    } else {
      const value = Number.parseInt(trimmed, 10);
      if (Number.isFinite(value)) {
        result.add(value);
      }
    }
  }
  return Array.from(result).sort((a, b) => a - b);
}

async function runCommand(command: string, args: string[], cwd: string): Promise<void> {
  const env = { ...process.env, PYTHONUNBUFFERED: '1' };
  await new Promise<void>((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout.on('data', (chunk) => {
      process.stdout.write(chunk);
    });
    child.stderr.on('data', (chunk) => {
      process.stderr.write(chunk);
    });
    child.on('error', (error) => {
      reject(error);
    });
    child.on('close', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`${command} exited with ${code}`));
      }
    });
  });
}

async function ensureLogTable(client: Client): Promise<void> {
  await client.query(LOG_TABLE_SQL);
}

async function recordRun(
  client: Client,
  task: string,
  status: LogStatus,
  notes: string,
  details: Record<string, unknown>,
  id?: number,
): Promise<number> {
  if (id) {
    await client.query(
      `
        UPDATE etl_run_log
        SET status = $1, completed_at = NOW(), notes = $2, details = $3
        WHERE id = $4
      `,
      [status, notes, details, id],
    );
    return id;
  }
  const result = await client.query<{ id: number }>(
    `
      INSERT INTO etl_run_log (task, status, notes, details)
      VALUES ($1, $2, $3, $4)
      RETURNING id
    `,
    [task, status, notes, details],
  );
  return result.rows[0].id;
}

async function fetchParkingYears(client: Client): Promise<Set<number>> {
  const response = await client.query<{ year: number }>(
    `
      SELECT DISTINCT EXTRACT(YEAR FROM date_of_infraction)::INT AS year
      FROM parking_tickets
      WHERE date_of_infraction IS NOT NULL
      ORDER BY year ASC
    `,
  );
  return new Set(response.rows.map((row) => row.year));
}

async function main(): Promise<void> {
  const repoRoot = process.env.REPO_ROOT ? process.env.REPO_ROOT : process.cwd();
  const pythonBin = process.env.PYTHON_BIN || 'python';
  const databaseUrl = process.env.DATABASE_URL;
  const postgisUrl = process.env.POSTGIS_DATABASE_URL || databaseUrl;

  if (!databaseUrl) {
    throw new Error('DATABASE_URL must be provided in the environment');
  }
  if (!postgisUrl) {
    throw new Error('POSTGIS_DATABASE_URL must be provided in the environment');
  }

  const client = new Client({ connectionString: databaseUrl });
  await client.connect();

  try {
    await ensureLogTable(client);

    const expectedParkingYears = parseYearSpec(process.env.PARKING_EXPECTED_YEARS);
    const existingYears = await fetchParkingYears(client);
    const missingYears = expectedParkingYears.filter((year) => !existingYears.has(year));

    const forceParking = process.env.FORCE_PARKING_LOAD === '1';
    const forceYearly = process.env.FORCE_YEARLY_METRICS === '1';
    const forceRedis = process.env.FORCE_REDIS_PUSH === '1';

    const shouldLoadParking = forceParking || missingYears.length > 0;
    const shouldRunYearly = forceYearly || shouldLoadParking;
    const shouldRunCamera = process.env.SKIP_CAMERA_DATA === '1' ? false : true;
    const shouldPushRedis = forceRedis || shouldRunYearly || shouldRunCamera;

    const details: Record<string, unknown> = {
      expectedParkingYears,
      missingParkingYears: missingYears,
      forcedSteps: {
        parking: forceParking,
        yearly: forceYearly,
        redis: forceRedis,
      },
    };

    const runId = await recordRun(
      client,
      'railway-cron-refresh',
      'running',
      shouldLoadParking
        ? `Loading parking years: ${missingYears.join(', ') || 'none'}`
        : 'No missing parking years detected',
      details,
    );

    try {
      if (shouldLoadParking) {
        const yearSpec = missingYears.length > 0 ? missingYears.join(',') : expectedParkingYears.join(',');
        console.log(`Running parking loader for years: ${yearSpec}`);
        await runCommand(pythonBin, ['scripts/load_parking_tickets_local.py', '--years', yearSpec], repoRoot);
      } else {
        console.log('Skipping parking loader - no missing years.');
      }

      if (shouldRunYearly) {
        console.log('Rebuilding yearly metrics...');
        await runCommand(pythonBin, ['scripts/build_yearly_metrics.py'], repoRoot);
      } else {
        console.log('Skipping yearly metrics rebuild.');
      }

      if (shouldRunCamera) {
        console.log('Regenerating camera datasets...');
        await runCommand(pythonBin, ['preprocessing/build_camera_datasets.py'], repoRoot);
      } else {
        console.log('Skipping camera dataset regeneration.');
      }

      if (shouldPushRedis) {
        console.log('Pushing datasets to Redis...');
        await runCommand(pythonBin, ['scripts/push_tickets_to_redis.py'], repoRoot);
      } else {
        console.log('Skipping Redis push.');
      }

      await recordRun(client, 'railway-cron-refresh', 'success', 'Refresh completed successfully', details, runId);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      details.error = message;
      await recordRun(client, 'railway-cron-refresh', 'failed', message, details, runId);
      throw error;
    }
  } finally {
    await client.end();
  }
}

main()
  .then(() => {
    console.log('ETL refresh script finished.');
  })
  .catch((error) => {
    console.error('ETL refresh script failed:', error);
    process.exitCode = 1;
  });
