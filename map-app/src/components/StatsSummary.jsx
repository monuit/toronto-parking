import { useCallback, useEffect, useMemo, useState } from 'react';
import { formatCurrency, formatNumber } from '../lib/dataTransforms';
import { useAppData } from '../context/AppDataContext.jsx';
import '../styles/StatsSummary.css';

const DATASET_TITLES = {
  parking_tickets: 'Toronto Parking Tickets',
  red_light_locations: 'Red Light Camera Charges',
  ase_locations: 'Automated Speed Enforcement Charges',
  cameras_combined: 'Traffic Enforcement Wards',
};

export function StatsSummary({
  viewportSummary,
  variant = 'default',
  showTotals = true,
  showViewport = true,
  dataset = 'parking_tickets',
  totalsOverride = null,
  title,
  viewportTitle = 'Current view',
  yearFilter = null,
  discrepancyInfo = null,
  onToggleLegacy = null,
  useLegacyTotals = false,
  combinedBreakdownOverride = null,
}) {
  const appData = useAppData();
  const datasetEntry = (appData?.datasets && appData.datasets[dataset]) || null;
  const currentTotals = datasetEntry?.totals || datasetEntry?.legacyTotals || {};
  const legacyTotals = datasetEntry?.legacyTotals || currentTotals;
  const baseTotals = totalsOverride && Object.keys(totalsOverride).length > 0
    ? totalsOverride
    : (useLegacyTotals ? legacyTotals : currentTotals);
  const totalsSource = baseTotals;
  const toNumber = useCallback((value, fallback = 0) => {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : fallback;
  }, []);
  const isParkingDataset = dataset === 'parking_tickets';
  const combinedBreakdown = useMemo(() => {
    if (dataset !== 'cameras_combined') {
      return null;
    }
    const fallback = datasetEntry?.breakdown || {};
    const aseTotals = appData?.datasets?.ase_locations?.totals || {};
    const redTotals = appData?.datasets?.red_light_locations?.totals || {};
    if (combinedBreakdownOverride) {
      return {
        ase: {
          ticketCount: toNumber(
            combinedBreakdownOverride.ase?.ticketCount
              ?? combinedBreakdownOverride.ase?.featureCount
              ?? fallback?.ase?.ticketCount
              ?? aseTotals.ticketCount,
            0,
          ),
          totalRevenue: toNumber(
            combinedBreakdownOverride.ase?.totalRevenue
              ?? fallback?.ase?.totalRevenue
              ?? aseTotals.totalRevenue,
            0,
          ),
          locationCount: toNumber(
            combinedBreakdownOverride.ase?.locationCount
              ?? combinedBreakdownOverride.ase?.featureCount
              ?? fallback?.ase?.locationCount
              ?? aseTotals.locationCount,
            0,
          ),
        },
        redLight: {
          ticketCount: toNumber(
            combinedBreakdownOverride.redLight?.ticketCount
              ?? combinedBreakdownOverride.redLight?.featureCount
              ?? fallback?.redLight?.ticketCount
              ?? redTotals.ticketCount,
            0,
          ),
          totalRevenue: toNumber(
            combinedBreakdownOverride.redLight?.totalRevenue
              ?? fallback?.redLight?.totalRevenue
              ?? redTotals.totalRevenue,
            0,
          ),
          locationCount: toNumber(
            combinedBreakdownOverride.redLight?.locationCount
              ?? combinedBreakdownOverride.redLight?.featureCount
              ?? fallback?.redLight?.locationCount
              ?? redTotals.locationCount,
            0,
          ),
        },
      };
    }
    return {
      ase: {
        ticketCount: toNumber(fallback?.ase?.ticketCount ?? aseTotals.ticketCount, 0),
        totalRevenue: toNumber(fallback?.ase?.totalRevenue ?? aseTotals.totalRevenue, 0),
        locationCount: toNumber(fallback?.ase?.locationCount ?? aseTotals.locationCount, 0),
      },
      redLight: {
        ticketCount: toNumber(fallback?.redLight?.ticketCount ?? redTotals.ticketCount, 0),
        totalRevenue: toNumber(fallback?.redLight?.totalRevenue ?? redTotals.totalRevenue, 0),
        locationCount: toNumber(fallback?.redLight?.locationCount ?? redTotals.locationCount, 0),
      },
    };
  }, [dataset, datasetEntry?.breakdown, appData?.datasets, toNumber, combinedBreakdownOverride]);

  const locationLabel = dataset === 'cameras_combined' ? 'Wards tracked' : 'Locations tracked';
  const locationCountValue = toNumber(
    totalsSource.locationCount ?? totalsSource.featureCount,
    0,
  );
  const ticketsCountValue = toNumber(
    totalsSource.ticketCount ?? totalsSource.featureCount,
    0,
  );
  const totalRevenueValue = toNumber(totalsSource.totalRevenue, 0);
  const heading = title || DATASET_TITLES[dataset] || 'Dataset overview';
  const hasViewportData = !viewportSummary?.zoomRestricted && typeof viewportSummary?.visibleCount === 'number';
  const classes = ['stats-summary'];
  const [infoOpen, setInfoOpen] = useState(false);
  const discrepancyCurrent = discrepancyInfo?.current || null;
  const discrepancyLegacy = discrepancyInfo?.legacy || null;
  const discrepancyDelta = discrepancyInfo?.delta || null;
  const discrepancyNote = discrepancyInfo?.note || null;
  const forceDiscrepancy = discrepancyInfo?.forceShow === true;
  const ticketDelta = Number(discrepancyDelta?.ticketCount ?? 0);
  const revenueDelta = Number(discrepancyDelta?.totalRevenue ?? 0);
  const locationDelta = Number(discrepancyDelta?.locationCount ?? 0);
  const filterLabel = yearFilter !== null ? `Filtered to ${yearFilter}` : null;
  const showLegacyToggle = typeof onToggleLegacy === 'function';
  useEffect(() => {
    setInfoOpen(false);
  }, [dataset, discrepancyInfo, showTotals]);
  const hasMeaningfulDelta = Boolean(
    discrepancyDelta
      && (ticketDelta !== 0 || revenueDelta !== 0 || (!isParkingDataset && locationDelta !== 0)),
  );
  const showDiscrepancyControl = Boolean(
    showTotals
      && discrepancyCurrent
      && discrepancyLegacy
      && (hasMeaningfulDelta || forceDiscrepancy || discrepancyNote)
      && ['parking_tickets', 'red_light_locations', 'ase_locations'].includes(dataset)
  );

  const handleInfoToggle = () => {
    setInfoOpen((current) => !current);
  };

  const legacyToggleControl = showLegacyToggle ? (
    <label className="discrepancy-toggle">
      <input
        type="checkbox"
        checked={useLegacyTotals}
        onChange={(event) => onToggleLegacy(Boolean(event.target?.checked))}
      />
      <span>Include earlier unverified totals</span>
    </label>
  ) : null;

  const renderDiscrepancyDetails = () => {
    if (!discrepancyCurrent || !discrepancyLegacy) {
      return null;
    }

    if (discrepancyNote?.type === 'parkingLegacy') {
      return (
        <>
          <p>
            Latest processed totals show {formatNumber(discrepancyCurrent.ticketCount)} tickets worth {formatCurrency(discrepancyCurrent.totalRevenue)}.
            {' '}Earlier exports recorded {formatNumber(discrepancyLegacy.ticketCount)} tickets worth {formatCurrency(discrepancyLegacy.totalRevenue)}.
          </p>
          {ticketDelta !== 0 ? (
            <p className="discrepancy-note">
              {formatNumber(Math.abs(ticketDelta))} tickets {ticketDelta > 0 ? 'remain in the legacy export' : 'have been removed from the reconciled dataset'}.
            </p>
          ) : null}
          {Math.abs(revenueDelta) > 0 ? (
            <p className="discrepancy-note">
              Revenue totals differ by {formatCurrency(Math.abs(revenueDelta))}.
            </p>
          ) : null}
          {discrepancyNote.footnote ? (
            <p className="discrepancy-note discrepancy-note--footnote">{discrepancyNote.footnote}</p>
          ) : null}
          {legacyToggleControl}
        </>
      );
    }

    if (discrepancyNote?.type === 'aseHistorical' || discrepancyNote?.type === 'rlcHistorical') {
      const resolvedLocations = toNumber(discrepancyNote.resolvedLocations, discrepancyCurrent.locationCount);
      const unresolvedLocations = toNumber(discrepancyNote.unresolvedLocations, 0);
      const totalLocations = toNumber(discrepancyNote.totalLocations, resolvedLocations + unresolvedLocations);
      const unresolvedTicketCount = toNumber(discrepancyNote.unresolvedTicketCount, 0);
      const datasetLabel = discrepancyNote.type === 'aseHistorical' ? 'ASE camera' : 'red light camera';
      return (
        <>
          <p>
            {formatNumber(resolvedLocations)} {datasetLabel}{resolvedLocations === 1 ? '' : 's'} now include mapped geometry in the live dataset.
          </p>
          {unresolvedLocations > 0 ? (
            <p className="discrepancy-note">
              {formatNumber(unresolvedLocations)} historical {datasetLabel}{unresolvedLocations === 1 ? '' : 's'} remain without coordinates, representing {formatNumber(unresolvedTicketCount)} tickets.
            </p>
          ) : (
            <p className="discrepancy-note">
              All {formatNumber(totalLocations)} historical {datasetLabel}{totalLocations === 1 ? '' : 's'} now include mapped geometry.
            </p>
          )}
          {discrepancyNote.footnote ? (
            <p className="discrepancy-note discrepancy-note--footnote">{discrepancyNote.footnote}</p>
          ) : null}
          {legacyToggleControl}
        </>
      );
    }

    if (discrepancyNote && (discrepancyNote.title || Array.isArray(discrepancyNote.lines))) {
      return (
        <>
          {discrepancyNote.title ? (
            <h4>{discrepancyNote.title}</h4>
          ) : null}
          {Array.isArray(discrepancyNote.lines) && discrepancyNote.lines.length > 0 ? (
            <ul>
              {discrepancyNote.lines.map((line, index) => (
                <li key={index}>{line}</li>
              ))}
            </ul>
          ) : null}
          {discrepancyNote.footnote ? (
            <p className="discrepancy-note discrepancy-note--footnote">{discrepancyNote.footnote}</p>
          ) : null}
          {legacyToggleControl}
        </>
      );
    }

    if (isParkingDataset) {
      return (
        <>
          <p>
            Latest processed totals show {formatNumber(discrepancyCurrent.ticketCount)} tickets worth {formatCurrency(discrepancyCurrent.totalRevenue)}.
            {' '}Previous snapshot captured {formatNumber(discrepancyLegacy.ticketCount)} tickets worth {formatCurrency(discrepancyLegacy.totalRevenue)}.
          </p>
          {ticketDelta !== 0 ? (
            <p className="discrepancy-note">
              Difference: {formatNumber(Math.abs(ticketDelta))} tickets {ticketDelta > 0 ? 'still present in the legacy export' : 'removed from the reconciled dataset'}.
            </p>
          ) : null}
          {Math.abs(revenueDelta) > 0 ? (
            <p className="discrepancy-note">
              Revenue totals differ by {formatCurrency(Math.abs(revenueDelta))}.
            </p>
          ) : null}
          {legacyToggleControl}
        </>
      );
    }

    return (
      <>
        {hasMeaningfulDelta ? (
          <p>
            Latest processed totals show {formatNumber(discrepancyCurrent.ticketCount)} tickets
            {ticketDelta !== 0 ? ` (${ticketDelta > 0 ? '+' : ''}${formatNumber(ticketDelta)} vs. earlier snapshot)` : ''}
            {discrepancyCurrent.totalRevenue !== null
              ? ` worth ${formatCurrency(discrepancyCurrent.totalRevenue)}`
              : ''}
            . Previous snapshot captured {formatNumber(discrepancyLegacy.ticketCount)} tickets
            {ticketDelta !== 0 ? ` (${ticketDelta > 0 ? '+' : ''}${formatNumber(ticketDelta)})` : ''}.
          </p>
        ) : (
          <p>
            Latest processed totals now match the published snapshot for this dataset.
          </p>
        )}
        {Math.abs(locationDelta) > 0 ? (
          <p className="discrepancy-note">
            Locations recorded differ by {formatNumber(Math.abs(locationDelta))}.
          </p>
        ) : null}
        {Math.abs(revenueDelta) > 0 ? (
          <p className="discrepancy-note">
            Revenue totals differ by {formatCurrency(Math.abs(revenueDelta))}.
          </p>
        ) : null}
        {legacyToggleControl}
      </>
    );
  };

  if (variant === 'compact') {
    classes.push('stats-summary--compact');
  }

  if (!showTotals) {
    classes.push('stats-summary--viewport-only');
  }

  return (
    <div className={classes.join(' ')}>
      {showTotals ? (
        <div className="totals">
          <div className="totals-header">
            <h2>{heading}</h2>
            {showDiscrepancyControl ? (
              <div className="discrepancy-control">
                <button
                  type="button"
                  className={`discrepancy-button${infoOpen ? ' active' : ''}`}
                  onClick={handleInfoToggle}
                  aria-expanded={infoOpen}
                  aria-controls={`dataset-discrepancy-${dataset}`}
                  aria-label="Explain totals discrepancy"
                >
                  <span className="sr-only">Explain totals discrepancy</span>
                  <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                    <circle cx="12" cy="12" r="9" stroke="currentColor" strokeWidth="1.6" fill="none" />
                    <path
                      d="M12 11.2c-1.2 0-2 .72-2 1.76 0 .48.16.9.64 1.28l.64.52c.24.22.4.4.4.64 0 .36-.28.6-.68.6-.36 0-.7-.18-.96-.4l-.58.94c.48.42 1.18.72 1.98.72 1.24 0 2.08-.78 2.08-1.84 0-.7-.3-1.18-.98-1.72l-.64-.48c-.24-.18-.36-.36-.36-.58 0-.3.26-.5.6-.5.32 0 .64.16.86.38l.62-.9c-.44-.42-1.04-.72-1.62-.72Zm0-3.06a1.2 1.2 0 1 0 0 2.4 1.2 1.2 0 0 0 0-2.4Z"
                      fill="currentColor"
                    />
                  </svg>
                </button>
                {infoOpen ? (
                  <div
                    id={`dataset-discrepancy-${dataset}`}
                    className="discrepancy-popover"
                  >
                    {renderDiscrepancyDetails()}
                  </div>
                ) : null}
              </div>
            ) : null}
          </div>
          {dataset === 'cameras_combined' ? (
            <>
              <div className="totals-grid totals-grid--single">
                <div>
                  <span className="label">{locationLabel}</span>
                  <span className="value">{formatNumber(locationCountValue)}</span>
                </div>
                <div>
                  <span className="label">Tickets issued</span>
                  <span className="value">{formatNumber(ticketsCountValue)}</span>
                </div>
                <div>
                  <span className="label">Total fines</span>
                  <span className="value">{formatCurrency(totalRevenueValue)}</span>
                </div>
              </div>
              {combinedBreakdown ? (
                <div className="totals-breakdown">
                  <div className="breakdown-card">
                    <span className="breakdown-title">ASE</span>
                    <div className="breakdown-metric">
                      <span>Tickets</span>
                      <span>{formatNumber(combinedBreakdown.ase.ticketCount)}</span>
                    </div>
                    <div className="breakdown-metric">
                      <span>Fines</span>
                      <span>{formatCurrency(combinedBreakdown.ase.totalRevenue)}</span>
                    </div>
                    <span className="breakdown-footnote">Locations: {formatNumber(combinedBreakdown.ase.locationCount)}</span>
                  </div>
                  <div className="breakdown-card">
                    <span className="breakdown-title">RLC</span>
                    <div className="breakdown-metric">
                      <span>Tickets</span>
                      <span>{formatNumber(combinedBreakdown.redLight.ticketCount)}</span>
                    </div>
                    <div className="breakdown-metric">
                      <span>Fines</span>
                      <span>{formatCurrency(combinedBreakdown.redLight.totalRevenue)}</span>
                    </div>
                    <span className="breakdown-footnote">Locations: {formatNumber(combinedBreakdown.redLight.locationCount)}</span>
                  </div>
                </div>
              ) : null}
            </>
          ) : (
            <div className="totals-grid">
              {isParkingDataset ? (
                <div>
                  <span className="label">Tickets recorded</span>
                  <span className="value">{formatNumber(ticketsCountValue)}</span>
                </div>
              ) : (
                <>
                  <div>
                    <span className="label">{locationLabel}</span>
                    <span className="value">{formatNumber(locationCountValue)}</span>
                  </div>
                  <div>
                    <span className="label">Tickets issued</span>
                    <span className="value">{formatNumber(ticketsCountValue)}</span>
                  </div>
                </>
              )}
              <div>
                <span className="label">Total fines</span>
                <span className="value">{formatCurrency(totalRevenueValue)}</span>
              </div>
            </div>
          )}
          {filterLabel ? (
            <p className="filter-note">{filterLabel}</p>
          ) : null}
        </div>
      ) : null}

      {showViewport ? (
        <div className="viewport">
          <h3>{viewportTitle}</h3>
          {viewportSummary?.zoomRestricted && (
            <p className="hint">Zoom in to see street-level insights and the top 5 streets for this area.</p>
          )}

          {hasViewportData ? (
            <div className="totals-grid">
              <div>
                <span className="label">Tickets in view</span>
                <span className="value">{formatNumber(viewportSummary.visibleCount)}</span>
              </div>
              <div>
                <span className="label">Fines in view</span>
                <span className="value">{formatCurrency(viewportSummary.visibleRevenue || 0)}</span>
              </div>
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
