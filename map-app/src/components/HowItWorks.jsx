/**
 * HowItWorks - Modal component explaining the dashboard
 */
import { useState } from 'react';
import '../styles/HowItWorks.css';

const TABS = [
  { id: 'parking', label: 'Parking Tickets' },
  { id: 'red_light', label: 'Red Light Cameras' },
  { id: 'ase', label: 'Automated Speed Enforcement' },
];

const KOFI_URL = 'https://ko-fi.com/Z8Z51MBSO5';

function KofiButton({ active }) {
  if (!active) {
    return null;
  }

  return (
    <div className="modal-kofi">
      <a
        className="modal-kofi-button"
        href={KOFI_URL}
        target="_blank"
        rel="noopener noreferrer"
      >
        <span aria-hidden="true">☕</span>
        <span>Support Moe on Ko-fi</span>
      </a>
    </div>
  );
}

export function HowItWorks() {
  const [isOpen, setIsOpen] = useState(false);
  const [activeTab, setActiveTab] = useState('parking');

  const openModal = () => {
    setActiveTab('parking');
    setIsOpen(true);
  };
  const closeModal = () => setIsOpen(false);

  const renderContent = () => {
    switch (activeTab) {
      case 'red_light':
        return (
          <div className="modal-section">
            <h3>🚦 Red Light Camera (RLC)</h3>
            <p>
              Toronto operates <strong>319 red light camera locations</strong> at intersections. These cameras photograph vehicles that
              run red lights and issue $325 fines.
            </p>
            <p>
              This dataset spans <strong>18 years</strong> (2007-2024) with over <strong>1&nbsp;million tickets</strong> issued. Like the ASE feed, it is
              disaggregated&mdash;each location has yearly counts with no pre-calculated totals.
            </p>
            <p><strong>How I processed it:</strong></p>
            <ol>
              <li>Downloaded the charges workbook from Toronto&apos;s Open Data Portal (319 intersection records in Excel).</li>
              <li>Skipped the four metadata rows at the top of the file.</li>
              <li>Parsed the <strong>18 yearly columns</strong> (2007-2024) to calculate per-location ticket totals.</li>
              <li>Merged those totals with the locations dataset to restore intersection context and ward information.</li>
              <li>Estimated fines using the standard <strong>$325 per ticket</strong> red light charge.</li>
            </ol>
            <p>
              The result surfaces the highest-violation intersections, the wards demanding the most enforcement, and an estimated
              <strong> $331.9&nbsp;million</strong> in fines over 18 years.
            </p>
            <p>
              Unlike parking tickets, the City only publishes red light charges as semi-annual aggregates, so the work here rebuilds the
              totals people actually want to analyze. You can scroll to find out which intersection has the highest amounts.
            </p>
            <p>
              Questions or suggestions? Reach out on <a href="https://x.com/moevals" target="_blank" rel="noreferrer">X</a> or email
              {' '}<a href="mailto:hi@monuit.dev">hi@monuit.dev</a>.
            </p>
            <p>
              By <a href="https://monuit.dev" target="_blank" rel="noreferrer">Moe</a>. Not affiliated with the Toronto city government.
            </p>
            <KofiButton active={activeTab === 'red_light'} />
          </div>
        );
      case 'ase':
        return (
          <div className="modal-section">
            <h3>🚗 Automated Speed Enforcement (ASE)</h3>
            <p>
              Toronto runs <strong>199 speed camera locations</strong> (as of current). Each camera photographs vehicles that exceed the posted
              speed limit and mails a ticket to the owner.
            </p>
            <p>
              <strong>62 months</strong> (July 2020&ndash;August 2025) of monthly data with more than <strong>2&nbsp;million tickets</strong> issued (wild amount).
              Every record includes a month-by-month breakdown, but totals are missing.
            </p>
            <p><strong>How I processed it:</strong></p>
            <ol>
              <li>Fetched the Excel charges dataset (631 location records) from Toronto&apos;s Open Data Portal.</li>
              <li>Converted the <strong>62 monthly columns</strong> into numeric totals for each site.</li>
              <li>Merged the results with the locations feed to recover ward details and precise coordinates.</li>
              <li>Estimated fines at <strong>$50 per ticket</strong> (a conservative average&mdash;actual fines range from $5 to $718).</li>
            </ol>
            <p>
              This adds leaderboards for the busiest cameras, ward summaries, and an estimated <strong>$102.7&nbsp;million</strong> in revenue. If you
              assume a more realistic $180&ndash;$200 per ticket (20&nbsp;km/h over), the revenue would push toward ~$400&nbsp;million.
            </p>
            <p>
              Questions or suggestions? Reach out on <a href="https://x.com/moevals" target="_blank" rel="noreferrer">X</a> or email
              {' '}<a href="mailto:hi@monuit.dev">hi@monuit.dev</a>.
            </p>
            <p>
              By <a href="https://monuit.dev" target="_blank" rel="noreferrer">Moe</a>. Not affiliated with the Toronto city government.
            </p>
            <KofiButton active={activeTab === 'ase'} />
          </div>
        );
      case 'parking':
      default:
        return (
          <div className="modal-section">
            <p>
              Toronto publishes every parking ticket as open data. You can download CSV files with everything: the location, the reason,
              date and time, fine amount, and more.
            </p>
            <p>
              Roughly <strong>40&nbsp;million</strong> parking tickets from 2008-2024. That&apos;s a lot to wrangle, especially because the older exports are
              pretty messy.
            </p>
            <p>
              I started by pulling every monthly CSV from the City&apos;s Open Data Portal. Each contains thousands of tickets tied to street
              addresses like &quot;123 YONGE ST&quot; or &quot;456 BLOOR ST W&quot;.
            </p>
            <p>
              Addresses aren&apos;t map coordinates, though. To put tickets on a map you need latitude/longitude pairs. So I geocoded roughly
              <strong>750k unique addresses</strong> gathered from those tickets.
            </p>
            <p>
              The result is this map&mdash;you can explore which streets rack up the most tickets, how much revenue each spot generates, and how
              patterns change over time. You can search for a street at the top left, or find a street individually by scrolling. Filter by
              year to see the City&apos;s yearly haul.
            </p>
            <p>
              Could you get near real-time ticket data? Technically, yes. It is by no means difficult, but I don&apos;t imagine you can get
              anything from these tickets, other than people&apos;s names + license plates. <del>By doing so, I would be breaking a few laws, so I
              choose not to do it.</del>
            </p>
            <p>
              I also layered in the Red Light Camera and Automated Speed Enforcement datasets (toggle them in the top-left) so you can see what
              those programs earn.
            </p>
            <p>
              Questions or suggestions? Reach out on <a href="https://x.com/moevals" target="_blank" rel="noreferrer">X</a> or email
              {' '}<a href="mailto:hi@monuit.dev">hi@monuit.dev</a>.
            </p>
            <p>
              By <a href="https://monuit.dev" target="_blank" rel="noreferrer">Moe</a>. Not affiliated with the Toronto city government.
            </p>
            <KofiButton active={activeTab === 'parking'} />
          </div>
        );
    }
  };
  
  return (
    <>
      <button className="how-it-works-button" onClick={openModal}>
        How does this work?
      </button>
      
      {isOpen && (
        <div className="modal-overlay" onClick={closeModal}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-traffic-lights">
              <button
                type="button"
                className="modal-light modal-light--red"
                aria-label="Close modal"
                onClick={closeModal}
              />
              <button
                type="button"
                className="modal-light modal-light--yellow"
                aria-label="Minimize"
                disabled
              />
              <button
                type="button"
                className="modal-light modal-light--green"
                aria-label="Maximize"
                disabled
              />
            </div>
            <h2>How does this work?</h2>
            <div className="modal-body">
              <div className="modal-tabs">
                {TABS.map((tab) => (
                  <button
                    key={tab.id}
                    type="button"
                    className={`modal-tab ${activeTab === tab.id ? 'modal-tab--active' : ''}`}
                    onClick={() => setActiveTab(tab.id)}
                  >
                    {tab.label}
                  </button>
                ))}
              </div>
              {renderContent()}
            </div>
          </div>
        </div>
      )}
    </>
  );
}
