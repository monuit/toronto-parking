/**
 * HowItWorks - Modal component explaining the dashboard
 */
import { useState } from 'react';
import '../styles/HowItWorks.css';

export function HowItWorks() {
  const [isOpen, setIsOpen] = useState(false);
  
  const openModal = () => setIsOpen(true);
  const closeModal = () => setIsOpen(false);
  
  return (
    <>
      <button className="how-it-works-button" onClick={openModal}>
        How does this work?
      </button>
      
      {isOpen && (
        <div className="modal-overlay" onClick={closeModal}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <button className="modal-close" onClick={closeModal}>×</button>
            <h2>How does this work?</h2>
            <div className="modal-body">
              <p><strong>This is gold!</strong> Toronto publishes every parking ticket as open data. You can download CSV files with everything: the location, the reason for the ticket, date and time, fine amount, and more.</p>
              
              <p><strong>The challenge?</strong> 26.5 million parking tickets from 2011-2024. That's a lot of data to make sense of.</p>
              
              <p>First, I downloaded all the monthly CSV files from Toronto's Open Data Portal. Each file contains thousands of tickets with street addresses like "123 YONGE ST" or "456 BLOOR ST W".</p>
              
              <p><strong>But addresses aren't map coordinates.</strong> To plot tickets on a map, I needed latitude and longitude for each location. This process is called geocoding.</p>
              
              <p>I extracted 676,782 unique addresses from all those tickets. Then I built a fast batch geocoding system that could process addresses at 3.7 queries per second. Running 24/7, it took about 2 days to geocode everything.</p>
              
              <p><strong>Here's what makes it efficient:</strong> Instead of geocoding each individual ticket (which would take months), I only geocoded unique addresses once. Then I matched all 26.5 million tickets back to those geocoded locations.</p>
              
              <p>The result? This interactive map shows you which streets and neighbourhoods get the most parking tickets, how much revenue each location generates, and patterns across the entire city.</p>
              
              <p><em>All data comes from the City of Toronto Open Data Portal and is updated as new monthly files are released.</em></p>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
