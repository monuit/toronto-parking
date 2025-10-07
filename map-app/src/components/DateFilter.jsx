/**
 * DateFilter Component
 * Apple-inspired glassmorphism date range filter
 */
import { useState, useEffect } from 'react';
import './DateFilter.css';

export function DateFilter({ onFilterChange }) {
  const currentYear = new Date().getFullYear();
  const [selectedYear, setSelectedYear] = useState(currentYear);
  const [selectedMonth, setSelectedMonth] = useState(null); // null = all months
  
  // Available years (2008-2024)
  const years = Array.from({ length: currentYear - 2008 + 1 }, (_, i) => 2008 + i);
  
  const months = [
    { value: 1, label: 'Jan' },
    { value: 2, label: 'Feb' },
    { value: 3, label: 'Mar' },
    { value: 4, label: 'Apr' },
    { value: 5, label: 'May' },
    { value: 6, label: 'Jun' },
    { value: 7, label: 'Jul' },
    { value: 8, label: 'Aug' },
    { value: 9, label: 'Sep' },
    { value: 10, label: 'Oct' },
    { value: 11, label: 'Nov' },
    { value: 12, label: 'Dec' },
  ];
  
  useEffect(() => {
    onFilterChange({ year: selectedYear, month: selectedMonth });
  }, [selectedYear, selectedMonth, onFilterChange]);
  
  const handleYearChange = (year) => {
    setSelectedYear(year);
  };
  
  const handleMonthClick = (month) => {
    setSelectedMonth(selectedMonth === month ? null : month);
  };
  
  return (
    <div className="date-filter glass-panel">
      <div className="filter-section">
        <label className="filter-label">Year</label>
        <select 
          className="year-select glass-input"
          value={selectedYear}
          onChange={(e) => handleYearChange(parseInt(e.target.value))}
        >
          {years.map(year => (
            <option key={year} value={year}>{year}</option>
          ))}
        </select>
      </div>
      
      <div className="filter-section">
        <label className="filter-label">Month</label>
        <div className="month-grid">
          <button
            className={`month-btn glass-btn ${selectedMonth === null ? 'active' : ''}`}
            onClick={() => setSelectedMonth(null)}
          >
            All
          </button>
          {months.map(({ value, label }) => (
            <button
              key={value}
              className={`month-btn glass-btn ${selectedMonth === value ? 'active' : ''}`}
              onClick={() => handleMonthClick(value)}
            >
              {label}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
