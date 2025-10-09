import '../styles/Controls.css';

export function YearFilter({
  years = [],
  value = null,
  onChange,
  disabled = false,
}) {
  const sortedYears = Array.isArray(years)
    ? [...new Set(years.map((year) => Number.parseInt(year, 10)).filter((year) => Number.isFinite(year)))].sort((a, b) => b - a)
    : [];

  const handleChange = (event) => {
    const raw = event.target.value;
    if (raw === 'all') {
      onChange?.(null);
      return;
    }
    const parsed = Number.parseInt(raw, 10);
    onChange?.(Number.isFinite(parsed) ? parsed : null);
  };

  return (
    <div className="year-filter">
      <label htmlFor="year-filter-select">Year</label>
      <select
        id="year-filter-select"
        className="year-filter__select"
        onChange={handleChange}
        value={value === null ? 'all' : String(value)}
        disabled={disabled}
      >
        <option value="all">Show all</option>
        {sortedYears.map((yearOption) => (
          <option key={yearOption} value={yearOption}>
            {yearOption}
          </option>
        ))}
      </select>
    </div>
  );
}
