import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import PropTypes from 'prop-types';
import { useCentrelineLookup } from '../context/CentrelineContext.jsx';
import { formatNumber } from '../lib/dataTransforms.js';

const NORMALISE_PATTERN = /\s+/g;
const MAX_RESULTS = 8;

function normalise(text) {
  if (!text) {
    return '';
  }
  return String(text).toUpperCase().replace(NORMALISE_PATTERN, ' ').trim();
}

function formatTicketCount(value) {
  const count = Number(value);
  if (!Number.isFinite(count) || count <= 0) {
    return 'No tickets';
  }
  if (count === 1) {
    return '1 ticket';
  }
  return `${formatNumber(count)} tickets`;
}

export function StreetSearch({ onSelect, variant }) {
  const { streetSummaries, isLoading, error } = useCentrelineLookup();
  const [query, setQuery] = useState('');
  const [focused, setFocused] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(-1);
  const containerRef = useRef(null);
  const inputRef = useRef(null);

  const baseId = variant === 'mobile' ? 'street-search-mobile' : 'street-search';
  const inputId = `${baseId}-input`;
  const listboxId = `${baseId}-results`;

  const searchIndex = useMemo(() => {
    return streetSummaries.map((summary) => ({
      summary,
      label: summary?.street || summary?.name || '',
      key: normalise(summary?.street || summary?.name || ''),
    }));
  }, [streetSummaries]);

  const suggestions = useMemo(() => {
    const normalisedQuery = normalise(query);
    if (normalisedQuery.length < 2) {
      return [];
    }
    const startsWith = [];
    const contains = [];
    for (const entry of searchIndex) {
      if (!entry.key) {
        continue;
      }
      if (entry.key.startsWith(normalisedQuery)) {
        startsWith.push(entry.summary);
      } else if (entry.key.includes(normalisedQuery)) {
        contains.push(entry.summary);
      }
      if (startsWith.length + contains.length >= MAX_RESULTS) {
        break;
      }
    }
    const combined = startsWith.concat(contains);
    if (combined.length > MAX_RESULTS) {
      combined.length = MAX_RESULTS;
    }
    return combined;
  }, [query, searchIndex]);

  const hasError = Boolean(error);
  const disabled = isLoading || hasError;

  const closeSuggestions = useCallback(() => {
    setFocused(false);
    setHighlightedIndex(-1);
  }, []);

  const handleSelect = useCallback((summary) => {
    if (!summary) {
      return;
    }
    setQuery(summary.street || summary.name || '');
    closeSuggestions();
    if (typeof onSelect === 'function') {
      onSelect(summary);
    }
  }, [closeSuggestions, onSelect]);

  const handleInputChange = useCallback((event) => {
    setQuery(event.target.value);
    setHighlightedIndex(-1);
  }, []);

  const handleKeyDown = useCallback((event) => {
    if (!suggestions.length) {
      return;
    }
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      setHighlightedIndex((prev) => {
        const next = prev + 1;
        if (next >= suggestions.length) {
          return 0;
        }
        return next;
      });
      return;
    }
    if (event.key === 'ArrowUp') {
      event.preventDefault();
      setHighlightedIndex((prev) => {
        if (prev <= 0) {
          return suggestions.length - 1;
        }
        return prev - 1;
      });
      return;
    }
    if (event.key === 'Enter') {
      const selected = highlightedIndex >= 0 ? suggestions[highlightedIndex] : suggestions[0];
      if (selected) {
        event.preventDefault();
        handleSelect(selected);
      }
    } else if (event.key === 'Escape') {
      if (query.length === 0) {
        closeSuggestions();
      } else {
        setQuery('');
        setHighlightedIndex(-1);
      }
    }
  }, [closeSuggestions, handleSelect, highlightedIndex, query.length, suggestions]);

  useEffect(() => {
    function handleClickOutside(event) {
      if (!containerRef.current) {
        return;
      }
      if (!containerRef.current.contains(event.target)) {
        closeSuggestions();
      }
    }

    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [closeSuggestions]);

  const statusLabel = useMemo(() => {
    if (isLoading) {
      return 'Loading streets…';
    }
    if (hasError) {
      return 'Street search unavailable';
    }
    return 'Search for a Toronto street';
  }, [hasError, isLoading]);

  return (
    <div
      ref={containerRef}
      className={`street-search street-search--${variant}`}
      aria-expanded={focused && suggestions.length > 0}
    >
      <label className="street-search__label" htmlFor={inputId}>
        <span className="street-search__caption">Street search</span>
        <input
          id={inputId}
          ref={inputRef}
          type="search"
          inputMode="search"
          autoComplete="off"
          placeholder="Find a street"
          className="street-search__input"
          value={query}
          disabled={disabled}
          onFocus={() => setFocused(true)}
          onChange={handleInputChange}
          onKeyDown={handleKeyDown}
          aria-label="Search for a street"
          aria-autocomplete="list"
          aria-controls={listboxId}
          aria-activedescendant={highlightedIndex >= 0 ? `${listboxId}-option-${highlightedIndex}` : undefined}
          aria-describedby={`${baseId}-status`}
        />
      </label>
      <div id={`${baseId}-status`} className="street-search__status" aria-live="polite">
        {statusLabel}
      </div>
      {focused && suggestions.length > 0 && (
        <ul id={listboxId} role="listbox" className="street-search__results">
          {suggestions.map((summary, index) => {
            const label = summary.street || summary.name || 'Unknown street';
            const ticketCount = formatTicketCount(summary.ticketCount ?? summary.count);
            const isActive = index === highlightedIndex;
            return (
              <li
                key={`${label}-${summary.ticketCount ?? summary.centrelineIds?.[0] ?? index}`}
                id={`${listboxId}-option-${index}`}
                role="option"
                aria-selected={isActive}
                className={`street-search__option${isActive ? ' street-search__option--active' : ''}`}
              >
                <button
                  type="button"
                  className="street-search__option-button"
                  onMouseEnter={() => setHighlightedIndex(index)}
                  onFocus={() => setHighlightedIndex(index)}
                  onClick={() => handleSelect(summary)}
                >
                  <span className="street-search__option-label">{label}</span>
                  <span className="street-search__option-meta">{ticketCount}</span>
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}

StreetSearch.propTypes = {
  onSelect: PropTypes.func.isRequired,
  variant: PropTypes.oneOf(['sidebar', 'mobile']),
};

StreetSearch.defaultProps = {
  variant: 'sidebar',
};

export default StreetSearch;
