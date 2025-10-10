import { useEffect, useState } from 'react';

const DEFAULT_BREAKPOINT = 768;

export function useBreakpoint(maxWidth = DEFAULT_BREAKPOINT) {
  const [isMatch, setIsMatch] = useState(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
      return false;
    }
    return window.matchMedia(`(max-width: ${maxWidth}px)`).matches;
  });

  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
      return () => {};
    }

    const mediaQuery = window.matchMedia(`(max-width: ${maxWidth}px)`);
    const handleChange = (event) => {
      setIsMatch(event.matches);
    };

    mediaQuery.addEventListener('change', handleChange);
    setIsMatch(mediaQuery.matches);

    return () => {
      mediaQuery.removeEventListener('change', handleChange);
    };
  }, [maxWidth]);

  return isMatch;
}

export function useTouchDevice() {
  const [isTouch, setIsTouch] = useState(() => {
    if (typeof window === 'undefined') {
      return false;
    }
    return window.matchMedia('(pointer: coarse)').matches;
  });

  useEffect(() => {
    if (typeof window === 'undefined') {
      return () => {};
    }

    const coarseQuery = window.matchMedia('(pointer: coarse)');
    const handleChange = (event) => {
      setIsTouch(event.matches);
    };

    coarseQuery.addEventListener('change', handleChange);
    setIsTouch(coarseQuery.matches);

    return () => {
      coarseQuery.removeEventListener('change', handleChange);
    };
  }, []);

  return isTouch;
}
