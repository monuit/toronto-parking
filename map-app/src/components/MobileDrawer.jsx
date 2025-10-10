import PropTypes from 'prop-types';
import { useEffect, useRef } from 'react';

export function MobileDrawer({ open, onClose, children }) {
  const startYRef = useRef(null);
  const deltaRef = useRef(0);

  useEffect(() => {
    if (!open) {
      return () => {};
    }

    const handleKeyDown = (event) => {
      if (event.key === 'Escape') {
        onClose();
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => {
      window.removeEventListener('keydown', handleKeyDown);
    };
  }, [open, onClose]);

  const handleTouchStart = (event) => {
    if (!open || !event.touches?.length) {
      return;
    }
    startYRef.current = event.touches[0].clientY;
    deltaRef.current = 0;
  };

  const handleTouchMove = (event) => {
    if (startYRef.current === null || !event.touches?.length) {
      return;
    }
    const currentY = event.touches[0].clientY;
    deltaRef.current = currentY - startYRef.current;
    if (deltaRef.current > 0) {
      event.currentTarget.style.transform = `translateY(${Math.min(deltaRef.current, 120)}px)`;
    }
  };

  const handleTouchEnd = (event) => {
    if (startYRef.current === null) {
      return;
    }
    event.currentTarget.style.transform = '';
    if (deltaRef.current > 80) {
      onClose();
    }
    startYRef.current = null;
    deltaRef.current = 0;
  };

  return (
    <div className={`mobile-drawer ${open ? 'mobile-drawer--open' : ''}`}>
      <button type="button" className="mobile-drawer__backdrop" onClick={onClose} aria-hidden={!open} />
      <div
        className="mobile-drawer__panel"
        role="dialog"
        aria-modal="true"
        onTouchStart={handleTouchStart}
        onTouchMove={handleTouchMove}
        onTouchEnd={handleTouchEnd}
      >
        <div className="mobile-drawer__handle" aria-hidden="true" />
        <div className="mobile-drawer__content">
          {children}
        </div>
      </div>
    </div>
  );
}

MobileDrawer.propTypes = {
  open: PropTypes.bool,
  onClose: PropTypes.func,
  children: PropTypes.node.isRequired,
};

MobileDrawer.defaultProps = {
  open: false,
  onClose: () => {},
};
