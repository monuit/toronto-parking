import PropTypes from 'prop-types';

export function MobileAccordion({
  title,
  children,
  isMobile,
  defaultOpen = false,
  summaryAccessory = null,
  id = null,
}) {
  if (!isMobile) {
    return (
      <section className="panel-section" id={id}>
        {children}
      </section>
    );
  }

  if (!title) {
    return (
      <div className="mobile-accordion__content" id={id}>
        {children}
      </div>
    );
  }

  return (
    <details className="mobile-accordion" open={defaultOpen} id={id}>
      <summary>
        <span>{title}</span>
        {summaryAccessory ? <span className="mobile-accordion__accessory">{summaryAccessory}</span> : null}
      </summary>
      <div className="mobile-accordion__body">
        {children}
      </div>
    </details>
  );
}

MobileAccordion.propTypes = {
  title: PropTypes.node,
  children: PropTypes.node.isRequired,
  isMobile: PropTypes.bool,
  defaultOpen: PropTypes.bool,
  summaryAccessory: PropTypes.node,
  id: PropTypes.string,
};

MobileAccordion.defaultProps = {
  title: null,
  isMobile: false,
  defaultOpen: false,
  summaryAccessory: null,
  id: null,
};
