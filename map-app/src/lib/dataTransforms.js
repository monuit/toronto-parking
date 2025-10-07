/**
 * Data transformation utilities
 * Single responsibility: normalize and transform geospatial data
 */

/**
 * Calculate tickets per capita for choropleth visualization
 */
export function calculateTicketsPerCapita(neighbourhoods, ticketCounts) {
  return neighbourhoods.features.map(feature => {
    const name = feature.properties.name || feature.properties.AREA_NAME;
    const population = feature.properties.population || 10000; // fallback
    const tickets = ticketCounts[name] || 0;
    
    return {
      ...feature,
      properties: {
        ...feature.properties,
        ticketCount: tickets,
        ticketsPerCapita: (tickets / population) * 1000,
        normalizedName: name
      }
    };
  });
}

/**
 * Aggregate ticket statistics by neighbourhood
 */
export function aggregateByNeighbourhood(tickets) {
  const stats = {};
  
  tickets.forEach(ticket => {
    const hood = ticket.neighbourhood || 'Unknown';
    if (!stats[hood]) {
      stats[hood] = {
        count: 0,
        totalFines: 0,
        infractions: {}
      };
    }
    
    stats[hood].count++;
    stats[hood].totalFines += ticket.fine || 0;
    
    const infraction = ticket.infraction_code || 'Unknown';
    stats[hood].infractions[infraction] = (stats[hood].infractions[infraction] || 0) + 1;
  });
  
  return stats;
}

/**
 * Aggregate ticket statistics by officer
 */
export function aggregateByOfficer(tickets) {
  const stats = {};
  
  tickets.forEach(ticket => {
    // Officer ID might be derived from tag number patterns or other fields
    const officer = ticket.officer_id || 'Unknown';
    if (!stats[officer]) {
      stats[officer] = {
        ticketCount: 0,
        totalRevenue: 0,
        topInfraction: null,
        infractionCounts: {}
      };
    }
    
    stats[officer].ticketCount++;
    stats[officer].totalRevenue += ticket.fine || 0;
    
    const infraction = ticket.infraction_code || 'Unknown';
    stats[officer].infractionCounts[infraction] = 
      (stats[officer].infractionCounts[infraction] || 0) + 1;
  });
  
  // Calculate top infraction for each officer
  Object.keys(stats).forEach(officer => {
    const infractions = stats[officer].infractionCounts;
    const sorted = Object.entries(infractions).sort((a, b) => b[1] - a[1]);
    if (sorted.length > 0) {
      stats[officer].topInfraction = sorted[0][0];
    }
  });
  
  return stats;
}

/**
 * Format currency for display
 */
export function formatCurrency(amount) {
  return new Intl.NumberFormat('en-CA', {
    style: 'currency',
    currency: 'CAD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0
  }).format(amount);
}

/**
 * Format large numbers with commas
 */
export function formatNumber(num) {
  return new Intl.NumberFormat('en-CA').format(num);
}

/**
 * Get color for choropleth based on value
 */
export function getChoroplethColor(value, stops) {
  for (let i = stops.length - 1; i >= 0; i--) {
    if (value >= stops[i][0]) {
      return stops[i][1];
    }
  }
  return stops[0][1];
}
