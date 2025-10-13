import { STYLE_CONSTANTS } from './mapSources.js';

export const DATASET_LEGEND_CONTENT = {
  parking_tickets: {
    title: 'Parking Tickets',
    items: [
      {
        label: 'Ticket location',
        color: STYLE_CONSTANTS.COLORS.TICKET_POINT,
      },
    ],
    note: 'Zoom in to reveal individual tickets plotted at their recorded location.',
  },
  red_light_locations: {
    title: 'Red Light Cameras',
    items: [
      {
        label: 'Camera site',
        color: STYLE_CONSTANTS.COLORS.RED_LIGHT_POINT,
        strokeColor: STYLE_CONSTANTS.COLORS.RED_LIGHT_STROKE,
      },
    ],
    note: 'Each point marks an active red light camera. Tap for annual charge totals.',
  },
  ase_locations: {
    title: 'Speed Enforcement Cameras',
    items: [
      {
        label: 'Camera site',
        color: STYLE_CONSTANTS.COLORS.ASE_POINT,
        strokeColor: STYLE_CONSTANTS.COLORS.ASE_STROKE,
      },
    ],
    note: 'These cameras monitor school/community safety zones. Tap for offence counts.',
  },
  cameras_combined: {
    title: 'Traffic Enforcement Cameras',
    items: [
      {
        label: 'ASE camera site',
        color: STYLE_CONSTANTS.COLORS.ASE_POINT,
        strokeColor: STYLE_CONSTANTS.COLORS.ASE_STROKE,
      },
      {
        label: 'Red-light camera site',
        color: STYLE_CONSTANTS.COLORS.RED_LIGHT_POINT,
        strokeColor: STYLE_CONSTANTS.COLORS.RED_LIGHT_STROKE,
      },
    ],
    note: 'Combined view of automated speed enforcement and red-light camera locations.',
  },
};
