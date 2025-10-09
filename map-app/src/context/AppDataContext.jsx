/* eslint-disable react-refresh/only-export-components */
import { createContext, useContext } from 'react';
import { CentrelineProvider } from './CentrelineContext.jsx';

const defaultData = {
  totals: {
    featureCount: 0,
    ticketCount: 0,
    totalRevenue: 0,
  },
  topStreets: [],
  topNeighbourhoods: [],
  datasets: {},
  yearlyMeta: {},
};

export const AppDataContext = createContext(defaultData);

export function AppDataProvider({ value, children }) {
  const mergedValue = value
    ? {
        ...defaultData,
        ...value,
        datasets: value.datasets || defaultData.datasets,
      }
    : defaultData;
  return (
    <AppDataContext.Provider value={mergedValue}>
      <CentrelineProvider>
        {children}
      </CentrelineProvider>
    </AppDataContext.Provider>
  );
}

export function useAppData() {
  return useContext(AppDataContext);
}
