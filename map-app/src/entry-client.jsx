import { StrictMode } from 'react';
import { hydrateRoot, createRoot } from 'react-dom/client';
import App from './App.jsx';
import './index.css';

const container = document.getElementById('root');
const initialData = window.__INITIAL_DATA__ || null;

if (container.hasChildNodes()) {
  hydrateRoot(
    container,
    <StrictMode>
      <App initialData={initialData} />
    </StrictMode>,
  );
} else {
  createRoot(container).render(
    <StrictMode>
      <App initialData={initialData} />
    </StrictMode>,
  );
}
