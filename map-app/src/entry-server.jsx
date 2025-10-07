import React from 'react';
import { renderToString } from 'react-dom/server';
import App from './App.jsx';

export function render(url, context = {}) {
  const { initialData = null } = context;
  const appHtml = renderToString(
    <App initialData={initialData} isServer />
  );

  return { appHtml };
}
