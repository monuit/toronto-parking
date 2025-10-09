export const IS_DEV = typeof import.meta !== 'undefined'
  ? import.meta.env?.MODE !== 'production'
  : false;
