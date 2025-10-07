export function normalizeStreetName(rawLocation) {
  if (!rawLocation) {
    return 'Unknown';
  }

  const text = String(rawLocation)
    .toUpperCase()
    .replace(/\b(NB|SB|EB|WB)\b/g, '')
    .replace(/\s+/g, ' ')
    .trim();

  const withoutLeadingNumber = text.replace(/^\d+[\s-]*/, '').trim();
  return withoutLeadingNumber.length > 0 ? withoutLeadingNumber : 'Unknown';
}
