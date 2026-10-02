export function calendarTravelDescription(details, bookingNotes, scopedNotes, scopedLabel) {
  const sections = [];
  const booking = typeof bookingNotes === 'string' ? bookingNotes.trim() : '';
  const scoped = typeof scopedNotes === 'string' ? scopedNotes.trim() : '';
  if (booking) sections.push(`Booking Notes:\n${booking}`);
  if (scoped) sections.push(`${scopedLabel}:\n${scoped}`);
  if (!sections.length) return details;
  return [...sections, details].filter(Boolean).join('\n\n');
}
