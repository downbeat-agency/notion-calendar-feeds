export function groundStopCalendarTitle(value) {
  return typeof value === 'string' && value.trim()
    ? value.trim()
    : 'Ground transportation';
}

export function groundTravelDropOffPresentation(transport = {}) {
  const address = typeof transport.drop_off_address === 'string'
    ? transport.drop_off_address.trim()
    : '';
  const name = typeof transport.drop_off_name === 'string'
    ? transport.drop_off_name.trim()
    : '';
  return {
    title: 'Drop Off',
    location: address || name,
  };
}

export function groundStopCalendarDescription(notes, details) {
  const stopNotes = typeof notes === 'string' ? notes.trim() : '';
  if (!stopNotes) return details;
  return [stopNotes, details === 'Ground transportation details' ? '' : details]
    .filter(Boolean)
    .join('\n\n');
}
