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

function stringList(value) {
  return Array.isArray(value)
    ? value.filter((entry) => typeof entry === 'string' && entry.trim())
    : [];
}

export function groundTravelOccurrencePeople(transport = {}, occurrence = 'pickup') {
  const prefix = occurrence === 'dropoff' ? 'drop_off' : 'pickup';
  const scopedPersonnel = transport[`${prefix}_personnel`]?.personnel_name;
  const scopedDrivers = transport[`${prefix}_drivers`];
  const scopedPassengers = transport[`${prefix}_passengers`];
  return {
    personnel: stringList(Array.isArray(scopedPersonnel)
      ? scopedPersonnel
      : transport.personnel?.personnel_name),
    drivers: stringList(Array.isArray(scopedDrivers) ? scopedDrivers : transport.drivers),
    passengers: stringList(Array.isArray(scopedPassengers) ? scopedPassengers : transport.passengers),
  };
}

export function groundStopCalendarDescription(notes, details) {
  const stopNotes = typeof notes === 'string' ? notes.trim() : '';
  if (!stopNotes) return details;
  return [stopNotes, details === 'Ground transportation details' ? '' : details]
    .filter(Boolean)
    .join('\n\n');
}
