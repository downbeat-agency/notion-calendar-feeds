export function groundStopCalendarTitle(value) {
  return typeof value === 'string' && value.trim()
    ? value.trim()
    : 'Ground transportation';
}

export function groundStopCalendarDescription(notes, details) {
  const stopNotes = typeof notes === 'string' ? notes.trim() : '';
  if (!stopNotes) return details;
  return [stopNotes, details === 'Ground transportation details' ? '' : details]
    .filter(Boolean)
    .join('\n\n');
}
