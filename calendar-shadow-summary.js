function emptyMetrics() {
  return {
    comparisons: 0,
    matches: 0,
    mismatches: 0,
    semanticMatches: 0,
    semanticMismatches: 0,
    errors: 0,
    baselineUnavailable: 0,
    notionEvents: 0,
    postgresEvents: 0,
    notionByType: {},
    postgresByType: {},
    missingFromPostgres: 0,
    extraInPostgres: 0,
    pairedEvents: 0,
    pairsByMethod: {},
    exactPairedEvents: 0,
    semanticPairedEvents: 0,
    unpairedNotion: 0,
    unpairedPostgres: 0,
    unpairedNotionByType: {},
    unpairedPostgresByType: {},
    fieldMismatchCounts: {},
    semanticFieldMismatchCounts: {},
    fieldMismatchCountsByType: {},
    semanticFieldMismatchCountsByType: {},
  };
}

function addNumberMap(target, source) {
  for (const [key, count] of Object.entries(source || {})) {
    target[key] = (target[key] || 0) + (Number(count) || 0);
  }
}

function addNestedNumberMap(target, source) {
  for (const [group, counts] of Object.entries(source || {})) {
    const targetGroup = target[group] || {};
    addNumberMap(targetGroup, counts);
    target[group] = targetGroup;
  }
}

function addEntry(target, entry, baselineUnavailableCodes) {
  target.comparisons += 1;
  if (entry?.errorCode || !entry?.comparison) {
    if (baselineUnavailableCodes.has(entry?.errorCode)) target.baselineUnavailable += 1;
    else target.errors += 1;
    return;
  }

  const comparison = entry.comparison;
  target[comparison.matches ? 'matches' : 'mismatches'] += 1;
  target[comparison.semanticMatches ? 'semanticMatches' : 'semanticMismatches'] += 1;
  target.notionEvents += Number(comparison.notionCount) || 0;
  target.postgresEvents += Number(comparison.postgresCount) || 0;
  addNumberMap(target.notionByType, comparison.notionByType);
  addNumberMap(target.postgresByType, comparison.postgresByType);
  target.missingFromPostgres += Number(comparison.missingFromPostgresCount) || 0;
  target.extraInPostgres += Number(comparison.extraInPostgresCount) || 0;
  target.pairedEvents += Number(comparison.pairedCount) || 0;
  target.exactPairedEvents += Number(comparison.exactPairCount) || 0;
  target.semanticPairedEvents += Number(comparison.semanticPairCount) || 0;
  target.unpairedNotion += Number(comparison.unpairedNotionCount) || 0;
  target.unpairedPostgres += Number(comparison.unpairedPostgresCount) || 0;
  addNumberMap(target.pairsByMethod, comparison.pairsByMethod);
  addNumberMap(target.unpairedNotionByType, comparison.unpairedNotionByType);
  addNumberMap(target.unpairedPostgresByType, comparison.unpairedPostgresByType);
  addNumberMap(target.fieldMismatchCounts, comparison.fieldMismatchCounts);
  addNumberMap(target.semanticFieldMismatchCounts, comparison.semanticFieldMismatchCounts);
  addNestedNumberMap(target.fieldMismatchCountsByType, comparison.fieldMismatchCountsByType);
  addNestedNumberMap(
    target.semanticFieldMismatchCountsByType,
    comparison.semanticFieldMismatchCountsByType
  );
}

export function summarizeCalendarShadowEntries(
  entries = [],
  { baselineUnavailableCodes = new Set() } = {}
) {
  const summary = {
    ...emptyMetrics(),
    byKind: {},
  };
  for (const entry of Array.isArray(entries) ? entries : []) {
    const kind = String(entry?.kind || 'unknown');
    const kindSummary = summary.byKind[kind] || emptyMetrics();
    addEntry(summary, entry, baselineUnavailableCodes);
    addEntry(kindSummary, entry, baselineUnavailableCodes);
    summary.byKind[kind] = kindSummary;
  }
  return summary;
}
