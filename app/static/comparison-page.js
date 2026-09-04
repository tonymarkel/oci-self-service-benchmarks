const comparisonPageHasDocument = typeof document !== 'undefined';
const comparisonPageView = typeof module !== 'undefined' && module.exports
    ? require('./comparison-view.js')
    : window.ComparisonView;

const comparisonPageHeading = comparisonPageHasDocument ? document.querySelector('#comparisonPageHeading') : null;
const comparisonPageStatus = comparisonPageHasDocument ? document.querySelector('#comparisonPageStatus') : null;
const comparisonPageContext = comparisonPageHasDocument ? document.querySelector('#comparisonPageContext') : null;
const comparisonPageResult = comparisonPageHasDocument ? document.querySelector('#comparisonPageResult') : null;
const comparisonBaseline = comparisonPageHasDocument ? document.querySelector('#comparisonBaseline') : null;
const comparisonRunLegend = comparisonPageHasDocument ? document.querySelector('#comparisonRunLegend') : null;
const comparisonMetricIndex = comparisonPageHasDocument ? document.querySelector('#comparisonMetricIndex') : null;
const comparisonCharts = comparisonPageHasDocument ? document.querySelector('#comparisonCharts') : null;
const comparisonPageWarnings = comparisonPageHasDocument ? document.querySelector('#comparisonPageWarnings') : null;
const comparisonPageWarningsList = comparisonPageHasDocument ? document.querySelector('#comparisonPageWarningsList') : null;
const comparisonExcluded = comparisonPageHasDocument ? document.querySelector('#comparisonExcluded') : null;
const comparisonExcludedList = comparisonPageHasDocument ? document.querySelector('#comparisonExcludedList') : null;
const comparisonPageBack = comparisonPageHasDocument ? document.querySelector('.comparison-page-back') : null;

let comparisonPageData = null;
let comparisonPageRunOrder = [];
let comparisonPageInitialMetric = '';
let comparisonPageReturnTo = '';

function parseComparisonState(search) {
    const params = new URLSearchParams(search || '');
    const rawRunIds = params.get('compare') || params.get('runs') || '';
    return {
        runIds: rawRunIds.split(',').filter(Boolean).slice(0, 9),
        resultId: params.get('result') || '',
        baselineId: params.get('baseline') || '',
        metricId: params.get('metric') || '',
        returnTo: params.get('return') || '',
    };
}

function validComparisonRunIds(runIds) {
    return Array.isArray(runIds)
        && runIds.length >= 2
        && runIds.length <= 8
        && new Set(runIds).size === runIds.length
        && runIds.every(id => /^[0-9a-f]{12}$/.test(String(id)));
}

function exclusionsForResult(data, resultId) {
    const excluded = Array.isArray(data?.excluded) ? data.excluded : [];
    if (!resultId) return excluded;
    return excluded.filter(item => String(item?.result_id || '') === String(resultId));
}

function metricRunIds(charts) {
    const ids = new Set();
    (charts || []).forEach(chart => {
        (chart?.values || []).forEach(value => {
            if (value?.run_id) ids.add(String(value.run_id));
        });
    });
    return ids;
}

function metricCoverage(charts, runIds) {
    const counts = Object.fromEntries((runIds || []).map(id => [String(id), 0]));
    (charts || []).forEach(chart => {
        const present = new Set(
            (chart?.values || [])
                .filter(value => comparisonPageView.isFiniteMetric(value?.value))
                .map(value => String(value.run_id)),
        );
        Object.keys(counts).forEach(id => {
            if (present.has(id)) counts[id] += 1;
        });
    });
    return counts;
}

function safeHistoryReturnPath(value) {
    if (!value) return '';
    try {
        const parsed = new URL(String(value), 'http://comparison.local');
        if (parsed.origin !== 'http://comparison.local' || parsed.pathname !== '/history') return '';
        return `${parsed.pathname}${parsed.search}${parsed.hash}`;
    } catch {
        return '';
    }
}

function comparisonPageRun(id) {
    return comparisonPageView.resolveRunFromLists(
        id,
        comparisonPageData?.runs,
    );
}

function selectedResultCharts() {
    return comparisonPageView.allChartsForResult(
        comparisonPageData,
        comparisonPageResult.value,
    );
}

function selectedResultLabel() {
    const selected = comparisonPageView.resultOptions(comparisonPageData)
        .find(result => result.id === comparisonPageResult.value);
    return selected?.label || comparisonPageResult.value || 'benchmark';
}

function setComparisonPageFailure(message) {
    comparisonPageStatus.textContent = message;
    comparisonPageStatus.classList.add('is-error');
    comparisonPageContext.textContent = 'Return to Previous runs and select between two and eight completed runs.';
    comparisonCharts.replaceChildren();
    const failure = document.createElement('div');
    failure.className = 'empty-state';
    failure.textContent = message;
    comparisonCharts.append(failure);
}

function populateResultOptions(preferredResultId) {
    const options = comparisonPageView.resultOptions(comparisonPageData);
    comparisonPageResult.replaceChildren();
    options.forEach(result => {
        const option = document.createElement('option');
        option.value = result.id;
        option.textContent = result.label;
        comparisonPageResult.append(option);
    });
    const primary = (comparisonPageData?.charts || []).find(chart => chart.primary)
        || comparisonPageData?.charts?.[0];
    const preferred = options.find(result => result.id === preferredResultId);
    comparisonPageResult.value = preferred?.id || String(primary?.result_id || options[0]?.id || '');
    comparisonPageResult.disabled = options.length < 2;
}

function populateBaselineOptions(preferredBaselineId) {
    comparisonBaseline.replaceChildren();
    const comparableCharts = selectedResultCharts().filter(chart => !chart.unavailable);
    const coverage = metricCoverage(comparableCharts, comparisonPageRunOrder);
    comparisonPageRunOrder.forEach(id => {
        const run = comparisonPageRun(id);
        const option = document.createElement('option');
        option.value = id;
        const available = coverage[id] || 0;
        const coverageLabel = comparableCharts.length && available < comparableCharts.length
            ? ` · ${available}/${comparableCharts.length} metrics`
            : '';
        option.textContent = `${comparisonPageView.runDisplayName(run)} · ${id}${coverageLabel}`;
        comparisonBaseline.append(option);
    });
    const defaultBaseline = comparisonPageRunOrder.find(
        id => comparableCharts.length && coverage[id] === comparableCharts.length,
    ) || comparisonPageRunOrder.find(id => coverage[id] > 0)
        || comparisonPageRunOrder[0];
    const preferred = comparisonPageRunOrder.includes(preferredBaselineId)
        ? preferredBaselineId
        : defaultBaseline;
    comparisonBaseline.value = preferred || '';
    comparisonBaseline.disabled = comparisonPageRunOrder.length < 2;
}

function updateComparisonPageLinks() {
    if (!comparisonPageData) return;
    const params = new URLSearchParams({
        compare: comparisonPageRunOrder.join(','),
        result: comparisonPageResult.value,
        baseline: comparisonBaseline.value,
    });
    if (comparisonPageReturnTo) params.set('return', comparisonPageReturnTo);
    const hash = window.location.hash || '';
    window.history.replaceState(null, '', `/comparison?${params}${hash}`);
    const historyRunOrder = [
        comparisonBaseline.value,
        ...comparisonPageRunOrder.filter(id => id !== comparisonBaseline.value),
    ];
    const returnPath = safeHistoryReturnPath(comparisonPageReturnTo);
    const returnUrl = new URL(returnPath || '/history?completed=1', window.location.origin);
    const previousResult = returnUrl.searchParams.get('result');
    returnUrl.searchParams.set('compare', historyRunOrder.join(','));
    returnUrl.searchParams.set('result', comparisonPageResult.value);
    const hashChart = selectedResultCharts().find(
        chart => `#${comparisonPageView.metricAnchorId(chart)}` === window.location.hash,
    );
    if (hashChart) returnUrl.searchParams.set('metric', comparisonPageView.chartKey(hashChart));
    else if (previousResult !== comparisonPageResult.value) {
        returnUrl.searchParams.delete('metric');
    }
    comparisonPageBack.href = `${returnUrl.pathname}${returnUrl.search}${returnUrl.hash}`;
}

function renderRunLegend(charts) {
    const comparableCharts = charts.filter(chart => !chart.unavailable);
    const represented = metricRunIds(comparableCharts);
    const coverage = metricCoverage(comparableCharts, comparisonPageRunOrder);
    comparisonRunLegend.replaceChildren();
    comparisonPageRunOrder.forEach(id => {
        const run = comparisonPageRun(id);
        const item = document.createElement('div');
        item.className = 'comparison-run-legend-item';
        if (id === comparisonBaseline.value) item.classList.add('is-baseline');
        if (!represented.has(id)) item.classList.add('is-excluded');
        const swatch = document.createElement('span');
        swatch.className = 'comparison-run-swatch';
        swatch.style.backgroundColor = comparisonPageView.runColor(id, comparisonPageRunOrder);
        const copy = document.createElement('span');
        const label = document.createElement('span');
        label.className = 'comparison-run-legend-label';
        label.textContent = comparisonPageView.runDisplayName(run);
        const meta = document.createElement('span');
        meta.className = 'comparison-run-legend-meta';
        const roles = [];
        if (id === comparisonBaseline.value) roles.push('Baseline');
        if (!represented.has(id)) roles.push('Not represented');
        else if (coverage[id] < comparableCharts.length) {
            roles.push(`${coverage[id]}/${comparableCharts.length} metrics`);
        }
        const role = roles.length ? ` · ${roles.join(' · ')}` : '';
        meta.textContent = `${id} · ${comparisonPageView.environmentText(run)}${role}`;
        copy.append(label, meta);
        item.append(swatch, copy);
        comparisonRunLegend.append(item);
    });
    comparisonRunLegend.hidden = comparisonPageRunOrder.length === 0;
}

function renderProvenanceWarnings(charts) {
    const warnings = comparisonPageView.provenanceWarnings(charts);
    comparisonPageWarningsList.replaceChildren();
    warnings.forEach(warning => {
        const item = document.createElement('li');
        item.textContent = warning;
        comparisonPageWarningsList.append(item);
    });
    comparisonPageWarnings.hidden = warnings.length === 0;
}

function renderMetricIndex(charts) {
    comparisonMetricIndex.replaceChildren();
    charts.forEach((chart, index) => {
        const link = document.createElement('a');
        link.href = `#${comparisonPageView.metricAnchorId(chart, index)}`;
        link.textContent = chart.label;
        comparisonMetricIndex.append(link);
    });
    comparisonMetricIndex.hidden = charts.length < 2;
}

function metricContext(chart, baseline, baselineId) {
    const direction = comparisonPageView.directionLabel(chart.direction);
    const deltaMeaning = chart.direction === 'neutral'
        ? 'Positive deltas indicate a larger value, not necessarily better performance.'
        : 'Positive deltas indicate better performance.';
    if (!baseline) {
        return `${direction}. Run ${baselineId} has no comparable measurement for this metric, so deltas are unavailable.`;
    }
    return `${direction}. Run ${baselineId} is the baseline. ${deltaMeaning}`;
}

function renderMetricCard(chart, index) {
    const values = comparisonPageView.orderedChartValues(chart, comparisonPageRunOrder);
    const baselineId = comparisonBaseline.value;
    const baseline = chart.unavailable
        ? null
        : values.find(value => String(value.run_id) === baselineId) || null;
    const card = document.createElement('article');
    card.className = 'comparison-metric-card';
    card.id = comparisonPageView.metricAnchorId(chart, index);

    const header = document.createElement('div');
    header.className = 'comparison-metric-card-header';
    const titleBlock = document.createElement('div');
    const title = document.createElement('h2');
    title.textContent = chart.label;
    const subtitle = document.createElement('p');
    subtitle.className = 'comparison-metric-card-subtitle';
    subtitle.textContent = chart.unit || 'Recorded value';
    titleBlock.append(title, subtitle);
    const direction = document.createElement('span');
    direction.className = 'comparison-metric-direction';
    direction.textContent = chart.unavailable
        ? 'Not comparable'
        : comparisonPageView.directionLabel(chart.direction);
    header.append(titleBlock, direction);

    const context = document.createElement('p');
    context.className = 'comparison-metric-context';
    context.textContent = chart.unavailable
        ? 'Fewer than two matching numeric measurements are available for this metric.'
        : metricContext(chart, baseline, baselineId);
    if (chart.unavailable) card.classList.add('is-unavailable');
    card.append(header, context);

    if (!values.length) {
        const empty = document.createElement('div');
        empty.className = 'empty-state';
        empty.textContent = 'This metric has no numeric measurements to chart.';
        card.append(empty);
        return card;
    }

    const chartContainer = document.createElement('div');
    chartContainer.className = 'comparison-chart comparison-metric-card-chart';
    chartContainer.append(comparisonPageView.renderComparisonFigure(document, {
        chart,
        values,
        baseline,
        resolveRun: comparisonPageRun,
        colorForRun: id => comparisonPageView.runColor(id, comparisonPageRunOrder),
    }));
    card.append(chartContainer);

    const present = new Set(values.map(value => String(value.run_id)));
    const missing = comparisonPageRunOrder.filter(id => !present.has(id));
    if (missing.length) {
        const note = document.createElement('p');
        note.className = 'comparison-metric-missing';
        note.textContent = `No comparable measurement for ${missing.map(id => `run ${id}`).join(', ')}.`;
        card.append(note);
    }

    const details = document.createElement('details');
    details.className = 'comparison-metric-details';
    const summary = document.createElement('summary');
    summary.textContent = 'Exact values and run details';
    const tableWrap = document.createElement('div');
    tableWrap.className = 'comparison-table-wrap';
    tableWrap.append(comparisonPageView.renderComparisonTable(document, {
        chart,
        values,
        baseline,
        resolveRun: comparisonPageRun,
    }));
    details.append(summary, tableWrap);
    card.append(details);
    return card;
}

function renderExcludedResults(resultId) {
    const excluded = exclusionsForResult(comparisonPageData, resultId);
    comparisonExcludedList.replaceChildren();
    excluded.forEach(item => {
        const entry = document.createElement('li');
        entry.textContent = comparisonPageView.exclusionText(item);
        comparisonExcludedList.append(entry);
    });
    comparisonExcluded.hidden = excluded.length === 0;
}

function renderComparisonPage() {
    const charts = selectedResultCharts();
    const comparableCharts = charts.filter(chart => !chart.unavailable);
    const resultLabel = selectedResultLabel();
    const represented = metricRunIds(comparableCharts);
    const coverage = metricCoverage(comparableCharts, comparisonPageRunOrder);
    const baselineCoverage = coverage[comparisonBaseline.value] || 0;
    comparisonPageHeading.textContent = `${resultLabel}: all metrics`;
    document.title = `${resultLabel}: all metrics · Cloud Benchmark Lab`;
    if (!comparableCharts.length) {
        comparisonPageContext.textContent = (
            'No metric currently has enough matching measurements to calculate performance deltas.'
        );
    } else if (baselineCoverage === comparableCharts.length) {
        comparisonPageContext.textContent = (
            'One consistent baseline across every like-for-like metric. '
            + `Run ${comparisonBaseline.value} is selected as the baseline.`
        );
    } else if (baselineCoverage > 0) {
        comparisonPageContext.textContent = (
            `Run ${comparisonBaseline.value} is available for ${baselineCoverage} of ${comparableCharts.length} `
            + 'like-for-like metrics. Charts without that measurement show no performance delta.'
        );
    } else {
        comparisonPageContext.textContent = (
            `Run ${comparisonBaseline.value} is not represented in the comparable cohort for this benchmark. `
            + 'Choose a represented run as the baseline to see performance deltas.'
        );
    }
    comparisonPageStatus.classList.remove('is-error');
    const unavailableCount = charts.length - comparableCharts.length;
    const availability = unavailableCount
        ? `${comparableCharts.length} like-for-like · ${unavailableCount} unavailable`
        : `${comparableCharts.length} like-for-like metric${comparableCharts.length === 1 ? '' : 's'}`;
    comparisonPageStatus.textContent = comparableCharts.length
        ? `${availability} · ${represented.size} of ${comparisonPageRunOrder.length} selected runs represented.`
        : charts.length
            ? `${charts.length} metric${charts.length === 1 ? '' : 's'} found, but none has two matching numeric measurements.`
            : `No like-for-like metrics are available for ${resultLabel}. Review the exclusions below.`;
    renderRunLegend(charts);
    renderProvenanceWarnings(charts);
    renderMetricIndex(charts);
    comparisonCharts.replaceChildren(...charts.map(renderMetricCard));
    if (!charts.length) {
        const empty = document.createElement('div');
        empty.className = 'empty-state';
        empty.textContent = 'No safely comparable metric charts are available for this benchmark and run selection.';
        comparisonCharts.append(empty);
    }
    renderExcludedResults(comparisonPageResult.value);
    updateComparisonPageLinks();
}

function clearComparisonPageHash() {
    if (!window.location.hash) return;
    window.history.replaceState(null, '', `${window.location.pathname}${window.location.search}`);
}

function scrollToInitialMetric() {
    let hashId = window.location.hash.slice(1);
    if (!hashId && comparisonPageInitialMetric) {
        const requested = selectedResultCharts().find(
            chart => comparisonPageView.chartKey(chart) === comparisonPageInitialMetric,
        );
        if (requested) hashId = comparisonPageView.metricAnchorId(requested);
    }
    if (!hashId) return;
    window.requestAnimationFrame(() => {
        document.getElementById(hashId)?.scrollIntoView({block: 'start'});
    });
}

async function loadComparisonPage() {
    const state = parseComparisonState(window.location.search);
    comparisonPageRunOrder = state.runIds;
    comparisonPageInitialMetric = state.metricId;
    comparisonPageReturnTo = safeHistoryReturnPath(state.returnTo);
    if (!validComparisonRunIds(comparisonPageRunOrder)) {
        setComparisonPageFailure('This comparison link must contain 2–8 unique saved run IDs.');
        return;
    }
    comparisonPageStatus.textContent = 'Loading and validating all comparable metrics…';
    try {
        const query = new URLSearchParams({runs: comparisonPageRunOrder.join(',')});
        const response = await fetch(`/api/comparisons?${query}`, {cache: 'no-store'});
        let data = {};
        try {
            data = await response.json();
        } catch {}
        if (!response.ok) throw Error(data.detail || 'Unable to load this comparison');
        comparisonPageData = data;
        populateResultOptions(state.resultId);
        populateBaselineOptions(state.baselineId);
        if (!window.location.hash && state.metricId) {
            const requested = selectedResultCharts().find(chart => (
                comparisonPageView.chartKey(chart) === state.metricId
                || String(chart.metric_id) === state.metricId
            ));
            if (requested) {
                window.history.replaceState(
                    null,
                    '',
                    `${window.location.pathname}${window.location.search}#${comparisonPageView.metricAnchorId(requested)}`,
                );
            }
        }
        renderComparisonPage();
        scrollToInitialMetric();
    } catch (error) {
        setComparisonPageFailure(error.message);
    }
}

if (comparisonPageHasDocument) {
    comparisonPageResult.addEventListener('change', () => {
        clearComparisonPageHash();
        populateBaselineOptions(comparisonBaseline.value);
        renderComparisonPage();
    });
    comparisonBaseline.addEventListener('change', renderComparisonPage);
    window.addEventListener('hashchange', updateComparisonPageLinks);
    loadComparisonPage();
}

if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        exclusionsForResult,
        metricCoverage,
        metricRunIds,
        parseComparisonState,
        safeHistoryReturnPath,
        validComparisonRunIds,
    };
}
