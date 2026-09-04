const comparisonView = typeof module !== 'undefined' && module.exports
    ? require('./comparison-view.js')
    : window.ComparisonView;
const hasDocument = typeof document !== 'undefined';
const runList = hasDocument ? document.querySelector('#runList') : null;
const historySummary = hasDocument ? document.querySelector('#historySummary') : null;
const historyActionStatus = hasDocument ? document.querySelector('#historyActionStatus') : null;
const clearHistory = hasDocument ? document.querySelector('#clearHistory') : null;
const clearHistoryDialog = hasDocument ? document.querySelector('#clearHistoryDialog') : null;
const clearHistoryDialogDescription = hasDocument ? document.querySelector('#clearHistoryDialogDescription') : null;
const cancelClearHistory = hasDocument ? document.querySelector('#cancelClearHistory') : null;
const confirmClearHistory = hasDocument ? document.querySelector('#confirmClearHistory') : null;
const historySearch = hasDocument ? document.querySelector('#historySearch') : null;
const benchmarkFilter = hasDocument ? document.querySelector('#benchmarkFilter') : null;
const providerFilter = hasDocument ? document.querySelector('#providerFilter') : null;
const completedFilter = hasDocument ? document.querySelector('#completedFilter') : null;
const comparisonTray = hasDocument ? document.querySelector('#comparisonTray') : null;
const selectionStatus = hasDocument ? document.querySelector('#selectionStatus') : null;
const compareSelected = hasDocument ? document.querySelector('#compareSelected') : null;
const clearComparison = hasDocument ? document.querySelector('#clearComparison') : null;
const comparisonWorkspace = hasDocument ? document.querySelector('#comparisonWorkspace') : null;
const comparisonHeading = hasDocument ? document.querySelector('#comparisonHeading') : null;
const comparisonStatus = hasDocument ? document.querySelector('#comparisonStatus') : null;
const comparisonContext = hasDocument ? document.querySelector('#comparisonContext') : null;
const comparisonResult = hasDocument ? document.querySelector('#comparisonResult') : null;
const comparisonMetric = hasDocument ? document.querySelector('#comparisonMetric') : null;
const viewAllMetrics = hasDocument ? document.querySelector('#viewAllMetrics') : null;
const comparisonChart = hasDocument ? document.querySelector('#comparisonChart') : null;
const comparisonTable = hasDocument ? document.querySelector('#comparisonTable') : null;
const comparisonExcluded = hasDocument ? document.querySelector('#comparisonExcluded') : null;
const comparisonExcludedList = hasDocument ? document.querySelector('#comparisonExcludedList') : null;

const statusLabels = {
    complete: 'Completed',
    destroyed: 'Completed · infrastructure destroyed',
    reported: 'Completed · saved report',
    failed: 'Failed',
    cleanup_failed: 'Completed · cleanup warning',
    interrupted: 'Interrupted',
};
const providerNames = {
    oci: 'OCI',
    aws: 'AWS',
    gcp: 'GCP',
    azure: 'Azure',
};
let allRuns = [];
let selectedRunIds = new Set();
let comparisonData = null;
let comparisonLoading = false;
let comparisonRequest = 0;
let historyClearPending = false;

function valueOrDash(value) {
    return value === null || value === undefined || value === '' ? '—' : String(value);
}

function runId(run) {
    return String(run?.id ?? run?.run_id ?? '');
}

function runField(run, name) {
    return run?.[name] ?? run?.plan?.[name] ?? null;
}

function providerFor(run) {
    return String(runField(run, 'provider') || 'oci').toLowerCase();
}

function architectureFor(run) {
    return runField(run, 'architecture')
        || runField(run, 'arch')
        || runField(run, 'cpu_architecture')
        || '—';
}

function comparisonResultIds(run) {
    const ids = run?.comparison_result_ids;
    return Array.isArray(ids) ? ids.map(String).filter(Boolean) : [];
}

function isCompletedRun(run) {
    if (run?.benchmark_status) return run.benchmark_status === 'complete';
    return ['complete', 'destroyed', 'reported', 'cleanup_failed'].includes(run?.status);
}

function isComparableRun(run) {
    return isCompletedRun(run) && comparisonResultIds(run).length > 0;
}

function searchableText(run) {
    return [
        runId(run),
        providerFor(run),
        providerNames[providerFor(run)],
        runField(run, 'azure_subscription_id'),
        runField(run, 'azure_zone'),
        runField(run, 'region'),
        runField(run, 'gcp_zone'),
        runField(run, 'shape'),
        architectureFor(run),
        ...(run?.benchmarks || []),
    ].filter(Boolean).join(' ').toLowerCase();
}

function matchesFilters(run, filters) {
    if (filters.search && !searchableText(run).includes(filters.search.toLowerCase())) {
        return false;
    }
    if (filters.provider && providerFor(run) !== filters.provider) return false;
    if (filters.benchmark && !(run?.benchmarks || []).includes(filters.benchmark)) {
        return false;
    }
    if (filters.completed && !isCompletedRun(run)) return false;
    return true;
}

function comparisonDomain(values) {
    return comparisonView.comparisonDomain(values);
}

function percentFromBaseline(value, baseline, direction = 'higher') {
    return comparisonView.percentFromBaseline(value, baseline, direction);
}

function addMeta(list, label, value) {
    const wrapper = document.createElement('div');
    const term = document.createElement('dt');
    const description = document.createElement('dd');
    term.textContent = label;
    description.textContent = valueOrDash(value);
    wrapper.append(term, description);
    list.append(wrapper);
}

function action(label, href, secondary = false) {
    const link = document.createElement('a');
    link.className = `button${secondary ? ' secondary' : ''}`;
    link.href = href;
    link.textContent = label;
    return link;
}

function currentFilters() {
    return {
        search: historySearch.value.trim(),
        benchmark: benchmarkFilter.value,
        provider: providerFilter.value,
        completed: completedFilter.checked,
    };
}

function savedRunLabel(count) {
    return `${count} saved run${count === 1 ? '' : 's'}`;
}

function responseCount(data, names) {
    for (const name of names) {
        const value = data?.[name];
        if (Array.isArray(value)) return value.length;
        const number = Number(value);
        if (Number.isInteger(number) && number >= 0) return number;
    }
    return null;
}

function setHistoryActionStatus(message, error = false) {
    if (!historyActionStatus) return;
    historyActionStatus.textContent = message;
    historyActionStatus.classList.toggle('is-error', error);
}

function updateClearHistoryButton() {
    if (!clearHistory) return;
    clearHistory.disabled = historyClearPending || allRuns.length === 0;
    clearHistory.textContent = historyClearPending ? 'Clearing…' : 'Clear saved runs';
    if (historyClearPending) clearHistory.setAttribute('aria-busy', 'true');
    else clearHistory.removeAttribute('aria-busy');
}

function openClearHistoryDialog() {
    if (
        !clearHistoryDialog
        || !clearHistoryDialogDescription
        || historyClearPending
        || allRuns.length === 0
        || clearHistoryDialog.open
    ) return;
    clearHistoryDialogDescription.textContent = (
        `Eligible safely finalized local run files from ${savedRunLabel(allRuns.length)} in this archive will be permanently deleted. `
        + 'Runs that are active, cannot be safely classified, or may still be needed for cloud cleanup will be preserved. '
        + 'This action does not destroy cloud infrastructure. Local deletion cannot be undone.'
    );
    clearHistoryDialog.showModal();
}

async function clearSavedRuns() {
    if (
        historyClearPending
        || allRuns.length === 0
        || !clearHistoryDialog
        || !clearHistoryDialog.open
    ) return;
    clearHistoryDialog.close();
    historyClearPending = true;
    updateClearHistoryButton();
    setHistoryActionStatus('Clearing eligible saved runs…');
    try {
        const response = await fetch('/api/reports', {
            method: 'DELETE',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({confirmed: true}),
        });
        let data = {};
        try {
            data = await response.json();
        } catch {}
        if (!response.ok) throw Error(data.detail || 'Unable to clear saved runs');

        const deleted = responseCount(data, ['deleted', 'deleted_count', 'deleted_run_ids']) ?? 0;
        allRuns = [];
        selectedRunIds.clear();
        comparisonRequest += 1;
        comparisonLoading = false;
        clearComparisonView();
        populateFilterOptions();
        renderRuns();
        updateSelectionTray();
        syncUrl();
        await loadRuns();

        const reason = allRuns.length
            ? ' Protected or unclassified entries were left untouched.'
            : '';
        setHistoryActionStatus(
            `${savedRunLabel(deleted)} deleted. Remaining in archive: ${savedRunLabel(allRuns.length)}.${reason}`,
        );
    } catch (error) {
        setHistoryActionStatus(`Unable to clear saved runs: ${error.message}`, true);
    } finally {
        historyClearPending = false;
        updateClearHistoryButton();
    }
}

function initialUrlState() {
    if (!hasDocument) return {};
    const params = new URLSearchParams(window.location.search);
    return {
        search: params.get('q') || '',
        benchmark: params.get('benchmark') || '',
        provider: params.get('provider') || '',
        completed: params.has('completed') ? params.get('completed') === '1' : true,
        runIds: (params.get('compare') || '').split(',').filter(Boolean).slice(0, 8),
        resultId: params.get('result') || '',
        metricId: params.get('metric') || '',
    };
}

function syncUrl() {
    const params = new URLSearchParams(window.location.search);
    const filters = currentFilters();
    const assignments = {
        q: filters.search,
        benchmark: filters.benchmark,
        provider: filters.provider,
        completed: filters.completed ? '1' : '0',
        compare: [...selectedRunIds].join(','),
    };
    Object.entries(assignments).forEach(([name, value]) => {
        if (value) params.set(name, value);
        else params.delete(name);
    });
    const chart = activeChart();
    if (comparisonData && comparisonResult.value && !comparisonWorkspace.hidden) {
        params.set('result', comparisonResult.value);
        if (chart) params.set('metric', chartKey(chart));
        else params.delete('metric');
    } else {
        params.delete('result');
        params.delete('metric');
    }
    const query = params.toString();
    window.history.replaceState(null, '', `${window.location.pathname}${query ? `?${query}` : ''}`);
}

function comparisonRun(id) {
    const comparisonRuns = Array.isArray(comparisonData?.runs) ? comparisonData.runs : [];
    return comparisonRuns.find(run => runId(run) === id)
        || allRuns.find(run => runId(run) === id)
        || {id};
}

function renderRun(run) {
    const provider = providerFor(run);
    const fixedCapacity = provider === 'aws' || provider === 'gcp' || provider === 'azure';
    const id = runId(run);
    const card = document.createElement('article');
    card.className = 'run-card';
    card.dataset.runId = id;
    if (selectedRunIds.has(id)) card.classList.add('run-card-selected');
    const head = document.createElement('div');
    head.className = 'run-card-head';
    const titleBlock = document.createElement('div');
    const title = document.createElement('h2');
    const time = document.createElement('div');
    const status = document.createElement('span');
    title.textContent = `Run ${id}`;
    time.className = 'run-time';
    const date = new Date(run.created_at);
    time.textContent = Number.isNaN(date.valueOf()) ? run.created_at : date.toLocaleString();
    titleBlock.append(title, time);
    const benchmarkLifecycleLabels = {
        failed: {
            complete: 'Failed',
            destroyed: 'Failed · infrastructure destroyed',
            cleanup_failed: 'Failed · cleanup warning',
            reported: 'Failed · saved report',
        },
        pending: {
            complete: 'Incomplete benchmark results',
            destroyed: 'Incomplete · infrastructure destroyed',
            cleanup_failed: 'Incomplete · cleanup warning',
            reported: 'Incomplete · saved report',
        },
        interrupted: {
            complete: 'Interrupted',
            destroyed: 'Interrupted · infrastructure destroyed',
            cleanup_failed: 'Interrupted · cleanup warning',
            reported: 'Interrupted · saved report',
        },
        unknown: {
            complete: 'Benchmark outcome unknown',
            destroyed: 'Outcome unknown · infrastructure destroyed',
            cleanup_failed: 'Outcome unknown · cleanup warning',
            reported: 'Outcome unknown · saved report',
        },
    };
    const benchmarkLifecycleLabel = benchmarkLifecycleLabels[run.benchmark_status]?.[run.status];
    status.className = `status-pill status-${benchmarkLifecycleLabel ? run.benchmark_status : run.status}`;
    status.textContent = benchmarkLifecycleLabel || statusLabels[run.status] || run.status;

    const selection = document.createElement('label');
    selection.className = 'run-comparison-choice';
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.checked = selectedRunIds.has(id);
    const eligible = isComparableRun(run);
    checkbox.disabled = !eligible || (selectedRunIds.size >= 8 && !checkbox.checked);
    checkbox.setAttribute('aria-label', `Select run ${id} for comparison`);
    const selectionCopy = document.createElement('span');
    selectionCopy.textContent = eligible ? 'Compare' : 'No structured results';
    selection.append(checkbox, selectionCopy);
    if (!eligible) selection.title = 'This run has no complete, structured benchmark results.';
    checkbox.addEventListener('change', () => setRunSelected(id, checkbox.checked));

    const headActions = document.createElement('div');
    headActions.className = 'run-card-status';
    headActions.append(status, selection);
    head.append(titleBlock, headActions);

    const meta = document.createElement('dl');
    meta.className = 'run-meta';
    addMeta(meta, 'Provider', providerNames[provider] || provider.toUpperCase());
    addMeta(meta, 'Region', run.region);
    const shapeLabel = provider === 'aws'
        ? 'Instance type'
        : (provider === 'azure'
            ? 'VM size'
            : (provider === 'gcp' ? 'Machine type' : 'Shape'));
    addMeta(meta, shapeLabel, run.shape);
    addMeta(meta, fixedCapacity ? 'vCPUs' : 'OCPUs', run.ocpus);
    addMeta(meta, 'Memory', run.memory_gb === null || run.memory_gb === undefined ? null : `${run.memory_gb} GB`);
    addMeta(meta, 'Benchmarks', (run.benchmarks || []).join(', ') || '—');

    const actions = document.createElement('div');
    actions.className = 'run-actions';
    if (run.report_ready !== false) {
        actions.append(
            action('View report', `/?report=${encodeURIComponent(id)}`),
            action('Download HTML', `/api/jobs/${encodeURIComponent(id)}/report?download=true`, true),
        );
    } else {
        actions.append(action('Review / clean up', `/?report=${encodeURIComponent(id)}`));
    }
    card.append(head, meta, actions);
    return card;
}

function renderRuns() {
    const filters = currentFilters();
    const visibleRuns = allRuns.filter(run => matchesFilters(run, filters));
    runList.replaceChildren();
    historySummary.textContent = `${visibleRuns.length} of ${allRuns.length} saved run${allRuns.length === 1 ? '' : 's'} shown. Select 2–8 completed runs with structured results to compare.`;
    if (!visibleRuns.length) {
        const empty = document.createElement('div');
        empty.className = 'empty-state';
        empty.textContent = allRuns.length ? 'No saved runs match these filters.' : 'No saved benchmark runs are available yet.';
        runList.append(empty);
        return;
    }
    visibleRuns.forEach(run => runList.append(renderRun(run)));
}

function clearComparisonView() {
    comparisonData = null;
    comparisonWorkspace.hidden = true;
    comparisonResult.replaceChildren();
    comparisonMetric.replaceChildren();
    comparisonResult.disabled = true;
    comparisonMetric.disabled = true;
    comparisonChart.replaceChildren();
    comparisonTable.replaceChildren();
    comparisonTable.hidden = true;
    comparisonExcluded.hidden = true;
    comparisonContext.textContent = '';
    comparisonStatus.textContent = '';
    if (viewAllMetrics) {
        viewAllMetrics.hidden = true;
        viewAllMetrics.removeAttribute('href');
    }
}

function refreshRunSelectionControls() {
    runList.querySelectorAll('.run-card').forEach(card => {
        const id = card.dataset.runId;
        const checkbox = card.querySelector('.run-comparison-choice input');
        const run = allRuns.find(item => runId(item) === id);
        const selected = selectedRunIds.has(id);
        card.classList.toggle('run-card-selected', selected);
        checkbox.checked = selected;
        checkbox.disabled = !isComparableRun(run)
            || (selectedRunIds.size >= 8 && !selected);
    });
}

function setRunSelected(id, selected) {
    if (selected) {
        if (selectedRunIds.size >= 8) {
            selectionStatus.textContent = 'You can compare up to 8 runs at a time.';
            refreshRunSelectionControls();
            return;
        }
        selectedRunIds.add(id);
    } else {
        selectedRunIds.delete(id);
    }
    comparisonRequest += 1;
    comparisonLoading = false;
    clearComparisonView();
    refreshRunSelectionControls();
    updateSelectionTray();
    syncUrl();
}

function updateSelectionTray() {
    const count = selectedRunIds.size;
    comparisonTray.hidden = count === 0;
    compareSelected.disabled = comparisonLoading || count < 2 || count > 8;
    if (count < 2) selectionStatus.textContent = `${count} selected · select ${2 - count} more`;
    else if (count === 8) selectionStatus.textContent = '8 selected · maximum reached';
    else selectionStatus.textContent = `${count} selected · ready to compare`;
}

function populateFilterOptions() {
    const initialBenchmark = benchmarkFilter.value;
    const initialProvider = providerFilter.value;
    const benchmarks = [...new Set(allRuns.flatMap(run => run.benchmarks || []))]
        .sort((a, b) => String(a).localeCompare(String(b)));
    const providers = [...new Set(allRuns.map(providerFor))]
        .sort((a, b) => (providerNames[a] || a).localeCompare(providerNames[b] || b));
    benchmarkFilter.replaceChildren(new Option('All benchmarks', ''));
    benchmarks.forEach(name => benchmarkFilter.add(new Option(name, name)));
    providerFilter.replaceChildren(new Option('All providers', ''));
    providers.forEach(name => providerFilter.add(new Option(providerNames[name] || name.toUpperCase(), name)));
    benchmarkFilter.value = initialBenchmark;
    providerFilter.value = initialProvider;
}

function chartKey(chart) {
    return comparisonView.chartKey(chart);
}

function resultCharts() {
    return comparisonView.chartsForResult(comparisonData, comparisonResult.value);
}

function activeChart() {
    return resultCharts().find(chart => chartKey(chart) === comparisonMetric.value) || null;
}

function updateAllMetricsLink() {
    if (!viewAllMetrics) return;
    const resultId = comparisonResult.value;
    const runIds = [...selectedRunIds];
    const allMetrics = comparisonView.allChartsForResult(comparisonData, resultId);
    const available = resultId && allMetrics.length > 0 && runIds.length >= 2;
    viewAllMetrics.hidden = !available;
    if (!available) {
        viewAllMetrics.removeAttribute('href');
        return;
    }
    const chart = activeChart();
    const baselineId = (chart ? orderedChartValues(chart)[0]?.run_id : null) || runIds[0];
    const params = new URLSearchParams({
        compare: runIds.join(','),
        result: resultId,
        baseline: String(baselineId),
    });
    const anchor = chart ? `#${comparisonView.metricAnchorId(chart)}` : '';
    const returnParams = new URLSearchParams(window.location.search);
    returnParams.set('compare', runIds.join(','));
    returnParams.set('result', resultId);
    if (chart) returnParams.set('metric', chartKey(chart));
    const returnQuery = returnParams.toString();
    params.set('return', `/history${returnQuery ? `?${returnQuery}` : ''}`);
    viewAllMetrics.href = `/comparison?${params}${anchor}`;
}

function populateComparisonSelectors(preferredResult = '', preferredMetric = '') {
    const charts = Array.isArray(comparisonData?.charts) ? comparisonData.charts : [];
    const results = comparisonView.resultOptions(comparisonData);
    comparisonResult.replaceChildren();
    results.forEach(result => comparisonResult.add(new Option(result.label, result.id)));
    const primary = charts.find(chart => chart.primary) || charts[0];
    const requestedResult = results.some(result => result.id === String(preferredResult))
        ? String(preferredResult)
        : String(primary?.result_id || results[0]?.id || '');
    comparisonResult.value = requestedResult;
    populateMetricOptions(preferredMetric || String(primary?.metric_id || ''));
    comparisonResult.disabled = results.length === 0;
    comparisonMetric.disabled = resultCharts().length === 0;
}

function populateMetricOptions(preferredMetric = '') {
    const charts = resultCharts();
    comparisonMetric.replaceChildren();
    charts.forEach(chart => {
        const unit = chart.unit ? ` (${chart.unit})` : '';
        const subtest = chart.subtest ? ` — ${chart.subtest}` : '';
        comparisonMetric.add(new Option(`${chart.label}${subtest}${unit}`, chartKey(chart)));
    });
    const preferred = charts.find(chart => chartKey(chart) === String(preferredMetric) || String(chart.metric_id) === String(preferredMetric));
    const selected = preferred || charts.find(chart => chart.primary) || charts[0];
    if (selected) comparisonMetric.value = chartKey(selected);
    comparisonMetric.disabled = charts.length === 0;
}

function orderedChartValues(chart) {
    return comparisonView.orderedChartValues(chart, [...selectedRunIds]);
}

function renderComparisonTable(chart, values, baseline) {
    comparisonTable.replaceChildren();
    comparisonTable.hidden = false;
    comparisonTable.append(comparisonView.renderComparisonTable(document, {
        chart,
        values,
        baseline,
        resolveRun: comparisonRun,
    }));
}

function renderComparisonChart() {
    comparisonChart.replaceChildren();
    comparisonTable.replaceChildren();
    comparisonTable.hidden = true;
    const chart = activeChart();
    if (!chart) {
        const empty = document.createElement('div');
        empty.className = 'empty-state';
        empty.textContent = 'No safely comparable metrics are available for this selection.';
        comparisonChart.append(empty);
        comparisonContext.textContent = '';
        updateAllMetricsLink();
        syncUrl();
        return;
    }
    const values = orderedChartValues(chart);
    if (!values.length) {
        const empty = document.createElement('div');
        empty.className = 'empty-state';
        empty.textContent = 'This metric has no numeric measurements to chart.';
        comparisonChart.append(empty);
        syncUrl();
        return;
    }
    const baseline = values[0];
    const direction = comparisonView.directionLabel(chart.direction);
    const subtest = chart.subtest ? ` · ${chart.subtest}` : '';
    const deltaMeaning = chart.direction === 'neutral'
        ? 'positive percentages indicate a larger value, not necessarily better performance.'
        : 'positive percentages indicate better performance.';
    const provenanceWarnings = Array.isArray(chart.provenance_warnings)
        ? chart.provenance_warnings.filter(Boolean)
        : [];
    const provenanceNote = provenanceWarnings.length
        ? ` ${provenanceWarnings.join(' ')}`
        : '';
    comparisonContext.textContent = `${chart.result_name} · ${chart.label}${subtest} · ${direction}. Run ${baseline.run_id} is the baseline; ${deltaMeaning}${provenanceNote}`;
    comparisonChart.append(comparisonView.renderComparisonFigure(document, {
        chart,
        values,
        baseline,
        resolveRun: comparisonRun,
    }));
    renderComparisonTable(chart, values, baseline);
    updateAllMetricsLink();
    syncUrl();
}

function exclusionText(item) {
    return comparisonView.exclusionText(item);
}

function renderExcludedResults() {
    const excluded = Array.isArray(comparisonData?.excluded) ? comparisonData.excluded : [];
    comparisonExcluded.hidden = excluded.length === 0;
    comparisonExcludedList.replaceChildren();
    excluded.forEach(item => {
        const entry = document.createElement('li');
        entry.textContent = exclusionText(item);
        comparisonExcludedList.append(entry);
    });
}

async function loadComparison({focus = true, resultId = '', metricId = ''} = {}) {
    const ids = [...selectedRunIds];
    if (ids.length < 2 || ids.length > 8) return;
    const request = ++comparisonRequest;
    comparisonLoading = true;
    comparisonWorkspace.hidden = false;
    comparisonStatus.textContent = 'Loading and validating comparable results…';
    comparisonContext.textContent = '';
    comparisonChart.replaceChildren();
    comparisonTable.replaceChildren();
    comparisonTable.hidden = true;
    comparisonExcluded.hidden = true;
    comparisonResult.disabled = true;
    comparisonMetric.disabled = true;
    updateSelectionTray();
    syncUrl();
    try {
        const query = new URLSearchParams({runs: ids.join(',')});
        const response = await fetch(`/api/comparisons?${query}`, {cache: 'no-store'});
        const data = await response.json();
        if (!response.ok) throw Error(data.detail || 'Unable to compare the selected runs');
        if (request !== comparisonRequest) return;
        comparisonData = {
            runs: Array.isArray(data.runs) ? data.runs : [],
            charts: Array.isArray(data.charts) ? data.charts : [],
            excluded: Array.isArray(data.excluded) ? data.excluded : [],
            groups: Array.isArray(data.groups) ? data.groups : [],
        };
        populateComparisonSelectors(resultId, metricId);
        renderExcludedResults();
        renderComparisonChart();
        const count = comparisonData.charts.length;
        comparisonStatus.textContent = count
            ? `${count} like-for-like metric${count === 1 ? '' : 's'} available. Choose a result and metric to explore.`
            : 'No like-for-like metrics were found. Review the mismatch explanations below.';
        if (focus) comparisonHeading.focus({preventScroll: true});
        comparisonWorkspace.scrollIntoView({behavior: focus ? 'smooth' : 'auto', block: 'start'});
    } catch (error) {
        if (request !== comparisonRequest) return;
        comparisonData = null;
        comparisonStatus.textContent = error.message;
        const failure = document.createElement('div');
        failure.className = 'status';
        failure.textContent = error.message;
        comparisonChart.replaceChildren(failure);
    } finally {
        if (request === comparisonRequest) {
            comparisonLoading = false;
            updateSelectionTray();
            syncUrl();
        }
    }
}

function applyFilterChange() {
    renderRuns();
    syncUrl();
}

async function loadRuns() {
    const url = initialUrlState();
    historySearch.value = url.search;
    completedFilter.checked = url.completed;
    try {
        const response = await fetch('/api/reports', {cache: 'no-store'});
        const data = await response.json();
        if (!response.ok) throw Error(data.detail || 'Unable to load runs');
        allRuns = Array.isArray(data.items) ? data.items : [];
        benchmarkFilter.value = url.benchmark;
        providerFilter.value = url.provider;
        populateFilterOptions();
        benchmarkFilter.value = url.benchmark;
        providerFilter.value = url.provider;
        const available = new Set(allRuns.filter(isComparableRun).map(runId));
        selectedRunIds = new Set(url.runIds.filter(id => available.has(id)));
        renderRuns();
        updateSelectionTray();
        updateClearHistoryButton();
        syncUrl();
        if (selectedRunIds.size >= 2) {
            await loadComparison({focus: false, resultId: url.resultId, metricId: url.metricId});
        }
    } catch (error) {
        historySummary.textContent = 'Unable to load saved runs.';
        const failure = document.createElement('div');
        failure.className = 'status';
        failure.textContent = error.message;
        runList.replaceChildren(failure);
        updateClearHistoryButton();
    }
}

if (hasDocument) {
    document.querySelector('#historyFilters').addEventListener('submit', event => {
        event.preventDefault();
        applyFilterChange();
    });
    historySearch.addEventListener('input', applyFilterChange);
    benchmarkFilter.addEventListener('change', applyFilterChange);
    providerFilter.addEventListener('change', applyFilterChange);
    completedFilter.addEventListener('change', applyFilterChange);
    clearHistory?.addEventListener('click', openClearHistoryDialog);
    cancelClearHistory?.addEventListener('click', () => clearHistoryDialog?.close());
    confirmClearHistory?.addEventListener('click', clearSavedRuns);
    compareSelected.addEventListener('click', () => loadComparison());
    clearComparison.addEventListener('click', () => {
        selectedRunIds.clear();
        comparisonRequest += 1;
        comparisonLoading = false;
        clearComparisonView();
        refreshRunSelectionControls();
        updateSelectionTray();
        syncUrl();
        historySearch.focus();
    });
    comparisonResult.addEventListener('change', () => {
        populateMetricOptions();
        renderComparisonChart();
    });
    comparisonMetric.addEventListener('change', renderComparisonChart);
    loadRuns();
}

if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        comparisonDomain,
        comparisonResultIds,
        isCompletedRun,
        matchesFilters,
        percentFromBaseline,
    };
}
