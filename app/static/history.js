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
const directionLabels = {
    higher: 'Higher is better',
    lower: 'Lower is better',
    neutral: 'Informational',
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

function isFiniteMetric(value) {
    return value !== null
        && value !== undefined
        && value !== ''
        && Number.isFinite(Number(value));
}

function comparisonDomain(values) {
    const points = [0];
    values.forEach(item => {
        for (const candidate of [item?.value, item?.error_low, item?.error_high]) {
            if (isFiniteMetric(candidate)) points.push(Number(candidate));
        }
    });
    let min = Math.min(...points);
    let max = Math.max(...points);
    if (min === max) {
        if (min === 0) max = 1;
        else if (min > 0) min = 0;
        else max = 0;
    }
    return {min, max, span: max - min};
}

function percentFromBaseline(value, baseline, direction = 'higher') {
    const numeric = Number(value);
    const base = Number(baseline);
    if (!Number.isFinite(numeric) || !Number.isFinite(base)) return null;
    if (direction === 'lower') {
        if (numeric === 0) return null;
        return ((base / numeric) - 1) * 100;
    }
    if (base === 0) return null;
    return ((numeric - base) / Math.abs(base)) * 100;
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
    if (comparisonData && chart && !comparisonWorkspace.hidden) {
        params.set('result', String(chart.result_id));
        params.set('metric', chartKey(chart));
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

function runDisplayName(run) {
    return `${providerNames[providerFor(run)] || providerFor(run).toUpperCase()} ${valueOrDash(runField(run, 'shape'))}`;
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
    if (provider === 'gcp') addMeta(meta, 'Project', run.gcp_project_id);
    if (provider === 'azure') {
        addMeta(meta, 'Subscription', runField(run, 'azure_subscription_id'));
    }
    addMeta(meta, 'Region', run.region);
    if (provider === 'gcp') addMeta(meta, 'Zone', run.gcp_zone);
    if (provider === 'azure') addMeta(meta, 'Zone', runField(run, 'azure_zone'));
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
    const base = String(chart?.id || `${chart?.result_id || ''}:${chart?.metric_id || ''}`);
    return chart?.subtest ? `${base}:${chart.subtest}` : base;
}

function resultCharts() {
    if (!comparisonData || !Array.isArray(comparisonData.charts)) return [];
    return comparisonData.charts.filter(chart => String(chart.result_id) === comparisonResult.value);
}

function activeChart() {
    return resultCharts().find(chart => chartKey(chart) === comparisonMetric.value) || null;
}

function populateComparisonSelectors(preferredResult = '', preferredMetric = '') {
    const charts = Array.isArray(comparisonData?.charts) ? comparisonData.charts : [];
    const results = new Map();
    charts.forEach(chart => {
        const id = String(chart.result_id);
        if (!results.has(id)) results.set(id, chart.result_name || id);
    });
    comparisonResult.replaceChildren();
    for (const [id, label] of results) comparisonResult.add(new Option(label, id));
    const primary = charts.find(chart => chart.primary) || charts[0];
    const requestedResult = [...results.keys()].includes(String(preferredResult)) ? String(preferredResult) : String(primary?.result_id || '');
    comparisonResult.value = requestedResult;
    populateMetricOptions(preferredMetric || String(primary?.metric_id || ''));
    const disabled = charts.length === 0;
    comparisonResult.disabled = disabled;
    comparisonMetric.disabled = disabled;
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
}

function orderedChartValues(chart) {
    const positions = new Map([...selectedRunIds].map((id, index) => [id, index]));
    return (Array.isArray(chart?.values) ? chart.values : [])
        .filter(item => isFiniteMetric(item?.value))
        .slice()
        .sort((a, b) => (
            (positions.get(String(a.run_id)) ?? Number.MAX_SAFE_INTEGER)
            - (positions.get(String(b.run_id)) ?? Number.MAX_SAFE_INTEGER)
        ));
}

function exactMetric(value, unit) {
    if (value === null || value === undefined || value === '') return '—';
    return `${String(value)}${unit ? ` ${unit}` : ''}`;
}

function deltaText(value, baseline, direction) {
    const delta = percentFromBaseline(value, baseline, direction);
    if (delta === null) return Number(value) === Number(baseline) ? 'Baseline' : '—';
    if (Math.abs(delta) < 0.05) return '0.0%';
    return `${delta > 0 ? '+' : ''}${delta.toFixed(1)}%`;
}

function environmentText(run) {
    const provider = providerFor(run);
    const cpu = runField(run, 'ocpus');
    const cpuLabel = provider === 'oci' ? 'OCPUs' : 'vCPUs';
    const memory = runField(run, 'memory_gb');
    return [`${valueOrDash(cpu)} ${cpuLabel}`, `${valueOrDash(memory)} GB memory`, architectureFor(run)].join(' · ');
}

function warningText(warnings) {
    if (Array.isArray(warnings)) return warnings.filter(Boolean).join('; ') || '—';
    return valueOrDash(warnings);
}

function addCell(row, text, header = false, scope = 'row') {
    const cell = document.createElement(header ? 'th' : 'td');
    if (header) cell.scope = scope;
    cell.textContent = text;
    row.append(cell);
    return cell;
}

function renderComparisonTable(chart, values, baseline) {
    comparisonTable.replaceChildren();
    comparisonTable.hidden = false;
    const table = document.createElement('table');
    table.className = 'comparison-table';
    const caption = document.createElement('caption');
    caption.textContent = `${chart.result_name}: ${chart.label}. Exact measurements and comparison metadata.`;
    const head = document.createElement('thead');
    const headRow = document.createElement('tr');
    const deltaHeading = chart.direction === 'neutral'
        ? 'Value vs baseline'
        : 'Performance vs baseline';
    ['Run', 'Environment', 'Value', deltaHeading, 'Uncertainty', 'Warnings', 'Report'].forEach(label => addCell(headRow, label, true, 'col'));
    head.append(headRow);
    const body = document.createElement('tbody');
    values.forEach(item => {
        const id = String(item.run_id);
        const run = comparisonRun(id);
        const row = document.createElement('tr');
        const runCell = addCell(row, `${runDisplayName(run)} · ${id}`, true);
        const runMeta = document.createElement('small');
        runMeta.textContent = valueOrDash(runField(run, 'region'));
        runCell.append(document.createElement('br'), runMeta);
        addCell(row, environmentText(run));
        addCell(row, exactMetric(item.value, chart.unit));
        addCell(row, id === String(baseline.run_id) ? 'Baseline' : deltaText(item.value, baseline.value, chart.direction));
        const uncertainty = item.error_low !== undefined || item.error_high !== undefined
            ? `${exactMetric(item.error_low, chart.unit)} – ${exactMetric(item.error_high, chart.unit)}`
            : '—';
        addCell(row, uncertainty);
        addCell(row, warningText(item.warnings));
        const reportCell = document.createElement('td');
        reportCell.append(action('View', `/?report=${encodeURIComponent(id)}`, true));
        row.append(reportCell);
        body.append(row);
    });
    table.append(caption, head, body);
    comparisonTable.append(table);
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
    const direction = directionLabels[chart.direction] || valueOrDash(chart.direction);
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
    const domain = comparisonDomain(values);
    const position = value => ((Number(value) - domain.min) / domain.span) * 100;
    const zero = position(0);
    const figure = document.createElement('figure');
    figure.className = 'comparison-figure';
    const caption = document.createElement('figcaption');
    caption.className = 'sr-only';
    caption.textContent = `Horizontal bar chart for ${chart.label}, with a zero baseline. Exact values are in the table that follows.`;
    const plot = document.createElement('div');
    plot.className = 'comparison-plot';
    plot.setAttribute('aria-hidden', 'true');
    values.forEach(item => {
        const id = String(item.run_id);
        const run = comparisonRun(id);
        const row = document.createElement('div');
        row.className = 'comparison-bar-row';
        const label = document.createElement('div');
        label.className = 'comparison-bar-label';
        const labelName = document.createElement('strong');
        labelName.textContent = runDisplayName(run);
        const labelMeta = document.createElement('small');
        labelMeta.textContent = `${id} · ${environmentText(run)}`;
        label.append(labelName, labelMeta);
        const measure = document.createElement('div');
        measure.className = 'comparison-bar-measure';
        const track = document.createElement('div');
        track.className = 'comparison-bar-track';
        track.style.setProperty('--zero-position', `${zero}%`);
        const valuePosition = position(item.value);
        const bar = document.createElement('span');
        bar.className = 'comparison-bar';
        bar.style.left = `${Math.min(zero, valuePosition)}%`;
        bar.style.width = `${Math.abs(valuePosition - zero)}%`;
        if (Number(item.value) < 0) bar.classList.add('comparison-bar-negative');
        track.append(bar);
        const low = Number(item.error_low);
        const high = Number(item.error_high);
        if (isFiniteMetric(item.error_low) && isFiniteMetric(item.error_high)) {
            const error = document.createElement('span');
            error.className = 'comparison-error-bar';
            error.style.left = `${Math.min(position(low), position(high))}%`;
            error.style.width = `${Math.abs(position(high) - position(low))}%`;
            track.append(error);
        }
        const exact = document.createElement('div');
        exact.className = 'comparison-bar-value';
        exact.textContent = `${exactMetric(item.value, chart.unit)} · ${id === String(baseline.run_id) ? 'baseline' : deltaText(item.value, baseline.value, chart.direction)}`;
        measure.append(track, exact);
        row.append(label, measure);
        plot.append(row);
    });
    figure.append(caption, plot);
    comparisonChart.append(figure);
    renderComparisonTable(chart, values, baseline);
    syncUrl();
}

function mismatchText(mismatch) {
    if (typeof mismatch === 'string') return mismatch;
    if (!mismatch || typeof mismatch !== 'object') return String(mismatch || 'Not comparable');
    const field = mismatch.field || mismatch.name || mismatch.key || 'Setting';
    if ('expected' in mismatch || 'actual' in mismatch) {
        return `${field}: expected ${valueOrDash(mismatch.expected)}, got ${valueOrDash(mismatch.actual)}`;
    }
    return mismatch.reason || mismatch.message || JSON.stringify(mismatch);
}

function exclusionText(item) {
    if (typeof item === 'string') return item;
    const id = item?.run_id || item?.id;
    const resultName = item?.result_name || item?.result_id;
    const subject = [id ? `Run ${id}` : '', resultName].filter(Boolean).join(' · ');
    const prefix = subject ? `${subject}: ` : '';
    const reasons = item?.reasons || item?.mismatches;
    if (Array.isArray(reasons) && reasons.length) return `${prefix}${reasons.map(mismatchText).join('; ')}`;
    if (reasons && typeof reasons === 'object') {
        const details = Object.entries(reasons).map(([field, detail]) => mismatchText(
            detail && typeof detail === 'object' ? {field, ...detail} : `${field}: ${detail}`,
        ));
        return `${prefix}${details.join('; ')}`;
    }
    return `${prefix}${item?.reason || item?.message || 'No matching methodology and workload contract.'}`;
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
