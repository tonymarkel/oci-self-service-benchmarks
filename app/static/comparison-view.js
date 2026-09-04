(function comparisonViewModule(factory) {
    const api = factory();
    if (typeof module !== 'undefined' && module.exports) module.exports = api;
    if (typeof window !== 'undefined') window.ComparisonView = api;
}(function buildComparisonView() {
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
    const runColors = (
        '#b42318 #175cd3 #027a48 #7a5af8 #b54708 #c11574 #0e7090 #344054'
    ).split(' ');

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

    function runDisplayName(run) {
        const provider = providerFor(run);
        return `${providerNames[provider] || provider.toUpperCase()} ${valueOrDash(runField(run, 'shape'))}`;
    }

    function environmentText(run) {
        const provider = providerFor(run);
        const cpu = runField(run, 'ocpus');
        const cpuLabel = provider === 'oci' ? 'OCPUs' : 'vCPUs';
        const memory = runField(run, 'memory_gb');
        return [`${valueOrDash(cpu)} ${cpuLabel}`, `${valueOrDash(memory)} GB memory`, architectureFor(run)].join(' · ');
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
        if (!isFiniteMetric(value) || !isFiniteMetric(baseline)) return null;
        const numeric = Number(value);
        const base = Number(baseline);
        if (direction === 'lower') {
            if (numeric === 0) return null;
            return ((base / numeric) - 1) * 100;
        }
        if (base === 0) return null;
        return ((numeric - base) / Math.abs(base)) * 100;
    }

    function chartKey(chart) {
        const base = String(chart?.id || `${chart?.result_id || ''}:${chart?.metric_id || ''}`);
        return chart?.subtest ? `${base}:${chart.subtest}` : base;
    }

    function metricAnchorId(chart, index = 0) {
        const slug = chartKey(chart)
            .toLowerCase()
            .replace(/[^a-z0-9_-]+/g, '-')
            .replace(/^-+|-+$/g, '');
        return `metric-${slug || index + 1}`;
    }

    function chartsForResult(data, resultId) {
        const charts = Array.isArray(data?.charts) ? data.charts : [];
        return charts.filter(chart => String(chart.result_id) === String(resultId));
    }

    function normalizedDirection(direction) {
        return {
            higher_is_better: 'higher',
            lower_is_better: 'lower',
        }[direction] || direction || 'neutral';
    }

    function metricIdentity(metric) {
        return JSON.stringify([
            String(metric?.metric_id || metric?.key || ''),
            String(metric?.unit || ''),
            normalizedDirection(metric?.direction),
            String(metric?.subtest || ''),
        ]);
    }

    function selectedResultGroup(data, resultId) {
        const groups = (Array.isArray(data?.groups) ? data.groups : [])
            .filter(group => String(group?.result_id || '') === String(resultId));
        const charts = chartsForResult(data, resultId);
        const fingerprint = charts.find(chart => chart?.fingerprint)?.fingerprint;
        if (fingerprint) {
            const matching = groups.find(group => group?.fingerprint === fingerprint);
            if (matching) return matching;
        }
        const comparable = groups.filter(group => group?.comparable);
        return comparable.reduce((selected, group) => (
            !selected || (group.run_ids || []).length > (selected.run_ids || []).length
                ? group
                : selected
        ), null);
    }

    function chartFromGroupMetric(group, metric, index) {
        const values = (Array.isArray(metric?.values) ? metric.values : [])
            .filter(item => isFiniteMetric(item?.value))
            .map(item => {
                const value = {
                    run_id: String(item.run_id),
                    value: item.value,
                    warnings: Array.isArray(item.warnings) ? item.warnings : [],
                };
                if (isFiniteMetric(item.uncertainty)) {
                    value.error_low = Number(item.value) - Number(item.uncertainty);
                    value.error_high = Number(item.value) + Number(item.uncertainty);
                }
                return value;
            });
        return {
            id: `unavailable:${group.result_id}:${metric.key || index}`,
            result_id: group.result_id,
            result_name: group.name || group.result_id,
            metric_id: metric.key,
            label: metric.label || metric.key,
            unit: metric.unit || '',
            direction: normalizedDirection(metric.direction),
            primary: Boolean(metric.primary),
            subtest: metric.subtest,
            fingerprint: group.fingerprint,
            provenance_warnings: group.provenance_warnings || [],
            values,
            missing_run_ids: metric.missing_run_ids || [],
            unavailable: true,
        };
    }

    function allChartsForResult(data, resultId) {
        const charts = chartsForResult(data, resultId);
        const group = selectedResultGroup(data, resultId);
        if (!group || !Array.isArray(group.metrics)) return charts;
        const available = new Map(charts.map(chart => [metricIdentity(chart), chart]));
        return group.metrics.map((metric, index) => {
            const chart = available.get(metricIdentity(metric));
            if (chart) {
                return {
                    ...chart,
                    missing_run_ids: metric.missing_run_ids || [],
                    unavailable: false,
                };
            }
            return chartFromGroupMetric(group, metric, index);
        });
    }

    function resultOptions(data) {
        const results = new Map();
        const charts = Array.isArray(data?.charts) ? data.charts : [];
        charts.forEach(chart => {
            const id = String(chart.result_id || '');
            if (id && !results.has(id)) results.set(id, String(chart.result_name || id));
        });
        const groups = Array.isArray(data?.groups) ? data.groups : [];
        groups.forEach(group => {
            const id = String(group.result_id || '');
            if (id && !results.has(id)) results.set(id, String(group.name || id));
        });
        return [...results].map(([id, label]) => ({id, label}));
    }

    function runColor(id, runOrder) {
        const index = (Array.isArray(runOrder) ? runOrder.map(String) : []).indexOf(String(id));
        return runColors[(index < 0 ? 0 : index) % runColors.length];
    }

    function orderedChartValues(chart, runOrder) {
        const order = Array.isArray(runOrder) ? runOrder.map(String) : [];
        const positions = new Map(order.map((id, index) => [id, index]));
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
        if (delta === null) {
            return isFiniteMetric(value) && isFiniteMetric(baseline) && Number(value) === Number(baseline)
                ? 'Baseline'
                : '—';
        }
        if (Math.abs(delta) < 0.05) return '0.0%';
        return `${delta > 0 ? '+' : ''}${delta.toFixed(1)}%`;
    }

    function warningText(warnings) {
        if (Array.isArray(warnings)) return warnings.filter(Boolean).join('; ') || '—';
        return valueOrDash(warnings);
    }

    function directionLabel(direction) {
        return directionLabels[direction] || valueOrDash(direction);
    }

    function resolveRunFromLists(id, primaryRuns, fallbackRuns = []) {
        const requested = String(id);
        return [...(primaryRuns || []), ...(fallbackRuns || [])]
            .find(run => runId(run) === requested) || {id: requested};
    }

    function addCell(documentRef, row, text, header = false, scope = 'row') {
        const cell = documentRef.createElement(header ? 'th' : 'td');
        if (header) cell.scope = scope;
        cell.textContent = text;
        row.append(cell);
        return cell;
    }

    function renderComparisonFigure(documentRef, {
        chart,
        values,
        baseline = null,
        resolveRun,
        colorForRun = null,
    }) {
        const domain = comparisonDomain(values);
        const position = value => ((Number(value) - domain.min) / domain.span) * 100;
        const zero = position(0);
        const figure = documentRef.createElement('figure');
        figure.className = 'comparison-figure';
        const caption = documentRef.createElement('figcaption');
        caption.className = 'sr-only';
        caption.textContent = `Horizontal bar chart for ${chart.label}, with a zero baseline. Exact values are in the table that follows.`;
        const plot = documentRef.createElement('div');
        plot.className = 'comparison-plot';
        plot.setAttribute('aria-hidden', 'true');
        values.forEach(item => {
            const id = String(item.run_id);
            const run = resolveRun(id);
            const row = documentRef.createElement('div');
            row.className = 'comparison-bar-row';
            if (typeof colorForRun === 'function') {
                row.style.setProperty('--comparison-bar-color', colorForRun(id));
            }
            const label = documentRef.createElement('div');
            label.className = 'comparison-bar-label';
            const labelName = documentRef.createElement('strong');
            labelName.textContent = runDisplayName(run);
            const labelMeta = documentRef.createElement('small');
            labelMeta.textContent = `${id} · ${environmentText(run)}`;
            label.append(labelName, labelMeta);
            const measure = documentRef.createElement('div');
            measure.className = 'comparison-bar-measure';
            const track = documentRef.createElement('div');
            track.className = 'comparison-bar-track';
            track.style.setProperty('--zero-position', `${zero}%`);
            const valuePosition = position(item.value);
            const bar = documentRef.createElement('span');
            bar.className = 'comparison-bar';
            bar.style.left = `${Math.min(zero, valuePosition)}%`;
            bar.style.width = `${Math.abs(valuePosition - zero)}%`;
            if (Number(item.value) < 0) bar.classList.add('comparison-bar-negative');
            track.append(bar);
            if (isFiniteMetric(item.error_low) && isFiniteMetric(item.error_high)) {
                const low = Number(item.error_low);
                const high = Number(item.error_high);
                const error = documentRef.createElement('span');
                error.className = 'comparison-error-bar';
                error.style.left = `${Math.min(position(low), position(high))}%`;
                error.style.width = `${Math.abs(position(high) - position(low))}%`;
                track.append(error);
            }
            const exact = documentRef.createElement('div');
            exact.className = 'comparison-bar-value';
            let comparison = 'baseline unavailable';
            if (baseline) {
                comparison = id === String(baseline.run_id)
                    ? 'baseline'
                    : deltaText(item.value, baseline.value, chart.direction);
            }
            exact.textContent = `${exactMetric(item.value, chart.unit)} · ${comparison}`;
            measure.append(track, exact);
            row.append(label, measure);
            plot.append(row);
        });
        figure.append(caption, plot);
        return figure;
    }

    function renderComparisonTable(documentRef, {
        chart,
        values,
        baseline = null,
        resolveRun,
        reportHref = id => `/?report=${encodeURIComponent(id)}`,
    }) {
        const table = documentRef.createElement('table');
        table.className = 'comparison-table';
        const caption = documentRef.createElement('caption');
        caption.textContent = `${chart.result_name}: ${chart.label}. Exact measurements and comparison metadata.`;
        const head = documentRef.createElement('thead');
        const headRow = documentRef.createElement('tr');
        const deltaHeading = chart.direction === 'neutral'
            ? 'Value vs baseline'
            : 'Performance vs baseline';
        ['Run', 'Environment', 'Value', deltaHeading, 'Uncertainty', 'Warnings', 'Report']
            .forEach(label => addCell(documentRef, headRow, label, true, 'col'));
        head.append(headRow);
        const body = documentRef.createElement('tbody');
        values.forEach(item => {
            const id = String(item.run_id);
            const run = resolveRun(id);
            const row = documentRef.createElement('tr');
            const runCell = addCell(documentRef, row, `${runDisplayName(run)} · ${id}`, true);
            const runMeta = documentRef.createElement('small');
            runMeta.textContent = valueOrDash(runField(run, 'region'));
            runCell.append(documentRef.createElement('br'), runMeta);
            addCell(documentRef, row, environmentText(run));
            addCell(documentRef, row, exactMetric(item.value, chart.unit));
            const delta = baseline
                ? (id === String(baseline.run_id)
                    ? 'Baseline'
                    : deltaText(item.value, baseline.value, chart.direction))
                : '—';
            addCell(documentRef, row, delta);
            const uncertainty = item.error_low !== undefined || item.error_high !== undefined
                ? `${exactMetric(item.error_low, chart.unit)} – ${exactMetric(item.error_high, chart.unit)}`
                : '—';
            addCell(documentRef, row, uncertainty);
            addCell(documentRef, row, warningText(item.warnings));
            const reportCell = documentRef.createElement('td');
            const report = documentRef.createElement('a');
            report.className = 'button secondary';
            report.href = reportHref(id);
            report.textContent = 'View';
            reportCell.append(report);
            row.append(reportCell);
            body.append(row);
        });
        table.append(caption, head, body);
        return table;
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
        if (Array.isArray(reasons) && reasons.length) {
            return `${prefix}${reasons.map(mismatchText).join('; ')}`;
        }
        if (reasons && typeof reasons === 'object') {
            const details = Object.entries(reasons).map(([field, detail]) => mismatchText(
                detail && typeof detail === 'object' ? {field, ...detail} : `${field}: ${detail}`,
            ));
            return `${prefix}${details.join('; ')}`;
        }
        return `${prefix}${item?.reason || item?.message || 'No matching methodology and workload contract.'}`;
    }

    function provenanceWarnings(charts) {
        const warnings = [];
        const seen = new Set();
        (charts || []).forEach(chart => {
            const entries = Array.isArray(chart?.provenance_warnings)
                ? chart.provenance_warnings
                : [];
            entries.filter(Boolean).forEach(warning => {
                const text = String(warning);
                if (!seen.has(text)) {
                    seen.add(text);
                    warnings.push(text);
                }
            });
        });
        return warnings;
    }

    return {
        architectureFor,
        allChartsForResult,
        chartKey,
        chartsForResult,
        comparisonDomain,
        deltaText,
        directionLabel,
        environmentText,
        exactMetric,
        exclusionText,
        isFiniteMetric,
        metricAnchorId,
        orderedChartValues,
        percentFromBaseline,
        provenanceWarnings,
        providerFor,
        providerNames,
        renderComparisonFigure,
        renderComparisonTable,
        resolveRunFromLists,
        resultOptions,
        runColor,
        runDisplayName,
        runField,
        runId,
        valueOrDash,
        warningText,
    };
}));
