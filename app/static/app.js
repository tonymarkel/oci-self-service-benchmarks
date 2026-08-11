let jobId;
let poller;
let shapes = [];
let sshDefaults = {configured: false};
let deathstarWorkloads = [];
let currentJobPrivateKey = '';

const terminalStatuses = ['complete', 'destroyed', 'failed', 'reported', 'cleanup_failed'];
const deathstarDefaults = {
    workload: 'media_microservices',
    warmup_seconds: 30,
    duration_seconds: 60,
    threads: 4,
    connections: 64,
    request_rate: 100,
};
const $ = selector => document.querySelector(selector);
const $$ = selector => [...document.querySelectorAll(selector)];
const escape = value => String(value).replace(
    /[&<>]/g,
    character => ({'&': '&amp;', '<': '&lt;', '>': '&gt;'}[character]),
);

async function api(url, options) {
    const response = await fetch(url, options);
    const data = await response.json();
    if (!response.ok) {
        const detail = Array.isArray(data.detail)
            ? data.detail.map(item => item.msg || String(item)).join('; ')
            : data.detail;
        throw Error(detail || 'Request failed');
    }
    return data;
}

function perfOptions(selector) {
    $(selector).innerHTML = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120]
        .map(value => `<option ${value === 10 ? 'selected' : ''}>${value}</option>`)
        .join('');
}

async function init() {
    try {
        const [boot, catalog, defaults] = await Promise.all([
            api('/api/oci/bootstrap'),
            api('/api/catalog'),
            api('/api/config/ssh-defaults'),
        ]);
        sshDefaults = defaults;
        $('#region').innerHTML = boot.regions
            .map(region => `<option ${region === boot.default_region ? 'selected' : ''}>${region}</option>`)
            .join('');
        $('#compartment').placeholder = `Defaults to ${boot.default_compartment}`;
        renderCatalog(catalog);
        perfOptions('#bootPerf');
        perfOptions('#dataPerf');
        applySshDefaults();
        await placement();
        await resumeLastJob();
    } catch (error) {
        alert(error.message);
    }
}

function applySshDefaults() {
    const status = $('#keyDefaultsStatus');
    if (sshDefaults.configured) {
        $('#key').value = sshDefaults.private_key;
        $('#publicKey').value = sshDefaults.public_key;
        status.textContent = 'Loaded the default SSH key pair configured in .env. Choose files to override it.';
        status.hidden = false;
    } else {
        status.textContent = sshDefaults.error || '';
        status.hidden = !sshDefaults.error;
    }
}

function updateDeathstarWorkloadDescription() {
    const workload = deathstarWorkloads.find(item => item.id === $('#deathstarWorkload').value);
    $('#deathstarWorkloadDescription').textContent = workload?.description || '';
}

function setDeathstarValues(options = deathstarDefaults) {
    $('#deathstarWorkload').value = options.workload || deathstarDefaults.workload;
    if (!$('#deathstarWorkload').value && deathstarWorkloads.length) {
        $('#deathstarWorkload').value = deathstarWorkloads[0].id;
    }
    $('#deathstarWarmup').value = options.warmup_seconds ?? deathstarDefaults.warmup_seconds;
    $('#deathstarDuration').value = options.duration_seconds ?? deathstarDefaults.duration_seconds;
    $('#deathstarThreads').value = options.threads ?? deathstarDefaults.threads;
    $('#deathstarConnections').value = options.connections ?? deathstarDefaults.connections;
    $('#deathstarRequestRate').value = options.request_rate ?? deathstarDefaults.request_rate;
    updateDeathstarWorkloadDescription();
}

function toggleDeathstarSettings() {
    const checkbox = $('input[data-kind="bench"][value="deathstarbench"]');
    const selected = Boolean(checkbox?.checked);
    $('#deathstarSettings').hidden = !selected;
    $$('#deathstarSettings input, #deathstarSettings select').forEach(field => {
        field.disabled = !selected;
    });
}

function renderCatalog(catalog) {
    $('#benchmarks').innerHTML = catalog.benchmarks.map(benchmark =>
        `<label><input type="checkbox" value="${benchmark.id}" data-kind="bench"> ` +
        `<span>${benchmark.name}<small>${benchmark.description}</small></span></label>`,
    ).join('');
    $('#llm').innerHTML = catalog.llm_benchmarks.map(benchmark =>
        `<label><input type="checkbox" value="${benchmark.id}" data-kind="llm"> ` +
        `<span>${benchmark.name}<small>${benchmark.description} · ${benchmark.license}</small></span></label>`,
    ).join('');

    deathstarWorkloads = catalog.deathstarbench_workloads || [];
    $('#deathstarWorkload').innerHTML = deathstarWorkloads.map(workload =>
        `<option value="${workload.id}">${workload.name}</option>`,
    ).join('');
    setDeathstarValues();

    const deathstarCheckbox = $('input[data-kind="bench"][value="deathstarbench"]');
    if (deathstarCheckbox) deathstarCheckbox.addEventListener('change', toggleDeathstarSettings);
    $('#deathstarWorkload').addEventListener('change', updateDeathstarWorkloadDescription);
    toggleDeathstarSettings();
}

async function placement() {
    const compartment = $('#compartment').value;
    const region = $('#region').value;
    const data = await api(
        `/api/oci/placement?region=${encodeURIComponent(region)}&compartment_id=${encodeURIComponent(compartment)}`,
    );
    $('#ad').innerHTML = '<option value="">First available</option>' + data.availability_domains
        .map(item => `<option>${item.name}</option>`)
        .join('');
    window.placements = data.availability_domains;
    await loadShapes();
}

function faultDomains() {
    const placementItem = (window.placements || []).find(item => item.name === $('#ad').value);
    $('#fd').innerHTML = '<option value="">Let OCI choose</option>' + (placementItem?.fault_domains || [])
        .map(item => `<option>${item}</option>`)
        .join('');
}

async function loadShapes() {
    const query = new URLSearchParams({
        region: $('#region').value,
        compartment_id: $('#compartment').value,
        availability_domain: $('#ad').value,
    });
    const data = await api(`/api/oci/shapes?${query}`);
    shapes = data.items;
    $('#shapeList').innerHTML = shapes.map(item =>
        `<option value="${item.shape}">${item.flexible ? 'flexible ' : ''}` +
        `${item.ocpus || ''} OCPU · ${item.memory_gb || ''} GB</option>`,
    ).join('');
}

$('#region').addEventListener('change', placement);
$('#compartment').addEventListener('change', placement);
$('#ad').addEventListener('change', () => {
    faultDomains();
    loadShapes();
});
$('#shape').addEventListener('change', () => {
    const shape = shapes.find(item => item.shape === $('#shape').value);
    $('#shapeMeta').textContent = shape
        ? `${shape.flexible ? 'Flexible shape. ' : ''}Listed capacity: ` +
            `${shape.ocpus || 'varies'} OCPUs / ${shape.memory_gb || 'varies'} GB`
        : 'Select a valid shape';
    if (shape && !shape.flexible) {
        $('#ocpus').value = shape.ocpus;
        $('#memory').value = shape.memory_gb;
    }
});
$$('input[name=security]').forEach(input => input.addEventListener('change', () => {
    $('#shielded').hidden = $('input[name=security]:checked').value !== 'shielded';
}));
$('#keyFile').addEventListener('change', async event => {
    const file = event.target.files[0];
    if (file) $('#key').value = await file.text();
});
$('#publicKeyFile').addEventListener('change', async event => {
    const file = event.target.files[0];
    if (file) $('#publicKey').value = await file.text();
});

function deathstarOptions(selected) {
    if (!selected) return {...deathstarDefaults};
    return {
        workload: $('#deathstarWorkload').value || deathstarDefaults.workload,
        warmup_seconds: Number($('#deathstarWarmup').value),
        duration_seconds: Number($('#deathstarDuration').value),
        threads: Number($('#deathstarThreads').value),
        connections: Number($('#deathstarConnections').value),
        request_rate: Number($('#deathstarRequestRate').value),
    };
}

$('#planForm').addEventListener('submit', async event => {
    event.preventDefault();
    const selected = kind => $$(`input[data-kind=${kind}]:checked`).map(input => input.value);
    const selectedBenchmarks = selected('bench');
    const deathstarbench = deathstarOptions(selectedBenchmarks.includes('deathstarbench'));
    if (selectedBenchmarks.includes('deathstarbench') && deathstarbench.connections < deathstarbench.threads) {
        alert('DeathStarBench connections must be greater than or equal to its worker threads.');
        return;
    }
    if (selectedBenchmarks.includes('deathstarbench') && deathstarbench.request_rate < deathstarbench.threads) {
        alert('DeathStarBench request rate must be greater than or equal to its worker threads.');
        return;
    }
    if (selectedBenchmarks.includes('deathstarbench') && deathstarbench.connections % deathstarbench.threads) {
        alert('DeathStarBench connections must be evenly divisible by its worker threads.');
        return;
    }
    if (selectedBenchmarks.includes('deathstarbench') && deathstarbench.request_rate % deathstarbench.threads) {
        alert('DeathStarBench request rate must be evenly divisible by its worker threads.');
        return;
    }
    if (selectedBenchmarks.includes('deathstarbench') && Number($('#memory').value) < 16) {
        alert('DeathStarBench requires at least 16 GB of memory for its microservice containers.');
        return;
    }
    const data = {
        region: $('#region').value,
        compartment_id: $('#compartment').value || null,
        availability_domain: $('#ad').value || null,
        fault_domain: $('#fd').value || null,
        shape: $('#shape').value,
        ocpus: Number($('#ocpus').value),
        memory_gb: Number($('#memory').value),
        ssh_private_key: $('#key').value,
        ssh_public_key: $('#publicKey').value,
        ssh_key_passphrase: $('#keyPassphrase').value || null,
        security: {
            mode: $('input[name=security]:checked').value,
            secure_boot: $('#secureBoot').checked,
            measured_boot: $('#measuredBoot').checked,
            trusted_platform_module: $('#tpm').checked,
        },
        networking: $('input[name=networking]:checked').value,
        storage: {
            boot_size_gb: Number($('#bootSize').value),
            boot_performance: Number($('#bootPerf').value),
            additional_volume: $('#additional').checked,
            additional_size_gb: Number($('#dataSize').value),
            additional_performance: Number($('#dataPerf').value),
            mount_style: $('#mount').value,
        },
        deathstarbench,
        destroy_after_completion: $('#destroy').checked,
        benchmarks: selectedBenchmarks,
        llm_benchmarks: selected('llm'),
    };
    try {
        const result = await api('/api/jobs', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(data),
        });
        jobId = result.id;
        currentJobPrivateKey = $('#key').value;
        localStorage.setItem('ociBenchmarkJobId', jobId);
        localStorage.removeItem('ociBenchmarkReportDismissed');
        $('#plan').hidden = true;
        $('#test').hidden = false;
        activatePhase(1);
        poller = setInterval(poll, 2500);
        poll();
    } catch (error) {
        alert(error.message);
    }
});

function activatePhase(index) {
    $$('.steps span').forEach((step, stepIndex) => step.classList.toggle('active', stepIndex === index));
}

function isPartialReport(job) {
    if (job.benchmark_status) return job.benchmark_status === 'failed';
    const statuses = (job.results || []).map(result => result.status).filter(Boolean);
    if (statuses.includes('failed')) return true;
    if (statuses.includes('completed')) return false;
    return job.status === 'failed';
}

function retainedConnections(job) {
    const resources = job.resources || {};
    const hosts = [
        ['Benchmark VM', resources.public_ip],
        ['Load-generator VM', resources.loadgen_public_ip],
    ];
    const seen = new Set();
    return hosts
        .filter(([, address]) => address && !seen.has(address) && seen.add(address))
        .map(([label, address]) => `${label}: ssh -i /path/to/private-key opc@${address}`);
}

function withRetainedConnections(message, job) {
    const connections = retainedConnections(job);
    return connections.length ? `${message}\n${connections.join('\n')}` : message;
}

function showReport(job) {
    $('#plan').hidden = true;
    $('#test').hidden = true;
    $('#report').hidden = false;
    activatePhase(2);
    const partial = isPartialReport(job);
    $('#reportTitle').textContent = partial ? 'Partial benchmark report' : 'Your benchmark report is ready.';
    $('#download').hidden = false;
    $('#download').href = `/api/jobs/${jobId}/report?download=true`;
    const frameUrl = `/api/jobs/${jobId}/report`;
    if ($('#reportFrame').getAttribute('src') !== frameUrl) $('#reportFrame').src = frameUrl;
    $('#destroyNow').hidden = true;
    $('#keyDownload').hidden = true;
    const explicitlyRetained = job.status === 'complete' || (
        job.status === 'failed' && job.plan?.destroy_after_completion === false
    );
    const connections = explicitlyRetained ? retainedConnections(job) : [];
    const managedResourcesRemain = Object.keys(job.resources || {})
        .some(key => key.endsWith('_id'));
    const canDestroy = job.live !== false && managedResourcesRemain && (
        explicitlyRetained || job.status === 'cleanup_failed'
    );
    if (job.status === 'destroying') {
        $('#reportStatus').textContent = 'Results are ready. Infrastructure cleanup is continuing in the background.';
    } else if (job.status === 'destroyed') {
        $('#reportStatus').textContent = partial
            ? 'The benchmark stopped with partial results. Its infrastructure has been destroyed.'
            : 'Results are saved. Benchmark infrastructure has been destroyed.';
    } else if (job.status === 'reported') {
        $('#reportStatus').textContent = 'Results are saved. The app was restarted, so the original infrastructure state is unavailable.';
    } else if (job.status === 'cleanup_failed') {
        const detail = job.cleanup_error ? ` ${job.cleanup_error}` : '';
        $('#reportStatus').textContent =
            `Results are saved, but infrastructure cleanup did not finish.${detail}`;
        $('#destroyNow').hidden = !canDestroy;
    } else if (job.status === 'complete') {
        $('#reportStatus').textContent = withRetainedConnections('Infrastructure retained.', job);
        $('#destroyNow').hidden = !canDestroy;
        $('#keyDownload').hidden = !currentJobPrivateKey;
    } else if (job.status === 'failed') {
        if (partial) {
            const message = connections.length
                ? 'The benchmark stopped after producing partial results. Infrastructure was retained.'
                : 'The benchmark stopped after producing partial results.';
            $('#reportStatus').textContent = withRetainedConnections(message, job);
        } else {
            $('#reportStatus').textContent = withRetainedConnections(
                `Benchmarks completed and the report is ready, but a later lifecycle step failed.` +
                `${job.error ? ` ${job.error}` : ''}`,
                job,
            );
        }
        $('#destroyNow').hidden = !canDestroy;
        $('#keyDownload').hidden = !currentJobPrivateKey;
    } else {
        $('#reportStatus').textContent = 'Results are ready while the run finishes.';
    }
}

async function poll() {
    try {
        const job = await api(`/api/jobs/${jobId}`);
        const events = job.events || [];
        $('#status').textContent =
            `${job.status.toUpperCase()} — ${job.cleanup_error || job.error || events.at(-1)?.message || ''}`;
        $('#events').innerHTML = events.map(item =>
            `<div class="event"><time>${new Date(item.at).toLocaleTimeString()}</time>` +
            `<b>${escape(item.stage)}</b>${escape(item.message)}</div>`,
        ).join('');
        if (job.report_ready) showReport(job);
        if (terminalStatuses.includes(job.status)) clearInterval(poller);
    } catch (error) {
        $('#status').textContent = `Unable to refresh job: ${error.message}`;
    }
}

async function resumeLastJob() {
    const requested = new URLSearchParams(window.location.search).get('report');
    const saved = localStorage.getItem('ociBenchmarkJobId');
    if (!requested && !saved && localStorage.getItem('ociBenchmarkReportDismissed')) return;
    let latest = null;
    try {
        latest = (await api('/api/reports/latest')).id;
    } catch {}
    const candidates = [requested, saved, latest]
        .filter((id, index, list) => id && list.indexOf(id) === index);
    for (const candidate of candidates) {
        try {
            jobId = candidate;
            const job = await api(`/api/jobs/${jobId}`);
            localStorage.setItem('ociBenchmarkJobId', jobId);
            if (job.report_ready) {
                showReport(job);
            } else {
                $('#plan').hidden = true;
                $('#test').hidden = false;
                activatePhase(1);
            }
            if (!terminalStatuses.includes(job.status)) {
                poller = setInterval(poll, 2500);
                poll();
            }
            return;
        } catch {}
    }
    localStorage.removeItem('ociBenchmarkJobId');
}

function leaveReport() {
    if (poller) clearInterval(poller);
    localStorage.removeItem('ociBenchmarkJobId');
    localStorage.setItem('ociBenchmarkReportDismissed', '1');
    history.replaceState(null, '', '/');
    jobId = undefined;
    currentJobPrivateKey = '';
    $('#reportFrame').removeAttribute('src');
    $('#report').hidden = true;
    $('#test').hidden = true;
    $('#plan').hidden = false;
    activatePhase(0);
    window.scrollTo({top: 0, behavior: 'smooth'});
}

async function resetToPlan() {
    let active = false;
    if (jobId) {
        try {
            const job = await api(`/api/jobs/${jobId}`);
            active = !terminalStatuses.includes(job.status);
        } catch {}
    }
    if (active && !window.confirm(
        'A benchmark is still running. Resetting the view will not stop it; ' +
        'it will continue in the background. Return to Plan?',
    )) return;
    leaveReport();
    $('#planForm').reset();
    $('#key').value = '';
    $('#publicKey').value = '';
    $('#keyPassphrase').value = '';
    $('#keyFile').value = '';
    $('#publicKeyFile').value = '';
    setDeathstarValues();
    toggleDeathstarSettings();
    applySshDefaults();
    $('#shapeMeta').textContent = '';
    await placement();
}

async function restorePlan(plan) {
    $('#region').value = plan.region;
    $('#compartment').value = plan.compartment_id || '';
    await placement();
    $('#ad').value = plan.availability_domain || '';
    faultDomains();
    $('#fd').value = plan.fault_domain || '';
    await loadShapes();
    $('#shape').value = plan.shape || '';
    $('#ocpus').value = plan.ocpus ?? 8;
    $('#memory').value = plan.memory_gb ?? 32;
    $$('input[name=security]').forEach(input => {
        input.checked = input.value === (plan.security?.mode || 'none');
    });
    $('#secureBoot').checked = Boolean(plan.security?.secure_boot);
    $('#measuredBoot').checked = Boolean(plan.security?.measured_boot);
    $('#tpm').checked = Boolean(plan.security?.trusted_platform_module);
    $('#shielded').hidden = plan.security?.mode !== 'shielded';
    $$('input[name=networking]').forEach(input => {
        input.checked = input.value === (plan.networking || 'paravirtualized');
    });
    $('#bootSize').value = plan.storage?.boot_size_gb ?? 100;
    $('#bootPerf').value = plan.storage?.boot_performance ?? 10;
    $('#additional').checked = plan.storage?.additional_volume ?? true;
    $('#dataSize').value = plan.storage?.additional_size_gb ?? 1024;
    $('#dataPerf').value = plan.storage?.additional_performance ?? 10;
    $('#mount').value = plan.storage?.mount_style || 'paravirtualized';
    $('#destroy').checked = plan.destroy_after_completion ?? true;
    $$('input[data-kind=bench]').forEach(input => {
        input.checked = (plan.benchmarks || []).includes(input.value);
    });
    $$('input[data-kind=llm]').forEach(input => {
        input.checked = (plan.llm_benchmarks || []).includes(input.value);
    });
    setDeathstarValues(plan.deathstarbench || deathstarDefaults);
    toggleDeathstarSettings();
    $('#key').value = '';
    $('#publicKey').value = '';
    $('#keyPassphrase').value = '';
    $('#keyFile').value = '';
    $('#publicKeyFile').value = '';
    applySshDefaults();
    $('#shape').dispatchEvent(new Event('change'));
}

$('#rerunPlan').addEventListener('click', async () => {
    const sourceJob = jobId;
    try {
        const plan = await api(`/api/jobs/${sourceJob}/plan`);
        leaveReport();
        await restorePlan(plan);
    } catch (error) {
        alert(`Unable to restore the saved plan: ${error.message}`);
    }
});
$('#resetPlan').addEventListener('click', resetToPlan);
$('#resetAll').addEventListener('click', resetToPlan);
$('#destroyNow').addEventListener('click', async () => {
    await api(`/api/jobs/${jobId}/destroy`, {method: 'POST'});
    $('#destroyNow').hidden = true;
    poller = setInterval(poll, 2500);
});
$('#keyDownload').addEventListener('click', () => {
    const anchor = document.createElement('a');
    anchor.href = URL.createObjectURL(new Blob([currentJobPrivateKey], {type: 'application/octet-stream'}));
    anchor.download = 'oci-benchmark-key.pem';
    anchor.click();
    URL.revokeObjectURL(anchor.href);
});

init();
