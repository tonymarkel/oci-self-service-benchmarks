let jobId;
let poller;
let shapes = [];
let sshDefaults = {configured: false};
let sysbenchWorkloads = [];
let iperf3Protocols = [];
let phoronixProfiles = [];
let deathstarWorkloads = [];
let apachebenchWorkloads = [];
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
const sysbenchDefaults = {workloads: ['cpu']};
const iperf3Defaults = {protocols: ['tcp']};
const phoronixDefaults = {profiles: ['compress_7zip']};
const apachebenchDefaults = {
    workloads: ['new_connections', 'keep_alive'],
    request_count: 500000,
    concurrency: 100,
    response_size_kib: 64,
    warmup_requests: 10000,
    trials: 3,
};
const legacySysbenchWorkloads = {
    sysbench_cpu: 'cpu',
    sysbench_memory: 'memory',
    sysbench_fileio: 'fileio',
};
const legacyIperf3Protocols = {
    iperf_tcp: 'tcp',
    iperf_udp: 'udp',
    iperf_sctp: 'sctp',
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

function setSysbenchValues(options = sysbenchDefaults) {
    const workloads = Array.isArray(options?.workloads)
        ? options.workloads
        : sysbenchDefaults.workloads;
    $$('#sysbenchWorkloads input[data-kind="sysbench-workload"]').forEach(input => {
        input.checked = workloads.includes(input.value);
    });
}

function toggleSysbenchSettings() {
    const checkbox = $('input[data-kind="bench"][value="sysbench"]');
    const selected = Boolean(checkbox?.checked);
    $('#sysbenchSettings').hidden = !selected;
    $$('#sysbenchSettings input').forEach(field => {
        field.disabled = !selected;
    });
}

function setIperf3Values(options = iperf3Defaults) {
    const protocols = Array.isArray(options?.protocols)
        ? options.protocols
        : iperf3Defaults.protocols;
    $$('#iperf3Protocols input[data-kind="iperf3-protocol"]').forEach(input => {
        input.checked = protocols.includes(input.value);
    });
}

function toggleIperf3Settings() {
    const checkbox = $('input[data-kind="bench"][value="iperf3"]');
    const selected = Boolean(checkbox?.checked);
    $('#iperf3Settings').hidden = !selected;
    $$('#iperf3Settings input').forEach(field => {
        field.disabled = !selected;
    });
}

function setPhoronixValues(options = phoronixDefaults) {
    const profiles = Array.isArray(options?.profiles)
        ? options.profiles
        : phoronixDefaults.profiles;
    $$('#phoronixProfiles input[data-kind="phoronix-profile"]').forEach(input => {
        input.checked = profiles.includes(input.value);
    });
}

function togglePhoronixSettings() {
    const checkbox = $('input[data-kind="bench"][value="phoronix"]');
    const selected = Boolean(checkbox?.checked);
    $('#phoronixSettings').hidden = !selected;
    $$('#phoronixSettings input').forEach(field => {
        field.disabled = !selected;
    });
}

function phoronixProfileMetadata(profile) {
    const architectures = Array.isArray(profile.architectures)
        ? profile.architectures.join(' / ')
        : profile.architectures;
    const direction = String(profile.direction || '').replaceAll('_', ' ');
    return [
        profile.category,
        architectures,
        profile.estimated_runtime_minutes
            ? `~${profile.estimated_runtime_minutes} min`
            : null,
        profile.unit,
        direction
            ? `${direction.charAt(0).toUpperCase()}${direction.slice(1)}`
            : null,
        profile.profile,
    ].filter(Boolean).map(escape).join(' · ');
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

function setApachebenchValues(options = apachebenchDefaults) {
    const workloads = Array.isArray(options?.workloads)
        ? options.workloads
        : apachebenchDefaults.workloads;
    $$('#apachebenchWorkloads input[data-kind="apachebench-workload"]').forEach(input => {
        input.checked = workloads.includes(input.value);
    });
    $('#apachebenchRequestCount').value = options.request_count ?? apachebenchDefaults.request_count;
    $('#apachebenchConcurrency').value = options.concurrency ?? apachebenchDefaults.concurrency;
    $('#apachebenchResponseSize').value = options.response_size_kib ?? apachebenchDefaults.response_size_kib;
    $('#apachebenchWarmupRequests').value = options.warmup_requests ?? apachebenchDefaults.warmup_requests;
    $('#apachebenchTrials').value = options.trials ?? apachebenchDefaults.trials;
}

function toggleApachebenchSettings() {
    const checkbox = $('input[data-kind="bench"][value="apachebench"]');
    const selected = Boolean(checkbox?.checked);
    $('#apachebenchSettings').hidden = !selected;
    $$('#apachebenchSettings input').forEach(field => {
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

    sysbenchWorkloads = catalog.sysbench_workloads || [];
    $('#sysbenchWorkloads').innerHTML = sysbenchWorkloads.map(workload =>
        `<label><input type="checkbox" value="${workload.id}" ` +
        `data-kind="sysbench-workload"> ` +
        `<span>${workload.name}<small>${workload.description}</small></span></label>`,
    ).join('');
    setSysbenchValues();

    iperf3Protocols = catalog.iperf3_protocols || [];
    $('#iperf3Protocols').innerHTML = iperf3Protocols.map(protocol =>
        `<label><input type="checkbox" value="${protocol.id}" ` +
        `data-kind="iperf3-protocol"> ` +
        `<span>${protocol.name}<small>${protocol.description}</small></span></label>`,
    ).join('');
    setIperf3Values();

    phoronixProfiles = catalog.phoronix_profiles || [];
    $('#phoronixProfiles').innerHTML = phoronixProfiles.map(profile =>
        `<label><input type="checkbox" value="${escape(profile.id)}" ` +
        `data-kind="phoronix-profile"> ` +
        `<span>${escape(profile.name)}<small>${escape(profile.description)}</small>` +
        `<small>${phoronixProfileMetadata(profile)}</small></span></label>`,
    ).join('');
    setPhoronixValues();

    apachebenchWorkloads = catalog.apachebench_workloads || [];
    $('#apachebenchWorkloads').innerHTML = apachebenchWorkloads.map(workload =>
        `<label><input type="checkbox" value="${escape(workload.id)}" ` +
        `data-kind="apachebench-workload"> ` +
        `<span>${escape(workload.name)}<small>${escape(workload.description)}</small></span></label>`,
    ).join('');
    setApachebenchValues();

    deathstarWorkloads = catalog.deathstarbench_workloads || [];
    $('#deathstarWorkload').innerHTML = deathstarWorkloads.map(workload =>
        `<option value="${workload.id}">${workload.name}</option>`,
    ).join('');
    setDeathstarValues();

    const sysbenchCheckbox = $('input[data-kind="bench"][value="sysbench"]');
    if (sysbenchCheckbox) sysbenchCheckbox.addEventListener('change', toggleSysbenchSettings);
    const iperf3Checkbox = $('input[data-kind="bench"][value="iperf3"]');
    if (iperf3Checkbox) iperf3Checkbox.addEventListener('change', toggleIperf3Settings);
    const phoronixCheckbox = $('input[data-kind="bench"][value="phoronix"]');
    if (phoronixCheckbox) phoronixCheckbox.addEventListener('change', togglePhoronixSettings);
    const apachebenchCheckbox = $('input[data-kind="bench"][value="apachebench"]');
    if (apachebenchCheckbox) apachebenchCheckbox.addEventListener('change', toggleApachebenchSettings);
    const deathstarCheckbox = $('input[data-kind="bench"][value="deathstarbench"]');
    if (deathstarCheckbox) deathstarCheckbox.addEventListener('change', toggleDeathstarSettings);
    $('#deathstarWorkload').addEventListener('change', updateDeathstarWorkloadDescription);
    toggleSysbenchSettings();
    toggleIperf3Settings();
    togglePhoronixSettings();
    toggleApachebenchSettings();
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

function sysbenchOptions(selected) {
    if (!selected) return {...sysbenchDefaults, workloads: [...sysbenchDefaults.workloads]};
    return {
        workloads: $$('input[data-kind="sysbench-workload"]:checked')
            .map(input => input.value),
    };
}

function sysbenchOptionsFromPlan(plan) {
    const benchmarks = plan.benchmarks || [];
    const legacyWorkloads = benchmarks
        .map(benchmark => legacySysbenchWorkloads[benchmark])
        .filter(Boolean);
    const selected = benchmarks.includes('sysbench') || legacyWorkloads.length;
    if (!selected) {
        return {...sysbenchDefaults, workloads: [...sysbenchDefaults.workloads]};
    }
    return {
        workloads: [...new Set([
            ...(plan.sysbench?.workloads || []),
            ...legacyWorkloads,
        ])],
    };
}

function iperf3Options(selected) {
    if (!selected) return {...iperf3Defaults, protocols: [...iperf3Defaults.protocols]};
    return {
        protocols: $$('input[data-kind="iperf3-protocol"]:checked')
            .map(input => input.value),
    };
}

function iperf3OptionsFromPlan(plan) {
    const benchmarks = plan.benchmarks || [];
    const legacyProtocols = benchmarks
        .map(benchmark => legacyIperf3Protocols[benchmark])
        .filter(Boolean);
    const selected = benchmarks.includes('iperf3') || legacyProtocols.length;
    if (!selected) {
        return {...iperf3Defaults, protocols: [...iperf3Defaults.protocols]};
    }
    const savedProtocols = Array.isArray(plan.iperf3?.protocols)
        ? plan.iperf3.protocols
        : [];
    const protocols = [...new Set([...savedProtocols, ...legacyProtocols])];
    return {
        protocols: protocols.length ? protocols : [...iperf3Defaults.protocols],
    };
}

function phoronixOptions(selected) {
    if (!selected) return {...phoronixDefaults, profiles: [...phoronixDefaults.profiles]};
    return {
        profiles: $$('input[data-kind="phoronix-profile"]:checked')
            .map(input => input.value),
    };
}

function phoronixOptionsFromPlan(plan) {
    if (!(plan.benchmarks || []).includes('phoronix')) {
        return {...phoronixDefaults, profiles: [...phoronixDefaults.profiles]};
    }
    const profiles = Array.isArray(plan.phoronix?.profiles)
        ? plan.phoronix.profiles
        : [];
    return {
        profiles: profiles.length ? profiles : [...phoronixDefaults.profiles],
    };
}

function apachebenchOptions(selected) {
    if (!selected) {
        return {
            ...apachebenchDefaults,
            workloads: [...apachebenchDefaults.workloads],
        };
    }
    return {
        workloads: $$('input[data-kind="apachebench-workload"]:checked')
            .map(input => input.value),
        request_count: Number($('#apachebenchRequestCount').value),
        concurrency: Number($('#apachebenchConcurrency').value),
        response_size_kib: Number($('#apachebenchResponseSize').value),
        warmup_requests: Number($('#apachebenchWarmupRequests').value),
        trials: Number($('#apachebenchTrials').value),
    };
}

function apachebenchOptionsFromPlan(plan) {
    if (!(plan.benchmarks || []).includes('apachebench')) {
        return {
            ...apachebenchDefaults,
            workloads: [...apachebenchDefaults.workloads],
        };
    }
    const saved = plan.apachebench || {};
    const workloads = Array.isArray(saved.workloads) ? saved.workloads : [];
    return {
        ...apachebenchDefaults,
        ...saved,
        workloads: workloads.length
            ? workloads
            : [...apachebenchDefaults.workloads],
    };
}

$('#planForm').addEventListener('submit', async event => {
    event.preventDefault();
    const selected = kind => $$(`input[data-kind=${kind}]:checked`).map(input => input.value);
    const selectedBenchmarks = selected('bench');
    const sysbench = sysbenchOptions(selectedBenchmarks.includes('sysbench'));
    const iperf3 = iperf3Options(selectedBenchmarks.includes('iperf3'));
    const phoronix = phoronixOptions(selectedBenchmarks.includes('phoronix'));
    const apachebench = apachebenchOptions(selectedBenchmarks.includes('apachebench'));
    const deathstarbench = deathstarOptions(selectedBenchmarks.includes('deathstarbench'));
    if (selectedBenchmarks.includes('sysbench') && !sysbench.workloads.length) {
        alert('Select at least one Sysbench workload.');
        return;
    }
    if (selectedBenchmarks.includes('iperf3') && !iperf3.protocols.length) {
        alert('Select at least one iperf3 protocol.');
        return;
    }
    if (selectedBenchmarks.includes('phoronix') && !phoronix.profiles.length) {
        alert('Select at least one Phoronix test profile.');
        return;
    }
    if (selectedBenchmarks.includes('apachebench') && !apachebench.workloads.length) {
        alert('Select at least one ApacheBench connection mode.');
        return;
    }
    if (
        selectedBenchmarks.includes('apachebench')
        && apachebench.concurrency > apachebench.request_count
    ) {
        alert('ApacheBench concurrency cannot exceed the measured request count.');
        return;
    }
    if (
        selectedBenchmarks.includes('sysbench')
        && sysbench.workloads.includes('fileio')
        && !$('#additional').checked
    ) {
        alert('Sysbench file I/O requires the additional /data volume.');
        return;
    }
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
        sysbench,
        iperf3,
        phoronix,
        apachebench,
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
    setSysbenchValues();
    setIperf3Values();
    setPhoronixValues();
    setApachebenchValues();
    setDeathstarValues();
    toggleSysbenchSettings();
    toggleIperf3Settings();
    togglePhoronixSettings();
    toggleApachebenchSettings();
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
    const planBenchmarks = plan.benchmarks || [];
    const hasLegacySysbench = planBenchmarks.some(
        benchmark => Boolean(legacySysbenchWorkloads[benchmark]),
    );
    const hasLegacyIperf3 = planBenchmarks.some(
        benchmark => Boolean(legacyIperf3Protocols[benchmark]),
    );
    $$('input[data-kind=bench]').forEach(input => {
        input.checked = planBenchmarks.includes(input.value)
            || (input.value === 'sysbench' && hasLegacySysbench)
            || (input.value === 'iperf3' && hasLegacyIperf3);
    });
    $$('input[data-kind=llm]').forEach(input => {
        input.checked = (plan.llm_benchmarks || []).includes(input.value);
    });
    setSysbenchValues(sysbenchOptionsFromPlan(plan));
    setIperf3Values(iperf3OptionsFromPlan(plan));
    setPhoronixValues(phoronixOptionsFromPlan(plan));
    setApachebenchValues(apachebenchOptionsFromPlan(plan));
    setDeathstarValues(plan.deathstarbench || deathstarDefaults);
    toggleSysbenchSettings();
    toggleIperf3Settings();
    togglePhoronixSettings();
    toggleApachebenchSettings();
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
