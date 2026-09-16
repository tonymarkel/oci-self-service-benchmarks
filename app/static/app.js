let jobId;
let poller;
let shapes = [];
let sshDefaults = { configured: false };
let sysbenchWorkloads = [];
let iperf3Protocols = [];
let phoronixProfiles = [];
let deathstarWorkloads = [];
let apachebenchWorkloads = [];
let currentJobPrivateKey = '';
let providerBootstrap = {};
let providerDefinitions = new Map();
let discoveryGeneration = 0;
let discoveryLoading = false;
let liveStopPending = false;

const terminalStatuses = ['complete', 'destroyed', 'failed', 'reported', 'cleanup_failed', 'interrupted'];
const liveStoppableStatuses = ['queued', 'provisioning', 'testing', 'reporting'];
const deathstarDefaults = {
    workload: 'media_microservices',
    warmup_seconds: 30,
    duration_seconds: 60,
    threads: 4,
    connections: 64,
    request_rate: 100,
};
const sysbenchDefaults = { workloads: ['cpu'] };
const iperf3Defaults = { protocols: ['tcp'] };
const phoronixDefaults = { profiles: ['compress_7zip'] };
const apachebenchDefaults = {
    workloads: ['new_connections', 'keep_alive'],
    request_count: 500000,
    concurrency: 100,
    response_size_kib: 64,
    warmup_requests: 10000,
    trials: 3,
};
const GCP_C4A_DATA_SIZE_GB = 100;
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
    character => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[character]),
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
        const [catalog, defaults, providerCatalog] = await Promise.all([
            api('/api/catalog'),
            api('/api/config/ssh-defaults'),
            api('/api/providers'),
        ]);
        sshDefaults = defaults;
        renderProviders(providerCatalog.items || []);
        renderCatalog(catalog);
        perfOptions('#bootPerf');
        perfOptions('#dataPerf');
        applySshDefaults();
        const resumed = await resumeLastJob();
        if (!resumed) await loadProvider();
    } catch (error) {
        alert(error.message);
    }
}

function currentProvider() {
    return $('#provider').value || 'oci';
}

function providerLabel(provider = currentProvider()) {
    const definition = providerDefinitions.get(provider);
    return definition?.short_name || definition?.name || provider.toUpperCase();
}

function renderProviders(items) {
    if (!Array.isArray(items) || !items.length) {
        throw Error('No cloud providers are available.');
    }
    const selected = $('#provider').value || 'oci';
    providerDefinitions = new Map(items.map(item => [item.id, item]));
    $('#provider').innerHTML = items.map(item =>
        `<option value="${escape(item.id)}">${escape(item.name)}</option>`,
    ).join('');
    $('#provider').value = providerDefinitions.has(selected)
        ? selected
        : (providerDefinitions.has('oci') ? 'oci' : items[0].id);
}

function providerCapabilitySet(name, provider = currentProvider()) {
    const values = providerDefinitions.get(provider)?.capabilities?.[name];
    return new Set(Array.isArray(values) ? values : []);
}

function clearShapeChoices(message = '') {
    shapes = [];
    $('#shape').value = '';
    $('#shapeList').innerHTML = '';
    $('#shapeMeta').textContent = message;
}

function setDiscoveryLoading(loading) {
    discoveryLoading = loading;
    $('#planForm').setAttribute('aria-busy', String(loading));
    $('#shape').disabled = loading;
    const submit = $('#planForm button');
    if (submit) submit.disabled = loading;
}

function beginDiscovery() {
    const generation = ++discoveryGeneration;
    clearShapeChoices('Loading available compute options…');
    setDiscoveryLoading(true);
    return generation;
}

function discoveryIsCurrent(generation) {
    return generation === discoveryGeneration;
}

function finishDiscovery(generation) {
    if (discoveryIsCurrent(generation)) setDiscoveryLoading(false);
}

function failCurrentDiscovery(generation) {
    if (!discoveryIsCurrent(generation)) return;
    clearShapeChoices('Compute options could not be loaded.');
}

function setProviderOnlyElement(selector, visible) {
    const element = $(selector);
    if (!element) return;
    element.hidden = !visible;
    element.querySelectorAll('input, select, textarea').forEach(field => {
        field.disabled = !visible;
    });
}

function applyProviderBenchmarkSupport() {
    const supportedBenchmarks = providerCapabilitySet('benchmarks');
    const supportedLlmBenchmarks = providerCapabilitySet('llm_benchmarks');
    const supportedSysbenchWorkloads = providerCapabilitySet('sysbench_workloads');
    const supportedIperf3Protocols = providerCapabilitySet('iperf3_protocols');
    $$('input[data-kind="bench"]').forEach(input => {
        const supported = supportedBenchmarks.has(input.value);
        input.disabled = !supported;
        input.closest('label')?.classList.toggle('unavailable', !supported);
        if (!supported) input.checked = false;
    });
    $$('input[data-kind="llm"]').forEach(input => {
        const supported = supportedLlmBenchmarks.has(input.value);
        input.disabled = !supported;
        input.closest('label')?.classList.toggle('unavailable', !supported);
        if (!supported) input.checked = false;
    });
    $$('input[data-kind="sysbench-workload"]').forEach(input => {
        const supported = supportedSysbenchWorkloads.has(input.value);
        input.disabled = !supported;
        input.closest('label')?.classList.toggle('unavailable', !supported);
        if (!supported) input.checked = false;
    });
    $$('input[data-kind="iperf3-protocol"]').forEach(input => {
        const supported = supportedIperf3Protocols.has(input.value);
        input.disabled = !supported;
        input.closest('label')?.classList.toggle('unavailable', !supported);
        if (!supported) input.checked = false;
    });
    const sysbenchSelected = $('input[data-kind="bench"][value="sysbench"]')?.checked;
    const selectedWorkload = $('input[data-kind="sysbench-workload"]:checked:not(:disabled)');
    const defaultWorkload = $$('#sysbenchWorkloads input').find(
        input => supportedSysbenchWorkloads.has(input.value),
    );
    if (sysbenchSelected && !selectedWorkload && defaultWorkload) {
        defaultWorkload.checked = true;
    }
    const iperf3Selected = $('input[data-kind="bench"][value="iperf3"]')?.checked;
    const selectedProtocol = $('input[data-kind="iperf3-protocol"]:checked:not(:disabled)');
    const defaultProtocol = $$('#iperf3Protocols input').find(
        input => supportedIperf3Protocols.has(input.value),
    );
    if (iperf3Selected && !selectedProtocol && defaultProtocol) {
        defaultProtocol.checked = true;
    }
    toggleSysbenchSettings();
    toggleIperf3Settings();
    togglePhoronixSettings();
    toggleApachebenchSettings();
    toggleDeathstarSettings();
}

function gcpDiskLabel(diskType) {
    const labels = {
        'hyperdisk-balanced': 'Hyperdisk Balanced',
        'pd-balanced': 'Balanced Persistent Disk (pd-balanced)',
    };
    return labels[diskType] || diskType || 'provider-selected storage';
}

function gcpNetworkInterfaceLabel(interfaceType) {
    const labels = {
        GVNIC: 'gVNIC',
        VIRTIO_NET: 'VirtIO-net',
    };
    return labels[interfaceType] || interfaceType || 'provider-selected NIC';
}

function applyGcpMachineStorageDefault(shape) {
    const dataSize = $('#dataSize');
    const usesHyperdisk = shape?.disk_type === 'hyperdisk-balanced';
    if (
        usesHyperdisk
        && dataSize.dataset.userEdited !== 'true'
        && Number(dataSize.value) === Number(dataSize.defaultValue)
    ) {
        dataSize.value = String(GCP_C4A_DATA_SIZE_GB);
        dataSize.dataset.gcpC4aDefaultApplied = 'true';
        return;
    }
    if (
        !usesHyperdisk
        && dataSize.dataset.gcpC4aDefaultApplied === 'true'
        && dataSize.dataset.userEdited !== 'true'
    ) {
        dataSize.value = dataSize.defaultValue;
        delete dataSize.dataset.gcpC4aDefaultApplied;
    }
}

function updateGcpStorageHint(shape = null) {
    const hint = $('#gcpStorageHint');
    if (currentProvider() !== 'gcp') return;
    applyGcpMachineStorageDefault(shape);
    if (!shape) {
        hint.textContent =
            'GCP /data volumes use pd-balanced by default; C4A uses Hyperdisk Balanced. ' +
            'Select a machine type to see the exact disk and network profile. ' +
            'These provider-required settings are recorded in the report.';
        return;
    }
    const diskLabel = gcpDiskLabel(shape.disk_type);
    const interfaceLabel = gcpNetworkInterfaceLabel(shape.network_interface_type);
    if (shape.disk_type === 'hyperdisk-balanced') {
        hint.textContent =
            `${shape.shape} uses ${diskLabel} for its boot and optional /data volumes ` +
            'at the benchmark baseline of 3,000 IOPS and 140 MiB/s, plus ' +
            `${interfaceLabel}. The app derives these required settings and records ` +
            'the provisioned values in every benchmark result. An untouched /data ' +
            `size defaults to ${GCP_C4A_DATA_SIZE_GB} GiB for C4A; launch still ` +
            'depends on remaining regional C4A vCPU and Hyperdisk capacity/performance ' +
            'quota and on zonal capacity.';
        return;
    }
    hint.textContent =
        `${shape.shape} uses ${diskLabel} for its boot and optional /data volumes, ` +
        `plus ${interfaceLabel}. Disk performance scales with volume size and the ` +
        "selected VM's vCPU count; there is no separate gp3-style IOPS control.";
}

function applyProviderUi() {
    const provider = currentProvider();
    const aws = provider === 'aws';
    const gcp = provider === 'gcp';
    const azure = provider === 'azure';
    const oci = provider === 'oci';
    const fixedCapacity = aws || gcp;
    const providerFixedCapacity = fixedCapacity || azure;
    setProviderOnlyElement('#compartmentField', oci);
    setProviderOnlyElement('#adField', oci);
    setProviderOnlyElement('#fdField', oci);
    setProviderOnlyElement('#ociComputeOptions', oci);
    setProviderOnlyElement('#bootPerfField', oci);
    setProviderOnlyElement('#dataPerfField', oci);
    setProviderOnlyElement('#mountField', oci);
    $('#awsStorageHint').hidden = !aws;
    $('#gcpStorageHint').hidden = !gcp;
    $('#azureStorageHint').hidden = !azure;
    if (gcp) {
        updateGcpStorageHint();
    } else {
        applyGcpMachineStorageDefault(null);
    }
    setProviderOnlyElement('#awsProfileField', aws);
    setProviderOnlyElement('#gcpProjectField', gcp);
    setProviderOnlyElement('#gcpZoneField', gcp);
    setProviderOnlyElement('#azureSubscriptionField', azure);
    setProviderOnlyElement('#azureZoneField', azure);
    $('#shapeLabel').textContent = aws
        ? 'EC2 Instance Type'
        : (azure
            ? 'Azure VM Size'
            : (gcp ? 'Compute Engine Machine Type' : 'Compute Shape'));
    $('#cpuLabel').textContent = providerFixedCapacity ? 'vCPUs' : 'OCPUs';
    $('#shape').placeholder = aws
        ? 'Search available EC2 instance types'
        : (azure
            ? 'Search VM sizes available in this zone'
            : (gcp
                ? 'Search machine types available in this zone'
                : 'Select a region and AD to load shapes'));
    $('#key').placeholder = aws
        ? 'Choose a file or paste the private key used to connect as ec2-user.'
        : (gcp || azure
            ? 'Choose a file or paste the private key used to connect as benchmark.'
            : 'Choose a file or paste the private key used to connect as opc.');
    $('#ocpus').readOnly = fixedCapacity || azure;
    $('#memory').readOnly = fixedCapacity || azure;
    $('#guestOsText').textContent = aws
        ? 'AWS runs use the latest Amazon Linux 2023 AMI.'
        : (azure
            ? 'Azure runs use a pinned Rocky Linux 9 image.'
            : (gcp
                ? 'GCP runs use the latest standard Rocky Linux 9 image.'
                : 'OCI runs use Oracle Linux.'));
    $('#keyPairHint').textContent = aws
        ? 'The key pair is checked before any AWS resources are created. AWS accepts RSA or Ed25519 for this flow. The public key configured in .env is imported for this run, and key material is held in memory only while the job runs.'
        : (azure
            ? 'The key pair is checked before any Azure resources are created. The public key configured in .env is installed for the benchmark user on this run only, and key material is held in memory only while the job runs.'
            : (gcp
                ? 'The key pair is checked before any Google Cloud resources are created. The public key configured in .env is installed for the benchmark user on this run only, and key material is held in memory only while the job runs.'
                : 'The key pair is checked before any OCI resources are created. OpenSSH RSA, ECDSA, and Ed25519 keys are supported and held in memory only while the job runs.'));
    $('#networkBenchmarkHint').textContent = aws
        ? 'AWS iperf3 TCP, UDP, and SCTP tests use a second same-type EC2 instance in the same Availability Zone and send benchmark traffic over private VPC addresses.'
        : (azure
            ? 'Azure iperf3 tests use a second same-size VM in the same zone and send benchmark traffic over private VNet addresses. The available protocols come from the Azure provider capabilities.'
            : (gcp
                ? 'GCP iperf3 TCP, UDP, and SCTP tests use a second same-type Compute Engine VM in the same zone and send benchmark traffic over private VPC addresses.'
                : 'Network tests use a temporary private peer VM so results measure OCI VCN traffic—not the public Internet.'));
    const privateNetwork = aws
        ? 'private AWS VPC address'
        : (azure
            ? 'private Azure VNet address'
            : (gcp ? 'private Google Cloud VPC address' : 'private OCI VCN address'));
    $('#deathstarDeploymentHint').textContent =
        'The selected microservices workload runs with native Podman and podman-compose. ' +
        `A separate fixed-size x86 load-generator VM sends traffic to the benchmark VM's ${privateNetwork}.`;
    $('#apachebenchDeploymentHint').textContent =
        'The benchmark VM serves fixed-size responses while a separate fixed-size x86 ' +
        `load-generator VM runs Apache HTTP Server Benchmarking Tool traffic over a ${privateNetwork}. ` +
        'Concurrency cannot exceed the measured request count.';
    $('#phoronixCompatibilityHint').textContent = aws
        ? 'Profiles are pinned and limited to tests validated for both x86_64 and Arm64 Amazon Linux 2023 instances.'
        : (gcp || azure
            ? 'Profiles are pinned to the curated x86_64 and Arm64-compatible set for Rocky Linux 9.'
            : 'Profiles are pinned and limited to tests validated for both x86_64 and AArch64 Oracle Linux instances.');
    $('#providerNotice').hidden = oci;
    $('#providerNotice').textContent = aws
        ? 'AWS: a fixed EC2 instance type determines vCPU and memory. SSH TCP/22 is open to 0.0.0.0/0; destroy retained resources promptly. OCI-only placement, security, networking, and storage performance controls are hidden.'
        : (azure
            ? 'Azure: a fixed VM size determines vCPU and memory. The local Azure CLI session selects the caller and subscription. Each run uses a dedicated resource group. SSH TCP/22 is open to 0.0.0.0/0; destroy retained resources promptly. OCI-only placement, security, networking, and storage performance controls are hidden.'
            : (gcp
                ? 'GCP: a fixed Compute Engine machine type determines vCPU and memory. Local Application Default Credentials select the caller and project. SSH TCP/22 is open to 0.0.0.0/0; destroy retained resources promptly. OCI-only security, networking, and storage performance controls are hidden.'
                : ''));
    if (providerFixedCapacity) {
        if ($('#additional').dataset.ociChecked === undefined) {
            $('#additional').dataset.ociChecked = String($('#additional').checked);
            $('#additional').checked = false;
        }
    } else if ($('#additional').dataset.ociChecked) {
        $('#additional').checked = $('#additional').dataset.ociChecked === 'true';
        delete $('#additional').dataset.ociChecked;
    }
    applyProviderBenchmarkSupport();
}

function normalizedRegions(boot) {
    return (boot.regions || []).map(region => (
        typeof region === 'string'
            ? region
            : region.name || region.region_name || region.region
    )).filter(Boolean);
}

async function loadProvider(preferredRegion = null) {
    const generation = beginDiscovery();
    const provider = currentProvider();
    applyProviderUi();
    const profile = ($('#awsProfile').value || 'default').trim();
    const gcpProject = $('#gcpProject').value.trim();
    const azureSubscription = $('#azureSubscription').value.trim();
    let url = '/api/oci/bootstrap';
    if (provider === 'aws') {
        url = `/api/providers/aws/bootstrap?profile=${encodeURIComponent(profile)}`;
    } else if (provider === 'gcp') {
        const query = new URLSearchParams();
        if (gcpProject) query.set('project_id', gcpProject);
        url = `/api/providers/gcp/bootstrap${query.size ? `?${query}` : ''}`;
    } else if (provider === 'azure') {
        const query = new URLSearchParams();
        if (azureSubscription) query.set('subscription_id', azureSubscription);
        url = `/api/providers/azure/bootstrap${query.size ? `?${query}` : ''}`;
    }
    try {
        const boot = await api(url);
        if (
            !discoveryIsCurrent(generation)
            || currentProvider() !== provider
            || (
                provider === 'aws'
                && ($('#awsProfile').value || 'default').trim() !== profile
            )
            || (
                provider === 'gcp'
                && $('#gcpProject').value.trim() !== gcpProject
            )
            || (
                provider === 'azure'
                && $('#azureSubscription').value.trim() !== azureSubscription
            )
        ) return false;
        providerBootstrap[provider] = boot;
        const regions = normalizedRegions(boot);
        const fallbackRegion = provider === 'aws' ? 'us-east-2'
            : (provider === 'gcp' ? 'us-east1'
                : (provider === 'azure' ? 'eastus2' : boot.default_region));
        const selectedRegion = regions.includes(preferredRegion)
            ? preferredRegion
            : (regions.includes(boot.default_region)
                ? boot.default_region
                : (regions.includes(fallbackRegion) ? fallbackRegion : regions[0] || fallbackRegion));
        $('#region').innerHTML = regions.map(region =>
            `<option ${region === selectedRegion ? 'selected' : ''}>${escape(region)}</option>`,
        ).join('');
        if (!$('#region').value && selectedRegion) {
            $('#region').innerHTML = `<option selected>${escape(selectedRegion)}</option>`;
        }
        if (provider === 'aws') {
            const identity = boot.account_id
                ? ` · account ${boot.account_id}`
                : '';
            $('#localContext').textContent = `Accesses AWS through CLI profile ${profile}${identity}`;
            return await loadShapes(generation);
        }
        if (provider === 'gcp') {
            $('#gcpProject').value = boot.project_id || boot.default_project || gcpProject;
            const principal = boot.principal ? ` · ${boot.principal}` : '';
            $('#localContext').textContent =
                `Accesses Google Cloud through ADC · project ${$('#gcpProject').value}${principal}`;
            return await placement(generation);
        }
        if (provider === 'azure') {
            $('#azureSubscription').value =
                boot.subscription_id || boot.default_subscription_id || azureSubscription;
            const subscriptionName = boot.subscription_name || boot.display_name || '';
            const subscription = subscriptionName
                ? `${subscriptionName} (${$('#azureSubscription').value})`
                : $('#azureSubscription').value;
            const tenant = boot.tenant_id ? ` · tenant ${boot.tenant_id}` : '';
            $('#localContext').textContent =
                `Accesses Azure through Azure CLI`;
            return await placement(generation);
        }
        $('#compartment').placeholder = `Defaults to ${boot.default_compartment}`;
        $('#localContext').textContent = 'Accesses OCI through your default profile';
        return await placement(generation);
    } catch (error) {
        if (!discoveryIsCurrent(generation)) return false;
        failCurrentDiscovery(generation);
        throw error;
    } finally {
        finishDiscovery(generation);
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
    const supported = providerCapabilitySet('sysbench_workloads');
    $$('#sysbenchSettings input').forEach(field => {
        field.disabled = !selected || !supported.has(field.value);
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
    const supported = providerCapabilitySet('iperf3_protocols');
    $$('#iperf3Settings input').forEach(field => {
        field.disabled = !selected || !supported.has(field.value);
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

async function placement(parentGeneration = null) {
    const ownsDiscovery = parentGeneration === null;
    const generation = ownsDiscovery ? beginDiscovery() : parentGeneration;
    const provider = currentProvider();
    try {
        if (provider === 'aws') {
            return await loadShapes(generation);
        }
        const region = $('#region').value;
        if (provider === 'gcp') {
            const project = $('#gcpProject').value.trim();
            const priorZone = $('#gcpZone').value;
            const data = await api(
                `/api/providers/gcp/placement?project_id=${encodeURIComponent(project)}` +
                `&region=${encodeURIComponent(region)}`,
            );
            if (
                !discoveryIsCurrent(generation)
                || currentProvider() !== provider
                || $('#region').value !== region
                || $('#gcpProject').value.trim() !== project
            ) return false;
            const zones = data.availability_zones || data.zones || [];
            const defaultZone = providerBootstrap.gcp?.default_zone;
            const selectedZone = zones.some(item => item.name === priorZone)
                ? priorZone
                : (zones.some(item => item.name === defaultZone)
                    ? defaultZone
                    : zones[0]?.name);
            $('#gcpZone').innerHTML = zones.map(item =>
                `<option value="${escape(item.name)}" ` +
                `${item.name === selectedZone ? 'selected' : ''}>${escape(item.name)}</option>`,
            ).join('');
            return await loadShapes(generation);
        }
        if (provider === 'azure') {
            const subscription = $('#azureSubscription').value.trim();
            const priorZone = $('#azureZone').value;
            const data = await api(
                `/api/providers/azure/placement?subscription_id=${encodeURIComponent(subscription)}` +
                `&region=${encodeURIComponent(region)}`,
            );
            if (
                !discoveryIsCurrent(generation)
                || currentProvider() !== provider
                || $('#region').value !== region
                || $('#azureSubscription').value.trim() !== subscription
            ) return false;
            const zones = (data.availability_zones || data.zones || []).map(item => (
                typeof item === 'string'
                    ? { name: item }
                    : { ...item, name: item.name || item.zone }
            )).filter(item => item.name);
            const defaultZone = providerBootstrap.azure?.default_zone;
            const selectedZone = zones.some(item => item.name === priorZone)
                ? priorZone
                : (zones.some(item => item.name === defaultZone)
                    ? defaultZone
                    : zones[0]?.name);
            $('#azureZone').innerHTML = zones.map(item =>
                `<option value="${escape(item.name)}" ` +
                `${item.name === selectedZone ? 'selected' : ''}>${escape(item.name)}</option>`,
            ).join('');
            return await loadShapes(generation);
        }
        const compartment = $('#compartment').value;
        const data = await api(
            `/api/oci/placement?region=${encodeURIComponent(region)}&compartment_id=${encodeURIComponent(compartment)}`,
        );
        if (
            !discoveryIsCurrent(generation)
            || currentProvider() !== provider
            || $('#region').value !== region
            || $('#compartment').value !== compartment
        ) return false;
        $('#ad').innerHTML = '<option value="">First available</option>' + data.availability_domains
            .map(item => `<option>${item.name}</option>`)
            .join('');
        window.placements = data.availability_domains;
        return await loadShapes(generation);
    } catch (error) {
        if (!discoveryIsCurrent(generation)) return false;
        failCurrentDiscovery(generation);
        throw error;
    } finally {
        if (ownsDiscovery) finishDiscovery(generation);
    }
}

function faultDomains() {
    const placementItem = (window.placements || []).find(item => item.name === $('#ad').value);
    $('#fd').innerHTML = '<option value="">Let OCI choose</option>' + (placementItem?.fault_domains || [])
        .map(item => `<option>${item}</option>`)
        .join('');
}

async function loadShapes(parentGeneration = null) {
    const ownsDiscovery = parentGeneration === null;
    const generation = ownsDiscovery ? beginDiscovery() : parentGeneration;
    const provider = currentProvider();
    const aws = provider === 'aws';
    const gcp = provider === 'gcp';
    const azure = provider === 'azure';
    const fixedCapacity = aws || gcp;
    const providerFixedCapacity = fixedCapacity || azure;
    const region = $('#region').value;
    const profile = ($('#awsProfile').value || 'default').trim();
    const gcpProject = $('#gcpProject').value.trim();
    const gcpZone = $('#gcpZone').value;
    const azureSubscription = $('#azureSubscription').value.trim();
    const azureZone = $('#azureZone').value;
    const compartment = $('#compartment').value;
    const availabilityDomain = $('#ad').value;
    const query = new URLSearchParams({ region });
    let endpoint = '/api/oci/shapes';
    if (aws) {
        query.set('profile', profile);
        endpoint = '/api/providers/aws/instance-types';
    } else if (gcp) {
        query.delete('region');
        query.set('project_id', gcpProject);
        query.set('zone', gcpZone);
        endpoint = '/api/providers/gcp/machine-types';
    } else if (azure) {
        query.set('subscription_id', azureSubscription);
        query.set('zone', azureZone);
        endpoint = '/api/providers/azure/vm-sizes';
    } else {
        query.set('compartment_id', compartment);
        query.set('availability_domain', availabilityDomain);
    }
    try {
        const data = await api(`${endpoint}?${query}`);
        if (
            !discoveryIsCurrent(generation)
            || currentProvider() !== provider
            || $('#region').value !== region
            || (aws && ($('#awsProfile').value || 'default').trim() !== profile)
            || (gcp && $('#gcpProject').value.trim() !== gcpProject)
            || (gcp && $('#gcpZone').value !== gcpZone)
            || (azure && $('#azureSubscription').value.trim() !== azureSubscription)
            || (azure && $('#azureZone').value !== azureZone)
            || (!providerFixedCapacity && $('#compartment').value !== compartment)
            || (!providerFixedCapacity && $('#ad').value !== availabilityDomain)
        ) return false;
        shapes = (data.items || []).map(item => ({
            ...item,
            shape: item.shape || item.instance_type || item.vm_size || item.size || item.name,
            ocpus: item.ocpus ?? item.vcpus ?? item.vcpu,
            memory_gb: item.memory_gb ?? item.memory_gib,
            flexible: providerFixedCapacity ? false : Boolean(item.flexible),
        })).filter(item => item.shape);
        // Firefox substitutes an option's text for its value while Chromium
        // renders both. Value-only options keep shape names consistent.
        $('#shapeList').innerHTML = shapes.map(item =>
            `<option value="${escape(item.shape)}"></option>`,
        ).join('');
        $('#shapeMeta').textContent = shapes.length
            ? ''
            : `No ${azure ? 'VM sizes' : (providerFixedCapacity ? 'machine types' : 'shapes')} are available for this selection.`;
        if (gcp) {
            updateGcpStorageHint(
                shapes.find(item => item.shape === $('#shape').value),
            );
        }
        return true;
    } catch (error) {
        if (!discoveryIsCurrent(generation)) return false;
        failCurrentDiscovery(generation);
        throw error;
    } finally {
        if (ownsDiscovery) finishDiscovery(generation);
    }
}

async function handleDiscovery(operation) {
    try {
        await operation();
    } catch (error) {
        alert(error.message);
    }
}

$('#provider').addEventListener('change', async () => {
    await handleDiscovery(() => loadProvider());
});
$('#awsProfile').addEventListener('change', async () => {
    if (currentProvider() !== 'aws') return;
    await handleDiscovery(() => loadProvider($('#region').value || 'us-east-2'));
});
$('#gcpProject').addEventListener('change', async () => {
    if (currentProvider() !== 'gcp') return;
    await handleDiscovery(() => loadProvider($('#region').value || 'us-east1'));
});
$('#azureSubscription').addEventListener('change', async () => {
    if (currentProvider() !== 'azure') return;
    await handleDiscovery(() => loadProvider($('#region').value || 'eastus2'));
});
$('#region').addEventListener('change', async () => {
    await handleDiscovery(() => placement());
});
$('#gcpZone').addEventListener('change', () => {
    if (currentProvider() === 'gcp') handleDiscovery(() => loadShapes());
});
$('#azureZone').addEventListener('change', () => {
    if (currentProvider() === 'azure') handleDiscovery(() => loadShapes());
});
$('#compartment').addEventListener('change', () => {
    if (currentProvider() === 'oci') handleDiscovery(() => placement());
});
$('#ad').addEventListener('change', () => {
    if (currentProvider() !== 'oci') return;
    faultDomains();
    handleDiscovery(() => loadShapes());
});
function updateSelectedShape() {
    const shape = shapes.find(item => item.shape === $('#shape').value);
    const provider = currentProvider();
    const aws = provider === 'aws';
    const gcp = provider === 'gcp';
    const azure = provider === 'azure';
    const fixedCapacity = aws || gcp;
    const providerFixedCapacity = fixedCapacity || azure;
    $('#shapeMeta').textContent = shape
        ? `${shape.flexible ? 'Flexible shape. ' : ''}Listed capacity: ` +
        `${shape.ocpus || 'varies'} ${providerFixedCapacity ? 'vCPUs' : 'OCPUs'} / ` +
        `${shape.memory_gb || 'varies'} GB` +
        `${shape.architecture ? ` / ${shape.architecture}` : ''}` +
        `${gcp && shape.disk_type ? ` / ${gcpDiskLabel(shape.disk_type)}` : ''}` +
        `${gcp && shape.network_interface_type ? ` / ${gcpNetworkInterfaceLabel(shape.network_interface_type)}` : ''}` +
        `${aws && shape.burstable ? ' / burstable performance' : ''}` +
        `${aws && shape.bare_metal ? ' / bare metal' : ''}`
        : `Select a valid ${aws ? 'instance type' : (azure ? 'VM size' : (gcp ? 'machine type' : 'shape'))}`;
    if (gcp) updateGcpStorageHint(shape);
    if (shape && (providerFixedCapacity || !shape.flexible)) {
        $('#ocpus').value = shape.ocpus;
        $('#memory').value = shape.memory_gb;
    }
}

// A datalist choice emits `input` before it emits `change`. Updating on both
// keeps fixed cloud capacity visible as soon as the user chooses an exact type.
$('#shape').addEventListener('input', updateSelectedShape);
$('#shape').addEventListener('change', updateSelectedShape);
$('#dataSize').addEventListener('input', () => {
    $('#dataSize').dataset.userEdited = 'true';
    delete $('#dataSize').dataset.gcpC4aDefaultApplied;
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
    if (!selected) return { ...deathstarDefaults };
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
    if (!selected) return { ...sysbenchDefaults, workloads: [...sysbenchDefaults.workloads] };
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
        return { ...sysbenchDefaults, workloads: [...sysbenchDefaults.workloads] };
    }
    return {
        workloads: [...new Set([
            ...(plan.sysbench?.workloads || []),
            ...legacyWorkloads,
        ])],
    };
}

function iperf3Options(selected) {
    if (!selected) return { ...iperf3Defaults, protocols: [...iperf3Defaults.protocols] };
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
        return { ...iperf3Defaults, protocols: [...iperf3Defaults.protocols] };
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
    if (!selected) return { ...phoronixDefaults, profiles: [...phoronixDefaults.profiles] };
    return {
        profiles: $$('input[data-kind="phoronix-profile"]:checked')
            .map(input => input.value),
    };
}

function phoronixOptionsFromPlan(plan) {
    if (!(plan.benchmarks || []).includes('phoronix')) {
        return { ...phoronixDefaults, profiles: [...phoronixDefaults.profiles] };
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
    if (discoveryLoading) {
        alert('Wait for the current cloud discovery request to finish.');
        return;
    }
    const provider = currentProvider();
    const aws = provider === 'aws';
    const gcp = provider === 'gcp';
    const azure = provider === 'azure';
    const fixedCapacity = aws || gcp;
    const providerFixedCapacity = fixedCapacity || azure;
    const selectedShape = shapes.find(item => item.shape === $('#shape').value);
    if (providerFixedCapacity && !selectedShape) {
        alert(aws
            ? 'Select a valid EC2 instance type from the searchable list.'
            : (azure
                ? 'Select a valid Azure VM size from the searchable list.'
                : 'Select a valid Compute Engine machine type from the searchable list.'));
        return;
    }
    if (providerFixedCapacity) {
        $('#ocpus').value = selectedShape.ocpus;
        $('#memory').value = selectedShape.memory_gb;
    }
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
    if (selectedBenchmarks.includes('fio') && !$('#additional').checked) {
        alert('fio requires the additional /data volume.');
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
        provider,
        aws_profile: ($('#awsProfile').value || 'default').trim(),
        gcp_project_id: gcp ? $('#gcpProject').value.trim() : null,
        gcp_zone: gcp ? $('#gcpZone').value : null,
        azure_subscription_id: azure ? $('#azureSubscription').value.trim() : null,
        azure_zone: azure ? $('#azureZone').value : null,
        region: $('#region').value,
        compartment_id: provider === 'oci' ? ($('#compartment').value || null) : null,
        availability_domain: provider === 'oci' ? ($('#ad').value || null) : null,
        fault_domain: provider === 'oci' ? ($('#fd').value || null) : null,
        shape: $('#shape').value,
        ocpus: Number($('#ocpus').value),
        memory_gb: Number($('#memory').value),
        ssh_private_key: $('#key').value,
        ssh_public_key: $('#publicKey').value,
        ssh_key_passphrase: $('#keyPassphrase').value || null,
        security: {
            mode: provider === 'oci' ? $('input[name=security]:checked').value : 'none',
            secure_boot: provider === 'oci' && $('#secureBoot').checked,
            measured_boot: provider === 'oci' && $('#measuredBoot').checked,
            trusted_platform_module: provider === 'oci' && $('#tpm').checked,
        },
        networking: provider === 'oci'
            ? $('input[name=networking]:checked').value
            : 'paravirtualized',
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
            headers: { 'Content-Type': 'application/json' },
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
    const statuses = (job.results || []).map(result => result.status).filter(Boolean);
    const hasCompletedResult = statuses.includes('completed');
    if (!hasCompletedResult) return false;
    if (job.benchmark_status) return job.benchmark_status !== 'complete';
    return statuses.some(status => status !== 'completed');
}

function isFailedBenchmarkReport(job) {
    if (isPartialReport(job)) return false;
    const statuses = (job.results || []).map(result => result.status).filter(Boolean);
    if (job.benchmark_status) return job.benchmark_status === 'failed';
    return statuses.includes('failed') || (!statuses.length && job.status === 'failed');
}

function recordedResourceEntries(job) {
    const metadataIds = new Set([
        'aws_account_id',
        'azure_image_id',
        'azure_peer_image_id',
        'azure_subscription_id',
        'azure_tenant_id',
        'gcp_compute_project_id',
        'gcp_peer_image_id',
        'gcp_project_id',
        'image_id',
    ]);
    return Object.entries(job.resources || {}).filter(([key, value]) => (
        value !== null
        && value !== undefined
        && value !== ''
        && (
            (key.endsWith('_id') && !metadataIds.has(key))
            || key.endsWith('_ip')
            || key === 'instance_type'
            || key === 'availability_zone'
        )
    ));
}

function renderLiveStopAction(job) {
    const panel = $('#liveRunStop');
    const button = $('#stopAndDestroy');
    const status = $('#liveRunStopStatus');
    const lifecycle = job.status || '';
    if (terminalStatuses.includes(lifecycle)) liveStopPending = false;
    const activelyStoppable = job.live !== false && liveStoppableStatuses.includes(lifecycle);
    const cancelling = lifecycle === 'cancelling' || (
        liveStopPending && liveStoppableStatuses.includes(lifecycle)
    );
    const destroyingAfterStop = lifecycle === 'destroying' && (
        liveStopPending || job.benchmark_interrupted === true
    );
    panel.hidden = !(activelyStoppable || cancelling || destroyingAfterStop);
    button.disabled = cancelling || destroyingAfterStop;
    if (destroyingAfterStop) {
        button.textContent = 'Destroying infrastructure…';
        status.textContent = 'The benchmark has stopped. Recorded infrastructure cleanup is in progress.';
    } else if (cancelling) {
        button.textContent = 'Stopping run…';
        status.textContent = 'Cancellation requested. Waiting for active benchmark work to stop before cleanup.';
    } else {
        button.textContent = 'Stop run and destroy infrastructure';
        status.textContent = 'Stopping discards unfinished benchmark work and destroys the run\'s recorded cloud infrastructure.';
    }
}

function renderJobProgress(job) {
    const events = job.events || [];
    $('#status').textContent =
        `${job.status.toUpperCase()} — ${job.cleanup_error || job.error || events.at(-1)?.message || ''}`;
    $('#events').innerHTML = events.map(item =>
        `<div class="event"><time>${new Date(item.at).toLocaleTimeString()}</time>` +
        `<b>${escape(item.stage)}</b>${escape(item.message)}</div>`,
    ).join('');
    const resources = recordedResourceEntries(job);
    $('#resources').hidden = !resources.length;
    $('#resources').textContent = resources.length
        ? `Recorded resources\n${resources.map(([key, value]) => `${key}: ${value}`).join('\n')}`
        : '';
    const recoverable = job.recoverable === true || (
        job.live !== false
        && resources.some(([key]) => key.endsWith('_id'))
    );
    $('#destroyInterrupted').hidden = !(
        recoverable
        && ['interrupted', 'cleanup_failed'].includes(job.status)
    );
    renderLiveStopAction(job);
}

function retainedConnections(job) {
    const resources = job.resources || {};
    const sshUsers = { oci: 'opc', aws: 'ec2-user', gcp: 'benchmark', azure: 'benchmark' };
    const sshUser = sshUsers[job.plan?.provider || 'oci'] || 'opc';
    const hosts = [
        ['Benchmark VM', resources.public_ip],
        ['Load-generator VM', resources.loadgen_public_ip],
    ];
    const seen = new Set();
    return hosts
        .filter(([, address]) => address && !seen.has(address) && seen.add(address))
        .map(([label, address]) => `${label}: ssh -i /path/to/private-key ${sshUser}@${address}`);
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
    const benchmarkFailed = isFailedBenchmarkReport(job);
    $('#reportTitle').textContent = benchmarkFailed
        ? 'Benchmark failed'
        : (partial ? 'Partial benchmark report' : 'Your benchmark report is ready.');
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
    const managedResourcesRemain = recordedResourceEntries(job)
        .some(([key]) => key.endsWith('_id'));
    const canDestroy = (job.live !== false || job.recoverable === true) && managedResourcesRemain && (
        explicitlyRetained || job.status === 'cleanup_failed' || job.status === 'interrupted'
    );
    if (job.status === 'destroying') {
        $('#reportStatus').textContent = benchmarkFailed
            ? 'The benchmark failed before producing results. Diagnostic details are saved while infrastructure cleanup continues.'
            : 'Results are ready. Infrastructure cleanup is continuing in the background.';
    } else if (job.status === 'destroyed') {
        $('#reportStatus').textContent = benchmarkFailed
            ? 'The benchmark failed before producing results. Diagnostic details are saved, and its infrastructure has been destroyed.'
            : (partial
                ? 'The benchmark stopped with partial results. Its infrastructure has been destroyed.'
                : 'Results are saved. Benchmark infrastructure has been destroyed.');
    } else if (job.status === 'reported') {
        $('#reportStatus').textContent = benchmarkFailed
            ? 'The benchmark failed before producing results. Diagnostic details are saved, but the original infrastructure state is unavailable.'
            : (partial
                ? 'The saved report is incomplete. The app was restarted, so the original infrastructure state is unavailable.'
                : 'Results are saved. The app was restarted, so the original infrastructure state is unavailable.');
    } else if (job.status === 'interrupted') {
        $('#reportStatus').textContent = 'This run was interrupted when the app stopped. Saved resource details may still be available for cleanup.';
        $('#destroyNow').hidden = !canDestroy;
    } else if (job.status === 'cleanup_failed') {
        const detail = job.cleanup_error ? ` ${job.cleanup_error}` : '';
        $('#reportStatus').textContent = benchmarkFailed
            ? `The benchmark failed before producing results, and infrastructure cleanup did not finish.${detail}`
            : (partial
                ? `The benchmark report is incomplete, and infrastructure cleanup did not finish.${detail}`
                : `Results are saved, but infrastructure cleanup did not finish.${detail}`);
        $('#destroyNow').hidden = !canDestroy;
    } else if (job.status === 'complete') {
        $('#reportStatus').textContent = withRetainedConnections(
            partial
                ? 'The benchmark report is incomplete. Infrastructure retained.'
                : 'Infrastructure retained.',
            job,
        );
        $('#destroyNow').hidden = !canDestroy;
        $('#keyDownload').hidden = !currentJobPrivateKey;
    } else if (job.status === 'failed') {
        if (partial) {
            const message = connections.length
                ? 'The benchmark stopped after producing partial results. Infrastructure was retained.'
                : 'The benchmark stopped after producing partial results.';
            $('#reportStatus').textContent = withRetainedConnections(message, job);
        } else if (benchmarkFailed) {
            const message = connections.length
                ? 'The benchmark failed before producing results. Diagnostic details are saved. Infrastructure was retained.'
                : 'The benchmark failed before producing results. Diagnostic details are saved.';
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
        $('#reportStatus').textContent = partial
            ? 'Partial results are available while the run finishes.'
            : 'Results are ready while the run finishes.';
    }
}

async function poll() {
    try {
        const job = await api(`/api/jobs/${jobId}`);
        renderJobProgress(job);
        if (job.report_ready) showReport(job);
        if (terminalStatuses.includes(job.status)) clearInterval(poller);
    } catch (error) {
        $('#status').textContent = `Unable to refresh job: ${error.message}`;
    }
}

async function resumeLastJob() {
    const requested = new URLSearchParams(window.location.search).get('report');
    const saved = localStorage.getItem('ociBenchmarkJobId');
    if (!requested && !saved && localStorage.getItem('ociBenchmarkReportDismissed')) return false;
    let latest = null;
    try {
        latest = (await api('/api/reports/latest')).id;
    } catch { }
    const candidates = [requested, saved, latest]
        .filter((id, index, list) => id && list.indexOf(id) === index);
    for (const candidate of candidates) {
        try {
            jobId = candidate;
            const job = await api(`/api/jobs/${jobId}`);
            localStorage.setItem('ociBenchmarkJobId', jobId);
            renderJobProgress(job);
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
            return true;
        } catch { }
    }
    localStorage.removeItem('ociBenchmarkJobId');
    jobId = undefined;
    return false;
}

function leaveReport() {
    if (poller) clearInterval(poller);
    liveStopPending = false;
    localStorage.removeItem('ociBenchmarkJobId');
    localStorage.setItem('ociBenchmarkReportDismissed', '1');
    history.replaceState(null, '', '/');
    jobId = undefined;
    currentJobPrivateKey = '';
    $('#reportFrame').removeAttribute('src');
    $('#report').hidden = true;
    $('#test').hidden = true;
    $('#destroyInterrupted').hidden = true;
    $('#liveRunStop').hidden = true;
    $('#stopAndDestroy').disabled = false;
    $('#stopAndDestroy').textContent = 'Stop run and destroy infrastructure';
    $('#resources').hidden = true;
    $('#resources').textContent = '';
    $('#status').textContent = '';
    $('#events').innerHTML = '';
    $('#plan').hidden = false;
    activatePhase(0);
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

async function resetToPlan() {
    let active = false;
    if (jobId) {
        try {
            const job = await api(`/api/jobs/${jobId}`);
            active = !terminalStatuses.includes(job.status);
        } catch { }
    }
    if (active && !window.confirm(
        'A benchmark or cleanup is still active. Reset view does not stop the run ' +
        'or destroy its infrastructure. Use "Stop run and destroy infrastructure" ' +
        'first if you want to end it. Return to Plan anyway?',
    )) return;
    leaveReport();
    $('#planForm').reset();
    $('#key').value = '';
    $('#publicKey').value = '';
    $('#keyPassphrase').value = '';
    $('#keyFile').value = '';
    $('#publicKeyFile').value = '';
    delete $('#additional').dataset.ociChecked;
    delete $('#dataSize').dataset.userEdited;
    delete $('#dataSize').dataset.gcpC4aDefaultApplied;
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
    await loadProvider();
}

async function restorePlan(plan) {
    const provider = plan.provider || 'oci';
    delete $('#dataSize').dataset.userEdited;
    delete $('#dataSize').dataset.gcpC4aDefaultApplied;
    $('#provider').value = provider;
    $('#awsProfile').value = plan.aws_profile || 'default';
    $('#gcpProject').value = plan.gcp_project_id || '';
    $('#azureSubscription').value = plan.azure_subscription_id || '';
    await loadProvider(plan.region);
    $('#region').value = plan.region;
    $('#compartment').value = plan.compartment_id || '';
    if (provider === 'oci') {
        await placement();
        $('#ad').value = plan.availability_domain || '';
        faultDomains();
        $('#fd').value = plan.fault_domain || '';
        await loadShapes();
    } else if (provider === 'gcp') {
        $('#gcpZone').value = plan.gcp_zone || '';
        await loadShapes();
    } else if (provider === 'azure') {
        $('#azureZone').value = plan.azure_zone || '';
        await loadShapes();
    }
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
    $('#dataSize').dataset.userEdited = 'true';
    $('#dataPerf').value = plan.storage?.additional_performance ?? 10;
    $('#mount').value = plan.storage?.mount_style === 'iscsi'
        ? 'iscsi'
        : 'paravirtualized';
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
    applyProviderBenchmarkSupport();
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
$('#resetPlan').addEventListener('click', () => handleDiscovery(() => resetToPlan()));
$('#resetAll').addEventListener('click', () => handleDiscovery(() => resetToPlan()));
async function stopLiveRunAndDestroy() {
    const button = $('#stopAndDestroy');
    if (!jobId || liveStopPending || button.disabled) return;
    if (!window.confirm(
        'Stop this benchmark run and destroy all recorded cloud infrastructure? ' +
        'Unfinished benchmark work will be lost. This cannot be undone.',
    )) return;
    liveStopPending = true;
    renderLiveStopAction({ status: 'cancelling', live: true, benchmark_interrupted: true });
    $('#status').textContent = 'CANCELLING — Waiting for active benchmark work to stop before cleanup.';
    try {
        const response = await api(`/api/jobs/${jobId}/destroy`, { method: 'POST' });
        renderLiveStopAction({
            status: response.status || 'cancelling',
            live: true,
            benchmark_interrupted: true,
        });
        if (poller) clearInterval(poller);
        poller = setInterval(poll, 2500);
        poll();
    } catch (error) {
        liveStopPending = false;
        button.disabled = false;
        button.textContent = 'Stop run and destroy infrastructure';
        $('#liveRunStopStatus').textContent = `Unable to request stop: ${error.message}`;
        $('#status').textContent = `Unable to stop job: ${error.message}`;
    }
}
async function destroyCurrentJob(button) {
    await api(`/api/jobs/${jobId}/destroy`, { method: 'POST' });
    button.hidden = true;
    $('#status').textContent = 'DESTROYING — Cleanup has started.';
    if (poller) clearInterval(poller);
    poller = setInterval(poll, 2500);
}
$('#stopAndDestroy').addEventListener('click', stopLiveRunAndDestroy);
$('#destroyNow').addEventListener('click', () => destroyCurrentJob($('#destroyNow')));
$('#destroyInterrupted').addEventListener('click', () => (
    destroyCurrentJob($('#destroyInterrupted'))
));
$('#keyDownload').addEventListener('click', () => {
    const anchor = document.createElement('a');
    anchor.href = URL.createObjectURL(new Blob([currentJobPrivateKey], { type: 'application/octet-stream' }));
    anchor.download = `${currentProvider()}-benchmark-key.pem`;
    anchor.click();
    URL.revokeObjectURL(anchor.href);
});

init();
