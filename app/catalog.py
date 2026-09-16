BENCHMARKS = [
    {"id": "deathstarbench", "category": "Comprehensive", "name": "DeathStarBench", "description": "Runs a selectable cloud-microservices workload with native Podman and a separate load-generator VM.", "requires_data": False},
    {"id": "apachebench", "category": "Comprehensive", "name": "ApacheBench", "description": "Measures Apache HTTP Server throughput and latency over the private VCN from a separate load-generator VM.", "requires_data": False},
    {"id": "sysbench", "category": "Comprehensive", "name": "Sysbench", "description": "Runs one or more selectable CPU, memory, and file I/O workloads."},
    {"id": "phoronix", "category": "Comprehensive", "name": "Phoronix Test Suite", "description": "Runs one or more selectable, pinned OpenBenchmarking CPU and memory profiles.", "requires_data": False},
    {"id": "stream", "category": "Memory", "name": "STREAM", "description": "Sustainable memory bandwidth (Copy, Scale, Add, Triad)."},
    {"id": "iperf3", "category": "Network", "name": "iperf3", "description": "Runs one or more selectable TCP, UDP, and SCTP tests against a temporary private peer."},
    {"id": "fio", "category": "Storage", "name": "fio", "description": "Sequential and random I/O on one blank, provider-verified instance-local NVMe device when available; otherwise on the provisioned /data volume. Local NVMe is ephemeral."},
]

LLM_BENCHMARKS = [
    {"id": "llama_bench", "name": "llama.cpp throughput", "description": "CPU prompt processing and token generation rate using a pinned GGUF model.", "license": "MIT"},
]


SYSBENCH_WORKLOADS = [
    {
        "id": "cpu",
        "name": "CPU",
        "description": "Prime-number throughput using the selected OCPU count and a maximum prime of 10,000.",
    },
    {
        "id": "memory",
        "name": "Memory",
        "description": "Sequential memory-write throughput using the selected OCPU count and 1 MiB blocks.",
    },
    {
        "id": "fileio",
        "name": "File I/O",
        "description": "A 60-second random read/write workload over 4 GiB. It prefers one blank, provider-verified instance-local NVMe device and otherwise uses the provisioned /data volume; local NVMe is ephemeral.",
        "requires_data": True,
    },
]


IPERF3_PROTOCOLS = [
    {
        "id": "tcp",
        "name": "TCP",
        "description": "A 60-second private-VCN bandwidth test using four parallel TCP streams, with retransmits in the JSON result.",
    },
    {
        "id": "udp",
        "name": "UDP",
        "description": "A 60-second private-VCN test at unlimited offered load, with throughput, jitter, and packet loss in the JSON result.",
    },
    {
        "id": "sctp",
        "name": "SCTP",
        "description": "A 60-second private-VCN bandwidth test using four parallel SCTP streams against the shared iperf3 peer.",
    },
]


PHORONIX_PROFILES = [
    {
        "id": "compress_7zip",
        "profile": "pts/compress-7zip-1.13.1",
        "name": "7-Zip Compression",
        "category": "CPU",
        "description": "Measures multi-threaded 7-Zip compression throughput using its integrated benchmark.",
        "architectures": ["x86_64", "aarch64"],
        "estimated_runtime_minutes": 3,
        "unit": "MIPS",
        "direction": "higher_is_better",
    },
    {
        "id": "openssl",
        "profile": "pts/openssl-3.6.0",
        "name": "OpenSSL",
        "category": "CPU",
        "description": "Measures SHA-256 digest throughput with OpenSSL's built-in speed benchmark.",
        "architectures": ["x86_64", "aarch64"],
        "estimated_runtime_minutes": 10,
        "unit": "byte/s",
        "direction": "higher_is_better",
    },
    {
        "id": "build_linux_kernel",
        "profile": "pts/build-linux-kernel-1.18.0",
        "name": "Linux Kernel Build",
        "category": "CPU",
        "description": "Measures the elapsed time to compile the Linux kernel with parallel jobs.",
        "architectures": ["x86_64", "aarch64"],
        "estimated_runtime_minutes": 12,
        "unit": "seconds",
        "direction": "lower_is_better",
    },
    {
        "id": "tinymembench",
        "profile": "pts/tinymembench-1.0.2",
        "name": "Tinymembench",
        "category": "Memory",
        "description": "Measures standard memcpy and memset memory bandwidth.",
        "architectures": ["x86_64", "aarch64"],
        "estimated_runtime_minutes": 6,
        "unit": "MB/s",
        "direction": "higher_is_better",
    },
]


DEATHSTARBENCH_WORKLOADS = [
    {
        "id": "media_microservices",
        "name": "Media Microservices",
        "description": "Movie-review composition across C++, Nginx, MongoDB, Redis, and Memcached services.",
    },
    {
        "id": "hotel_reservation",
        "name": "Hotel Reservation",
        "description": "Mixed search, recommendation, login, and reservation requests across Go microservices.",
    },
    {
        "id": "social_network",
        "name": "Social Network",
        "description": "Mixed post composition and timeline reads across the social-network service graph.",
    },
]
