BENCHMARKS = [
    {"id": "baseline", "category": "Comprehensive", "name": "Cloud baseline suite", "description": "Runs sysbench CPU/memory, STREAM, fio, and selected network tests.", "requires_data": True},
    {"id": "deathstarbench", "category": "Comprehensive", "name": "DeathStarBench", "description": "Runs a selectable cloud-microservices workload with native Podman and a separate load-generator VM.", "requires_data": False},
    {"id": "phoronix", "category": "CPU", "name": "Phoronix 7-Zip CPU", "description": "Runs a pinned OpenBenchmarking 7-Zip compression profile and reports its MIPS score.", "requires_data": False},
    {"id": "sysbench_cpu", "category": "CPU", "name": "sysbench CPU", "description": "Prime-number throughput at one thread and all selected OCPUs."},
    {"id": "stream", "category": "Memory", "name": "STREAM", "description": "Sustainable memory bandwidth (Copy, Scale, Add, Triad)."},
    {"id": "sysbench_memory", "category": "Memory", "name": "sysbench memory", "description": "Read and write memory throughput."},
    {"id": "iperf_tcp", "category": "Network", "name": "iperf3 TCP", "description": "Private-VCN TCP bandwidth and retransmits; creates a temporary peer."},
    {"id": "iperf_udp", "category": "Network", "name": "iperf3 UDP", "description": "Private-VCN UDP throughput, jitter, and packet loss; creates a temporary peer."},
    {"id": "fio", "category": "Storage", "name": "fio", "description": "Sequential and random I/O on /data, with IOPS and latency."},
    {"id": "sysbench_fileio", "category": "Storage", "name": "sysbench file I/O", "description": "File I/O cross-check on /data.", "requires_data": True},
]

LLM_BENCHMARKS = [
    {"id": "llama_bench", "name": "llama.cpp throughput", "description": "CPU prompt processing and token generation rate using a pinned GGUF model.", "license": "MIT"},
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
