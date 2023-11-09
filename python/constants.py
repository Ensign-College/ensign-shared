"""
Constants
"""

COLUMNS = [
    'table', 'result', 'measurement', 'field', 'value', 'start', 'stop',
    'time', 'host', 'cpu', 'device', 'fstype', 'node', 'path', 'interface'
]
MEASUREMENTS = [
    'cpu', 'disk', 'diskio', 'kernel', 'mem', 'net', 'netstat', 'processes',
    'swap', 'system'
]
FILTERS = {
    'cpu': {
        'default': '''|> filter(fn: (r) => r["_field"] == "usage_guest" or r["_field"] == "usage_guest_nice" or r["_field"] == "usage_idle" or r["_field"] == "usage_iowait" or r["_field"] == "usage_irq" or r["_field"] == "usage_nice" or r["_field"] == "usage_softirq" or r["_field"] == "usage_steal" or r["_field"] == "usage_system" or r["_field"] == "usage_user")''',
    },
    'disk': {
        'default': '''|> filter(fn: (r) => r["_field"] == "free" or r["_field"] == "inodes_free" or r["_field"] == "inodes_total" or r["_field"] == "inodes_used" or r["_field"] == "total" or r["_field"] == "used" or r["_field"] == "used_percent")''',
    },
    'diskio': {
        'default': '''|> filter(fn: (r) => r["_field"] == "io_time" or r["_field"] == "iops_in_progress" or r["_field"] == "merged_reads" or r["_field"] == "merged_writes" or r["_field"] == "read_bytes" or r["_field"] == "read_time")''',
        'second': '''|> filter(fn: (r) => r["_field"] == "reads" or r["_field"] == "weighted_io_time" or r["_field"] == "write_bytes" or r["_field"] == "write_time" or r["_field"] == "writes")''',
    },
    'kernel': {
        'default': '''|> filter(fn: (r) => r["_field"] == "boot_time" or r["_field"] == "context_switches" or r["_field"] == "entropy_avail" or r["_field"] == "interrupts" or r["_field"] == "processes_forked")''',
    },
    'mem': {
        'default': '''|> filter(fn: (r) => r["_field"] == "active" or r["_field"] == "available" or r["_field"] == "available_percent" or r["_field"] == "buffered" or r["_field"] == "cached" or r["_field"] == "commit_limit" or r["_field"] == "committed_as" or r["_field"] == "dirty" or r["_field"] == "free" or r["_field"] == "high_free" or r["_field"] == "high_total" or r["_field"] == "huge_page_size" or r["_field"] == "huge_pages_free" or r["_field"] == "huge_pages_total")''',
        'second': '''|> filter(fn: (r) =>  r["_field"] == "inactive" or r["_field"] == "low_free" or r["_field"] == "low_total" or r["_field"] == "mapped" or r["_field"] == "page_tables" or r["_field"] == "shared" or r["_field"] == "slab" or r["_field"] == "sreclaimable" or r["_field"] == "sunreclaim" or r["_field"] == "swap_cached" or r["_field"] == "swap_free" or r["_field"] == "swap_total" or r["_field"] == "total" or r["_field"] == "used" or r["_field"] == "used_percent" or r["_field"] == "vmalloc_chunk" or r["_field"] == "vmalloc_total" or r["_field"] == "vmalloc_used" or r["_field"] == "write_back" or r["_field"] == "write_back_tmp")''',
    },
    'net': {
        'default': '''|> filter(fn: (r) => r["_field"] == "bytes_recv" or r["_field"] == "bytes_sent" or r["_field"] == "drop_in" or r["_field"] == "drop_out" or r["_field"] == "err_in" or r["_field"] == "err_out" or r["_field"] == "icmp_inaddrmaskreps" or r["_field"] == "icmp_inaddrmasks" or r["_field"] == "icmp_incsumerrors" or r["_field"] == "icmp_indestunreachs" or r["_field"] == "icmp_inechoreps" or r["_field"] == "icmp_inechos" or r["_field"] == "icmp_inerrors" or r["_field"] == "icmp_inmsgs" or r["_field"] == "icmp_inparmprobs" or r["_field"] == "icmp_inredirects" or r["_field"] == "icmp_insrcquenchs" or r["_field"] == "icmp_intimeexcds" or r["_field"] == "icmp_intimestampreps" or r["_field"] == "icmp_intimestamps" or r["_field"] == "icmp_outaddrmaskreps" or r["_field"] == "icmp_outaddrmasks" or r["_field"] == "icmp_outdestunreachs" or r["_field"] == "icmp_outechoreps" or r["_field"] == "icmp_outechos" or r["_field"] == "icmp_outerrors" or r["_field"] == "icmp_outmsgs" or r["_field"] == "icmp_outparmprobs" or r["_field"] == "icmp_outredirects" or r["_field"] == "icmp_outsrcquenchs" or r["_field"] == "icmp_outtimeexcds" or r["_field"] == "icmp_outtimestampreps" or r["_field"] == "icmp_outtimestamps")''',
        'second': '''|> filter(fn: (r) => r["_field"] == "icmpmsg_intype0" or r["_field"] == "icmpmsg_intype13" or r["_field"] == "icmpmsg_intype17" or r["_field"] == "icmpmsg_intype3" or r["_field"] == "icmpmsg_intype37" or r["_field"] == "icmpmsg_intype8" or r["_field"] == "icmpmsg_outtype0" or r["_field"] == "icmpmsg_outtype3" or r["_field"] == "icmpmsg_outtype5" or r["_field"] == "icmpmsg_outtype8" or r["_field"] == "ip_defaultttl" or r["_field"] == "ip_forwarding" or r["_field"] == "ip_forwdatagrams" or r["_field"] == "ip_fragcreates" or r["_field"] == "ip_fragfails" or r["_field"] == "ip_fragoks" or r["_field"] == "ip_inaddrerrors" or r["_field"] == "ip_indelivers" or r["_field"] == "ip_indiscards" or r["_field"] == "ip_inhdrerrors" or r["_field"] == "ip_inreceives" or r["_field"] == "ip_inunknownprotos" or r["_field"] == "ip_outdiscards" or r["_field"] == "ip_outnoroutes" or r["_field"] == "ip_outrequests" or r["_field"] == "ip_reasmfails" or r["_field"] == "ip_reasmoks" or r["_field"] == "ip_reasmreqds" or r["_field"] == "ip_reasmtimeout" or r["_field"] == "packets_recv" or r["_field"] == "packets_sent")''',
        'third': '''|> filter(fn: (r) => r["_field"] == "tcp_activeopens" or r["_field"] == "tcp_attemptfails" or r["_field"] == "tcp_currestab" or r["_field"] == "tcp_estabresets" or r["_field"] == "tcp_incsumerrors" or r["_field"] == "tcp_inerrs" or r["_field"] == "tcp_insegs" or r["_field"] == "tcp_maxconn" or r["_field"] == "tcp_outrsts" or r["_field"] == "tcp_outsegs" or r["_field"] == "tcp_passiveopens" or r["_field"] == "tcp_retranssegs" or r["_field"] == "tcp_rtoalgorithm" or r["_field"] == "tcp_rtomax" or r["_field"] == "tcp_rtomin" or r["_field"] == "udp_ignoredmulti" or r["_field"] == "udp_incsumerrors" or r["_field"] == "udp_indatagrams" or r["_field"] == "udp_inerrors" or r["_field"] == "udp_memerrors" or r["_field"] == "udp_noports" or r["_field"] == "udp_outdatagrams" or r["_field"] == "udp_rcvbuferrors" or r["_field"] == "udp_sndbuferrors" or r["_field"] == "udplite_ignoredmulti" or r["_field"] == "udplite_incsumerrors" or r["_field"] == "udplite_indatagrams" or r["_field"] == "udplite_inerrors" or r["_field"] == "udplite_memerrors" or r["_field"] == "udplite_noports" or r["_field"] == "udplite_outdatagrams" or r["_field"] == "udplite_rcvbuferrors" or r["_field"] == "udplite_sndbuferrors")''',
    },
    'netstat': {
        'default': '''|> filter(fn: (r) => r["_field"] == "tcp_close" or r["_field"] == "tcp_close_wait" or r["_field"] == "tcp_closing" or r["_field"] == "tcp_established" or r["_field"] == "tcp_fin_wait1" or r["_field"] == "tcp_fin_wait2" or r["_field"] == "tcp_last_ack" or r["_field"] == "tcp_listen" or r["_field"] == "tcp_none" or r["_field"] == "tcp_syn_recv" or r["_field"] == "tcp_syn_sent" or r["_field"] == "tcp_time_wait" or r["_field"] == "udp_socket")''',
    },
    'processes': {
        'default': '''|> filter(fn: (r) => r["_field"] == "blocked" or r["_field"] == "dead" or r["_field"] == "idle" or r["_field"] == "paging" or r["_field"] == "running" or r["_field"] == "sleeping" or r["_field"] == "stopped" or r["_field"] == "total" or r["_field"] == "total_threads" or r["_field"] == "unknown" or r["_field"] == "zombies")''',
    },
    'swap': {
        'default': '''|> filter(fn: (r) => r["_field"] == "free" or r["_field"] == "in" or r["_field"] == "out" or r["_field"] == "total" or r["_field"] == "used" or r["_field"] == "used_percent")''',
    },
    'system': {
        'default': '''|> filter(fn: (r) => r["_field"] == "load1" or r["_field"] == "load15" or r["_field"] == "load5" or r["_field"] == "n_cpus" or r["_field"] == "n_unique_users" or r["_field"] == "n_users" or r["_field"] == "uptime")''',
    }
}
FIRST_QUERY = '''
    from(bucket: "%(bucket)s")
        |> range(start: %(start)s, stop: %(stop)s)
        |> filter(fn: (r) => r["_measurement"] == "%(measurement)s")
        %(filters)s'''
FINAL_QUERY = '''
        |> aggregateWindow(every: %(every)s, fn: %(fn)s, createEmpty: false)
'''
