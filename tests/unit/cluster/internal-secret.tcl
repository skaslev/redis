proc num_unique_secrets {num_nodes} {
    set secrets [list]
    for {set i 0} {$i < $num_nodes} {incr i} {
        lappend secrets [R $i debug internal_secret]
    }
    set num_secrets [llength [lsort -unique $secrets]]
    return $num_secrets
}

proc wait_for_secret_sync {maxtries delay num_nodes} {
    wait_for_condition $maxtries $delay {
        [num_unique_secrets $num_nodes] eq 1
    } else {
        fail "Failed waiting for secrets to sync"
    }
}

# Build a raw cluster bus PING packet with an INTERNALSECRET extension.
# sender_name: 40-char hex node ID to spoof as the sender.
# secret: 40-byte binary string to inject as the internal secret.
# client_port: the client port to announce.
proc build_cluster_bus_ping_with_secret {sender_name secret client_port} {
    set CLUSTER_NAMELEN 40
    set CLUSTER_SLOTS 16384
    set NET_IP_STR_LEN 46
    set CLUSTERMSG_TYPE_PING 0
    set CLUSTERMSG_EXT_TYPE_INTERNALSECRET 4
    set CLUSTERMSG_FLAG0_EXT_DATA 4
    set CLUSTER_INTERNALSECRETLEN 40

    # Extension: length(4) + type(2) + unused(2) + secret(40) = 48 bytes
    set ext_len 48
    set ext [binary format ISS $ext_len $CLUSTERMSG_EXT_TYPE_INTERNALSECRET 0]
    append ext $secret

    set base_header_size 2256
    set totlen [expr {$base_header_size + $ext_len}]
    set cport [expr {$client_port + 10000}]

    # Pad sender to CLUSTER_NAMELEN
    set sender_padded [binary format a${CLUSTER_NAMELEN} $sender_name]

    # Build header fields up to the data section
    set hdr {}
    append hdr "RCmb"
    append hdr [binary format I $totlen]
    append hdr [binary format S 1]               ;# ver
    append hdr [binary format S $client_port]     ;# port
    append hdr [binary format S $CLUSTERMSG_TYPE_PING] ;# type
    append hdr [binary format S 0]                ;# count (0 gossip entries)
    append hdr [binary format W 0]                ;# currentEpoch
    append hdr [binary format W 0]                ;# configEpoch
    append hdr [binary format W 0]                ;# offset
    append hdr $sender_padded                     ;# sender
    append hdr [string repeat "\x00" [expr {$CLUSTER_SLOTS / 8}]] ;# myslots
    append hdr [string repeat "\x00" $CLUSTER_NAMELEN] ;# slaveof
    set myip [binary format a${NET_IP_STR_LEN} "127.0.0.1"]
    append hdr $myip                              ;# myip
    append hdr [binary format S 1]                ;# extensions count
    append hdr [string repeat "\x00" 30]          ;# notused1
    append hdr [binary format S 0]                ;# pport
    append hdr [binary format S $cport]           ;# cport
    append hdr [binary format S 1]                ;# flags (CLUSTER_NODE_MASTER)
    append hdr [binary format c 0]                ;# state
    append hdr [binary format ccc $CLUSTERMSG_FLAG0_EXT_DATA 0 0] ;# mflags

    # Pad to base_header_size
    set cur_len [string length $hdr]
    if {$cur_len < $base_header_size} {
        append hdr [string repeat "\x00" [expr {$base_header_size - $cur_len}]]
    }

    append hdr $ext
    return $hdr
}

start_cluster 3 3 {tags {external:skip cluster}} {
    test "Test internal secret sync" {
        wait_for_secret_sync 50 100 6
    }

    
    set first_shard_host [srv 0 host]
    set first_shard_port [srv 0 port]
    
    if {$::verbose} {
        puts {cluster internal secret:}
        puts [R 1 debug internal_secret]
    }

    test "Join a node to the cluster and make sure it gets the same secret" {
        start_server {tags {"external:skip"} overrides {cluster-enabled {yes}}} {
            r cluster meet $first_shard_host $first_shard_port
            wait_for_condition 50 100 {
                [r debug internal_secret] eq [R 1 debug internal_secret]
            } else {
                puts [r debug internal_secret]
                puts [R 1 debug internal_secret]
                fail "Secrets not match"
            }
        }
    }

    test "Join another cluster, make sure clusters sync on the internal secret" {
        start_server {tags {"external:skip"} overrides {cluster-enabled {yes}}} {
            set new_shard_host [srv 0 host]
            set new_shard_port [srv 0 port]
            start_server {tags {"external:skip"} overrides {cluster-enabled {yes}}} {
                r cluster meet $new_shard_host $new_shard_port
                wait_for_condition 50 100 {
                    [r debug internal_secret] eq [r -1 debug internal_secret]
                } else {
                    puts [r debug internal_secret]
                    puts [r -1 debug internal_secret]
                    fail "Secrets not match"
                }
                if {$::verbose} {
                    puts {new cluster internal secret:}
                    puts [r -1 debug internal_secret]
                }
                r cluster meet $first_shard_host $first_shard_port
                wait_for_secret_sync 50 100 8
                if {$::verbose} {
                    puts {internal secret after join to bigger cluster:}
                    puts [r -1 debug internal_secret]
                }
            }
        }
    }
}

start_cluster 1 0 {tags {external:skip cluster}} {
    test "Inbound cluster bus connection cannot inject a forged internal secret" {
        set host [srv 0 host]
        set port [srv 0 port]
        set cport [expr {$port + 10000}]
        set node_id [R 0 CLUSTER MYID]

        set secret_before [R 0 debug internal_secret]

        # Open a raw TCP socket to the cluster bus and send a forged PING
        # spoofing the server's own node name, with a zero-byte secret that
        # would win the lexicographic comparison against any real secret.
        set zero_secret [string repeat "\x00" 40]
        set pkt [build_cluster_bus_ping_with_secret $node_id $zero_secret $port]
        set fd [socket $host $cport]
        fconfigure $fd -translation binary -buffering full
        puts -nonewline $fd $pkt
        flush $fd
        after 500
        close $fd

        # The internal secret must not have changed.
        set secret_after [R 0 debug internal_secret]
        assert_equal $secret_before $secret_after

        # AUTH with the forged zero-byte secret must be rejected.
        assert_error {*WRONGPASS*} {R 0 auth "internal connection" $zero_secret}
    }
}
