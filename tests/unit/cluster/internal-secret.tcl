# Tests for the cluster internal-connection secret.
#
# The internal secret is an operator-configured, out-of-band credential
# (cluster-internal-secret, falling back to masterauth). It is identical on
# every node and is NEVER negotiated over the (unauthenticated) cluster bus, so
# a party with access to the bus port cannot inject or overwrite it. The full
# attacker flows are exercised end-to-end by ~/poc2.py (inbound injection) and
# ~/poc3.py (MEET-induced outbound injection); the tests below cover the same
# protections at the unit level.
#
# NOTE: start_cluster configures cluster-internal-secret to the value below on
# every node (see tests/support/cluster_util.tcl).
set ::internal_secret_value "0123456789abcdef0123456789abcdef01234567"

# Build a raw cluster bus PING packet carrying an INTERNALSECRET extension.
# sender_name: 40-char hex node ID to spoof as the sender.
# secret:      40-byte binary string to try to inject.
# client_port: the announced client port (bus port is client_port + 10000).
proc build_cluster_bus_ping_with_secret {sender_name secret client_port} {
    set CLUSTER_NAMELEN 40
    set CLUSTER_SLOTS 16384
    set NET_IP_STR_LEN 46
    set CLUSTERMSG_TYPE_PING 0
    set CLUSTERMSG_EXT_TYPE_INTERNALSECRET 4
    set CLUSTERMSG_FLAG0_EXT_DATA 4

    # Extension: length(4) + type(2) + unused(2) + secret(40) = 48 bytes
    set ext_len 48
    set ext [binary format ISS $ext_len $CLUSTERMSG_EXT_TYPE_INTERNALSECRET 0]
    append ext $secret

    set base_header_size 2256
    set totlen [expr {$base_header_size + $ext_len}]
    set cport [expr {$client_port + 10000}]

    set sender_padded [binary format a${CLUSTER_NAMELEN} $sender_name]

    set hdr {}
    append hdr "RCmb"
    append hdr [binary format I $totlen]
    append hdr [binary format S 1]                ;# ver
    append hdr [binary format S $client_port]     ;# port
    append hdr [binary format S $CLUSTERMSG_TYPE_PING] ;# type
    append hdr [binary format S 0]                ;# count
    append hdr [binary format W 0]                ;# currentEpoch
    append hdr [binary format W 0]                ;# configEpoch
    append hdr [binary format W 0]                ;# offset
    append hdr $sender_padded                     ;# sender
    append hdr [string repeat "\x00" [expr {$CLUSTER_SLOTS / 8}]] ;# myslots
    append hdr [string repeat "\x00" $CLUSTER_NAMELEN] ;# slaveof
    append hdr [binary format a${NET_IP_STR_LEN} "127.0.0.1"] ;# myip
    append hdr [binary format S 1]                ;# extensions count
    append hdr [string repeat "\x00" 30]          ;# notused1
    append hdr [binary format S 0]                ;# pport
    append hdr [binary format S $cport]           ;# cport
    append hdr [binary format S 1]                ;# flags (CLUSTER_NODE_MASTER)
    append hdr [binary format c 0]                ;# state
    append hdr [binary format ccc $CLUSTERMSG_FLAG0_EXT_DATA 0 0] ;# mflags

    set cur_len [string length $hdr]
    if {$cur_len < $base_header_size} {
        append hdr [string repeat "\x00" [expr {$base_header_size - $cur_len}]]
    }
    append hdr $ext
    return $hdr
}

start_cluster 3 3 {tags {external:skip cluster}} {
    test "All nodes share the configured internal secret" {
        # DEBUG internal_secret returns crc16 of the configured secret, which is
        # identical on every node because it comes from config, not the bus.
        set s0 [R 0 debug internal_secret]
        for {set i 1} {$i < 6} {incr i} {
            assert_equal $s0 [R $i debug internal_secret]
        }
    }

    test "AUTH with the configured internal secret succeeds" {
        set rd [redis_client]
        assert_equal OK [$rd auth "internal connection" $::internal_secret_value]
        $rd close
    }

    test "AUTH with a wrong internal secret is rejected" {
        assert_error {*WRONGPASS*} {R 0 auth "internal connection" [string repeat "\x00" 40]}
    }

    test "Forged inbound bus PING cannot inject a forged internal secret" {
        set host [srv 0 host]
        set port [srv 0 port]
        set cport [expr {$port + 10000}]
        set node_id [R 0 CLUSTER MYID]
        set before [R 0 debug internal_secret]

        # A zero-byte secret would win the old lexicographic "lowest wins" rule.
        set zero_secret [string repeat "\x00" 40]
        set pkt [build_cluster_bus_ping_with_secret $node_id $zero_secret $port]
        set fd [socket $host $cport]
        fconfigure $fd -translation binary -buffering full
        puts -nonewline $fd $pkt
        flush $fd
        after 500
        close $fd

        # The secret must be unchanged and the injected value must not authenticate.
        assert_equal $before [R 0 debug internal_secret]
        assert_error {*WRONGPASS*} {R 0 auth "internal connection" $zero_secret}
    }
}

# A cluster-enabled node with neither cluster-internal-secret nor masterauth set.
# (singledb avoids the framework's "SELECT 9", which is rejected in cluster mode.)
set old_singledb $::singledb
set ::singledb 1
start_server {tags {"external:skip cluster"} overrides {cluster-enabled yes}} {
    test "Internal auth is rejected when no secret is configured" {
        assert_error {*not configured*} {r auth "internal connection" "anything"}
    }
}
set ::singledb $old_singledb
