package PVE::Storage::ISCSIUtils;

use strict;
use warnings;
use File::stat;
use IO::Dir;
use IO::File;
use PVE::Tools qw(run_command file_read_firstline trim dir_glob_regex dir_glob_foreach);
use Net::Ping;

# iscsi helper function

my $ISCSIADM = '/usr/bin/iscsiadm';
$ISCSIADM = undef if ! -X $ISCSIADM;

sub check_iscsi_support {
    my $noerr = shift;

    if (!$ISCSIADM) {
	my $msg = "no iscsi support - please install open-iscsi";
	if ($noerr) {
	    warn "warning: $msg\n";
	    return 0;
	}

	die "error: $msg\n";
    }

    return 1;
}

sub iscsi_session_list {

    check_iscsi_support ();

    my $cmd = [$ISCSIADM, '--mode', 'session'];

    my $res = {};

    eval {
	run_command($cmd, errmsg => 'iscsi session scan failed', outfunc => sub {
	    my $line = shift;
	    
	    if ($line =~ m/^tcp:\s+\[(\S+)\]\s+\S+\s+(\S+)\s*$/) {
		my ($session, $target) = ($1, $2);
		# there can be several sessions per target (multipath)
		push @{$res->{$target}}, $session;   
	    }
	});
    };
    if (my $err = $@) {
	die $err if $err !~ m/: No active sessions.$/i;
    }

    return $res;
}

sub iscsi_test_portal {
    my ($portal) = @_;

    my ($server, $port) = split(':', $portal);
    my $p = Net::Ping->new("tcp", 2);
    $p->port_number($port || 3260);
    return $p->ping($server);
}

sub iscsi_discovery {
    my ($portal) = @_;

    check_iscsi_support ();

    my $cmd = [$ISCSIADM, '--mode', 'discovery', '--type', 'sendtargets', 
	       '--portal', $portal];

    my $res = {};

    return $res if !iscsi_test_portal($portal); # fixme: raise exception here?

    run_command($cmd, outfunc => sub {
	my $line = shift;

	if ($line =~ m/^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+)\,\S+\s+(\S+)\s*$/) {
	    my $portal = $1;
	    my $target = $2;
	    # one target can have more than one portal (multipath).
	    push @{$res->{$target}}, $portal;
	}
    });

    return $res;
}

sub iscsi_login {
    my ($target, $portal_in) = @_;

    check_iscsi_support ();

    eval { iscsi_discovery ($portal_in); };
    warn $@ if $@;

    my $cmd = [$ISCSIADM, '--mode', 'node', '--targetname',  $target, '--login'];
    run_command($cmd);
}

sub iscsi_logout {
    my ($target, $portal) = @_;

    check_iscsi_support ();

    my $cmd = [$ISCSIADM, '--mode', 'node', '--targetname', $target, '--logout'];
    run_command($cmd);
}

my $rescan_filename = "/var/run/pve-iscsi-rescan.lock";

sub iscsi_session_rescan {
    my ($session_list, $force) = @_;

    check_iscsi_support();

    my $rstat = stat($rescan_filename);

    if (!$rstat) {
	if (my $fh = IO::File->new($rescan_filename, "a")) {
	    utime undef, undef, $fh;
	    close($fh);
	}
    } elsif (!$force) {
	my $atime = $rstat->atime;
	my $tdiff = time() - $atime;
	# avoid frequent rescans
	return if !($tdiff < 0 || $tdiff > 10);
	utime undef, undef, $rescan_filename;
    }

    foreach my $session (@$session_list) {
	my $cmd = [$ISCSIADM, '--mode', 'session', '-r', $session, '-R'];
	eval { run_command($cmd, outfunc => sub {}); };
	warn $@ if $@;
    }
}

sub iscsi_target_rescan {
    my ($target, $force) = @_;

    check_iscsi_support();

    my $rstat = stat($rescan_filename);

    if (!$rstat) {
        if (my $fh = IO::File->new($rescan_filename, "a")) {
            utime undef, undef, $fh;
            close($fh);
        }
    } elsif (!$force) {
        my $atime = $rstat->atime;
        my $tdiff = time() - $atime;
        # avoid frequent rescans
        return if !($tdiff < 0 || $tdiff > 10);
        utime undef, undef, $rescan_filename;
    }

    my $cmd = [$ISCSIADM, '--mode', 'node', '-T', $target, '-R'];
    eval { run_command($cmd, outfunc => sub {}); };
    warn $@ if $@;
}

sub load_stable_scsi_paths {

    my $stable_paths = {};

    my $stabledir = "/dev/disk/by-id";

    if (my $dh = IO::Dir->new($stabledir)) {
       while (defined(my $tmp = $dh->read)) {
           # exclude filenames with part in name (same disk but partitions)
           # use only filenames with scsi(with multipath i have the same device 
	   # with dm-uuid-mpath , dm-name and scsi in name)
           if($tmp !~ m/-part\d+$/ && $tmp =~ m/^scsi-/) {
                 my $path = "$stabledir/$tmp";
                 my $bdevdest = readlink($path);
		 if ($bdevdest && $bdevdest =~ m|^../../([^/]+)|) {
		     $stable_paths->{$1}=$tmp;
		 }
	   }
       }
       $dh->close;
    }
    return $stable_paths;
}

sub iscsi_device_list {

    my $res = {};

    my $dirname = '/sys/class/iscsi_session';

    my $stable_paths = load_stable_scsi_paths();

    dir_glob_foreach($dirname, 'session(\d+)', sub {
	my ($ent, $session) = @_;

	my $target = file_read_firstline("$dirname/$ent/targetname");
	return if !$target;

	my (undef, $host) = dir_glob_regex("$dirname/$ent/device", 'target(\d+):.*');
	return if !defined($host);

	dir_glob_foreach("/sys/bus/scsi/devices", "$host:" . '(\d+):(\d+):(\d+)', sub {
	    my ($tmp, $channel, $id, $lun) = @_;

	    my $type = file_read_firstline("/sys/bus/scsi/devices/$tmp/type");
	    return if !defined($type) || $type ne '0'; # list disks only

	    my $bdev;
	    if (-d "/sys/bus/scsi/devices/$tmp/block") { # newer kernels
		(undef, $bdev) = dir_glob_regex("/sys/bus/scsi/devices/$tmp/block/", '([A-Za-z]\S*)');
	    } else {
		(undef, $bdev) = dir_glob_regex("/sys/bus/scsi/devices/$tmp", 'block:(\S+)');
	    }
	    return if !$bdev;

	    #check multipath           
	    if (-d "/sys/block/$bdev/holders") { 
		my $multipathdev = dir_glob_regex("/sys/block/$bdev/holders", '[A-Za-z]\S*');
		$bdev = $multipathdev if $multipathdev;
	    }

            #get multipath slaves
            my @slaves = ();
            if (-d "/sys/block/$bdev/slaves") {
                dir_glob_foreach("/sys/block/$bdev/slaves", '[A-Za-z]\S*', sub {
                    push @slaves, shift;
                });
            }

            my $dmuuid = undef;
            if (-f "/sys/block/$bdev/dm/uuid") {
                $dmuuid = file_read_firstline("/sys/block/$bdev/dm/uuid");
            }

	    my $blockdev = $stable_paths->{$bdev};
	    return if !$blockdev;

	    my $size = file_read_firstline("/sys/block/$bdev/size");
	    return if !$size;

	    my $volid = "$channel.$id.$lun.$blockdev";

	    $res->{$target}->{$volid} = {
		'format' => 'raw', 
		'size' => int($size * 512), 
		'vmid' => 0, # not assigned to any vm
		'channel' => int($channel),
		'id' => int($id),
		'lun' => int($lun),
                'blockdev' => $blockdev,
                'slaves' => \@slaves,
                'dmuuid' => $dmuuid,
            };

	    #print "TEST: $target $session $host,$bus,$tg,$lun $blockdev\n"; 
	});

    });

    return $res;
}

1;
