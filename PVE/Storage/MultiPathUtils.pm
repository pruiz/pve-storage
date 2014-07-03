package PVE::Storage::MultiPathUtils;

use strict;
use warnings;
use File::stat;
use IO::Dir;
use IO::File;
use PVE::Tools qw(run_command file_read_firstline trim dir_glob_regex dir_glob_foreach);
use PVE::Storage::ISCSIUtils;

my $MULTIPATH = '/sbin/multipath';
$MULTIPATH = undef if ! -X $MULTIPATH;

my $MULTIPATHD = '/sbin/multipathd';
$MULTIPATHD = undef if ! -X $MULTIPATHD;

sub check_multipath_support {
    my $noerr = shift;

    if (!$MULTIPATH) {
	my $msg = "no multipath support - please install and configure multipath support.";
	if ($noerr) {
	    warn "warning: $msg\n";
	    return 0;
	}

	die "error: $msg\n";
    }

    return 1;
}

sub find_multipath_item {
    my ($portal, $target, $lun) = @_;

    check_multipath_support();

    my $iscsi_devices = PVE::Storage::ISCSIUtils::iscsi_device_list();
    my $target_devices = $iscsi_devices->{$target};
    die "Target ($target) has no attached multipath devices." if !$target_devices;

    foreach my $vid (keys %$target_devices) {
        my $item = $target_devices->{$vid};
        #print "$target -- $vid: $item->{lun} -- $item->{blockdev}\n";
        if ($item->{lun} == $lun) {
            return $item;
        }
    }

    die "Unable to find multipath device for iscsi://$portal/$target/$lun";
}

sub find_multipath_device {
    my ($portal, $target, $lun) = @_; 

    my $item = find_multipath_item($portal, $target, $lun);
    my ($blockdev) = $item->{blockdev} =~ /(\S+)/; # taint filtering
    return "/dev/disk/by-id/$blockdev";
}

sub free_multipath_device {
    my ($portal, $target, $lun) = @_;

    my $item = find_multipath_item($portal, $target, $lun);
    my ($blockdev) = $item->{blockdev} =~ /(\S+)/; # taint filtering

    # Delete each lun's scsi device..
    foreach my $dev (@{$item->{slaves}}) {
        print "Deleting $blockdev slave $dev\n";
        system("echo 1 > '/sys/block/$dev/device/delete'") == 0 ||
            die "Unable to delete multipath slave: $dev";
    }

    # Flush (remove) multipath DM device.
    my $cmd = [$MULTIPATH, '-f', "/dev/disk/by-id/$blockdev" ];
    eval { run_command($cmd, outfunc => sub {}); };
    warn $@ if $@;
}

sub resize_multipath_device {
    my ($portal, $target, $lun) = @_;

    my $item = find_multipath_item($portal, $target, $lun);
    my ($blockdev) = $item->{blockdev} =~ /(\S+)/; # taint filtering

    my $cmd = [$MULTIPATHD, "-k\"resize map $blockdev\"" ];
    eval { run_command($cmd, outfunc => sub {}); };

    die "Resizing multipath device failed: $@" if $@;
}

1;
