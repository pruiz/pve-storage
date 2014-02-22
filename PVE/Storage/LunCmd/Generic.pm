package PVE::Storage::LunCmd::Generic;

use strict;
use warnings;
use Digest::MD5 qw(md5_hex);
use PVE::Tools qw(run_command file_read_firstline trim dir_glob_regex dir_glob_foreach);
use Data::Dumper;

my @ssh_opts = ('-o', 'BatchMode=yes');
my @ssh_cmd = ('/usr/bin/ssh', @ssh_opts);
my $id_rsa_path = '/etc/pve/priv/zfs';

sub get_base {
    return '/dev';
}

sub encode_cfg_value {
    my ($key, $value) = @_;

    if ($key eq 'nodes' || $key eq 'content') {
        return join(',', keys(%$value));
    }

    return $value;
}

sub run_lun_command {
    my ($scfg, $timeout, $method, @params) = @_;

    my $msg = '';
    my %vars = ();
    my ($guid, $env, $lundev, $size) = undef;

    my $helper = $scfg->{iscsihelper};
    die "No 'iscsihelper' defined" if !$helper;

    $timeout = 10 if !$timeout;

    if ($method eq 'create_lu') {
        $lundev = $params[0];
        $helper .= " create-lun $lundev";
    } elsif ($method eq 'modify_lu') {
        $size = $params[0];
        $guid = $params[1];
        $helper .= " resize-lun ";
    } elsif ($method eq 'delete_lu') {
        $guid = $params[0];
        $helper .= " delete-lun $guid";
    } elsif ($method eq 'add_view') {
        $lundev = $params[0];
        $helper .= " add-view $lundev";
    } elsif ($method eq 'list_view') {
        $guid = $params[0];
        $helper .= " list-view $guid";
    } elsif ($method eq 'list_lu') {
        $guid = $params[0];
        $helper .= " list-lun $guid";
    } else {
        die "$method not implemented yet!";
    }

    # Common environment variables
    $vars{SSHKEY} = "$id_rsa_path/$scfg->{portal}_id_rsa";
    $vars{LUNDEV} = $lundev if $lundev;
    $vars{LUNUUID} = $guid if $guid;

    foreach my $k (keys %$scfg) { 
        $env .= "PMXCFG_$k=\"". encode_cfg_value($k, $scfg->{$k}) ."\" "; 
    }
    foreach my $k (keys %vars) { 
        $env .= "PMXVAR_$k=\"$vars{$k}\" "; 
    }

    my $output = sub {
        my $line = shift;
        $msg .= "$line";
    };

    my $target = 'root@' . $scfg->{portal};
    my $cmd = !$scfg->{remotehelper} ? "$env $helper"
        : [@ssh_cmd, '-i'. "$id_rsa_path/$scfg->{portal}_id_rsa", $target, "$env $helper"];

    run_command($cmd, timeout => $timeout, outfunc => $output);

    return $msg;
}

