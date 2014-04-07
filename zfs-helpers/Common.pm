package Common;

use strict;
use warnings;
use POSIX qw(EINTR);
use IO::Socket::INET;
use IO::Select;
use File::Basename;
use File::Path qw(make_path);
use IO::File;
use IO::Dir;
use IPC::Open3;
use Fcntl qw(:DEFAULT :flock);
#use base 'Exporter';
use URI::Escape;
use Encode;
use Digest::SHA;
use Text::ParseWords;
use String::ShellQuote;

our @EXPORT_OK = qw(
run_command 
);

sub run_command {
    my ($cmd, %param) = @_;

    my $old_umask;
    my $cmdstr;

    if (!ref($cmd)) {
	$cmdstr = $cmd;
	if ($cmd =~ m/|/) {
	    # see 'man bash' for option pipefail
	    $cmd = [ '/bin/bash', '-c', "set -o pipefail && $cmd" ];
	} else {
	    $cmd = [ $cmd ];
	}
    } else {
	$cmdstr = cmd2string($cmd);
    }

    my $errmsg;
    my $laststderr;
    my $timeout;
    my $oldtimeout;
    my $pid;

    my $outfunc;
    my $errfunc;
    my $logfunc;
    my $input;
    my $output;
    my $afterfork;

    eval {

	foreach my $p (keys %param) {
	    if ($p eq 'timeout') {
		$timeout = $param{$p};
	    } elsif ($p eq 'umask') {
		$old_umask = umask($param{$p});
	    } elsif ($p eq 'errmsg') {
		$errmsg = $param{$p};
	    } elsif ($p eq 'input') {
		$input = $param{$p};
	    } elsif ($p eq 'output') {
		$output = $param{$p};
	    } elsif ($p eq 'outfunc') {
		$outfunc = $param{$p};
	    } elsif ($p eq 'errfunc') {
		$errfunc = $param{$p};
	    } elsif ($p eq 'logfunc') {
		$logfunc = $param{$p};
	    } elsif ($p eq 'afterfork') {
		$afterfork = $param{$p};
	    } else {
		die "got unknown parameter '$p' for run_command\n";
	    }
	}

	if ($errmsg) {
	    my $origerrfunc = $errfunc;
	    $errfunc = sub {
		if ($laststderr) {
		    if ($origerrfunc) {
			&$origerrfunc("$laststderr\n");
		    } else {
			print STDERR "$laststderr\n" if $laststderr;
		    }
		}
		$laststderr = shift; 
	    };
	}

	my $reader = $output && $output =~ m/^>&/ ? $output : IO::File->new();
	my $writer = $input && $input =~ m/^<&/ ? $input : IO::File->new();
	my $error  = IO::File->new();

	# try to avoid locale related issues/warnings
	my $lang = $param{lang} || 'C'; 
 
	my $orig_pid = $$;

	eval {
	    local $ENV{LC_ALL} = $lang;

	    # suppress LVM warnings like: "File descriptor 3 left open";
	    local $ENV{LVM_SUPPRESS_FD_WARNINGS} = "1";

	    $pid = open3($writer, $reader, $error, @$cmd) || die $!;

	    # if we pipe fron STDIN, open3 closes STDIN, so we we
	    # a perl warning "Filehandle STDIN reopened as GENXYZ .. "
	    # as soon as we open a new file.
	    # to avoid that we open /dev/null
	    if (!ref($writer) && !defined(fileno(STDIN))) {
		POSIX::close(0);
		open(STDIN, "</dev/null");
	    }
	};

	my $err = $@;

	# catch exec errors
	if ($orig_pid != $$) {
	    warn "ERROR: $err";
	    POSIX::_exit (1); 
	    kill ('KILL', $$); 
	}

	die $err if $err;

	local $SIG{ALRM} = sub { die "got timeout\n"; } if $timeout;
	$oldtimeout = alarm($timeout) if $timeout;

	&$afterfork() if $afterfork;

	if (ref($writer)) {
	    print $writer $input if defined $input;
	    close $writer;
	}

	my $select = new IO::Select;
	$select->add($reader) if ref($reader);
	$select->add($error);

	my $outlog = '';
	my $errlog = '';

	my $starttime = time();

	while ($select->count) {
	    my @handles = $select->can_read(1);

	    foreach my $h (@handles) {
		my $buf = '';
		my $count = sysread ($h, $buf, 4096);
		if (!defined ($count)) {
		    my $err = $!;
		    kill (9, $pid);
		    waitpid ($pid, 0);
		    die $err;
		}
		$select->remove ($h) if !$count;
		if ($h eq $reader) {
		    if ($outfunc || $logfunc) {
			eval {
			    $outlog .= $buf;
			    while ($outlog =~ s/^([^\010\r\n]*)(\r|\n|(\010)+|\r\n)//s) {
				my $line = $1;
				&$outfunc($line) if $outfunc;
				&$logfunc($line) if $logfunc;
			    }
			};
			my $err = $@;
			if ($err) {
			    kill (9, $pid);
			    waitpid ($pid, 0);
			    die $err;
			}
		    } else {
			print $buf;
			*STDOUT->flush();
		    }
		} elsif ($h eq $error) {
		    if ($errfunc || $logfunc) {
			eval {
			    $errlog .= $buf;
			    while ($errlog =~ s/^([^\010\r\n]*)(\r|\n|(\010)+|\r\n)//s) {
				my $line = $1;
				&$errfunc($line) if $errfunc;
				&$logfunc($line) if $logfunc;
			    }
			};
			my $err = $@;
			if ($err) {
			    kill (9, $pid);
			    waitpid ($pid, 0);
			    die $err;
			}
		    } else {
			print STDERR $buf;
			*STDERR->flush();
		    }
		}
	    }
	}

	&$outfunc($outlog) if $outfunc && $outlog;
	&$logfunc($outlog) if $logfunc && $outlog;

	&$errfunc($errlog) if $errfunc && $errlog;
	&$logfunc($errlog) if $logfunc && $errlog;

	waitpid ($pid, 0);
  
	if ($? == -1) {
	    die "failed to execute\n";
	} elsif (my $sig = ($? & 127)) {
	    die "got signal $sig\n";
	} elsif (my $ec = ($? >> 8)) {
	    if (!($ec == 24 && ($cmdstr =~ m|^(\S+/)?rsync\s|))) {
		if ($errmsg && $laststderr) {
		    my $lerr = $laststderr;
		    $laststderr = undef;
		    die "$lerr\n";
		}
		die "exit code $ec\n";
	    }
	}

        alarm(0);
    };

    my $err = $@;

    alarm(0);

    if ($errmsg && $laststderr) {
	&$errfunc(undef); # flush laststderr
    }

    umask ($old_umask) if defined($old_umask);

    alarm($oldtimeout) if $oldtimeout;

    if ($err) {
	if ($pid && ($err eq "got timeout\n")) {
	    kill (9, $pid);
	    waitpid ($pid, 0);
	    die "command '$cmdstr' failed: $err";
	}

	if ($errmsg) {
	    $err =~ s/^usermod:\s*// if $cmdstr =~ m|^(\S+/)?usermod\s|;
	    die "$errmsg: $err";
	} else {
	    die "command '$cmdstr' failed: $err";
	}
    }

    return undef;
}

sub shellquote {
    my $str = shift;

    return String::ShellQuote::shell_quote($str);
}

sub cmd2string {
    my ($cmd) = @_;

    die "no arguments" if !$cmd;

    return $cmd if !ref($cmd);

    my @qa = ();
    foreach my $arg (@$cmd) { push @qa, shellquote($arg); }

    return join (' ', @qa);
}

1;
