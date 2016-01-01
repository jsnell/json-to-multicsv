#!/usr/bin/perl -w

use strict;
use Cwd qw(getcwd abs_path);
use File::Basename qw(dirname);
use File::Slurp;
use File::Path qw(rmtree);
use Fatal qw(chdir);

sub runtest {
    my ($dir) = @_;

    my $FLAGS = join ' ', map { chomp; $_; } read_file "$dir/FLAGS";

    chdir $dir;
    rmtree 'output.actual';
    mkdir 'output.actual';
    chdir 'output.actual';

    system "perl ../../../json-to-multicsv.pl --file ../input.json $FLAGS" and die "Exit failed: $!\n";

    chdir '..';

    system "diff -u output.expected output.actual" and die "\n";

    chdir '..';
}

chdir dirname abs_path $0;
for my $dir (glob "*/") {
    local $| = 1;
    next if !-f "$dir/FLAGS";
    print "Testing $dir ... ";
    my $cwd = getcwd;
    eval {
        runtest $dir;
    }; if ($@) {
        print "FAILED\n";
        print "  $@\n";
    } else {
        print "ok\n";
    }
    chdir $cwd;
}    

