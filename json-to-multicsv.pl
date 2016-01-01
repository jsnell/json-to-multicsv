#!/usr/bin/perl -w

=pod

=head1 NAME

$0 - Split a JSON file with hierarchical data to multiple CSV files

=head1 SYNOPSIS

B<$0> [ B<--path pathspec:handler> ... ] [ B<--file input-file> ]

=head1 DESCRIPTION

Read in a JSON file, process it according as specified by the
B<--path> arguments, and output one or multiple CSV files with the
same data in tabular format.

=cut

use strict;

use Getopt::Long;
use File::Slurp;
use JSON;
use Pod::Usage;
use Text::CSV;

use vars qw($path @key @table $field $row);

sub usage {
    my ($exitval) = @_;
    pod2usage -exitval => $exitval;
}

my %tables = ();
my %handlers = ();

sub record {
    die "No open row (path $path)\n" if !defined $row;
    ($row->{$field}) = @_;
}

sub collect_row (&) {
    local $row = {};
    my $index = 0;
    for (@key) {
        $row->{join(".", @table[0..$index++])."._key"} = $_;
    }
    $_[0]->();
    my $table = join '.', @table;
    push @{$tables{$table}{data}}, $row;
    for my $field (keys %{$row}) {
        $tables{$table}{fields}{$field} = 1;
    }
}

sub grovel {
    my $val = shift;
    if (!defined $val or !ref $val) {
        record $val;
    } elsif ('HASH' eq ref $val) {
        my $handler = $handlers{$path};
        die "Don't know how to handle object at $path (pass --path $path:table:name or --path $path:row or --path $path:column)\n" if !$handler;
        
        for my $key (sort keys %{$val}) {
            if ($handler->{kind} eq 'table') {
                local @key = (@key, $key);
                local $path = "$path*/";
                local $field = $handler->{args}[0];
                local @table = (@table, $field);
                collect_row {
                    grovel($val->{$key});
                };
            } elsif ($handler->{kind} eq 'row') {
                local $field = (defined $field ? "$field.$key" : $key);
                local $path = "$path$key/";
                grovel($val->{$key});
            } else {
                die "Unknown handler type '$handler->{kind}' for path $path\n";
            }
        }
    } elsif ('ARRAY' eq ref $val) {
        my $handler = $handlers{$path};
        die "Don't know how to handle object at $path (pass --path $path:table:name or --path $path:row)\n" if !$handler;

        my $index = 0;
        for my $subval (@{$val}) {
            $index++;
            if ($handler->{kind} eq 'table') {
                local @key = (@key, $index);
                local $path = "$path*/";
                local $field = $handler->{args}[0];
                local @table = (@table, $field);
                collect_row {
                    grovel($subval);
                };
            } else {
                die "Unknown handler type '$handler->{kind}' for path $path\n";
            }
        }
    }
}

sub output_tables {
    for my $name (keys %tables) {
        open my $fh, ">", "$name.csv";
        my $csv = Text::CSV->new;
        $csv->eol("\n");

        my $data = $tables{$name}{data};
        my @fields = sort keys %{$tables{$name}{fields}};
        print STDERR "Table: $name [@fields]\n";
        $csv->print($fh, \@fields);
        for my $row (@{$data}) {
            my @row = ();
            for my $field (@fields) {
                push @row, $row->{$field};
            }
            $csv->print($fh, \@row);
        }
    }
}

sub add_handler {
    my ($arg, $value) = @_;
    my ($path, $handler, @args) = split /:/, $value;
    if (!defined $handler or $handler !~ /^(table|column|row|skip)$/) {
        die "Can't parse pathspec ($value)!\n";
    }
    if ($handler eq 'table' and !@args) {
        die "table handler '$value' needs a table name argument\n";
    }
    $handlers{$path} = { kind => $handler, args => \@args }
}

sub main {
    local @key = ();
    local @table = ();
    local $field;
    local $row;
    local $path = qw(/);

    my $file;

    if (!GetOptions("path=s" => \&add_handler,
                    "file=s" => \$file)) {
        usage 2;
    }

    if (!defined $file) {
        die "No --file given\n";
        usage 2;
    }

    
    my $json = decode_json read_file $file;
    grovel $json;

    output_tables \%tables;
}

main;

