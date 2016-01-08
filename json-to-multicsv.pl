#!/usr/bin/perl -w

=pod

=head1 NAME

json-to-multicsv.pl - Split a JSON file with hierarchical data to multiple CSV files

=head1 SYNOPSIS

B<json-to-multicsv.pl> [ B<--path pathspec:handler> ... ] [ B<--file input-file> ] [ B<--table name> ]

=head1 DESCRIPTION

Read in a JSON file, process it according as specified by the
B<--path> arguments, and output one or multiple CSV files with the
same data in tabular format.

=head1 EXAMPLE

Assuming the following input file:

   {
       "item 1": {
           "title": "The First Item",
           "genres": ["sci-fi", "adventure"],
           "rating": {
               "mean": 9.5,
               "votes": 190
           }
       },
       "item 2": {
           "title": "The Second Item",
           "genres": ["history", "economics"],
           "rating": {
               "mean": 7.4,
               "votes": 865
           },
           "sales": [
               { "count": 76, "country": "us" },
               { "count": 13, "country": "de" },
               { "count": 4, "country": "fi" }
           ]
       }
   }

And the following command line flags:

   --path /:table:item
   --path /*/rating:column
   --path /*/sales:table:sales
   --path /*/genres:table:genres

You'd get the following output files, which can be joined together
using the B<*._key> fields.

B<item.csv>:

   item._key,item.rating.mean,item.rating.votes,item.title
   "item 1",9.5,190,"The First Item"
   "item 2",7.4,865,"The Second Item"

B<item.genres.csv>:

   genres,item._key,item.genres._key
   sci-fi,"item 1",1
   adventure,"item 1",2
   history,"item 2",1
   economics,"item 2",2

B<item.sales.csv>:

   item._key,item.sales._key,sales.count,sales.country
   "item 2",1,76,us
   "item 2",2,13,de
   "item 2",3,4,fi

=head1 OPTIONS

=over 8

=item B<--file> I<input-file>

Read the JSON input from I<input-file>.

=item B<--path> I<pathspec>:B<table>:I<name>

Values matching I<pathspec> should be used to open a new table, with
the specified I<name>. The value should be either an object or an
array. For an object, each field of the object will be used to output
a row in the CSV file corresponding to the new table. The name of the
field stored in the B<tablename>._key column. For an array, each
element of the array will be used to output a row, with the index of
the element (starting from 1) stored in the B<tablename>._key column.

If multiple tables are nested, the key columns of all outer tables
will be also emitted in the inner tables.

=item B<--path> I<pathspec>:B<column>

Values matching I<pathspec> should be used to emit one or more columns
in the CSV file matching the innermost currently open table, on the
currently open row. (If no table is currently open).

If the value is a scalar, that value will be output to a column named
after the field containing the value as the column name. Note: Scalar
values have an implicit B<column> handler.

If the value is an object, each of the fields of the object will be used
to to output a column with the name being based on both the name of that
field, and the name of the field that contained the object.

=item B<--path> I<pathspec>:B<row>

The values matching I<pathspec> will be emitted as new rows. The value
must be an object. The name of the field containing the value will be
ignored. This is generally only useful for the toplevel JSON value.

=item B<--path> I<pathspec>:B<ignore>

Values matching I<pathspec> (and any of their subvalues) will not
be processed at all.

=item B<--table> I<name>

Specifies the I<name> of the toplevel table, assuming the toplevel
JSON value is not used to define a table but row data. You will
probably want to use a B<row> handler for the toplevel element.

=back

=head1 PATHS AND PATHSPECS

The path to a specific JSON value is determined by the following
rules:

- The path of the root element is /
- The path of a value that's directly contained inside an object
  is the concatenation of: a) the path of the parent object, b)
  the '/', c) the field in the object that this value is for.
- The path of a value that's directly contained inside an array
  is the concatenation of: a) the path of the parent object, b)
  the '/', c) the 1-based index in the array of the value.

Paths are matched against with pathspecs. In a pathspec any of the
elements of the path can instead be replaced with a C<*>, which will
match any element in that position (but not multiple adjacent ones).
That is, the pathspec C</a/*/c> will match C<a/b/c> but not C<a/b/b/c>.

=head1 AUTHOR

Juho Snellman, <jsnell@iki.fi>

=head1 LICENSE

Standard MIT license

=cut

use strict;

use Getopt::Long;
use File::Slurp;
use JSON;
use Pod::Usage;
use Text::CSV;

use vars qw($path @path @key @table $field $row);

sub usage {
    my ($exitval) = @_;
    pod2usage -exitval => $exitval;
}

my %tables = ();
my @handlers = ();

sub record {
    die "No open row (path $path)\n" if !defined $row;
    ($row->{$field}) = @_;
}

sub collect_row {
    if (!@table) {
        die "No open table (path $path). Suggestions:\n --table main --table tablename\n --path /:table:tablename\n";
    }

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

sub collect_column {
    my ($name, $thunk) = @_;
    local $field = (defined $field ? "$field.$name" : $name);
    local @path = (@path, $name);
    local $path = "$path$name/";
    $thunk->();
}

sub find_handler {
    my $fallback = undef;
    for my $handler (@handlers) {
        if ($handler->{match}(@path)) {
            if ($handler->{fallback}) {
                $fallback = $handler;
            } else {
                return $handler;
            }
        }
    }
    $fallback;
}

sub grovel {
    my $val = shift;
    my $handler = find_handler;
    if (defined $handler and $handler->{kind} eq 'ignore') {
        return;
    }
    if (!defined $val or !ref $val) {
        record $val;
    } elsif ('HASH' eq ref $val) {
        die "Don't know how to handle object at $path. Suggestions:\n --path $path:table:name\n --path $path:column\n --path $path:row\n --path $path:ignore" if !$handler;
        
        if ($handler->{kind} eq 'table') {
            for my $key (sort keys %{$val}) {
                local @key = (@key, $key);
                local @path = (@path, $key);
                local $path = "$path$key/";
                local $field = $handler->{args}[0];
                local @table = (@table, $field);
                collect_row sub {
                    grovel($val->{$key});
                };
            }
        } elsif ($handler->{kind} eq 'column') {
            for my $key (sort keys %{$val}) {
                collect_column $key, sub {
                    grovel($val->{$key});
                };
            }
        } elsif ($handler->{kind} eq 'row') {
            collect_row sub {
                for my $key (sort keys %{$val}) {
                    collect_column $key, sub {
                        grovel($val->{$key});
                    };
                }
            }
        } else {
            die "Unsupported handler type '$handler->{kind}' for path $path\n";
        }
    } elsif ('ARRAY' eq ref $val) {
        die "Don't know how to handle object at $path. Suggestions:\n --path $path:table:name\n --path $path:column\n --path $path:ignore\n" if !$handler;

        my $index = 0;
        for my $subval (@{$val}) {
            $index++;
            if ($handler->{kind} eq 'table') {
                local @key = (@key, $index);
                local @path = (@path, $index);
                local $path = "$path$index/";
                local $field = $handler->{args}[0];
                local @table = (@table, $field);
                collect_row sub {
                    grovel($subval);
                };
            } elsif ($handler->{kind} eq 'column') {
                collect_column $index, sub {
                    grovel($subval);
                };
            } else {
                die "Unknown handler type '$handler->{kind}' for path $path\n";
            }
        }
    } else {
        die "Unexpected JSON data. Not a scalar, array or hash\n"
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

sub make_matcher {
    my @components = @_;
    return sub {
        my @match_components = @_;
        # print STDERR "match: [@match_components] [@components]\n";
        return 0 if @match_components != @components;
        for my $i (0..$#components) {
            next if ($components[$i] eq '*');
            if ($components[$i] ne $match_components[$i]) {
                return 0;
            }
        }
        return 1;
    }
}

sub add_handler {
    my ($arg, $value) = @_;
    my ($path, $handler, @args) = split /:/, $value;
    if (!defined $handler or $handler !~ /^(table|column|row|ignore)$/) {
        die "Can't parse pathspec '$value'!\n";
    }
    if ($handler eq 'table' and !@args) {
        die "table handler '$value' needs a table name argument\n";
    }
    if ($path !~ m{^/}) {
        die "Invalid path '$path' (must start with '/')\n";
    }
    $path =~ s{/$}{};
    my @components = split /\//, $path;
    shift @components;
    push @handlers, {
        kind => $handler,
        args => \@args,
        match => make_matcher @components
    };
    push @handlers, {
        kind => 'column',
        fallback => 1,
        match => make_matcher @components, '*'
    };
}

sub main {
    local @key = ();
    local @table = ();
    local $field;
    local $row;
    local @path = qw();
    local $path = qw(/);

    my $file;

    if (!GetOptions("path=s" => \&add_handler,
                    "table=s" => sub { push @table, $_[1] },
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

