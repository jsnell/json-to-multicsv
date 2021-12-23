use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME         => 'Multicsv',
    AUTHOR       => q{Juho Snellman <jsnell@iki.fi>},
    VERSION      => '1.0',
    ABSTRACT     => 'Split a JSON file with hierarchical data to multiple CSV files',
    EXE_FILES    => ['json-to-multicsv.pl'],
    ( $ExtUtils::MakeMaker::VERSION >= 6.3002
        ? ( 'LICENSE' => 'MIT' )
        : () ),
    PREREQ_PM => {
        'Text::CSV' => 0,
	'File::Slurp' => 0,
	'JSON' => 0,    
    },
);
