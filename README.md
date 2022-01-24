# Kmer matching

This repo contains code for generating databases of kmers from sequencing data
and then matching kmers between two databases.  Normally this would be between
a database created from read data and a database created from a reference set.

## Programs

To run these programs use:
`java -jar Kmers.jar <program name> <options>`

Running them with no options will give you a list of available options.

### Database.MakeDatabase

Takes in a fasta/fastq file and creates a kmer databases from it.

### Database.Matcher

Matches kmers between two databases.

### OtherFiles.SeqToTaxID

Takes in a fasta file (with sequence ids) and a file with mappings from sequence
id to taxa ids and outputs a fasta file with the sequence id replaced by
the appropriate taxa id.

### Utils.TaxaCoutns

Takes in a reference database and returns kmer counts per taxa

### Other Utils.*

To be documented...

## Common Usages

### Match read kmers to a reference database

1) Run OtherFiles.SeqToTaxID to create an output file that contains taxa id
rather than sequence id.
2) Run Database.MakeDatabase with the reference fasta created in step 1 to
create a kmer database for the reference data
3) Run Database.MakeDatabase with the reads fastq to create a kmer database
for the reads data
4) Run Database.Matcher to create matches between the reference database 
(step 2) and the the reads database (step 3)

### Get Kmer counts in a reference database

1) Run OtherFiles.SeqToTaxID to create an output file that contains taxa id
rather than sequence id.
2) Run Database.MakeDatabase with the reference fasta created in step 1 to
create a kmer database for the reference data
3) Run Utils.TaxaCounts with the database file from step 2 to get counts
