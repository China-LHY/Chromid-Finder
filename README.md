# Chromid-Finder
To address the growing volume of metagenomic data, we developed a high-performance automated identification program, Chromid-Finder, designed to identify chromids and their corresponding bacterial main chromosomes within extensive metagenomic datasets.

# Requirements
System Requirements: Chromid-Finder has been tested and successfully run on Linux and Ubuntu systems.
Program Dependencies: Python≥3.7, Prodigal, HMMER3, Kofasmscan,Seqkit 
Python Dependencies: BioPython, Pandas,ProcessPoolExecutor,Math,multiprocessing

# Running Chromid-Finder
Please input a single FASTA file containing multiple sequences, ensuring that each sequence is relatively complete, and avoid situations where a sequence is composed of multiple fragments such as xx. bin1, xx.bin2.

Quick Start
Download all Chromid-Finder related files and place them in the same directory. Also, place the input file and Chromid-finder_run.py in the same directory.

Command
python Chromid-finder_run.py -i input.fasta -o output.txt -n 2 -d 1.6
-i:Input FASTA file
-o:output file
-n:number of threads used
-d:Tetranucleotide relative abundance

# Output Explanations
The output results are presented in the form of clusters, where each cluster represents a possible bacterial genome, and clusters are separated by '-----'.
Possible bacterial chromosome:refers to the chromosome of a bacterium that may carry a chromid
Possible bacterial chromids:refer to the chromids associated with this chromosome
