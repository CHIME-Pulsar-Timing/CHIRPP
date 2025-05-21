#!/bin/bash

#SBATCH --error=%x-%j.err     # Error file name = jobname-jobID.err

# Script to unpack tar files of old CHIME data
# Old chime data on Cedar lives: ~/nearline/rrg-istairs-ad/archive/pulsar/chime/fold_mode/pulsar_name

# Print job ID
echo "Job ID: $SLURM_JOB_ID"

for file in *.tar; do
    if [ -f "$file" ]; then  # Check if it's a regular file
        echo "Extracting $file..."
        tar -xf "$file"
        count=$(tar -tf "$file" | wc -l) # list all the files in the tar and count each line
        echo "Extracted $count file(s) from $file"
    fi
done

