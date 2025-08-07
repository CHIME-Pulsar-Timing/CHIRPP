#!/bin/bash

#SBATCH --error=%x-%j.err                 # Error file name = jobname-jobID.err

# Print job ID
echo "Job ID: $SLURM_JOB_ID"

source config.sh

# Check if there is more than one pulsar data in the directory
pulsar_name=$(check_pulsar_names "$data_directory")
status=$?

# Check the status and handle errors
if [ $status -ne 0 ]; then
    exit $status
fi

# If we reach here, it means exactly one pulsar name was found and stored in pulsar_name
echo "Pulsar name found: $pulsar_name"

# Scrunch files in time and frequency
# Use nsubbands value from config.sh
for f in $(ls CHIME*bmwt.clfd); do 
    nsub=$(vap -nc length $f | awk '{print int(\$2/${max_subint}) + 1}')  # does this work???
    pam --setnchn $nsubbands -e ftp --setnsub $nsub $f >> scrunch_${pulsar_name}-${SLURM_JOB_ID}.out 2>>scrunch_${pulsar_name}-${SLURM_JOB_ID}.err
done