#!/bin/bash

#SBATCH --error=%x-%j.err                 # Error file name = jobname-jobID.out

# Print job ID
echo "Job ID: \$SLURM_JOB_ID"

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

# Zap known bad channels (5G zapping from Bradley plus list of commonly bad channels from Emmanuel)
for f in \$(ls CHIME*.ar); do 
    psrsh chime_zap.psh -e ar.zap \$f >> clean5G-\${SLURM_JOB_ID}.out 2>>clean5G-\${SLURM_JOB_ID}.err
done
