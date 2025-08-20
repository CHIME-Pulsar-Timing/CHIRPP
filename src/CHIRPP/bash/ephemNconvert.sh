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

# Find par file to install ephemeris
par_file=$(find_par "$pulsar_name")
status=$?

# Check the status and handle errors
if [ $status -ne 0 ]; then
    exit $status
fi

# If we reach here, it means the par file was found and stored in par_file
echo "Par file found: $par_file"
echo "---------------------------------------------"

# Our par files use the tempo site code for CHIME, 'CH'. So we set:
export TEMPO2_ALIAS='tempo'

# Install ephemeris before averaging to ensure best data quality (Bradley Meyers)
# And convert to a psrfits format for compatibility downstream
# Also update header DMs, if desired
if [ "$dm" = "ephemeris" ]; then
    for f in $(ls CHIME*.ar); do 
        pam -p -E ${par_file} --update_dm -a PSRFITS -u . $f >> ephemNconvert_${pulsar_name}-${SLURM_JOB_ID}.out 2>>ephemNconvert_${pulsar_name}-${SLURM_JOB_ID}.err
    done
elif [ "$dm" = true ]; then
    for f in $(ls CHIME*.ar); do 
        pam -p -E ${par_file} -d ${dm} -a PSRFITS -u . $f >> ephemNconvert_${pulsar_name}-${SLURM_JOB_ID}.out 2>>ephemNconvert_${pulsar_name}-${SLURM_JOB_ID}.err
    done
else
    for f in $(ls CHIME*.ar); do 
        pam -p -E ${par_file} -a PSRFITS -u . $f >> ephemNconvert_${pulsar_name}-${SLURM_JOB_ID}.out 2>>ephemNconvert_${pulsar_name}-${SLURM_JOB_ID}.err
    done
fi
