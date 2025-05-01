#!/bin/bash

# Generator script to create files needed to produce TOA file aka *.tim 
# Also checks that a smoothed profile exists from (psrsmooth) and renames it
# File created: tim_run.sh

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
echo "---------------------------------------------"

###########################################################
##                     .tim creation                     ##
###########################################################

# Check that the template specified in config.sh exists
template_path="${data_directory}/${template}"

if [ -e "$template_path" ]; then
    tim_sh="tim_run.sh" 

    echo "Template file found: $template"

    # Create the .tim file name for pat to write to
    today=$(date +"%Y-%m-%d")
    tim="${pulsar_name}.Rcvr_CHIME.CHIME.${today}.nb.tim"
    echo "---------------------------------------------"

    # Create the tim_run.sh file
    {
        echo '#!/bin/bash'
        echo ''
        echo "#SBATCH --job-name=tim_run_${pulsar_name}"  # Job name
        echo '#SBATCH --error=%x-%j.err'        # Error file name = jobname-jobID.err
        # echo '' # these lines aren't necessary if the environment is set up correctly
        # echo 'module use /project/6004902/chimepsr-software/v2/environment-modules'
        # echo 'module load psrchive'
        echo ''
        echo "# Our par files use the tempo site code for CHIME, 'CH'. So we set:"
        echo "export TEMPO2_ALIAS='tempo'"
        echo ''
        echo '# Print the Job ID'
        echo "echo Job ID: \$SLURM_JOB_ID"
        echo ''
        echo "pat -A FDM -e mcmc=0 -C chan -C subint -C snr -C wt -f \"tempo2\" -X \"${tim_flags}\" -s $template *$template_ext > ${tim}"
        # NB: adding a line containing any characters after the pat command will break run_pipeline.py :) -Will
    } > "$tim_sh"

    # Check if tim_run.sh was created successfully
    check_file_exists "$tim_sh"
    echo "---------------------------------------------"

else
    echo "Error: smoothed template specified by config.sh (${template}) not found in ${data_directory}."
    echo "---------------------------------------------"
    exit 1
fi

