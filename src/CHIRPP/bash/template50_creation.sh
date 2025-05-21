#!/bin/bash

#SBATCH --ntasks=1             # Single Job is created, sets the # of jobs/commands launched
#SBATCH --error=%x-%j.err      # Error file name = jobname-jobID.err

# Generator file for creating a smoothed template
# Also checks that one pulsar's data is present and if a par file exists in par_directory with the same name
# File created: template_run.sh

source config.sh

# Print job ID
echo "Job ID: $SLURM_JOB_ID"

###########################################################
##                    Gather files                       ##
###########################################################

# The most recent nbin value is determined in allParamCheck.sh
echo "Gathering 50 highest S/N files with the most recent nbin value (=${template_nbin})..."

# Use psrstat to get S/N and nbin for each file, sort by S/N descending,
# and only accept files that match the most recent nbin value stored as template_nbin in config.sh
sorted_files=$(for file in "$data_directory/"*${template_ext}; do
                    # Extract frequency-scrunched SNR snr and nbin values
                    data=$(psrstat -c snr,nbin -j DFTp -Q "$file")
                    snr=$(echo $data | awk '{print $2}')
                    nbin=$(echo $data | awk '{print $3}')

                    # Convert snr and nbin to integers
                    snr=$(printf "%.0f" "$snr")
                    nbin=$(printf "%.0f" "$nbin")

		    # Compare nbin with template_nbin
                    if [ "$nbin" -eq "$template_nbin" ]; then
                        echo "$file $snr $nbin"
                    fi
               done | sort -k2,2nr)


# Check if sorted_files is empty
if [ -z "$sorted_files" ]; then
    echo "No files found with nbin=${template_nbin}. Investigate files. Exiting..."
    exit 1
fi

# Store all filenames, snr, and nbin values in template_allFiles.txt
echo "$sorted_files" > template_allFiles.txt

# Select the top 50 files with highest SNR and matching nbin
top_50_files=$(echo "$sorted_files" | head -n 50 | awk '{print $1}')

# Check if top_50_files is empty or has fewer than 50 files
if [ -z "$top_50_files" ] || [ "$(echo "$top_50_files" | wc -l)" -lt 50 ]; then
    echo "Not enough top 50 files found with nbin=${template_nbin}. Investigate template_allFiles.txt. Exiting..."
    exit 1
fi

# Store the top 50 filenames in template_50.txt
echo "$top_50_files" > template_50.txt

# Did it run?
check_file_exists template_allFiles.txt
check_file_exists template_50.txt
echo "---------------------------------------------"

###########################################################
##                Create template commands               ##
###########################################################

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

# We will name the template with the pulsar name and today's date
today=$(date +"%Y-%m-%d")
template="${pulsar_name}.Rcvr_CHIME.CHIME.${today}.sum.sm"

# Check if par file was found and create template_run.sh
if [ -n "$par_file" ]; then
    template_sh="template_run.sh"

    # Create the template_run.sh file
    {
        echo '#!/bin/bash'
        echo ''
        echo '#SBATCH --cpus-per-task=1'  # One task per CPU
        echo "#SBATCH --job-name=template_run_${pulsar_name}"  # Job name
        echo '#SBATCH --error=%x-%j.err'        # Error file name = jobname-jobID.err
        # echo '' # these lines aren't necessary if the environment is set up correctly
        # echo 'module use /project/6004902/chimepsr-software/v2/environment-modules'
        # echo ''
        # echo 'module load psrchive'
        echo ''
        echo "# Our par files use the tempo site code for CHIME, 'CH'. So we set:"
        echo "export TEMPO2_ALIAS='tempo'"
        echo ''
        echo '# Print job ID.'
        echo "echo Job ID: \$SLURM_JOB_ID"
        echo ''
        echo "# Inital model = Gaussian w/ width=0.1, max iterations=3, save output to add.trimmed, on *${template_ext}'s."
        echo 'autotoa -g0.1 -i3 -S added.trimmed -M template_50.txt'
        echo '# Rotate template so peak is centered.'
        echo 'pam -r0.5 -m added.trimmed'
        echo 'psrsmooth -W added.trimmed'
        echo '# Rename the template to [JB]####+/-####.Rcvr_CHIME.CHIME.YYYY-MM-DD.sum.sm'
        echo "mv added.trimmed.sm ${template}"
    } > "$template_sh"

    # Check if template_run.sh was created successfully
    check_file_exists "$template_sh"
    echo "---------------------------------------------"
fi

