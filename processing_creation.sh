#!/bin/bash

# Generator script that checks that pulsar's data are present and outputs the following files:
## ephemNconvert.txt, clean5G.txt, clean.txt, beamWeight.txt, scrunch.txt - text files with lists of commands parallalized by beam_# 
## parallel_${step}.sh - shell scripts to run the above text files using 'parallel' (launch as SLURM job) 

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

#--------------------------------------#
#---------------CLEAN RFI--------------#

# Name the text files to store the commands
ephem_txt="ephemNconvert.txt"
clean5G_txt="clean5G.txt"
clean_txt="clean.txt"

# Remove pre-existing text files
remove_file_if_exists "$ephem_txt"
remove_file_if_exists "$clean5G_txt"
remove_file_if_exists "$clean_txt"

# RFI clean each .ar file by beam
# Install ephemeris before averaging to ensure best data quality (Bradley Meyers)" > "$ephem_txt"
# And convert to a psrfits format for compatibility downstream" >> "$ephem_txt"
# Each text file is a list of arguments to run with parallel (don't include comments or blank spaces - everything will be ran as a job)

# Iterate through each unique beam variation
for beam in $beam_variations; do
    pulsarbeam="${pulsar_name}_${beam}"
    # Install ephemeris before averaging to ensure best data quality (Bradley Meyers)
    echo "for f in \$(ls CHIME*${beam}*.ar); do pam -p -E ${par_file} -a PSRFITS -u . \$f >> ephemNconvert_${pulsarbeam}-\${SLURM_JOB_ID}.out 2>>ephemNconvert_${pulsarbeam}-\${SLURM_JOB_ID}.err; done" >> "$ephem_txt"
    # Zap known bad channels (5G zapping from Bradley plus list of commonly bad channels from Emmanuel)
    echo "for f in \$(ls CHIME*${beam}*.ar); do psrsh chime_zap.psh -e ar.zap \$f >> clean5G_${pulsarbeam}-\${SLURM_JOB_ID}.out 2>>clean5G_${pulsarbeam}-\${SLURM_JOB_ID}.err; done" >> "$clean5G_txt"
    # Run clfd
    echo "for f in \$(ls CHIME*${beam}*.zap); do clfd \$f >> clean_${pulsarbeam}-\${SLURM_JOB_ID}.out 2>>clean_${pulsarbeam}-\${SLURM_JOB_ID}.err; done" >> "$clean_txt"
done

# Did it run?
check_file_exists "$ephem_txt"
check_file_exists "$clean5G_txt"
check_file_exists "$clean_txt"

#--------------------------------------#
#-------------Beam Weight--------------#

bmWt_txt='beamWeight.txt'
remove_file_if_exists "$bmWt_txt"
# Run beam weighting on each file > "$bmWt_txt"

# Iterate through each unique beam variation
for beam in $beam_variations; do
    outfile_base="beamWeight_${pulsar_name}_${beam}"
    # Create a script for the specific beam variation
    echo "add_beam -vv -e bmwt CHIME*${beam}*.clfd >>${outfile_base}-\${SLURM_JOB_ID}.out 2>>${outfile_base}-\${SLURM_JOB_ID}.err" >> "$bmWt_txt"
done

# Did it run?
check_file_exists "$bmWt_txt"

#--------------------------------------#
#---------------Scrunch----------------#

scrunch_txt='scrunch.txt'
remove_file_if_exists "$scrunch_txt"
# Scrunch files in time and frequency > "$scrunch.txt"

# Iterate through each unique beam variation
for beam in $beam_variations; do
    outfile_base="scrunch_${pulsar_name}_${beam}"
    # Create a script for the specific beam variation
    # Use nsubbands value from config.sh
    echo "for f in \$(ls CHIME*${beam}*bmwt.clfd); do nsub=\$(vap -nc length \$f | awk '{print int(\$2/$max_subint) + 1}'); pam --setnchn $nsubbands -e ftp --setnsub \$nsub \$f >> ${outfile_base}-\${SLURM_JOB_ID}.out 2>>${outfile_base}-\${SLURM_JOB_ID}.err; done" >> "$scrunch_txt"
done

# Did it run?
check_file_exists "$scrunch_txt"

#--------------------------------------#
#----------mk parallel_run.sh----------#

# Define the steps in an array
steps=("ephemNconvert" "clean5G" "clean" "beamWeight" "scrunch")

# Function to generate the parallel script for each step
generate_parallel_script() {
    local step=$1
    local script_name="parallel_${step}.sh"

    remove_file_if_exists "$script_name"
    
    # Write the SLURM header, modules to load, and general parallel command.
    cat <<EOL > $script_name
#!/bin/bash

#SBATCH --cpus-per-task=$num_beam
#SBATCH --job-name=${step}_${pulsar_name} # Job name
#SBATCH --error=%x-%j.err                 # Error file name = jobname-jobID.out

# Our par files use the tempo site code for CHIME, 'CH'. So we set:

export TEMPO2_ALIAS='tempo'

# Print job ID
echo "Job ID: \$SLURM_JOB_ID"

# Run the parallel command for $step
parallel --env _ --jobs \$SLURM_CPUS_PER_TASK --joblog ${step}_${pulsar_name}.log < ./${step}.txt

# Join the individual .out/.err files into one
cat ${step}_${pulsar_name}_beam*-\${SLURM_JOB_ID}.out >> ${step}_${pulsar_name}.out
rm ${step}_${pulsar_name}_beam*-\${SLURM_JOB_ID}.out
cat ${step}_${pulsar_name}_beam*-\${SLURM_JOB_ID}.err >> ${step}_${pulsar_name}-\${SLURM_JOB_ID}.err
rm ${step}_${pulsar_name}_beam*-\${SLURM_JOB_ID}.err

EOL

    check_file_exists "$script_name"

    # Make the script executable
    chmod +x $script_name
}

# Loop through the steps array and create a script for each step
for step in "${steps[@]}"; do
    generate_parallel_script $step
done
