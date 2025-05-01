#!/bin/bash 

#SBATCH --ntasks=1             # Single Job is created, sets the # of jobs/commands launched
#SBATCH --error=%x-%j.err      # Error file name = jobname-jobID.err

# File to check all archive parameters and standardize them

# Print job ID
echo "Job ID: $SLURM_JOB_ID"

###########################################################
##                     Params to input                   ##
###########################################################

# Define the extension of the files to check
ext=".ar"

# Function to log time and compute duration e.g. add time stamping
log_time() {
    local label="$1"
    local start="$2"
    local end=$(date +%s)
    local duration=$((end - start))
    local formatted_time=$(printf '%02d:%02d:%02d' $((duration/3600)) $(( (duration%3600)/60 )) $((duration%60)))
    echo "$label completed in $formatted_time"
    echo "---------------------------------------------"
}

# Record the script start time
script_start_time=$(date +%s)
echo "Script started at: $(date +"%Y-%m-%d %H:%M:%S")"

###########################################################
##                      General set-up                   ##
###########################################################

# Timestamp
general_setup_time=$(date +%s)

# File to store filenames and parameter values 
paramList="paramList.txt"

# Create a file of filename followed by param values
echo "# File of parameter values - columns = filename,nbin,nchan,freq,bw,npol,dm,nsub,length" > "$paramList"
echo "" >> "$paramList"

# Use psrstat to pull parameters from the file
vap -nc nbin,nchan,freq,bw,npol,dm,nsub,length *${ext} >> "$paramList" # by-file parameters

# Sort the parameter list from most recent to oldest
echo "# File of parameter values - columns = filename,nbin,nchan,freq,bw,npol,dm,nsub,length" > sorted_"$paramList"
echo "# Copy of $paramList but reordered to be in descending order of data e.g. most recent is first." >> sorted_"$paramList"
echo "" >> sorted_"$paramList"
sort -t '_' -k5,5nr "$paramList" >> sorted_"$paramList"

# Map the file lines to arrays
mapfile -t lines < sorted_"$paramList"

# Make an array for each column/parameter values, and arguments used in subint check later
declare -a filename nbin nchan freq bw npol dm nsub length process_args

# Loop through each line, extract parameters, and store in arrays
for line in "${lines[@]}"; do
    # Skip the header and empty lines
    if [[ "$line" =~ ^#.* ]] || [[ -z "$line" ]]; then
        continue
    fi
    
    # Split line into fields based on whitespace
    read -r file_val nbin_val nchan_val freq_val bw_val npol_val dm_val nsub_val len_val<<< "$line"
    
    # Populate respective arrays
    filename+=("$file_val")
    nbin+=("$nbin_val")
    nchan+=("$nchan_val")
    freq+=("$freq_val")
    bw+=("$bw_val")
    npol+=("$npol_val")
    dm+=("$dm_val")
    process_args+=("$file_val $nsub_val $len_val")
done

# Create folders to store files that fail each parameter's check
mkdir -p nbinFail nchanFail freqFail bwFail npolFail dmFail

# Timestamp
log_time "General setup" "$general_setup_time"

###########################################################
##                 subint duration check                 ##
###########################################################

# Check that each file has subintegrations of uniform length, within specified threshold
# If not, remove first and last subint

# Record start of subint check
subint_start_time=$(date +%s)

# Define the function to check subint duration for each file
process_file() {
    file=$(echo $1 | awk '{print $1}')
    nsub=$(echo $1 | awk '{print $2}')
    length=$(echo $1 | awk '{print $3}')

    # Final subint index (after first subint is removed)
    endsub=$((nsub-2))

    # Define the length of each subint, should always be 10 sec
    nsub_duration=10.0 # sec

    # Define the threshold, arbitrary
    threshold=0.01

    # Calculate length % 10 sec
    # If there is a remainder, one of the subints is non-uniform
    # The first/last subints will be shorter due to backend start-up/shutdown
    mod=$(python -c "print(f'{float($length) % float($nsub_duration):.4f}')")

    # Check if mod > threshold
    if python -c "import sys; sys.exit(0 if float($mod) > float($threshold) else 1)"; then
        echo "File: $file meets the condition (length=$length % 10 sec = $mod > $threshold), removing first and last subintegration."

        # Remove first and last subintegration
        echo "delete subint 0
delete subint $endsub" > rmsubints_${file}.psrsh

        psrsh rmsubints_${file}.psrsh -m "$file"

        rm rmsubints_${file}.psrsh

    else
        echo "File: $file does not meet the condition (length=$length % 10 sec = $mod <= $threshold), no subintegrations removed."
    fi

    echo "---------------------------------------------"
}

export -f process_file

# Use parallel to process files concurrently (limit ea. batch to $SLURM_CPUS_PER_TASK jobs at a time)
printf '%s\n' "${process_args[@]}" | parallel --env _ -j $SLURM_CPUS_PER_TASK process_file 

# Timestamp
log_time "Subint duration check" "$subint_start_time"

echo "SUBINT Check complete..."
echo "---------------------------------------------"
echo "---------------------------------------------"

###########################################################
##                      NBIN check                       ##
###########################################################

# Logic: Whatever nbin value is most recent is presumably vetted and decreed to be "best" for that pulsar (Ingrid Stairs)
# Templates will be made from the highest resolution data (e.g. greatest nbin values). 
# A higher resolution template can be applied to lower resolution data as pat will automatically downsample. The reverse is not true.
# NOTE: older data should always be lower-res (Emmanuel Fonseca) 

# Timestamp
nbin_check_time=$(date +%s)

# Store for use in pam --setnbin in template creation - give priority to recent file parameters
template_nbin="${nbin[0]}"

# Initialize high_res
high_res="$template_nbin"

for val in "${nbin[@]}"; do
    # Skip non-numeric values
    if ! [[ "$val" =~ ^-?[0-9]+(\.[0-9]+)?$ ]]; then
        continue
    fi
    
    # Find max e.g. highest-resolution nbin
    if (( $(echo "$val > $high_res" | bc -l) )); then
        high_res="$val"
    fi
done

# Check if high_res is equal to template_nbin
if [ "$high_res" = "$template_nbin" ]; then
    echo "Recent data is the highest resolution data (nbin=$high_res)"
    echo "---------------------------------------------"
else
    echo "Recent data is NOT the highest resolution (high resolution nbin=$high_res, most recent nbin=$template_nbin)"
    echo "Investigate this. The most recent data in CHIME should be the highest/optimal resolution for that pulsar."
    echo "---------------------------------------------"
fi

# Print results
log_time "NBIN check" "$nbin_check_time"
echo "The following will be input into config.sh:"
echo "template_nbin=${nbin[0]} # Most recent nbin value"
echo "NBIN Check complete..."
echo "---------------------------------------------"
echo "---------------------------------------------"

###########################################################
##               All other params check                  ##
###########################################################

# For all other parameters we will find the most common parameter value
#    and reject any file that does not have that value for the given parameter
#
# Temporary log files are created for each parameter recording the filename and value that failed the check
#    all files that failed at least one check is moved into the common_failures/ folder which contains a folder
#    for each parameter and the log detailing each file that failed
# The file: unique_failures.txt records the filename only once from each check
#    e.g. if a file failed freq and dm checks the filename and value will be recorded in freqFail/freqFail.log and dmFail/dmFail.log
#    but the filename will only be recorded once in unique_failures.txt and the file will be in common_failures/

# Timestamp
params_check_time=$(date +%s)

# Function to find the most common value in an array
find_most_common() {
    local array=("$@")
    printf '%s\n' "${array[@]}" | sort | uniq -c | sort -nr | head -n1 | awk '{print $2}'
}

# Function to check array against the most common value and log mismatches
check_array() {
    local -n array=$1
    local array_name=$2
    local common_value=$(find_most_common "${array[@]}")

    echo "Most common value in $array_name: $common_value"

    local log_file="${array_name}Fail/${array_name}Fail.log"
    touch "$log_file"

    # Initialize flag to check if we have written the header
    local header_written=false

    for i in "${!array[@]}"; do
        if [ "${array[$i]}" != "$common_value" ]; then
            if [ "$header_written" = false ]; then
                echo "# Filenames and parameter values for files that are not the most common value ($common_value) for parameter: $array_name" >> "$log_file"
                header_written=true
            fi
            echo "${filename[$i]}    ${array[$i]}" >> "$log_file"
        fi
    done

    # If no mismatches were found, log that all parameter values are uniform
    if [ "$header_written" = false ]; then
        echo "# All parameter values are uniform for $array_name = $common_value" >> "$log_file"
    fi
}

# Check all arrays against their most common values
check_array nchan "nchan"
check_array freq "freq"
check_array bw "bw"
check_array npol "npol"
check_array dm "dm"
echo ""

# Create a new file that lists a filename only once from all *Fail.log files
declare -a all_files
for fail_log in *Fail/*.log; do
    # Read the log file line by line
    while IFS= read -r line; do
        # Skip lines that are comments or empty
        if [[ "$line" =~ ^#.* ]] || [[ -z "$line" ]]; then
            continue
        fi
        # Add the filename (first word) to the all_files array
        file=$(echo "$line" | awk '{print $1}')
        all_files+=("$file")
    done < "$fail_log"
done

# Ensure all_files array is not empty before creating unique_failures.txt
if [ "${#all_files[@]}" -gt 0 ]; then
    echo "All files that failed a parameter check have been logged in unique_failures.txt"
    printf '%s\n' "${all_files[@]}" | sort | uniq > unique_failures.txt
else
    echo "No files to list in unique_failures.txt - no files failed a parameter check."
fi

mkdir -p common_failures

# Move all files listed in unique_failures.txt into common_failures
while IFS= read -r file; do
    if [ -f "$file" ]; then
        mv "$file" common_failures/
    else
        echo "File not found: $file"
    fi
done < unique_failures.txt
echo "All files that failed a parameter check have been moved to common_failures/"

# Move all *Fail folders into common_failures
for fail_dir in *Fail; do
    if [ -d "$fail_dir" ]; then
        mv "$fail_dir" common_failures/
    else
        echo "Directory not found: $fail_dir"
    fi
done
mv sorted_paramList.txt common_failures/
mv paramList.txt common_failures/
echo "All PARAMETERFail, parameter logs, and PARAMETERFail.log's and files have been moved to common_failures/"

# Timestamp
log_time "All other parameters check" "$params_check_time"
echo "All other parameter (nchan,freq,bw,npol,dm) check's are complete..."
echo "---------------------------------------------"
echo "---------------------------------------------"

# Final script duration calculation
script_end_time=$(date +%s)
total_script_duration=$((script_end_time - script_start_time))
total_runtime=$(printf '%02d:%02d:%02d' $((total_script_duration/3600)) $(((total_script_duration%3600)/60)) $((total_script_duration%60)))

echo "Script completed at: $(date +"%Y-%m-%d %H:%M:%S")"
echo "Total script runtime: $total_runtime"

echo "---------------------------------------------"
echo "---------------------------------------------"
echo "allParamCheck.sh has finished...  .      .        ."
