#!/bin/bash

# Configuration file for CHIME/Pulsar data processing and TOA generation
# Based on the pipeline created for the NANOGrav 20-year data set

###########################################################
##                     Paths to input                    ##
###########################################################

data_directory=$(pwd)

# Directory containing the par files
par_directory="${HOME}/projects/rrg-istairs-ad/DR3/NANOGrav_15y/par/tempo2"

###########################################################
##                  Ext/params to input                  ##
###########################################################

# This variable is output by allParamCheck.sh 
template_nbin=512 # Most recent nbin value

# This variable is output by allParamCheck.sh 
dm=false # Most common DM value in file headers
         # A value of "false" will skip the step of updating header DM values
         # A value of "ephemeris" will use the DM value in the provided ephemeris
# Maximum subint duration in seconds (lesser of 1 hour or 2.5% of orbital period, if any)
# Note: the scrunch step is designed to produce subints of equal length.
# So if files have multiple subints, their length will likely be shorter than this.
max_subint=3600.0

# Desired number of subbands
nsubbands=64

# What is the file extension to run template creation on?
template_ext=".ftp" # ex: .zap or _trimmed.fits

# Flags to use in each TOA saved in the *.tim file
tim_flags="-f CHIME -be CHIME -fe Rcvr_CHIME"

# Smoothed template filename (.sm)
template="added.trimmed.sm"

# Sanity check
echo "Sanity check..."
echo "---------------------------------------------"
echo "Looking for pulsar data in directory: $data_directory"
echo "Looking for par files in directory: $par_directory"
echo "Template nbin: $template_nbin"
echo "Maximum subint duration: $max_subint s"
echo "Number of subbands: $nsubbands"
echo "Template creation will use files with extension: $template_ext"
echo "Your tim file will use these flags: $tim_flags"
echo "---------------------------------------------"
echo "---------------------------------------------"
echo "Is this what you mean to do? If not edit config.sh (ctrl+C now)!"
echo "---------------------------------------------"
echo "---------------------------------------------"

# Find all unique beam variations
beam_variations=$(find "$data_directory" -maxdepth 1 -name 'CHIME*beam_[0-9]*.ar' | grep -oE 'beam_[0-9]+' | sort -u)
num_beam=$(echo "$beam_variations" | wc -l)

###########################################################
##                      Convience funcs                  ##
###########################################################

# Function to check if a file exists
check_file_exists() {
    local file_path="$data_directory/$1"
    if [ -e "$file_path" ]; then
        echo "File '$file_path' was created."
    else
        echo "Error: File '$file_path' was not created."
    fi
}

# Function to remove a file, if it exists
remove_file_if_exists() {
    local file_path="$1"
    if [ -e "$file_path" ]; then
        echo "Removing '$file_path'."
        rm $file_path
    fi
}

# Function to check for multiple pulsar data in the directory
# Error messages are redirected to stderr (e.g. >&2) so only the pulsar name/par path/parfile are stored in the output
check_pulsar_names() {
    local data_directory="$1"
    local pulsar_names=()

    # Loop through each file in the directory
    for file in "$data_directory"/*; do
        # Check if the file exists and is a regular file
        if [ -f "$file" ]; then
            # Find the pulsar name pattern in the file name
            local pulsar_name=$(basename "$file" | grep -Eo '[JB][0-9]{4}[+-][0-9]{2,4}')

            # If a pulsar name is found, add it to the array
            if [ -n "$pulsar_name" ]; then
                pulsar_names+=("$pulsar_name")
            fi
        fi
    done

    # Determine the result based on the number of unique pulsar names
    local unique_pulsar_names=($(printf "%s\n" "${pulsar_names[@]}" | sort -u))

    if [ "${#unique_pulsar_names[@]}" -eq 0 ]; then
        echo "Error: No pulsar names found." >&2
        return 1
    elif [ "${#unique_pulsar_names[@]}" -eq 1 ]; then
        echo "${unique_pulsar_names[0]}"
        return 0
    else
        echo "Error: Multiple pulsar data is in the directory." >&2
        echo "Pulsar names found: $(printf "%s\n" "${unique_pulsar_names[@]}")" >&2
        return 2
    fi
}

# Function to find the par file
find_par() {
    local pulsar_name="$1"

    if [ -z "$par_directory" ]; then
        echo "Error: Par directory is not defined." >&2
        return 1
    fi

    # Find the par file
    local par_file=$(find "$par_directory" -type f -name "[JB]${pulsar_name:1}*.par" -print -quit)

    if [ -n "$par_file" ]; then
        echo "$par_file"
        return 0
    else
        echo "Error: Par file not found in $par_directory" >&2
        return 1
    fi
}

