#!/bin/bash

# Script to move files based on MJD cutoff (MJD<=58600)

# Counter to track the number of moved files
count=0

# Create the dateFail directory if it doesn't exist
mkdir -p dateFail

# Loop through files and move based on MJD cutoff (MJD<=58600)
for number in $(ls | grep -oE 'beam_[0-9]+_([0-9]{5})_' | grep -oE '[0-9]{5}' | awk '$1 <= 58600'); do
    # Find and move files with matching MJD in filename to dateFail directory
    find . -maxdepth 1 -type f -name "*beam_[0-9]_${number}_*" -exec mv {} dateFail/ \; -exec sh -c 'echo "Moved: $1"' sh {} \;
    ((count++)) # Increment count
done

# Print the total number of moved files
echo "Total files moved: $count"

