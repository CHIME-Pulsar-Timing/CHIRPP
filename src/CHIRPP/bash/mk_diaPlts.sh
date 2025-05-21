#!/bin/bash

ext="ftp"
# Loop through each file with the extension .ar_trimmed.ftp
for file in *"$ext"; do
    # Extract the base name without extension
    base_name=$(basename "$file" "$ext")

    # Run the pav command for each file
    pav -g "${base_name}_dGTp.ps/cps" -dGTp "$file"
    pav -g "${base_name}_dYFp.ps/cps" -dYFp "$file"
done

# zip all .ps files to get off Cedar and examine
$ find . -name "*.ps" -exec zip diagnostic_plts.zip {} +

