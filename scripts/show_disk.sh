#!/bin/bash

# FINAL SCRIPT. Uses raw data from df for accurate calculations and formatting.

# Requesting data in Megabytes (--block-size=1M) to get raw numbers.
LC_ALL=C df --block-size=1M --output=source,target,pcent,size,used,avail | awk '
function human(megabytes) {
    # This function now does the REAL calculation and formatting.
    val = ""
    if (megabytes >= 1024*1024) {
        # Convert MB to TB
        val = sprintf("%.1fT", megabytes / (1024*1024))
    } else if (megabytes >= 1024) {
        # Convert MB to GB
        val = sprintf("%.1fG", megabytes / 1024)
    } else {
        # Keep as MB
        val = sprintf("%.1fM", megabytes)
    }
    gsub(/\./, ",", val) # Use comma as decimal separator
    return val
}

BEGIN {
    total_size = 0
    total_used = 0
    total_avail = 0
    count = 0

    # Headers remain the same
    printf "\033[1;37m%-15s | %-22s | %6s | %8s | %8s | %8s\033[0m\n", "Device", "Mount Point", "Use%", "Size", "Used", "Avail"
}

# Process only the relevant mount points
$2 ~ /^\/mnt\/disk/ {
    # $4, $5, $6 are now raw numbers in Megabytes, e.g., "17825792"
    
    # Summing raw values for accurate totals
    total_size  += $4
    total_used  += $5
    total_avail += $6
    count++

    usage = $3 + 0
    gsub(/%/, "", usage) # Remove % for color logic
    color = (usage > 90) ? "\033[1;31m" : (usage > 80) ? "\033[1;33m" : "\033[1;32m"
    
    # --- CORE CHANGE ---
    # We now format the raw megabyte values using our human() function
    f_size = human($4)
    f_used = human($5)
    f_avail = human($6)

    # Print the correctly calculated and formatted values
    printf "\033[1;37m%-15s | %-22s\033[0m | %s%6s%s | %8s | %8s | %8s\n", \
           $1, $2, color, sprintf("%d%%", usage), "\033[0m", f_size, f_used, f_avail
}

END {
    if (count > 0) {
        if (total_size > 0) {
            total_percent_val = (total_used / total_size) * 100
        } else {
            total_percent_val = 0
        }
        
        total_percent_str = sprintf("%.1f%%", total_percent_val)
        gsub(/\./, ",", total_percent_str)

        # The human() function correctly formats the totals as well
        fs = human(total_size)
        fu = human(total_used)
        fa = human(total_avail)
        tcolor = (total_percent_val > 90) ? "\033[1;31m" : (total_percent_val > 80) ? "\033[1;33m" : "\033[1;32m"

        printf "\033[1;37m%-15s | %-22s\033[0m | %s%6s%s | %8s | %8s | %8s\n", \
               "Total:", sprintf("%d disks", count), tcolor, total_percent_str, "\033[0m", fs, fu, fa
    }
}'
