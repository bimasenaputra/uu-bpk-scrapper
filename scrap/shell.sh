#!/bin/bash

# Initialize i and j
i=1
j=50

# Loop 84 times
for ((count=1; count<=171; count++))
do
    # Execute the Python script with current offset and limit values
    python code.py --offset i --limit j
    
    # Increment i and j for the next iteration
    i=$((i+50))
    j=$((j+50))
done
