#!/bin/bash
#SBATCH -J extractIDmobility       # job name to display in squeue
#SBATCH --array=1-30
#SBATCH -p htc     # requested partition
#SBATCH -c 1 --mem=500G
#SBATCH -t 1000              # maximum runtime in minutes

# Load necessary modules
module load conda
conda activate ds_1300

# Get the file name for this job from the list
FILE=$(sed -n "${SLURM_ARRAY_TASK_ID}p" file_list.txt)

gzip -d $FILE

# Run the Python script
python extractIDmobility.py "$FILE"

gzip "${FILE%.gz}"
