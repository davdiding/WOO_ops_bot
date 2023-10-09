#!/bin/bash

# Activate your venv
source venv/bin/activate

current_day=$(date +\%u)  # Monday is 1, Sunday is 7
current_date=$(date +\%d)
current_month_day=$(date +\%m)

# Daily job
python3 -m job_manager --volume --num 10

#
## Weekly jobs
#if [ "$current_day" -eq "1" ]; then  # Runs on Mondays
#  python /path/to/your/weekly_job1.py
#fi
#
#if [ "$current_day" -eq "5" ]; then  # Runs on Fridays
#  python /path/to/your/weekly_job2.py
#fi
#
## Monthly job
#if [ "$current_date" -eq "01" ]; then  # Runs on the first day of the month
#  python /path/to/your/monthly_job.py
#fi
