#!/bin/bash
# script to setup environment variables and arguments to launch the compute_manifest_script
# /!\ This script should be modified and adapted to each run.

# Check if jq is installed
if ! command -v jq &>/dev/null; then
    echo "Error: 'jq' is not installed. Please install it to run this script."
    exit 1
fi

export SE_ACCESS_KEY='' # add your access key here
export SE_SECRET_KEY='' # add your secret key here

# initialize all values for launching rebuilder script
export output_bucket='' # TODO fill in
export input_bucket='' # TODO fill in

export pvc_path="/home/$USER_NAME/dhlab-data/data/$USER_NAME-data"

# log file
logfile_name="self_explanatory_logfilename.log" # TODO change
touch $pvc_path/impresso-pycommons/impresso_commons/data/manifest_logs/$logfile_name
export log_file="${pvc_path}/impresso-pycommons/impresso_commons/data/manifest_logs/${logfile_name}"

# manifest config
mft_config_filename='chosen_or_created_config_file.json' # TODO change
export mft_config="${pvc_path}/impresso-pycommons/impresso_commons/data/manifest_config/${mft_config_filename}"

# Check if the JSON file exists
if [[ ! -f $mft_config ]]; then
  echo "Error: JSON file '$mft_config' not found."
  exit 1
fi

# extract data stage
data_stage=$(jq -r '.data_stage // empty' "$mft_config")
echo "Data-stage value extracted: ${data_stage}"

# TODO modify if using a different git repo
export git_repo="${pvc_path}/impresso-pycommons"

# temp dir
mkdir -p $pvc_path/temp_manifest_$data_stage
export temp_dir="${pvc_path}/temp_manifest_${data_stage}"

# Modify JSON values for git repo and temp_directory to add the pvc_path prefix
jq ".git_repository = \"$git_repo\" | .temp_directory = \"$temp_dir\"" "$mft_config" > temp.json && mv temp.json "$mft_config"

echo "Set values ${git_repo} for 'git_repository' and ${temp_dir} for temp_directory."