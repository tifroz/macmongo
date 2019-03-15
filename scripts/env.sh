
SCRIPT_DIR="${PWD}"
LOCAL_ROOT=`dirname "$SCRIPT_DIR"`
ROOT_NAME=`basename "$LOCAL_ROOT"`
BUILDLOGFILE="~/Library/Logs/${ROOT_NAME}Build.log"
SERVERLOGFILE="~/Library/Logs/${ROOT_NAME}.log"

REMOTE_ROOT="/opt/${ROOT_NAME}"

TIMESTAMP=$(date +"%m-%d-%Y_%khr%Mmin%S")