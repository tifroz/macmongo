
#!/bin/sh
#

# Initial setup
source ./env.sh
SCRIPT_FILE_NAME=`basename "$0"`
echo "${SCRIPT_FILE_NAME} script executed from: ${PWD}"



# Start children processes
pids=""
echo coffee -wcbo "$LOCAL_ROOT/lib/" "$LOCAL_ROOT/lib/"
coffee -wcbo "$LOCAL_ROOT/lib/" "$LOCAL_ROOT/lib/"  2>&1 | tee -a "$BUILDLOGFILE" &
pids="$pids $!"


#echo coffee -wcbo "$LOCAL_ROOT/lib/dist/node_modules" "$LOCAL_ROOT/lib/src_modules"
#coffee -wcbo "$LOCAL_ROOT/lib/dist/node_modules" "$LOCAL_ROOT/lib/src_modules"  2>&1 | tee -a "$BUILDLOGFILE" &
#pids="$pids $!"

echo "${SCRIPT_FILE_NAME} started children processes with pids ${pids}"


# Cleanup on exit
trap "echo Terminating ${SCRIPT_FILE_NAME}, killing children processes ${pids};trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT



# Tail

echo "tail $BUILDLOGFILE"
tail -n0 -F "$BUILDLOGFILE" | while read; do
    csmsg=$(echo "$REPLY" | egrep "^In.*")
    if [ -n "$csmsg" ]; then
			file=$(echo "$REPLY" | egrep -o "\w*.coffee")
			line=$(echo "$REPLY" | egrep -o "line [0-9]*")
			message=$(echo "$REPLY" | egrep -o "[\:|,].*")
			growlnotify -s -t "$file $line" --html -m "$message"  --image "/Users/hugo/Library/Growl/PNG/Error.png" --identifier "error_line"
    fi
		btmsg=$(echo "$REPLY" | egrep "Compiled")
		if [ -n "$btmsg" ]; then
			growlnotify -t "Auto-build" --html -m "$btmsg" --image "/Users/hugo/Library/Growl/PNG/Status.png"
		fi
		errmsg=$(echo "$REPLY" | egrep -i "error|fail")
		if [ -n "$errmsg" ]; then
			growlnotify -s -t "Auto-build Error" -s -m "$errmsg" --image "/Users/hugo/Library/Growl/PNG/Error.png"
		fi
done &

wait






