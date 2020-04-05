jq
ls
cd /root/
ls
cat log.json | jq
exit
ls
cd /root/
ls
cat log.json | jq
cat log.json | jq --raw-output '.run_id'
cat log.json | jq --raw-output 'jobs[0].run_id'
cat log.json | jq --raw-output 'jobs[0].run_id'
cat log.json | jq --raw-output '[0].run_id'
cat log.json | jq --raw-output '.[0].run_id'
cat log.json | jq --raw-output '.[].run_id'
cat log.json | jq --raw-output '.jobs.[].run_id'
cat log.json | jq --raw-output '.jobs'
cat log.json | jq --raw-output '.jobs | .[0]'
cat log.json | jq --raw-output '.jobs | .[0] | .run_id'
exit
exit
cd /root/
cat log.json | jq --raw-output '.jobs | .[0] | .run_id'
cat log.json | jq --raw-output '.jobs | .[0] | .id'
exit
cd /root/
cat log.json | jq --raw-output '.jobs | .[0] | .run_url'
exit
cd /root/
cat log.json | jq --raw-output '.jobs | .[0] | .url'
exit
cd /roo
cd /root
cat log.json | jq --raw-output '.jobs | .[0] | .url'
cat log.json | jq --raw-output '.jobs | .[1] | .url'
cat log.json | jq --raw-output '.jobs | .[] | .url'
cat log.json | jq --raw-output '.jobs | .[0] | .status'
exit
echo https://senzing.atlassian.netbrowse/PBF-74 | sed -e 's/netbrowse/net\/browse/g'
exit
