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
