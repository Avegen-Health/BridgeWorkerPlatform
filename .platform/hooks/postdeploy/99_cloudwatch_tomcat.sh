#!/bin/bash
# Add Tomcat catalina.out to CloudWatch log streaming.
#
# Why this exists: EB's AL2 Tomcat platform generates beanstalk.json (the CWAgent config)
# and runs fetch-config during deployment, but it does NOT include Tomcat logs in that config.
# .ebextensions commands run BEFORE the platform CWAgent setup, so any config written there
# gets overwritten. postdeploy hooks run AFTER the platform CWAgent setup, making this
# the correct place to append Tomcat log streaming.
#
# On each deploy this script:
#   1. Reads the EB environment name from the already-generated beanstalk.json
#   2. Writes a tomcat.json to the CWAgent merge-config directory
#   3. Reloads CWAgent with beanstalk.json + tomcat.json merged

set -e

BEANSTALK_CFG="/etc/amazon/amazon-cloudwatch-agent/beanstalk.json"
TOMCAT_CFG="/etc/amazon/amazon-cloudwatch-agent/amazon-cloudwatch-agent.d/tomcat.json"
CW_CTL="/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl"

if [ ! -f "$BEANSTALK_CFG" ]; then
  echo "[99_cloudwatch_tomcat] beanstalk.json not found, skipping" >&2
  exit 0
fi

# Extract environment name from the first log_group_name in beanstalk.json
# Format: /aws/elasticbeanstalk/<env-name>/var/log/...
ENV_NAME=$(python3 -c "
import json, sys
try:
    d = json.load(open('$BEANSTALK_CFG'))
    lg = d['logs']['logs_collected']['files']['collect_list'][0]['log_group_name']
    print(lg.split('/')[3])
except Exception as e:
    sys.exit(1)
")

if [ -z "$ENV_NAME" ]; then
  echo "[99_cloudwatch_tomcat] could not determine ENV_NAME from beanstalk.json, skipping" >&2
  exit 0
fi

echo "[99_cloudwatch_tomcat] configuring Tomcat log streaming for env: $ENV_NAME"

python3 -c "
import json
env_name = '$ENV_NAME'
cfg = {
  'logs': {
    'logs_collected': {
      'files': {
        'collect_list': [
          {
            'file_path': '/var/log/tomcat/catalina.out',
            'log_group_name': f'/aws/elasticbeanstalk/{env_name}/var/log/tomcat/catalina.out',
            'log_stream_name': '{instance_id}',
            'timestamp_format': '%Y-%m-%d %H:%M:%S,%f'
          },
          {
            'file_path': '/var/log/tomcat/localhost_access_log*.txt',
            'log_group_name': f'/aws/elasticbeanstalk/{env_name}/var/log/tomcat/access',
            'log_stream_name': '{instance_id}'
          }
        ]
      }
    }
  },
  'metrics': {
    'metrics_collected': {
      'disk': {
        'measurement': ['disk_used_percent'],
        'metrics_collection_interval': 300,
        'resources': ['/']
      }
    }
  }
}
with open('$TOMCAT_CFG', 'w') as f:
    json.dump(cfg, f, indent=2)
print('wrote $TOMCAT_CFG')
"

# Append Tomcat log config on top of whatever EB's beanstalk.json already configured.
# append-config writes tomcat.json into the CWAgent .d/ merge directory and reloads
# without wiping the beanstalk entries. fetch-config with a comma-separated -c is not
# supported via shell expansion and fails with "no such file or directory".
$CW_CTL -a append-config -m ec2 -s \
  -c "file:${TOMCAT_CFG}"

echo "[99_cloudwatch_tomcat] CWAgent reloaded with Tomcat log streaming enabled"
