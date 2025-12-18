#!/bin/bash
# Start cron in the background
cron
# Keep container running
tail -f /app/logs/cron.log
