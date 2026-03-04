#!/bin/bash
# nanobot cron catchup - run this every 30 minutes

cd /Users/xinhao/Desktop/nanobot
/Users/xinhao/miniforge3/bin/python -m nanobot.cli.commands cron catchup 2>&1 >> /tmp/nanobot-cron.log
