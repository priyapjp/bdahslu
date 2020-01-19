#!/bin/bash

cat hosts >> /etc/hosts
python ccxt_scheduler.py