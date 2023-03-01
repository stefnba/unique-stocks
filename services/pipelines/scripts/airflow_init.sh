#!/bin/bash
set -e

airflow db upgrade

airflow users create -r Admin -u $AIRFLOW_WEB_USER -e $AIRFLOW_WEB_EMAIL -f admin -l user -p $AIRFLOW_WEB_PASSWORD

airflow webserver