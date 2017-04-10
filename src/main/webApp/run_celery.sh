#!/usr/bin/env bash


celery worker -A webApp.celery --loglevel=info --concurrency=1