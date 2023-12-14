"""gunicorn WSGI server configuration."""
from os import environ

bind = '0.0.0.0:' + environ.get('PORT', '5000')
max_requests = 1000
worker_class = 'gevent'
workers = 1  # Set to 1 worker
timeout = 3600 # 1 hour
