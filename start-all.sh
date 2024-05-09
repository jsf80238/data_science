"""
This script is only executed by Docker.
It starts the four microservices of this application:
1. The Redis server: hosts a stock price/volume topic and accepts subscriptions
2. The Flask app: mimics what 3rd-party stock quoting service might provide
3. publish_to_stock_topic.py: generates random ticker/date combinations, fetches a price/volume from the 3rd-party stock quoting service, and publishes such data to the Redis topic.
4. update_analysis.py: subscribes to the Redis topic, updates the machine-learning model with the new data
"""

export PYTHONPATH="data_science:"

# 1.
/etc/init.d/redis-server start

# 3.
python database/publish_to_stock_topic.py &

# 4.
python database/update_analysis.py &

# 2.
uwsgi --http 0.0.0.0:5000 --master --processes 1 --wsgi data_science.stock_quote_server:app
