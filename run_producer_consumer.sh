#!/bin/bash

# Define your producer and consumer commands here
PRODUCER_COMMAND="python3 producer.py"
CONSUMER_COMMAND="python3 consumer_apriori.py"
SECOND_CONSUMER_COMMAND="python3 consumer_pcy.py"
THIRD_CONSUMER_COMMAND="python3 consumer.py"

# Run producer and pipe its output to both consumers
$PRODUCER_COMMAND | tee >( $CONSUMER_COMMAND ) >( $SECOND_CONSUMER_COMMAND ) >( $THIRD_CONSUMER_COMMAND )

echo "Both consumers have finished processing."