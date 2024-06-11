"""
    This program listens for food A temperature messages continuously and checks for stalls.

    Author: Jake Rood
    Date: June 7, 2024

"""

import pika
import sys
import re
from collections import deque

# Import and configure the logger 
from util_logger import setup_logger
logger, logname = setup_logger(__file__)

# Define variables
foodA_queue = '02-food-A'
food_stall_threshold = 1
food_window = 20 # Readings every 30 seconds for 10 minutes = 20 readings

# Initialize the deque for smoker temperatures
foodA_temps = deque(maxlen=food_window)

# Define program functions
# Define a callback function to be called when a smoker temperature message is received
def callback(ch, method, properties, body):
    """ Define behavior on getting a food A temperature message."""
    # decode the binary message body
    logger.info(f" [x] Received {body.decode()}")
    try: # Use regex to extract timestamp and temperature from the message
        match = re.match(r"Temperature at (.*) is (.*)", body.decode())
        if not match:
            raise ValueError("Message format incorrect")
        timestamp = match.group(1)
        temp = float(match.group(2))
    except (ValueError, IndexError) as e:
        logger.error(f"Failed to parse message: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    # append the most recent temperature reading to the deque
    foodA_temps.append(temp)

    # check if a smoker alert should be triggered
    if len(foodA_temps) == food_window:
        initial_temp = foodA_temps[0]
        current_temp = foodA_temps[-1]
        temp_change = round(current_temp - initial_temp, 1) # round temperature change to one decimal place
        if abs(temp_change) <= food_stall_threshold:
            logger.warning(f"{timestamp}: FOOD A STALL ALERT! Temperature change is {temp_change} F in {food_window / 2} minutes.")
    
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# define a main function to run the program
def main(hn: str = "localhost", qn: str = foodA_queue):
    """ Continuously listen for food A temperature messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        logger.error("ERROR: connection to RabbitMQ server failed.")
        logger.error(f"Verify the server is running on host={hn}.")
        logger.error(f"The error says: {e}")
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume( queue=qn, on_message_callback=callback)

        # print a message to the console for the user
        logger.info(" [*] Waiting for food A temperature messages. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logger.error("ERROR: something went wrong.")
        logger.error(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.error(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logger.info("Closing connection. Goodbye.")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main()