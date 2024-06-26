"""
    This program sends a message to one of three queues on the RabbitMQ server.
    Messages are sent from a CSV file containing temperatures for a smart smoker.
    Each column in the CSV file uses a different queue to send messages.


    Author: Jake Rood
    Date: June 7, 2024

"""

import pika
import sys
import webbrowser
import csv
import time

# Import and configure the logger 
from util_logger import setup_logger
logger, logname = setup_logger(__file__)

# Define variables
host_name = 'localhost'
csv_file = 'smoker-temps.csv'
queue_01 = '01-smoker'
queue_02 = '02-food-A'
queue_03 = '03-food-B'
sleep_secs = 30

# Define functions
def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()
        logger.info("Opened the RabbitMQ Admin website.")

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # log a message for the user
        logger.info(f" [x] Sent to {queue_name}: {message}")
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

def read_send_tasks(file_name: str, host: str):
    """
    Reads and sends tasks from the CSV file to the appropriate queue
    """
    with open(file_name) as input_file:
        reader = csv.reader(input_file)
        next(reader) # skip header row
        for row in reader:
            timestamp = row[0] # first column in CSV file is timestamp
            smoker_temp = row[1] # second column in CSV file is smoker_temp
            food_A_temp = row[2] # third column in CSV file is food_A_temp
            food_B_temp = row[3] # fourth column in CSV file is food_B_temp

            if smoker_temp:
                message = f"Temperature at {timestamp} is {smoker_temp}"
                send_message(host, queue_01, message) # send smoker_temp to queue defined as queue_01
            if food_A_temp:
                message = f"Temperature at {timestamp} is {food_A_temp}"
                send_message(host, queue_02, message) # send food_A_temp to queue defined as queue_02
            if food_B_temp:
                message = f"Temperature at {timestamp} is {food_B_temp}"
                send_message(host, queue_03, message) # send food_B_temp to queue defined as queue_03
            
            # Sleep for the defined interval
            time.sleep(sleep_secs)

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()
    # get the message from the command line
    # if no arguments are provided, use the default message
    # use the join method to convert the list of arguments into a string
    # join by the space character inside the quotes

    # Run the function to read and send tasks from a CSV file
    read_send_tasks(csv_file, host_name)