"""
    
    This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.
    The csv_rabbit_reader function offers a way to stream messages to and from the queues of Rabbit MQ
    I also replaced the print statements with logging functions
    This was preferred so the terminal can be updated as the information is being read and recieved from RabbitMQ

    Author: Nic Vetter
    Date: May 25, 2024

"""

import pika
import sys
import webbrowser
import csv

#logging setup - NV
from util_logger import setup_logger
logger, logname = setup_logger(__file__)

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

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
        # print a message to the console for the user
        # I have set up the logger to print to the console instead of the print statement - NV
        logger.info(f" [x] Sent {message}")

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

# Reading in the csv file and sending to Rabbit MQ - NV

def csv_rabbit_reader(file_path: str, host: str, queue_name: str):
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader: 
            message = " ".join(row)
            send_message(host, queue_name, message)

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below - NV
if __name__ == "__main__":  
    # Proposes to open the RabbitMQ site
    offer_rabbitmq_admin_site()
    # Variable definition
    file_name = 'tasks.csv'
    host = "localhost"
    queue_name = "task_queue3"
    # Calling the send function to get the tasks sent to the queue
    csv_rabbit_reader(file_name, host, queue_name)