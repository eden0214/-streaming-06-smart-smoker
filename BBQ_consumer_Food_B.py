"""
    This program listens for temperature readings continuously. 
    Start multiple versions to add more workers.  

    Author: Eden Anderson
    Date: 02/18/2023
    Based on Module 4 Version 3 .py program

"""

# Eden Anderson / 2.18.23 / Creating a consumer

import pika
import sys
import time
from collections import deque

# define variables
host = "localhost"
queue3 = "02-food-B"

# define deque for Food B queue

queue3_deque = deque(maxlen=20)

# define alert threshold for significant event/temperature change 
# if temperature changes by this amount, generate alert

queue3_alert = 1

# define a callback function to be called when a message is received
def BBQ_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    message = body.decode()
    # decode the binary message body to a string
    print(f" [x] Received {message} on 02-food-B")
    # simulate work by sleeping for the number of dots in the message
    time.sleep(body.count(b"."))
   
    # create initial smoker deque with parameters that store x amount of messages
    queue3_deque.append(message)
    # establish first deque item
    foodB_deque_item = queue3_deque[0]
    # split temperature and timestamp to create list
    foodB_deque_split = foodB_deque_item.split(", ")
    # convert tempmerature to correct format
    foodB_deque_temp1 = float(foodB_deque_split[1][:-1])

    # create current smoker temp with parameters
    foodB_deque_current = message
    # establish first deque item
    foodB_deque_itemc = queue3_deque[-1]
    # split temperature and timestamp to create list
    foodB_deque_splitc = foodB_deque_itemc.split(", ")
    # convert tempmerature to correct format
    foodB_deque_tempc = float(foodB_deque_splitc[1][:-1])

    # create alert for Food B if significant event
    if len(queue3_deque) == 20:

        # define and calculate change in temperature 
        foodB_temp_change = round(foodB_deque_temp1 - foodB_deque_tempc, 1)

        # create alert for smoker if significant event
        if foodB_temp_change >= queue3_alert:
            print(f" ALERT:  Food B temperature has changed beyond the threshold (1 F within 10 minutes/20 readings). \n          Food B temp change = {foodB_temp_change} degrees F = {foodB_deque_temp1} - {foodB_deque_tempc}")
        else:
            print("Current Food B temp is: ", foodB_deque_tempc)
    else:
            print("Current Food B temp is: ", foodB_deque_tempc)

# acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    time.sleep(1)

# define a main function to run the program
def main(hn: str = "localhost", qn: str = "task_queue"):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=queue3, durable=True)

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
        channel.basic_consume(queue=qn, on_message_callback=BBQ_callback)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        connection.close() 


# use channel to create function to delete queue
"""
Set up a function that will delete a queue every time the program is run - 
to clear out old messages 
"""

def delete_queue(host: str, queue_name: str):
    conn = pika.BlockingConnection(pika.ConnectionParameters(host))
    ch = conn.connection()
    ch.queue_delete(queue=queue_name)

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost", "queue2")
