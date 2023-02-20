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
queue1 = "01-smoker"

# define deque for smoker queue

queue1_deque = deque(maxlen=5)

# set alert for significant event/temperature change 
# if temperature changes by this amount, generate alert

queue1_alert = 15

# define a callback function to be called when a message is received
def BBQ_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    message = body.decode()
    # decode the binary message body to a string
    print(f" [x] Received {message} on 01-smoker")
    # simulate work by sleeping for the number of dots in the message
    time.sleep(body.count(b"."))
    # when done with task, tell the user
    print(" [x] Done.")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    time.sleep(1)


    # create initial smoker deque with parameters that store x amount of messages
    queue1_deque.append(message)
    # establish first deque item
    smoker_deque_item = queue1_deque[0]
    # split temperature and timestamp to create list
    smoker_deque_split = smoker_deque_item.split(",")
    # convert tempmerature to correct format
    smoker_deque_temp1 = float(smoker_deque_split[1][:-1])

    # create current smoker temp with parameters
    smoker_deque_current = message
    # establish first deque item
    smoker_deque_itemc = queue1_deque[0]
    # split temperature and timestamp to create list
    smoker_deque_splitc = smoker_deque_itemc.split(",")
    # convert tempmerature to correct format
    smoker_deque_tempc = float(smoker_deque_splitc[1][:-1])

    # define and calculate change in temperature 
    smoker_temp_change = round(smoker_deque_temp1 - smoker_deque_tempc, 1)

    # create alert for smoker if significant event
    if smoker_temp_change >= queue1_alert:
        print(f" ALERT:  Smoker temperature has changed beyond the threshold. \n          Smoker temp decrease = {smoker_temp_change} degrees F = {smoker_deque_temp1} - {smoker_deque_tempc}")


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
        channel.queue_declare(queue=queue1, durable=True)

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
        channel.basic_consume(queue=queue1, on_message_callback=BBQ_callback)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    except ValueError:
        pass    
    finally:
        print("\nClosing connection. Goodbye.\n")
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
    main("localhost", "queue1")
