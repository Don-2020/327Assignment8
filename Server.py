import socket
import psycopg2
import time
from statistics import mean
import threading

# Connect to NeonDB using psycopg2
conn = psycopg2.connect(
    "postgresql://neondb_owner:npg_ZnYm7GfUb4rp@ep-rapid-cherry-a42dud78-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require"
)
cursor = conn.cursor()

# Mapping device unique IDs to readable names
device_names = {
    "37w-i09-7rw-42a": "SmartFridge",
    "rr5-z94-68f-6ui": "SmartFridge2",
    "1k2-0e3-n2o-as5": "DishWasher"
}

# Query 1: Average moisture inside the SmartFridge over the last 3 hours
def query_1():
    three_hours_ago = int(time.time()) - (3 * 60 * 60)

    cursor.execute("""
        SELECT sensor_value FROM device_data
        WHERE board_name = %s
        AND sensor_name = %s
        AND timestamp >= %s
    """, (
        "arduino uno - SmartFridge - fridgeboard",
        "Moisture Meter - Moisture Meter - SmartFridge",
        three_hours_ago
    ))

    results = cursor.fetchall()
    values = [row[0] for row in results]

    if values:
        avg_val = sum(values) / len(values)
        return f"The average moisture inside the fridge in the past 3 hours is: {avg_val:.2f} RH%."
    else:
        return "No data available for the SmartFridge in the past 3 hours."


# Query 2: Average water usage by DishWasher over the last 3 hours
def query_2():
    three_hours_ago = int(time.time()) - (3 * 60 * 60)

    cursor.execute("""
        SELECT sensor_value FROM device_data
        WHERE board_name = %s
        AND sensor_name = %s
        AND timestamp >= %s
    """, (
        "arduino uno - DishWasher - dishwasher board",
        "Water Consumption - DishWasher",
        three_hours_ago
    ))

    results = cursor.fetchall()
    values = [row[0] for row in results]

    avg_val = mean(values) if values else 0
    return f"Average water consumption per cycle in the DishWasher: {avg_val:.2f} gallons"


# Query 3: Determine which device used the most electricity (based on ammeter readings)
def query_3():
    three_hours_ago = int(time.time()) - (3 * 60 * 60)

    cursor.execute("""
        SELECT asset_uid, sensor_name, sensor_value FROM device_data
        WHERE board_name IN (
            'arduino uno - SmartFridge - fridgeboard',
            'arduino uno - SmartFridge2 - Motherboard',
            'arduino uno - DishWasher - dishwasher board'
        )
        AND timestamp >= %s
    """, (three_hours_ago,))

    results = cursor.fetchall()
    consumption = {}

    for asset_uid, sensor_name, sensor_value in results:
        if "Ammeter" in sensor_name:
            consumption[asset_uid] = consumption.get(asset_uid, 0) + float(sensor_value)

    if not consumption:
        return "No electricity consumption data available."

    max_device_uid = max(consumption, key=consumption.get)
    max_device_name = device_names.get(max_device_uid, "Unknown Device")
    
    return f"The device with the highest electricity consumption is: {max_device_name}"


# Function to handle each client's request over the TCP connection
def handle_client_request(client_socket):
    try:
        while True:
            query_number = client_socket.recv(1024).decode("utf-8")

            if query_number == '1':
                response = query_1()
            elif query_number == '2':
                response = query_2()
            elif query_number == '3':
                response = query_3()
            else:
                response = "Sorry, this query cannot be processed. Please try one of the following: [1, 2, 3]."
            
            client_socket.send(response.encode("utf-8"))
    except socket.error as e:
        print(f"Error with client connection: {e}")
    finally:
        client_socket.close()


# Main function that sets up and runs the TCP server
def main():
    port = input("Enter port number to listen on: ")
    
    try:
        port = int(port)

        myTCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        myTCPSocket.bind(('0.0.0.0', port))
        myTCPSocket.listen(5)
        print(f"Server listening on port {port}...")

        while True:
            incomingSocket, incomingAddress = myTCPSocket.accept()
            print(f"Connection established with {incomingAddress}")
            client_thread = threading.Thread(target=handle_client_request, args=(incomingSocket,))
            client_thread.start()

    except ValueError:
        print("Port number must be an integer.")
    except socket.error as e:
        print(f"Socket error: {e}")
    finally:
        myTCPSocket.close()
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()