
from fastapi import FastAPI, HTTPException
from nats.aio.client import Client as NATS
import asyncio
import time
import json
import paho.mqtt.client as mqtt
import zenoh


app = FastAPI()
nc = NATS()
nc_mec = NATS()
nc_cloud = NATS()

# MQTT setup
mqtt_client_cloud = mqtt.Client()
mqtt_client_mec = mqtt.Client()



# ZENOH Global variables
zenoh_session = None
pub = None
sub = None
server_url = None


# MQTT Server for Could

@app.get("/connect_to_cloud_mqtt/")
async def connect_to_cloud_mqtt(server_url: str) -> str:
    print("Trying to connect to cloud MQTT")

    try:
        mqtt_client_cloud.on_connect = on_mqtt_connect
        mqtt_client_cloud.on_message = on_mqtt_message
        mqtt_client_cloud.connect(f"{server_url}", 1883, 60)  # Use the appropriate MQTT broker URL and port
        mqtt_client_cloud.loop_start()  # Start the MQTT client loop
        return "Connected to Cloud MQTT"
    
    except Exception as e:
        print(f"Error connecting to cloud MQTT: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to connect to Cloud MQTT. Please check the server URL and try again.")


# MQTT Server for MEC
@app.get("/connect_to_mec_mqtt/")
async def connect_to_mec_mqtt(server_url: str) -> str:
    print("Trying to connect to MEC MQTT")

    try:
        mqtt_client_mec.on_connect = on_mqtt_connect_mec
        mqtt_client_mec.on_message = on_mqtt_message_mec
        mqtt_client_mec.connect(f"{server_url}", 1883, 60)  # Use the appropriate MQTT broker URL and port
        mqtt_client_mec.loop_start()  # Start the MQTT client loop
        return "Connected to MEC MQTT"
    
    except Exception as e:
        print(f"Error connecting to MEC MQTT: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to connect to MEC MQTT. Please check the server URL and try again.")


# MQTT Callbacks
def on_mqtt_message(client, userdata, msg):
    mqtt_replier_reception_time = time.time()
    data = msg.payload.decode()
    print(f"MQTT Replier Received Message: {data}")

    # Create a response
    response = {
        "mqtt_server_location": "REGION",
        "replier_location": "REGION",
        "mqtt_replier_reception_time": mqtt_replier_reception_time,
    }
    json_data = json.dumps(response)

    # Publish the response to the MQTT client
    mqtt_client_cloud.publish("response_topic_MQ", json_data)
    print(f"MQTT Replier Sent Response: {json_data}")

def on_mqtt_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        mqtt_client_cloud.subscribe("request_topic_wlz")
    else:
        print(f"Failed to connect to MQTT broker with code {rc}")

# MQTT MEC Callbacks
def on_mqtt_message_mec(client, userdata, msg):
    mqtt_replier_reception_time = time.time()
    data = msg.payload.decode()
    print(f"MEC MQTT Replier Received Message: {data}")

    # Create a response
    response = {
        "mqtt_server_location": "MEC",
        "replier_location": "MEC",
        "mqtt_replier_reception_time": mqtt_replier_reception_time,
    }
    json_data = json.dumps(response)

    # Publish the response to the MQTT client
    mqtt_client_mec.publish("response_topic_MQ", json_data)
    print(f"MEC MQTT Replier Sent Response: {json_data}")
   
    

def on_mqtt_connect_mec(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MEC MQTT broker")
        mqtt_client_mec.subscribe("request_topic_wlz")
        print("Subscribed to response_topic_MQ on MEC ")
    else:
        print(f"Failed to connect to MQTT broker with code {rc}")

#  Testing Relooad

@app.get("/connect_to_cloud_nats/")
async def connect_to_cloud_nats(server_url: str) -> str:
    """
    Attempts to connect to NATS servers at cloud_url. Closes any existing connections,
    and sets up subscriptions to a topic with message echoing.
    """
    # Disconnect if already connected
    try:
        if nc_cloud.is_connected:
            await nc_cloud.close()
    except Exception as e:
        print("Encountered an exception when closing NATS connections:")
        print(e)

    retries = 5
    for i in range(retries):
        try:
            # Connect to the Cloud server and set up subscription
            await nc_cloud.connect(f"nats://{server_url}:4222")
            async def response_handler(msg):
                nats_replier_reception_time = time.time()
                print("#####################")
                print("RECEIVED new message")
                print(msg)
                subject = msg.subject
                reply = msg.reply
                data = msg.data.decode()
                print(f"Received a message on '{subject}' with data '{data}'.")
                print("#####################")
                # Send a response
                
                response = {
                    "nats_server_location": "REGION",
                    "replier_location": "REGION",
                    "nats_replier_reception_time": nats_replier_reception_time,
                 }
                json_data = json.dumps(response)
                print("REPLY = ", reply)
                await nc_cloud.publish(reply, json_data.encode('utf-8'))
            # Subscribe to receive messages for "request.subject"
            await nc_cloud.subscribe("request_topic_wlz", cb=response_handler)

            return f"Connected to NATS hosted on {server_url}"
        except Exception as e:
            if i < retries - 1:
                wait = 2 ** i  # Exponential backoff
                print(f"Failed to connect to NATS, retrying in {wait}s...")
                await asyncio.sleep(wait)
            else:
                print(f"Failed to connect to NATS  {server_url}, no more retries.")
                raise e

    return "All connection attempts completed. Check logs for any connection errors."



@app.get("/connect_to_mec_nats/")
async def connect_to_mec_nats(server_url: str) -> str:
    """
    Attempts to connect to NATS servers at cloud_url. Closes any existing connections,
    and sets up subscriptions to a topic with message echoing.
    """
    # Disconnect if already connected
    try:
        if nc_mec.is_connected:
            await nc_mec.close()
    except Exception as e:
        print("Encountered an exception when closing NATS connections:")
        print(e)

    retries = 5
    for i in range(retries):
        try:
            # Connect to the Cloud server and set up subscription
            await nc_mec.connect(f"nats://{server_url}:4222")
            async def mec_message_handler(msg):
                nats_replier_reception_time = time.time()
                data = msg.data.decode()
                print(f"MEC: Received via NATS client: {data}")
                response = {
                        "nats_server_location": "MEC",
                        "replier_location": "MEC",
                        "nats_replier_reception_time": nats_replier_reception_time,
                }
                # Convert dictionary to JSON string
                json_data = json.dumps(response)
#                 // Get the time
#                   timeAsBytes := []byte(time.Now().String())

#                   // Send the time as the response.
#               msg.Respond(timeAsBytes)
                # await msg.respond(json_data.encode('utf-8'))
                await nc_mec.publish(msg.reply, json_data.encode('utf-8'))
            await nc_mec.subscribe("request_topic_wlz", cb=mec_message_handler)

            return f"Connected to NATS hosted on {server_url}"
        except Exception as e:
            if i < retries - 1:
                wait = 2 ** i  # Exponential backoff
                print(f"Failed to connect to NATS, retrying in {wait}s...")
                await asyncio.sleep(wait)
            else:
                print(f"Failed to connect to NATS  {server_url}, no more retries.")
                raise e

    return "All connection attempts completed. Check logs for any connection errors."



@app.get("/service_request/")
async def service_request(message: str):
    received_time = time.time()
    response = {
                "request_type": "http",
                "received_time": received_time,
                }
    # Convert dictionary to JSON string
    return response

@app.on_event("shutdown")
async def shutdown_event():
    if nc.is_connected:
        await nc.close()
@app.get("/")
async def main():
    # Get the current time in seconds since epoch
    received_time = time.time()
    
    # Get the system's timezone and current time in human-readable format
    tz = pytz.timezone(time.tzname[0])  # Get system's current timezone
    current_time = datetime.now(tz)
    human_readable_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

    response = {
        "location": "WLZ",
        "request_type": "http",
        "received_time": received_time,
        "system_timezone": tz.zone,
        "human_readable_time": human_readable_time,
    }
    
    return response

# Listener to handle incoming messages
def listener(msg):
    global pub
    received_by_peer = time.time()
    received_data = msg.payload.decode('utf-8')

    # Split the received data
    data_parts = received_data.split(',')
    print("#################### ZENOH MESSAGE RECEIVED")
    print(data_parts)
    if len(data_parts) >= 2:
        message = data_parts[1]
        request_id = data_parts[0] if len(data_parts) >= 2 else None
    else:
        print("Received data is not in the expected format.")
        return

    print(f"Replier Service: Received ('{msg.key_expr}': '{received_data}')")

    # Prepare the response message
    pub_key = 'eco/cloud/pong'
    replier_message = f"{message},{received_by_peer}"
    if request_id:
        replier_message += f",{request_id}"

    print("############################################")
    print(replier_message.split(','))
    print("############################################")

    # Publish the response
    pub.put(replier_message)
    print(f"Replier Service: Putting Data ('{pub_key}': '{replier_message}')")

# Endpoint to connect to Zenoh
@app.get("/connect_to_zenoh")
async def connect_to_zenoh(broker_address: str):
    global zenoh_session, pub, sub, server_url
    server_url = f"tcp/{broker_address}:7447"
    # server_url = "tcp/15.222.60.100:7447"
    if zenoh_session:
        return {"status": "Already connected to Zenoh"}
    if not server_url:
        raise HTTPException(status_code=400, detail="Server URL not set. Please set the server URL before connecting.")
    print("Starting Zenoh session...")
    conf = {
        "mode": "client",
        "connect": {
            "endpoints": [server_url]  # Use the server URL set by the user
        }
    }
    zenoh_session = zenoh.open(conf)
    # Declare the subscriber to listen for incoming messages
    sub_key = 'eco/wlz/ping'
    sub = zenoh_session.declare_subscriber(sub_key, listener)
    # Declare the publisher to send replies
    pub_key = 'eco/cloud/pong'
    pub = zenoh_session.declare_publisher(pub_key)
    print("Zenoh session started.")
    print("==============================")
    print(zenoh_session)
    print("==============================")
    return {"status": "Connected to Zenoh", "server_url": pub_key}

@app.get("/zenoh_switch_layer")
async def zenoh_switch_layer(server_url: str) -> str:
    global zenoh_session
    print("Closing Zenoh session...")
    if zenoh_session:
        zenoh_session.close()
    print("Zenoh session closed.")
    await connect_to_zenoh(server_url)
    print(f"connected to another zenoh server on {server_url}")
    return f"connected to another zenoh server on {server_url}"