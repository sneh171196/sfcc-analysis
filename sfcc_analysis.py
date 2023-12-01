from influxdb import InfluxDBClient
import requests
from requests.auth import HTTPBasicAuth
import json
import schedule
import time

# Function to connect to InfluxDB
def connect_to_influxdb(host, port, username, password, database):
    return InfluxDBClient(host=host, port=port, username=username, password=password, database=database)

# Function to create a database if it doesn't exist
def create_database(client, database):
    databases = client.get_list_database()
    if {'name': database} not in databases:
        client.create_database(database)

# Function to insert data into InfluxDB
def insert_data(client, data):
    try:
        client.write_points(data)
        print(f"Data inserted successfully for orderNo: {data[0]['tags']['orderNo']}")
    except Exception as e:
        print(f"Error inserting data: {e}")

# Function to convert order data to InfluxDB format
def convert_to_influxdb_format(order):
    measurement = "orders"
    tags = {"orderNo": order["orderNo"]}
    fields = order
    time = order["creationDate"]  # Assuming creationDate is a valid timestamp field
    
    exclude_columns = [
        "billingAddress",
        "bonusDiscountLineItems",
        "c_stripePaymentIntentID",
        "c_stripeRiskLevel",
        "c_stripeRiskScore",
        "createdBy",
        "customerInfo",
        "customerLocale",
        "lastModified",
        "notes",
        "orderNo_1",
        "paymentInstruments",
        "placeDate",
        "productItems",
        "remoteHost",
        "shipments",
        "shippingItems",
        "siteId"
    ]
    
    for column in exclude_columns:
        fields.pop(column, None)
        
    # Convert boolean values to lowercase strings
    fields = {key: str(value).lower() if isinstance(value, bool) else value for key, value in fields.items()}

    # Convert nested structures to string
    fields = {key: json.dumps(value) if isinstance(value, (dict, list)) else value for key, value in fields.items()}

    influxdb_data = {
        "measurement": measurement,
        "tags": tags,
        "fields": fields,
        "time": time
    }

    return influxdb_data

# Function to get access token
def get_access_token():
    url = "https://account.demandware.com/dwsso/oauth2/access_token"
    client_id = "d8e52c5b-e233-4307-a052-6e4173786e4e"
    client_secret = "GS8KmdDEUKfWnEnv"
    scope = "SALESFORCE_COMMERCE_API:zyne_001 sfcc.orders"

    auth = HTTPBasicAuth(client_id, client_secret)
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    data = {
        'grant_type': 'client_credentials',
        'scope': scope
    }

    response = requests.post(url, auth=auth, headers=headers, data=data)

    if response.status_code == 200:
        access_token = response.json().get('access_token')
        return access_token
    else:
        print(f"Error getting access token: {response.text}")
        return None

# Function to fetch orders
def fetch_and_insert_orders():
    global your_access_token
    print("Fetching orders...")

    # Fetch orders from the API
    url_to_protected_resource = "https://kv7kzm78.api.commercecloud.salesforce.com/checkout/orders/v1/organizations/f_ecom_zyne_001/orders?siteId=RefArch"

    headers = {
        "Authorization": f"Bearer {your_access_token}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url_to_protected_resource, headers=headers)
        orders_data = response.json()["data"] if response.status_code == 200 else []
    except requests.RequestException as e:
        print(f"Error fetching orders: {e}")
        orders_data = []

    # InfluxDB configuration
    influx_host = "localhost"
    influx_port = 8086
    influx_username = "admin"
    influx_password = "admin"
    influx_database = "sfcc_analysis"

    # Connect to InfluxDB
    influx_client = connect_to_influxdb(influx_host, influx_port, influx_username, influx_password, influx_database)

    # Create database if not exists
    create_database(influx_client, influx_database)

    # Iterate through orders and insert into InfluxDB
    try:
        for order in orders_data:
            order_no = order.get("orderNo")

            # Check if order already exists in the database
            query = f'SELECT * FROM "orders" WHERE "orderNo"=\'{order_no}\''
            result = influx_client.query(query)

            if not result:
                # If order doesn't exist, insert it into the database
                influx_data = convert_to_influxdb_format(order)
                insert_data(influx_client, [influx_data])
    except requests.RequestException as e:
        print(f"Error getting orders: {e}")

    # Close InfluxDB connection
    influx_client.close()

# Function to update access token
def update_access_token():
    global your_access_token
    print("Updating access token...")
    your_access_token = get_access_token()

# Schedule tasks
schedule.every(1).minutes.do(fetch_and_insert_orders)
schedule.every(30).minutes.do(update_access_token)

# Initial execution
your_access_token = get_access_token()

# Run scheduled tasks in an infinite loop
while True:
    schedule.run_pending()
    time.sleep(1)
