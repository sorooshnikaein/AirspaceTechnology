# Using python and pandas library to ETL to MySQL
# 

import pandas as pd 
import json, datetime, sqlalchemy, os 
from dateutil import tz


# Return distance driven using driving_searches
def driving_searches_distance(row):
	json_obj_driving_searches = json.loads(row)['routes']
	total_drive_distance = 0
	for json_obj in json_obj_driving_searches:
		legs = json_obj['legs']
		for distance in legs:
			total_drive_distance += distance['distance']['value'] / 1609 # value is meters, divided into Miles
	return round(total_drive_distance, 2)

# Return Order_type using delivery_route_segments
def get_order_type(agg_list):
	order_types = ['drive', 'hfpu', 'nfo']
	if(len(agg_list)):
		return order_types[len(agg_list) - 1]
	return None

# Return UTC to local timezone
def datetime_UCT_local(UTC_datetime, ending_timezone):
	from_zone = tz.tzutc()
	to_zone = tz.gettz(ending_timezone)
	if pd.isnull(UTC_datetime) == False:
		UTC_datetime_initial = UTC_datetime.replace(tzinfo=from_zone)
		return UTC_datetime_initial.astimezone(to_zone)
	return UTC_datetime

# Return local timezone and minutes_to_pickup
def datetime_transformation(datetime_orders):
	minutes_to_pickup = round((datetime_orders['pick_up_time'] - datetime_orders['created_at']).total_seconds() / 60)
	# Some pick_up_time > created_at, so catch if value < 0
	datetime_orders['minutes_to_pickup'] = minutes_to_pickup if  minutes_to_pickup > 0 else 0
	datetime_orders['quoted_delivery_time'] = datetime_UCT_local(datetime_orders['quoted_delivery_time'], datetime_orders['time_zone_destination_city']) 
	datetime_orders['pick_up_time'] = datetime_UCT_local(datetime_orders['pick_up_time'], datetime_orders['time_zone_origin_city'])
	return datetime_orders

# origin and destination city name and their time_zone
def merge_start_end_address(deliver_route, address, field_name, end_name):
	deliver_route = deliver_route.merge(address, on= field_name, how='left')
	deliver_route = deliver_route.drop(field_name, 1)
	deliver_route.rename(columns={'city': end_name, 'time_zone': 'time_zone_' + end_name}, inplace=True)
	return deliver_route

# Create engine and drop and create table in mysql
def create_table_mysql(orders):
	engine = sqlalchemy.create_engine(os.environ.get('DB_HOST'))
	orders.to_sql(name='Data_engineer_screening_orders', con=engine, index= False,if_exists='replace')


if __name__ == '__main__':

	# Fields needed: *id (joining key), company_id = *company_id, created_at - pick_up_time = *minutes_to_pickup, Convert_local_time(pick_up_time) = *pick_up_time_local, Convert_local_time(quoted_delivery_time) = *delivery_time_local, 
	orders = pd.read_csv('orders.csv', usecols = ['id', 'company_id', 'created_at', 'pick_up_time', 'quoted_delivery_time'], parse_dates=['created_at', 'pick_up_time', 'quoted_delivery_time'])
	

	# Fields needed: case(type) = *order_type, order_id (joining key), min(start_address_id, key=order_id) = *origin_city, max(end_address_id, key=order_id) = *destination_city
	# Assuming start/end_address_id are sequentially numbered => (assumption based on orders with flights have json_obj[legs][0]['origin_airport']['city'] == min(start_address_id)['city'])
	delivery_route_segments = pd.read_csv('delivery_route_segments.csv', usecols = ['type', 'order_id', 'start_address_id', 'end_address_id'])
	delivery_route_segments['order_type'] = delivery_route_segments.groupby('order_id')['type'].transform(get_order_type)
	deliver_route_segments_grouped = delivery_route_segments.groupby(['order_id', 'order_type']).agg({'start_address_id': min, 'end_address_id': max}).reset_index()
	deliver_route_segments_grouped.rename(columns={'order_id': 'id'}, inplace=True)
	orders = orders.merge(deliver_route_segments_grouped, on= 'id', how='left')

	# Fields needed: id (joining key delivery_route_segments), city = *origin_city
	start_address = pd.read_csv('start_addresses.csv', usecols = ['id', 'city', 'time_zone'])
	start_address.columns = ['start_address_id', 'city', 'time_zone']
	orders = merge_start_end_address(orders, start_address, 'start_address_id', 'origin_city')


	# Fields needed: id (joining key delivery_route_segments), city = *destination_city
	end_address = pd.read_csv('end_addresses.csv', usecols = ['id', 'city', 'time_zone'])
	end_address.columns = ['end_address_id', 'city', 'time_zone']
	orders = merge_start_end_address(orders, end_address, 'end_address_id', 'destination_city')

	# Change dates to their timezone and get the minutes until pickup
	orders = orders.apply(datetime_transformation, axis=1)
	orders.rename(columns={'pick_up_time': 'pick_up_time_local', 'quoted_delivery_time': 'delivery_time_local'}, inplace=True)

	orders = orders.drop(['created_at', 'time_zone_origin_city', 'time_zone_destination_city'], 1)

	# Fields needed: order_id (joining key for order), json_obj['routes'].map(['legs'].map(return round(distance['value'] / 1609 (meter_miles), 2)) = total_drive_distance
	driving_searches = pd.read_csv('driving_searches.csv', usecols = ['order_id', 'json_obj'])
	driving_searches['total_drive_distance'] = driving_searches.json_obj.map(driving_searches_distance)
	driving_searches = driving_searches.drop('json_obj', 1)
	driving_searches = driving_searches.groupby('order_id').agg({'total_drive_distance': sum}).reset_index()
	driving_searches.rename(columns={'order_id': 'id'}, inplace=True)

	orders = orders.merge(driving_searches, on= 'id', how='left')
	print(orders)

	create_table_mysql(orders)







