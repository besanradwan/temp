flags.DEFINE_string(
    'influx_uri', 'http://localhost:8086/',
    'The Influx DB endpoint for queries address and port')

flags.DEFINE_string(
    'influx_db_name', 'perfkit',
    'Name of Influx DB database that you wish to publish to or create')


class InfluxDBPublisher(SamplePublishser):

	def __init__(self, influx_uri=None, influx_db_name=None ):  #set to default above in flags unless changed
		self.influx_uri = influx_uri
		self.influx_db_name = influx_db_name


	def PublishSamples(self, samples):
		samples_constructed_body = ''
		for sample in samples:
			type_of_test = sample['metric']
			rwmixwrite = sample['rwmixwrite']
			mean_bandwidth =sample['mean_bandwidth']
			mean_latency = sample['mean_latency']
			median_latency = sample['median_latency']
			99th_p_latency = sample['99th_p_latency']
			read_iops = sample['read_iops']
			write_iops = sample['write_iops']
			timestamp = (10**9)*(int(sample['timestamp'])) #converting seconds to nanoseconds
			
			sample_constructed_body = type_of_test + ',' + rwmixwrite + ',' +
									  mean_bandwidth + ',' + mean_latency + ',' +
									  mean_latency + ',' + median_latency + ',' +
									  99th_p_latency + ',' + read_iops + ' ' +
									  write_iops + ' ' + timestamp + '\n'				  
			
			samples_constructed_body = samples_constructed_body + sample

		influx_db_exists = self._influxDBExists(influx_uri, influx_db_name)
		if influx_db_exists == True:
			write_data_status, write_data_response = self._writeData(influx_uri, influx_db_name)
			if status == 204:
				print('Success! Data Written')
			else:
				print(response, 'Request could not be completed due to: ', response)
		else:
			create_db_status, create_db_response = self._createInfluxDB(influx_uri, influx_db_name)
			if status == 204:
				print('Success!', influx_db_name, ' DB Created')
			else:
				print(response, 'Request could not be completed due to: ', response)	
			
			self._writeData(influx_uri, influx_db_name)


	def _influxDBExists(influx_uri, influx_db_name):
		conn = httplib.HTTPConnection(influx_uri + '/query')
		conn.request('GET', '')
		response = conn.getresponse()
		response_status = response.status
		response_response = response.response
		if response_status == 204:
			return True
		else:
			return False


	def _createInfluxDB(influx_uri, influx_db_name):
		params = urllib.urlencode({'CREATE DATABASE': influx_db_name})
		conn = httplib.HTTPConnection(influx_uri + '/query')
		conn.request('POST', '', params)
		response = conn.getresponse()
		response_status = response.status
		response_response = response.response
		return response_status, response_response


	def _writeData(influx_uri, influx_db_name):
		params = urllib.urlencode({'db': influx_db_name, 'data-binary': samples_constructed_body})
		conn = httplib.HTTPConnection(influx_uri + '/write')
		conn.request('POST', '', params)
		response = conn.getresponse()
		response_status = response.status
		response_response = response.response
		return response_status, response_response