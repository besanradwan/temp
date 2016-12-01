flags.DEFINE_string(
    'influx_uri', 'http://localhost:8086/',
    'The Influx DB endpoint for queries address and port')

flags.DEFINE_string(
    'influx_db_name', 'perfkit',
    'Name of Influx DB database that you wish to publish to or create')


class InfluxDBPublisher(SamplePublishser):

	def __init__(self, influx_uri=None, influx_db_name=None):  #set to default above in flags unless changed
		self.influx_uri = influx_uri
		self.influx_db_name = influx_db_name


	def PublishSamples(self, samples):
		sample = list(samples)
		final_sample = samples[-1]
		samples = del samples[-1]
		
		for sample in samples:
			sample['timestamp'] = self._FormatTimestampForInfluxDB(
          			sample['timestamp']
      		)
      		samples_constructed_body = ''
      		samples_constructed_body += (FormatSampleForInfluxDB(sample))
      	samples_constructed_body += ' ' + final_sample
							  		
		create_db_status, create_db_response = self._createInfluxDB(influx_uri, influx_db_name)
		
		if create_db_status in (200, 202, 204):
			print('Success!', influx_db_name, ' DB Created')
		else:
			print(response, 'Request could not be completed due to: ', create_db_response)	
		
		print('writing samples to publisher')
		self._writeData(influx_uri, influx_db_name, samples_constructed_body)

	
	def FormatSampleForInfluxDB(sample):
		for k, v in sample.iteritems():
      			return k + '=' + v + ','

	def _FormatTimestampForInfluxDB(self, epoch_us): #converting seconds to nanoseconds
		new_timestamp = (10**9)*(int(epoch_us))
		return new_timestamp


	def _createInfluxDB(influx_uri, influx_db_name):
		headers = {'Content-type': 'application/x-www-form-urlencoded', 'Accept': 'text/plain'}
		params = urllib.urlencode({'q': 'CREATE DATABASE' + influx_db_name})
		conn = httplib.HTTPConnection(influx_uri)
		conn.request('POST', '/query?' + params, headers)
		response = conn.getresponse()
		response_status = response.status
		response_response = response.response
		return response_status, response_response


	def _writeData(influx_uri, influx_db_name, data):
		params = data
		conn = httplib.HTTPConnection(influx_uri)
		conn.request('POST', '/write?' + 'db=' + influx_db_name, params)
		response = conn.getresponse()
		response_status = response.status
		response_response = response.response
		return response_status, response_response

