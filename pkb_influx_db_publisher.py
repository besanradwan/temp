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
    successful_http_request_codes = [200, 202, 204]
    sample = list(samples)
    number_of_samples = 0
    for sample in samples:
      number_of_samples+=1
      timestamp = int((10**9)*sample['timestamp'])
      measurement = 'perfkitbenchmarker'
      tag_set_metadata = ', '.join(FormatSampleForInfluxDB(sample['metadata']))
      tag_set = ','.join((tag_set_metadata, 
                                FormatSampleForInfluxDB({k: sample[k] for k in ('test', 
                                'official', 'owner', 'run_uri', 'sample_uri')})))
      field_set = '%s_%s=%s' % (FormatSampleForInfluxDB({k: sample[k] for k in ('metric', 
                                'unit', 'value')}))
      samples_constructed_body_ = (' '.join((','.join((measurement, tag_set_sample))),
                                           (' '.join((field_set, timestamp))))) + "\n"

    create_db_status, create_db_response = self._createInfluxDB(influx_uri, influx_db_name)
    if create_db_status in successful_http_request_codes:
      logging.debug('Success!', influx_db_name, ' DB Created')
    else:
      logging.debug(create_db_status, 'Request could not be completed due to: ', create_db_response)  
    
    logging.debug('writing samples to publisher: writing', number_of_samples, ' samples.')
    self._writeData(influx_uri, influx_db_name, samples_constructed_body) 
    logging.debug(create_db_status, 'Request could not be completed due to: ', create_db_response)  
  

  def FormatSampleForInfluxDB(sample):
    for k, v in sample.iteritems():
            return '%s=%s' %(k, v)


  def _CreateDB(influx_uri, influx_db_name):
    headers = {'Content-type': 'application/x-www-form-urlencoded', 'Accept': 'text/plain'}
    body = urllib.urlencode({'q': 'CREATE DATABASE' + influx_db_name})
    conn = httplib.HTTPConnection(influx_uri)
    conn.request('POST', '/query?' + body, headers)
    response = conn.getresponse()
    response_status = response.status
    response_response = response.response
    return response_status, response_response


  def _WriteData(influx_uri, influx_db_name, data):
    body = data
    headers = {"Content-type": "application/octet-stream"}
    conn = httplib.HTTPConnection(influx_uri)
    conn.request('POST', '/write?' + 'db=' + influx_db_name, body, headers)
    response = conn.getresponse()
    response_status = response.status
    response_response = response.response
    return response_status, response_response

