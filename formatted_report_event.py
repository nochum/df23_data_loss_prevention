import avro.io, avro.schema, certifi, io, json, grpc, urllib3, requests, threading, time
import pubsub_api_pb2 as pb2
import pubsub_api_pb2_grpc as pb2_grpc
from credentials import client_id, client_secret, username, password

# globals
topic = "/event/ReportEventStream"
semaphore = threading.Semaphore(1)     # set a semaphore to keep the client running indefinitely
latest_replay_id = None                # Create a global variable to store the replay ID
instance_url = ''                      # to be used for API calls
access_token = ''                      # to be used for API calls

schema = None                          # hold the schema so we don't retrieve it for each event separately
parsed_schema = None

def fetchReqStream(topic):
    global semaphore

    while True:
        semaphore.acquire()
        yield pb2.FetchRequest(
            topic_name = topic,
            replay_preset = pb2.ReplayPreset.LATEST,
            num_requested = 4) # represents the max we can process at one time. limit is 100.

def decode(schema_info, payload):
  global parsed_schema

  if not parsed_schema:
    parsed_schema = avro.schema.parse(schema_info)
  decoder = avro.io.BinaryDecoder(io.BytesIO(payload))
  reader = avro.io.DatumReader(parsed_schema)
  return reader.read(decoder)

def login():
    global client_id, client_secret, username, password

    login_endpoint = 'https://login.salesforce.com/services/oauth2/token'

    # Define the payload for the login request
    payload = {
        'grant_type': 'password',
        'client_id': client_id,
        'client_secret': client_secret,
        'username': username,
        'password': password
    }

    # Make the login request and get the access token
    response = requests.post(login_endpoint, data=payload)
    if response.status_code == 200:
        access_token = response.json().get('access_token')
        instance_url = response.json().get('instance_url')
        return(instance_url, access_token)
    else:
        print('Login failed. Response:', response.text)

def call_composite_graph(composite_req):
    global instance_url, access_token
    # returns a list of session IDs for the user
    query_path = '/services/data/v57.0/composite/graph'
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + access_token}
    res = requests.post(instance_url + query_path, headers=headers, json=composite_req, verify=False)
    if res.status_code == 200:
      json_res =  json.loads(res.content)
      return(json_res)
    else:
        print('Response does not contain records: ', res.status_code, res.reason)

def build_req(obj_type, num, rec_id):
  req = {}
  req['url'] = '/services/data/v57.0/sobjects/' + obj_type + '/' + rec_id
  req['method'] = 'GET'
  req['referenceId'] = 'reference_id_' + obj_type.lower() + '_' + str(num)
  return(req)

def build_composite(evt):
  composite = {}
  graphs = []

  column_headers = evt['ColumnHeaders']
  print(column_headers[1:-1].replace(', ', '\t'))

  records =  evt['Records'] # object, not an array
  records_json =  json.loads(records)
  rows = records_json['rows']
  count = 0
  for row in rows:
    graph = {}
    count += 1
    if count == 76:
      break
    graph['graphId'] =  str(count)
    graph['compositeRequest'] = []
    account_id = ''
    contact_id = ''
    user_id = ''

    for cell in row['datacells']:
      if cell.startswith('001'):
        graph['compositeRequest'].append(build_req('Account', count, cell))
      elif cell.startswith('003'):
        graph['compositeRequest'].append(build_req('Contact', count, cell))
      elif cell.startswith('005'):
        graph['compositeRequest'].append(build_req('User', count, cell))
    graphs.append(graph)
  composite['graphs'] = graphs
  return(composite)

def display_report(json_res):
  for graph in json_res['graphs']:
    account = {}
    contact = {}
    user = {}
    for response in graph['graphResponse']['compositeResponse']:
      if response['referenceId'].find('account') > 0:
        account = response['body']
      elif response['referenceId'].find('contact') > 0:
        contact = response['body']
      elif response['referenceId'].find('user') > 0:
        user = response['body']

    report_line = [f"{contact['Salutation']:<10}", f"{contact['FirstName']:<10}", f"{contact['LastName']:<9}", f"{contact['Title']:<30}", f"{account['Name']:<30}", 
      f"{str(contact['MailingStreet']):<30}", contact['MailingCity'],contact['MailingState'],contact['MailingPostalCode'],contact['MailingCountry'],
      contact['Phone'],contact['Fax'],contact['MobilePhone'],contact['Email'],user['Name']]
    print(*report_line, sep='\t')

def subscribe(access_token, instance_url, org_id):
    global latest_replay_id, schema

    auth_metadata = (('accesstoken', access_token),
    ('instanceurl', instance_url),
    ('tenantid', org_id))

    with open(certifi.where(), 'rb') as f:
        creds = grpc.ssl_channel_credentials(f.read())

    with grpc.secure_channel('api.pubsub.salesforce.com:7443', creds) as channel:
        stub = pb2_grpc.PubSubStub(channel)

        print(f"Subscribing to {topic}")
        substream = stub.Subscribe(fetchReqStream(topic), metadata=auth_metadata)
        for response in substream:
          if response.pending_num_requested == 0:
            semaphore.release()
          for e in response.events:
            payload_bytes = e.event.payload
            schema_id = e.event.schema_id
            if not schema: # This is an expensive call. Do it once and cache the schema to gain performance.
              schema_info = stub.GetSchema(pb2.SchemaRequest(schema_id=schema_id), metadata=auth_metadata).schema_json
            decoded_dict = decode(schema_info, payload_bytes)
            composite_req = build_composite(decoded_dict)
            json_res = call_composite_graph(composite_req)
            display_report(json_res)
          else:
            print(f"[{time.strftime('%b %d, %Y %l:%M%p %Z')}] The subscription is active.")
          latest_replay_id = response.latest_replay_id

if __name__ == "__main__":
    # disable noisy warning message about certificate validation
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    instance_url, access_token = login()
    org_id = access_token.split('!')[0]

    # print(access_token, instance_url, org_id)
    subscribe(access_token, instance_url, org_id)
