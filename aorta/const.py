import os


#: The channel at which all clients listen for enterprise-level
#: events, as specified in the Sovereign Charter.
EVENTS_CHANNEL = 'events.global'


#: The channel to which events are published.
PUBLISHING_CHANNEL = 'events.origin'


HOST = os.getenv('AORTA_TEST_PROTON_HOST') or 'localhost'
try:
    PORT = int(os.getenv('AORTA_TEST_PROTON_PORT') or 5672)
except ValueError:
    print("Provide a valid integer for AORTA_TEST_PROTON_PORT")

# The channel must be an exchange of type fanout
CHANNEL = 'aorta.test'
