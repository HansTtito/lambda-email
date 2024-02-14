from lambda_function import *

#For GET testing
event = {}

context = {}

response = lambda_handler(event, context)
print(response)