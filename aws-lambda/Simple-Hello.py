def lambda_handler(event, context):
    print "Received event:", event
    print "Hello ", event["name"]
    print "Log stream name:", context.log_stream_name
    print "Log group name:",  context.log_group_name
    print "Request ID:",context.aws_request_id
    print "Mem. limits(MB):", context.memory_limit_in_mb
    print "Time remaining (MS):", context.get_remaining_time_in_millis()
    return 'Hello from '+event["name"]
