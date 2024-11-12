from datetime import datetime, date, time

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    return obj

def serialize_dict(message: dict):
    return {k: json_serial(v) for k,v in message.items()}