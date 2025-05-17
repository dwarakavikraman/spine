import frappe

def get_kafka_conf():
    frappe.logger().debug("Frappe site config - {}".format(frappe.as_json(frappe.local.conf)))
    kafka_conf = {
        "bootstrap.servers": "localhost:9092",
        "client.id": "default.consumer",
        "default.topic.config": {
            "acks": "all",
        },
        "topic_suffix" : ""
    }
    kafka_conf.update(frappe.local.conf.get("kafka", {}))
    return kafka_conf

def get_topic(topic : str, conf : dict)-> str:
    if 'topic_suffix' not in conf or not conf['topic_suffix']:
        return topic
    if topic.endswith(f"-{conf['topic_suffix']}"):
        return topic
    return f"{topic}-{conf['topic_suffix']}"