# -*- coding: utf-8 -*-
# Copyright (c) 2019, ElasticRun and contributors
# For license information, please see license.txt

from __future__ import unicode_literals

from frappe.model.document import Document
from spine.utils.command_controller import publish_command
from spine.utils import get_kafka_conf

class SpineConsumerConfig(Document):
    def on_update(self):
        publish_command('reload_config')
    
    def on_change(self):
        kafka_config = get_kafka_conf()
        if 'topic_suffix' not in kafka_config or not kafka_config['topic_suffix']:
            return
        
        kakfa_topic_suffix = kafka_config['topic_suffix']
        if not isinstance(kakfa_topic_suffix, str):
            return
        kakfa_topic_suffix = kakfa_topic_suffix.strip()
        if len(kakfa_topic_suffix) == 0:
            return
        for i in self.configs:
            if not i.get('topic').endswith(f"-{kakfa_topic_suffix}"):
                i.topic = f"{i.get('topic')}-{kakfa_topic_suffix}"
