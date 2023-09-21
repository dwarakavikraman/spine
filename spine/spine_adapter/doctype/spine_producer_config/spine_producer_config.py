# -*- coding: utf-8 -*-
# Copyright (c) 2019, ElasticRun and contributors
# For license information, please see license.txt

from __future__ import unicode_literals

import frappe
from frappe.model.document import Document
from spine.spine_adapter.docevents.eventhandler import handle_event


class SpineProducerConfig(Document):
    pass

@frappe.whitelist()
def trigger_event(doctype, event, filters=None):
    doc_list = frappe.get_list(doctype, filters=filters, pluck="name")
    for d in doc_list:
        doc = frappe.get_doc(doctype, d)
        handle_event(doc, event)
    return doc_list